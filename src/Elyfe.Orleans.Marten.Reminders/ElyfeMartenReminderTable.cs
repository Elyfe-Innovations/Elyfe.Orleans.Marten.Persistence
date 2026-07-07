using JasperFx;
using Marten;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;
using Orleans.Configuration;
using Orleans.Runtime;

namespace Elyfe.Orleans.Marten.Reminders;

public sealed class ElyfeMartenReminderTable : IReminderTable
{
    private const string ProviderVersion = "1";

    private readonly IDocumentStore _documentStore;
    private readonly ElyfeMartenReminderOptions _options;
    private readonly ClusterOptions _clusterOptions;
    private readonly ILogger<ElyfeMartenReminderTable> _logger;
    private readonly NpgsqlDataSource? _dataSource;
    private readonly string _qualifiedMartenTable;
    private TimescaleCapability _timescale;

    public ElyfeMartenReminderTable(
        IDocumentStore documentStore,
        IOptions<ElyfeMartenReminderOptions> options,
        IOptions<ClusterOptions> clusterOptions,
        ILogger<ElyfeMartenReminderTable> logger)
    {
        _documentStore = documentStore;
        _options = options.Value;
        _clusterOptions = clusterOptions.Value;
        _logger = logger;
        _qualifiedMartenTable = $"{QuoteIdentifier(_options.SchemaName)}.{QuoteIdentifier($"mt_doc_{_options.DocumentAlias}")}";

        if (!string.IsNullOrWhiteSpace(_options.ConnectionString))
        {
            _dataSource = new NpgsqlDataSourceBuilder(_options.ConnectionString).Build();
        }
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await Init();
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public async Task Init()
    {
        if (_options.AutoCreateSchema)
        {
            await _documentStore.Storage.ApplyAllConfiguredChangesToDatabaseAsync(AutoCreate.CreateOrUpdate);
        }

        _timescale = await DetectTimescaleAsync(CancellationToken.None);
        _logger.LogInformation(
            "Elyfe Marten reminder table initialized. Schema={Schema} Alias={Alias} TimescaleInstalled={TimescaleInstalled} TableIsHypertable={TableIsHypertable}",
            _options.SchemaName,
            _options.DocumentAlias,
            _timescale.ExtensionInstalled,
            _timescale.TableIsHypertable);
    }

    public async Task<ReminderTableData> ReadRows(GrainId grainId)
    {
        await using var session = _documentStore.QuerySession();
        var grainKey = grainId.ToString();
        var reminders = await session.Query<ElyfeMartenReminderDocument>()
            .Where(reminder => reminder.ServiceId == ServiceId && reminder.GrainId == grainKey)
            .OrderBy(reminder => reminder.ReminderName)
            .ToListAsync();

        return new ReminderTableData(reminders.Select(static reminder => reminder.ToReminderEntry()).ToList());
    }

    public async Task<ReminderTableData> ReadRows(uint begin, uint end)
    {
        await using var session = _documentStore.QuerySession();
        var beginHash = Convert.ToInt64(begin);
        var endHash = Convert.ToInt64(end);
        var query = session.Query<ElyfeMartenReminderDocument>()
            .Where(reminder => reminder.ServiceId == ServiceId);

        query = begin >= end
            ? query.Where(reminder => reminder.GrainHash > beginHash || reminder.GrainHash <= endHash)
            : query.Where(reminder => reminder.GrainHash > beginHash && reminder.GrainHash <= endHash);

        var reminders = await query
            .OrderBy(reminder => reminder.GrainHash)
            .ThenBy(reminder => reminder.GrainId)
            .ThenBy(reminder => reminder.ReminderName)
            .ToListAsync();

        return new ReminderTableData(reminders.Select(static reminder => reminder.ToReminderEntry()).ToList());
    }

    public async Task<ReminderEntry?> ReadRow(GrainId grainId, string reminderName)
    {
        await using var session = _documentStore.QuerySession();
        var document = await session.LoadAsync<ElyfeMartenReminderDocument>(BuildId(grainId, reminderName));
        return document is null || document.ServiceId != ServiceId ? null : document.ToReminderEntry();
    }

    public async Task<string> UpsertRow(ReminderEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentException.ThrowIfNullOrWhiteSpace(entry.ReminderName);

        var etag = Guid.NewGuid().ToString("N");
        var document = new ElyfeMartenReminderDocument
        {
            Id = BuildId(entry.GrainId, entry.ReminderName),
            ServiceId = ServiceId,
            ClusterId = _clusterOptions.ClusterId ?? string.Empty,
            GrainId = entry.GrainId.ToString(),
            GrainHash = entry.GrainId.GetUniformHashCode(),
            ReminderName = entry.ReminderName,
            StartAt = entry.StartAt.Kind == DateTimeKind.Utc ? entry.StartAt : entry.StartAt.ToUniversalTime(),
            PeriodTicks = entry.Period.Ticks,
            ETag = etag,
            ProviderVersion = ProviderVersion
        };

        await using var session = _documentStore.LightweightSession();
        session.Store(document);
        await session.SaveChangesAsync();
        return etag;
    }

    public async Task<bool> RemoveRow(GrainId grainId, string reminderName, string eTag)
    {
        await using var session = _documentStore.LightweightSession();
        var document = await session.LoadAsync<ElyfeMartenReminderDocument>(BuildId(grainId, reminderName));
        if (document is null || document.ServiceId != ServiceId || document.ETag != eTag)
        {
            return false;
        }

        session.Delete<ElyfeMartenReminderDocument>(document.Id);
        await session.SaveChangesAsync();
        return true;
    }

    public async Task TestOnlyClearTable()
    {
        await using var session = _documentStore.LightweightSession();
        var reminders = await session.Query<ElyfeMartenReminderDocument>()
            .Where(reminder => reminder.ServiceId == ServiceId)
            .ToListAsync();

        foreach (var reminder in reminders)
        {
            session.Delete<ElyfeMartenReminderDocument>(reminder.Id);
        }

        await session.SaveChangesAsync();
    }

    private string ServiceId => string.IsNullOrWhiteSpace(_clusterOptions.ServiceId) ? string.Empty : _clusterOptions.ServiceId;

    private string BuildId(GrainId grainId, string reminderName) => ElyfeMartenReminderDocument.BuildId(ServiceId, grainId, reminderName);

    private async Task<TimescaleCapability> DetectTimescaleAsync(CancellationToken cancellationToken)
    {
        if (!_options.PreferTimescale || _dataSource is null)
        {
            return new TimescaleCapability(false, false);
        }

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var extensionCommand = CreateCommand(connection, "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')");
        var installed = (bool)(await extensionCommand.ExecuteScalarAsync(cancellationToken) ?? false);
        if (!installed)
        {
            return new TimescaleCapability(false, false);
        }

        const string hypertableSql = """
            SELECT EXISTS (
                SELECT 1
                FROM timescaledb_information.hypertables
                WHERE hypertable_schema = @schemaName AND hypertable_name = @tableName)
            """;
        await using var hypertableCommand = CreateCommand(connection, hypertableSql);
        AddText(hypertableCommand, "schemaName", _options.SchemaName);
        AddText(hypertableCommand, "tableName", $"mt_doc_{_options.DocumentAlias}");
        var isHypertable = (bool)(await hypertableCommand.ExecuteScalarAsync(cancellationToken) ?? false);
        return new TimescaleCapability(true, isHypertable);
    }

    private async Task EnsureTimescaleHypertableAsync(CancellationToken cancellationToken)
    {
        if (!_options.PreferTimescale || _dataSource is null)
        {
            return;
        }

        var capability = await DetectTimescaleAsync(cancellationToken);
        if (!capability.ExtensionInstalled || capability.TableIsHypertable)
        {
            return;
        }

        const string sql = """
            SELECT create_hypertable(
                @qualifiedTableName::regclass,
                'mt_created_at',
                chunk_time_interval => @chunkInterval::interval,
                if_not_exists => TRUE,
                migrate_data => TRUE)
            """;
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = CreateCommand(connection, sql);
        AddText(command, "qualifiedTableName", _qualifiedMartenTable);
        AddInterval(command, "chunkInterval", _options.TimescaleChunkInterval);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private NpgsqlCommand CreateCommand(NpgsqlConnection connection, string sql)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = _options.CommandTimeoutSeconds;
        return command;
    }

    private static void AddText(NpgsqlCommand command, string name, string value)
    {
        command.Parameters.Add(new NpgsqlParameter(name, NpgsqlDbType.Text) { Value = value });
    }

    private static void AddInterval(NpgsqlCommand command, string name, TimeSpan value)
    {
        command.Parameters.Add(new NpgsqlParameter(name, NpgsqlDbType.Interval) { Value = value });
    }

    private static string QuoteIdentifier(string identifier)
    {
        if (identifier.Length == 0)
        {
            throw new ArgumentException("Identifier cannot be empty.", nameof(identifier));
        }

        return "\"" + identifier.Replace("\"", "\"\"") + "\"";
    }
}
