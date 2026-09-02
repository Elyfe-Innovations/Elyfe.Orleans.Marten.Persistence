using JasperFx;
using Marten;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;
using Orleans.Configuration;

namespace Elyfe.Orleans.Marten.Reminders;

public sealed class ElyfeMartenReminderTable : IReminderTable
{
    private const string ProviderVersion = "1";

    private readonly IDocumentStore _documentStore;
    private readonly ElyfeMartenReminderOptions _options;
    private readonly ClusterOptions _clusterOptions;
    private readonly ILogger<ElyfeMartenReminderTable> _logger;

    internal ElyfeMartenReminderTable(
        IElyfeMartenReminderStore storeProvider,
        IOptions<ElyfeMartenReminderOptions> options,
        IOptions<ClusterOptions> clusterOptions,
        ILogger<ElyfeMartenReminderTable> logger)
    {
        _documentStore = storeProvider.Store;
        _options = options.Value;
        _clusterOptions = clusterOptions.Value;
        _logger = logger;
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

        await AssertDocumentTableIsUpsertableAsync(CancellationToken.None);
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

    /// <summary>
    /// Marten writes documents with an inline <c>INSERT ... ON CONFLICT (id) DO UPDATE</c>, so the
    /// reminder document table must carry a single-column primary key on <c>id</c>. A key that
    /// includes any other column - a TimescaleDB hypertable partitioned on <c>mt_created_at</c>, for
    /// instance - cannot be inferred as the arbiter index, and PostgreSQL then rejects every reminder
    /// write with 42P10 ("there is no unique or exclusion constraint matching the ON CONFLICT
    /// specification"). Nothing observes that until a grain registers a reminder, so the shape is
    /// asserted while the silo is still starting rather than discovered as a permanently broken
    /// reminder subsystem.
    /// </summary>
    private async Task AssertDocumentTableIsUpsertableAsync(CancellationToken cancellationToken)
    {
        var tableName = $"mt_doc_{_options.DocumentAlias}";

        const string primaryKeySql = """
            SELECT array_agg(att.attname ORDER BY key.ordinality)
            FROM pg_constraint con
            JOIN pg_class cls ON cls.oid = con.conrelid
            JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace
            CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS key(attnum, ordinality)
            JOIN pg_attribute att ON att.attrelid = cls.oid AND att.attnum = key.attnum
            WHERE nsp.nspname = @schemaName
              AND cls.relname = @tableName
              AND con.contype = 'p'
            """;

        // Deliberately Marten's own database rather than a separately configured connection string:
        // the assertion must cover the connection Marten actually writes reminders through, and must
        // not be skippable by a host that registers the store without repeating its connection string.
        await using var connection = _documentStore.Storage.Database.CreateConnection();
        await connection.OpenAsync(cancellationToken);
        await using var command = CreateCommand(connection, primaryKeySql);
        AddText(command, "schemaName", _options.SchemaName);
        AddText(command, "tableName", tableName);
        var primaryKeyColumns = await command.ExecuteScalarAsync(cancellationToken) as string[];

        if (primaryKeyColumns is null || primaryKeyColumns.Length == 0)
        {
            throw new InvalidOperationException(
                $"The Orleans reminder document table \"{_options.SchemaName}\".\"{tableName}\" is missing or has no primary key. Apply the platform database migrations before starting the silo.");
        }

        if (primaryKeyColumns is not ["id"])
        {
            throw new InvalidOperationException(
                $"The Orleans reminder document table \"{_options.SchemaName}\".\"{tableName}\" has primary key ({string.Join(", ", primaryKeyColumns)}), but reminders are written with ON CONFLICT (id) and require a single-column primary key on id. A composite key - a TimescaleDB hypertable partitioned on mt_created_at, for example - fails every reminder write with PostgreSQL 42P10. Apply platform migration 002-orleans-reminders-regular-document-table.");
        }

        _logger.LogInformation(
            "Elyfe Marten reminder table initialized. Schema={Schema} Table={Table} PrimaryKey={PrimaryKey}",
            _options.SchemaName,
            tableName,
            primaryKeyColumns);
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
}
