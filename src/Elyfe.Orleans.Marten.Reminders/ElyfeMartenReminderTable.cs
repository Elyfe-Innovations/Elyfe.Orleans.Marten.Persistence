using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;
using Orleans;
using Orleans.Configuration;
using Orleans.Runtime;

namespace Elyfe.Orleans.Marten.Reminders;

public sealed class ElyfeMartenReminderTable : IReminderTable
{
    private const string ProviderVersion = "1";

    private readonly ElyfeMartenReminderOptions _options;
    private readonly ClusterOptions _clusterOptions;
    private readonly ILogger<ElyfeMartenReminderTable> _logger;
    private readonly NpgsqlDataSource _dataSource;
    private readonly string _table;
    private TimescaleCapability _timescale;

    public ElyfeMartenReminderTable(
        IOptions<ElyfeMartenReminderOptions> options,
        IOptions<ClusterOptions> clusterOptions,
        ILogger<ElyfeMartenReminderTable> logger)
    {
        _options = options.Value;
        _clusterOptions = clusterOptions.Value;
        _logger = logger;

        ArgumentException.ThrowIfNullOrWhiteSpace(_options.ConnectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(_options.SchemaName);
        ArgumentException.ThrowIfNullOrWhiteSpace(_options.TableName);

        var builder = new NpgsqlDataSourceBuilder(_options.ConnectionString);
        _dataSource = builder.Build();
        _table = $"{QuoteIdentifier(_options.SchemaName)}.{QuoteIdentifier(_options.TableName)}";
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
        await using var connection = await _dataSource.OpenConnectionAsync();
        if (_options.AutoCreateSchema)
        {
            await EnsureSchemaAsync(connection, CancellationToken.None);
        }

        _timescale = await DetectTimescaleAsync(connection, CancellationToken.None);
        _logger.LogInformation(
            "Elyfe Marten reminder table initialized. Schema={Schema} Table={Table} TimescaleInstalled={TimescaleInstalled} TableIsHypertable={TableIsHypertable}",
            _options.SchemaName,
            _options.TableName,
            _timescale.ExtensionInstalled,
            _timescale.TableIsHypertable);
    }

    public async Task<ReminderTableData> ReadRows(GrainId grainId)
    {
        const string sql = """
            SELECT grain_id, reminder_name, start_at, period_ticks, etag
            FROM {0}
            WHERE service_id = @serviceId AND grain_id = @grainId
            ORDER BY reminder_name
            """;

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = CreateCommand(connection, string.Format(sql, _table));
        AddText(command, "serviceId", ServiceId);
        AddText(command, "grainId", grainId.ToString());
        var entries = await ReadEntriesAsync(command);
        return new ReminderTableData(entries);
    }

    public async Task<ReminderTableData> ReadRows(uint begin, uint end)
    {
        var wraps = begin >= end;
        var sql = wraps
            ? $"""
               SELECT grain_id, reminder_name, start_at, period_ticks, etag
               FROM {_table}
               WHERE service_id = @serviceId AND (grain_hash > @beginHash OR grain_hash <= @endHash)
               ORDER BY grain_hash, grain_id, reminder_name
               """
            : $"""
               SELECT grain_id, reminder_name, start_at, period_ticks, etag
               FROM {_table}
               WHERE service_id = @serviceId AND grain_hash > @beginHash AND grain_hash <= @endHash
               ORDER BY grain_hash, grain_id, reminder_name
               """;

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = CreateCommand(connection, sql);
        AddText(command, "serviceId", ServiceId);
        AddLong(command, "beginHash", begin);
        AddLong(command, "endHash", end);
        var entries = await ReadEntriesAsync(command);
        return new ReminderTableData(entries);
    }

    public async Task<ReminderEntry?> ReadRow(GrainId grainId, string reminderName)
    {
        const string sql = """
            SELECT grain_id, reminder_name, start_at, period_ticks, etag
            FROM {0}
            WHERE service_id = @serviceId AND grain_id = @grainId AND reminder_name = @reminderName
            LIMIT 1
            """;

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = CreateCommand(connection, string.Format(sql, _table));
        AddText(command, "serviceId", ServiceId);
        AddText(command, "grainId", grainId.ToString());
        AddText(command, "reminderName", reminderName);
        var entries = await ReadEntriesAsync(command);
        return entries.Count == 0 ? null : entries[0];
    }

    public async Task<string> UpsertRow(ReminderEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentException.ThrowIfNullOrWhiteSpace(entry.ReminderName);

        var etag = Guid.NewGuid().ToString("N");
        const string lockSql = "SELECT pg_advisory_xact_lock(hashtextextended(@serviceId || ':' || @grainId || ':' || @reminderName, 0))";
        var deleteSql = $"""
            DELETE FROM {_table}
            WHERE service_id = @serviceId
              AND grain_id = @grainId
              AND reminder_name = @reminderName
            """;
        var insertSql = $"""
            INSERT INTO {_table}
                (service_id, cluster_id, grain_id, grain_hash, reminder_name, start_at, period_ticks, etag, provider_version, created_at, updated_at)
            VALUES
                (@serviceId, @clusterId, @grainId, @grainHash, @reminderName, @startAt, @periodTicks, @etag, @providerVersion, transaction_timestamp(), transaction_timestamp())
            RETURNING etag
            """;

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        try
        {
            await using (var lockCommand = CreateCommand(connection, lockSql))
            {
                lockCommand.Transaction = transaction;
                AddText(lockCommand, "serviceId", ServiceId);
                AddText(lockCommand, "grainId", entry.GrainId.ToString());
                AddText(lockCommand, "reminderName", entry.ReminderName);
                await lockCommand.ExecuteNonQueryAsync();
            }

            await using (var deleteCommand = CreateCommand(connection, deleteSql))
            {
                deleteCommand.Transaction = transaction;
                AddText(deleteCommand, "serviceId", ServiceId);
                AddText(deleteCommand, "grainId", entry.GrainId.ToString());
                AddText(deleteCommand, "reminderName", entry.ReminderName);
                await deleteCommand.ExecuteNonQueryAsync();
            }

            await using var insertCommand = CreateCommand(connection, insertSql);
            insertCommand.Transaction = transaction;
            AddText(insertCommand, "serviceId", ServiceId);
            AddText(insertCommand, "clusterId", _clusterOptions.ClusterId ?? string.Empty);
            AddText(insertCommand, "grainId", entry.GrainId.ToString());
            AddLong(insertCommand, "grainHash", entry.GrainId.GetUniformHashCode());
            AddText(insertCommand, "reminderName", entry.ReminderName);
            AddTimestamp(insertCommand, "startAt", entry.StartAt);
            AddLong(insertCommand, "periodTicks", entry.Period.Ticks);
            AddText(insertCommand, "etag", etag);
            AddText(insertCommand, "providerVersion", ProviderVersion);

            var result = await insertCommand.ExecuteScalarAsync();
            await transaction.CommitAsync();
            return (string)result!;
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }
    }

    public async Task<bool> RemoveRow(GrainId grainId, string reminderName, string eTag)
    {
        var sql = $"""
            DELETE FROM {_table}
            WHERE service_id = @serviceId
              AND grain_id = @grainId
              AND reminder_name = @reminderName
              AND etag = @etag
            """;

        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = CreateCommand(connection, sql);
        AddText(command, "serviceId", ServiceId);
        AddText(command, "grainId", grainId.ToString());
        AddText(command, "reminderName", reminderName);
        AddText(command, "etag", eTag);
        return await command.ExecuteNonQueryAsync() == 1;
    }

    public async Task TestOnlyClearTable()
    {
        var sql = $"DELETE FROM {_table} WHERE service_id = @serviceId";
        await using var connection = await _dataSource.OpenConnectionAsync();
        await using var command = CreateCommand(connection, sql);
        AddText(command, "serviceId", ServiceId);
        await command.ExecuteNonQueryAsync();
    }

    private string ServiceId => string.IsNullOrWhiteSpace(_clusterOptions.ServiceId) ? string.Empty : _clusterOptions.ServiceId;

    private async Task<List<ReminderEntry>> ReadEntriesAsync(NpgsqlCommand command)
    {
        var entries = new List<ReminderEntry>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            entries.Add(new ReminderEntry
            {
                GrainId = GrainId.Parse(reader.GetString(0)),
                ReminderName = reader.GetString(1),
                StartAt = reader.GetFieldValue<DateTime>(2),
                Period = TimeSpan.FromTicks(reader.GetInt64(3)),
                ETag = reader.GetString(4)
            });
        }

        return entries;
    }

    private async Task<TimescaleCapability> DetectTimescaleAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        if (!_options.PreferTimescale)
        {
            return new TimescaleCapability(false, false);
        }

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
        AddText(hypertableCommand, "tableName", _options.TableName);
        var isHypertable = (bool)(await hypertableCommand.ExecuteScalarAsync(cancellationToken) ?? false);
        return new TimescaleCapability(true, isHypertable);
    }

    private async Task EnsureSchemaAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var schemaCommand = CreateCommand(connection, $"CREATE SCHEMA IF NOT EXISTS {QuoteIdentifier(_options.SchemaName)}");
        await schemaCommand.ExecuteNonQueryAsync(cancellationToken);

        var sql = $"""
            CREATE TABLE IF NOT EXISTS {_table} (
                service_id text NOT NULL,
                cluster_id text NOT NULL,
                grain_id text NOT NULL,
                grain_hash bigint NOT NULL,
                reminder_name text NOT NULL,
                start_at timestamptz NOT NULL,
                period_ticks bigint NOT NULL,
                etag text NOT NULL,
                provider_version text NOT NULL,
                created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
                updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
                CONSTRAINT pkey_{_options.TableName}_service_grain_name_start PRIMARY KEY (service_id, grain_id, reminder_name, start_at)
            )
            """;
        await using var tableCommand = CreateCommand(connection, sql);
        await tableCommand.ExecuteNonQueryAsync(cancellationToken);

        await EnsureTimescaleHypertableAsync(connection, cancellationToken);

        await ExecuteNonQueryAsync(connection, $"CREATE INDEX IF NOT EXISTS idx_{_options.TableName}_service_hash ON {_table} (service_id, grain_hash)", cancellationToken);
        await ExecuteNonQueryAsync(connection, $"CREATE INDEX IF NOT EXISTS idx_{_options.TableName}_service_grain ON {_table} (service_id, grain_id)", cancellationToken);
        await ExecuteNonQueryAsync(connection, $"CREATE INDEX IF NOT EXISTS idx_{_options.TableName}_start_at ON {_table} (start_at DESC)", cancellationToken);
    }

    private async Task EnsureTimescaleHypertableAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        if (!_options.PreferTimescale)
        {
            return;
        }

        var capability = await DetectTimescaleAsync(connection, cancellationToken);
        if (!capability.ExtensionInstalled || capability.TableIsHypertable)
        {
            return;
        }

        const string sql = """
            SELECT create_hypertable(
                @qualifiedTableName::regclass,
                'start_at',
                chunk_time_interval => @chunkInterval::interval,
                if_not_exists => TRUE,
                migrate_data => TRUE)
            """;
        await using var command = CreateCommand(connection, sql);
        AddText(command, "qualifiedTableName", $"{QuoteIdentifier(_options.SchemaName)}.{QuoteIdentifier(_options.TableName)}");
        AddInterval(command, "chunkInterval", _options.TimescaleChunkInterval);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task ExecuteNonQueryAsync(NpgsqlConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = CreateCommand(connection, sql);
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

    private static void AddLong(NpgsqlCommand command, string name, long value)
    {
        command.Parameters.Add(new NpgsqlParameter(name, NpgsqlDbType.Bigint) { Value = value });
    }

    private static void AddInterval(NpgsqlCommand command, string name, TimeSpan value)
    {
        command.Parameters.Add(new NpgsqlParameter(name, NpgsqlDbType.Interval) { Value = value });
    }

    private static void AddTimestamp(NpgsqlCommand command, string name, DateTime value)
    {
        command.Parameters.Add(new NpgsqlParameter(name, NpgsqlDbType.TimestampTz) { Value = value.Kind == DateTimeKind.Utc ? value : value.ToUniversalTime() });
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
