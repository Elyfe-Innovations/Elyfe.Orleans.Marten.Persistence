using AwesomeAssertions;
using JasperFx;
using Marten;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using Orleans;
using Orleans.Configuration;
using Orleans.Runtime;
using Testcontainers.PostgreSql;
using Weasel.Core;
using Xunit;

namespace Elyfe.Orleans.Marten.Reminders.Tests;

/// <summary>
/// Regression cover for the reminder document table's storage shape. Marten writes documents with an
/// inline <c>INSERT ... ON CONFLICT (id) DO UPDATE</c>, so a reminder table whose primary key is not
/// exactly <c>(id)</c> - a TimescaleDB hypertable partitioned on <c>mt_created_at</c> carries
/// <c>(id, mt_created_at)</c>, because a hypertable cannot have a unique index that omits its
/// partitioning column - makes PostgreSQL reject every reminder write with 42P10. Staging and
/// production both ran in that shape and held zero reminder rows until the platform lane migration
/// 002-orleans-reminders-regular-document-table converted the table back to a plain document table.
/// </summary>
[Collection("Elyfe Marten Reminder Tests")]
public sealed class ReminderDocumentTableShapeTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("timescale/timescaledb:2.29.1-pg18")
        .WithDatabase("reminders_shape")
        .WithUsername("postgres")
        .WithPassword("postgres")
        .Build();

    public Task InitializeAsync() => _postgres.StartAsync();

    public async Task DisposeAsync() => await _postgres.DisposeAsync();

    [Fact]
    public async Task Init_rejects_a_composite_primary_key_instead_of_silently_breaking_every_reminder()
    {
        await CreateHypertableShapedReminderTableAsync();
        using var store = CreateStore(AutoCreate.None);
        var table = CreateTable(store);

        var init = async () => await table.Init();

        (await init.Should().ThrowAsync<InvalidOperationException>())
            .Which.Message.Should()
            .Contain("primary key (id, mt_created_at)").And
            .Contain("ON CONFLICT (id)").And
            .Contain("002-orleans-reminders-regular-document-table");
    }

    [Fact]
    public async Task Upsert_on_a_composite_primary_key_table_fails_with_42P10()
    {
        // The failure the assertion above guards against: this is the exact production symptom,
        // MartenCommandException wrapping PostgreSQL 42P10 out of infer_arbiter_indexes.
        await CreateHypertableShapedReminderTableAsync();
        using var store = CreateStore(AutoCreate.None);
        var table = CreateTable(store);

        var upsert = async () => await table.UpsertRow(new ReminderEntry
        {
            GrainId = GrainId.Parse("shape-test/grain-1"),
            ReminderName = "sweep",
            StartAt = DateTime.UtcNow.AddMinutes(1),
            Period = TimeSpan.FromMinutes(5)
        });

        var thrown = await upsert.Should().ThrowAsync<Exception>();
        var postgres = FindPostgresException(thrown.Which);
        postgres.Should().NotBeNull();
        postgres!.SqlState.Should().Be("42P10");
    }

    [Fact]
    public async Task Init_accepts_the_marten_document_shape_and_upserts_round_trip()
    {
        using var store = CreateStore(AutoCreate.CreateOrUpdate);
        await store.Storage.ApplyAllConfiguredChangesToDatabaseAsync(AutoCreate.CreateOrUpdate);
        var table = CreateTable(store);
        await table.Init();

        var grainId = GrainId.Parse("shape-test/grain-2");
        var etag = await table.UpsertRow(new ReminderEntry
        {
            GrainId = grainId,
            ReminderName = "sweep",
            StartAt = DateTime.UtcNow.AddMinutes(1),
            Period = TimeSpan.FromMinutes(5)
        });

        (await table.ReadRow(grainId, "sweep"))!.ETag.Should().Be(etag);

        await using var connection = new NpgsqlConnection(_postgres.GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT array_to_string(array_agg(att.attname ORDER BY key.ordinality), ',')
            FROM pg_constraint con
            JOIN pg_class cls ON cls.oid = con.conrelid
            JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace
            CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS key(attnum, ordinality)
            JOIN pg_attribute att ON att.attrelid = cls.oid AND att.attnum = key.attnum
            WHERE nsp.nspname = 'reminders'
              AND cls.relname = 'mt_doc_orleans_reminders'
              AND con.contype = 'p'
            """;

        (await command.ExecuteScalarAsync()).Should().Be("id");
    }

    private static PostgresException? FindPostgresException(Exception exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is PostgresException postgres)
            {
                return postgres;
            }
        }

        return null;
    }

    private async Task CreateHypertableShapedReminderTableAsync()
    {
        // Mirrors 001-platform-baseline, which reproduced the live staging shape.
        await using var connection = new NpgsqlConnection(_postgres.GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE EXTENSION IF NOT EXISTS timescaledb;
            CREATE SCHEMA IF NOT EXISTS reminders;
            CREATE TABLE reminders.mt_doc_orleans_reminders (
                id character varying NOT NULL,
                data jsonb NOT NULL,
                mt_last_modified timestamp with time zone DEFAULT transaction_timestamp(),
                mt_version uuid DEFAULT (md5(((random())::text || (clock_timestamp())::text)))::uuid NOT NULL,
                mt_dotnet_type character varying,
                mt_created_at timestamp with time zone DEFAULT transaction_timestamp() NOT NULL,
                headers jsonb,
                CONSTRAINT pkey_mt_doc_orleans_reminders_id PRIMARY KEY (id, mt_created_at)
            );
            SELECT create_hypertable(
                'reminders.mt_doc_orleans_reminders',
                'mt_created_at',
                chunk_time_interval => INTERVAL '7 days',
                migrate_data => true,
                create_default_indexes => false);
            """;
        await command.ExecuteNonQueryAsync();
    }

    private IDocumentStore CreateStore(AutoCreate autoCreate) =>
        DocumentStore.For(options =>
        {
            options.Connection(_postgres.GetConnectionString());
            options.UseSystemTextJsonForSerialization(EnumStorage.AsString);
            options.AutoCreateSchemaObjects = autoCreate;
            options.Schema.For<ElyfeMartenReminderDocument>()
                .DatabaseSchemaName("reminders")
                .DocumentAlias("orleans_reminders")
                .Identity(document => document.Id)
                .Metadata(config =>
                {
                    config.CreatedAt.Enabled = true;
                    config.Headers.Enabled = true;
                });
        });

    private ElyfeMartenReminderTable CreateTable(IDocumentStore store) =>
        new(
            new ElyfeMartenReminderDefaultStore(store),
            Options.Create(new ElyfeMartenReminderOptions { AutoCreateSchema = false }),
            Options.Create(new ClusterOptions { ServiceId = "shape-service", ClusterId = "shape-cluster" }),
            NullLogger<ElyfeMartenReminderTable>.Instance);
}
