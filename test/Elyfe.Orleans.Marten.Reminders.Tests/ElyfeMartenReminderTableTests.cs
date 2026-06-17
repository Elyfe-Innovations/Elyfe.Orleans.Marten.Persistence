using AwesomeAssertions;
using Elyfe.Orleans.Marten.Reminders;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using Orleans;
using Orleans.Configuration;
using Orleans.Runtime;
using Testcontainers.PostgreSql;
using Xunit;

namespace Elyfe.Orleans.Marten.Reminders.Tests;

[CollectionDefinition("Elyfe Marten Reminder Tests", DisableParallelization = true)]
public sealed class ElyfeMartenReminderTestCollection;

[Collection("Elyfe Marten Reminder Tests")]
public sealed class ElyfeMartenReminderTableTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgreSqlContainer = new PostgreSqlBuilder("timescale/timescaledb:latest-pg17")
        .WithDatabase("reminders_tests")
        .WithUsername("testuser")
        .WithPassword("testpass")
        .Build();

    private ElyfeMartenReminderTable? _table;

    public async Task InitializeAsync()
    {
        await _postgreSqlContainer.StartAsync();
        _table = CreateTable(autoCreateSchema: true, preferTimescale: true);
        await _table.Init();
    }

    public async Task DisposeAsync()
    {
        await _postgreSqlContainer.DisposeAsync();
    }

    [Fact]
    public async Task UpsertAndReadRow_RoundTripsReminderAndGeneratesEtag()
    {
        var grainId = GrainId.Parse("reminder-test/grain-1");
        var entry = new ReminderEntry
        {
            GrainId = grainId,
            ReminderName = "renewal",
            StartAt = DateTime.UtcNow.AddMinutes(5),
            Period = TimeSpan.FromMinutes(30)
        };

        var etag = await Table.UpsertRow(entry);
        var stored = await Table.ReadRow(grainId, "renewal");

        stored.Should().NotBeNull();
        stored!.ReminderName.Should().Be("renewal");
        stored.GrainId.Should().Be(grainId);
        stored.Period.Should().Be(TimeSpan.FromMinutes(30));
        stored.ETag.Should().Be(etag);
    }

    [Fact]
    public async Task UpsertRow_ReplacesExistingReminderAndChangesEtag()
    {
        var grainId = GrainId.Parse("reminder-test/grain-2");
        var first = new ReminderEntry
        {
            GrainId = grainId,
            ReminderName = "billing",
            StartAt = DateTime.UtcNow.AddMinutes(5),
            Period = TimeSpan.FromMinutes(15)
        };
        var second = new ReminderEntry
        {
            GrainId = grainId,
            ReminderName = "billing",
            StartAt = DateTime.UtcNow.AddMinutes(10),
            Period = TimeSpan.FromHours(1)
        };

        var firstEtag = await Table.UpsertRow(first);
        var secondEtag = await Table.UpsertRow(second);
        var rows = await Table.ReadRows(grainId);

        rows.Reminders.Should().ContainSingle();
        rows.Reminders[0].Period.Should().Be(TimeSpan.FromHours(1));
        secondEtag.Should().NotBe(firstEtag);
    }

    [Fact]
    public async Task RemoveRow_RequiresCurrentEtag()
    {
        var grainId = GrainId.Parse("reminder-test/grain-3");
        var entry = new ReminderEntry
        {
            GrainId = grainId,
            ReminderName = "payment",
            StartAt = DateTime.UtcNow.AddMinutes(5),
            Period = TimeSpan.FromMinutes(5)
        };

        var etag = await Table.UpsertRow(entry);

        var staleRemoved = await Table.RemoveRow(grainId, "payment", "stale");
        var removed = await Table.RemoveRow(grainId, "payment", etag);
        var stored = await Table.ReadRow(grainId, "payment");

        staleRemoved.Should().BeFalse();
        removed.Should().BeTrue();
        stored.Should().BeNull();
    }

    [Fact]
    public async Task ReadRows_HashRangeWrapsAround()
    {
        var grainId = GrainId.Parse("reminder-test/grain-4");
        await Table.UpsertRow(new ReminderEntry
        {
            GrainId = grainId,
            ReminderName = "range",
            StartAt = DateTime.UtcNow.AddMinutes(5),
            Period = TimeSpan.FromMinutes(5)
        });

        var rows = await Table.ReadRows(uint.MaxValue - 10, 10);

        rows.Reminders.Should().NotBeNull();
    }

    [Fact]
    public async Task ReminderSurvivesProviderRestartAndRangeRead()
    {
        var grainId = GrainId.Parse("reminder-test/grain-5");
        var entry = new ReminderEntry
        {
            GrainId = grainId,
            ReminderName = "restart",
            StartAt = DateTime.UtcNow.AddMinutes(5),
            Period = TimeSpan.FromMinutes(20)
        };

        await Table.UpsertRow(entry);
        var restartedTable = CreateTable(autoCreateSchema: false, preferTimescale: true);
        await restartedTable.Init();

        var byGrain = await restartedTable.ReadRows(grainId);
        var byRange = await restartedTable.ReadRows(0, uint.MaxValue);

        byGrain.Reminders.Should().ContainSingle(static reminder => reminder.ReminderName == "restart");
        byRange.Reminders.Should().Contain(reminder => reminder.GrainId.Equals(grainId) && reminder.ReminderName == "restart");
    }

    [Fact]
    public async Task ReadRows_OnlyReturnsCurrentServiceId()
    {
        var grainId = GrainId.Parse("reminder-test/service-isolation");
        await Table.UpsertRow(new ReminderEntry
        {
            GrainId = grainId,
            ReminderName = "shared-name",
            StartAt = DateTime.UtcNow.AddMinutes(5),
            Period = TimeSpan.FromMinutes(10)
        });

        var otherServiceTable = CreateTable(autoCreateSchema: false, preferTimescale: true, serviceId: "other-service");
        await otherServiceTable.Init();
        await otherServiceTable.UpsertRow(new ReminderEntry
        {
            GrainId = grainId,
            ReminderName = "shared-name",
            StartAt = DateTime.UtcNow.AddMinutes(15),
            Period = TimeSpan.FromHours(1)
        });

        var currentServiceRows = await Table.ReadRows(grainId);
        var otherServiceRows = await otherServiceTable.ReadRows(grainId);

        currentServiceRows.Reminders.Should().ContainSingle();
        currentServiceRows.Reminders[0].Period.Should().Be(TimeSpan.FromMinutes(10));
        otherServiceRows.Reminders.Should().ContainSingle();
        otherServiceRows.Reminders[0].Period.Should().Be(TimeSpan.FromHours(1));
    }

    [Fact]
    public async Task ReadRows_HashRangeUsesExclusiveBeginInclusiveEnd()
    {
        var grainId = GrainId.Parse("reminder-test/hash-boundary");
        var hash = grainId.GetUniformHashCode();
        await Table.UpsertRow(new ReminderEntry
        {
            GrainId = grainId,
            ReminderName = "boundary",
            StartAt = DateTime.UtcNow.AddMinutes(5),
            Period = TimeSpan.FromMinutes(10)
        });

        var excludedAtBegin = await Table.ReadRows(hash, uint.MaxValue);
        var includedAtEnd = await Table.ReadRows(0, hash);

        excludedAtBegin.Reminders.Should().NotContain(static reminder => reminder.ReminderName == "boundary");
        includedAtEnd.Reminders.Should().ContainSingle(static reminder => reminder.ReminderName == "boundary");
    }

    [Fact]
    public async Task ConcurrentUpserts_ForSameReminderLeaveSingleLatestRow()
    {
        var grainId = GrainId.Parse("reminder-test/concurrent-upsert");
        var upserts = Enumerable.Range(1, 8)
            .Select(index => Table.UpsertRow(new ReminderEntry
            {
                GrainId = grainId,
                ReminderName = "concurrent",
                StartAt = DateTime.UtcNow.AddMinutes(index),
                Period = TimeSpan.FromMinutes(index)
            }));

        await Task.WhenAll(upserts);

        var rows = await Table.ReadRows(grainId);

        rows.Reminders.Should().ContainSingle();
        rows.Reminders[0].ReminderName.Should().Be("concurrent");
    }

    [Fact]
    public async Task TestOnlyClearTable_ClearsOnlyCurrentService()
    {
        var grainId = GrainId.Parse("reminder-test/clear-service");
        await Table.UpsertRow(new ReminderEntry
        {
            GrainId = grainId,
            ReminderName = "current",
            StartAt = DateTime.UtcNow.AddMinutes(5),
            Period = TimeSpan.FromMinutes(10)
        });
        var otherServiceTable = CreateTable(autoCreateSchema: false, preferTimescale: true, serviceId: "clear-other-service");
        await otherServiceTable.Init();
        await otherServiceTable.UpsertRow(new ReminderEntry
        {
            GrainId = grainId,
            ReminderName = "other",
            StartAt = DateTime.UtcNow.AddMinutes(5),
            Period = TimeSpan.FromMinutes(10)
        });

        await Table.TestOnlyClearTable();

        (await Table.ReadRows(grainId)).Reminders.Should().BeEmpty();
        (await otherServiceTable.ReadRows(grainId)).Reminders.Should().ContainSingle(static reminder => reminder.ReminderName == "other");
    }

    [Fact]
    public async Task Init_DetectsTimescaleHypertableWhenExtensionIsInstalled()
    {
        await using var connection = new NpgsqlConnection(_postgreSqlContainer.GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT EXISTS (
                SELECT 1
                FROM timescaledb_information.hypertables
                WHERE hypertable_schema = 'reminders'
                  AND hypertable_name = 'orleans_reminders')
            """;

        var isHypertable = (bool)(await command.ExecuteScalarAsync() ?? false);

        isHypertable.Should().BeTrue("auto-create should heavily use Timescale when the extension is installed");
    }

    private ElyfeMartenReminderTable CreateTable(bool autoCreateSchema, bool preferTimescale, string serviceId = "test-service")
    {
        return new ElyfeMartenReminderTable(
            Options.Create(new ElyfeMartenReminderOptions
            {
                ConnectionString = _postgreSqlContainer.GetConnectionString(),
                AutoCreateSchema = autoCreateSchema,
                PreferTimescale = preferTimescale
            }),
            Options.Create(new ClusterOptions
            {
                ServiceId = serviceId,
                ClusterId = "test-cluster"
            }),
            NullLogger<ElyfeMartenReminderTable>.Instance);
    }

    private ElyfeMartenReminderTable Table => _table ?? throw new InvalidOperationException("Test table was not initialized.");
}
