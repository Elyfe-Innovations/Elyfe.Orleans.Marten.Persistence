using JasperFx;
using Marten;
using Orleans.Concurrency;
using Orleans.Runtime;
using Orleans.TestingHost;
using Testcontainers.PostgreSql;
using Weasel.Core;
using Xunit;

namespace Elyfe.Orleans.Marten.Reminders.Tests;

/// <summary>
/// Test grain for exercising the Elyfe Marten reminder provider through Orleans' reminder subsystem.
/// Key format: arbitrary test probe id.
/// </summary>
public interface IReminderProbeGrain : IGrainWithStringKey
{
    Task RegisterAsync(TimeSpan dueTime, TimeSpan period);

    Task UnregisterAsync();

    [AlwaysInterleave]
    Task<int> GetTickCountAsync();
}

public sealed class ReminderProbeGrain : Grain, IReminderProbeGrain, IRemindable
{
    private const string ReminderName = "probe";
    private int _tickCount;

    public async Task RegisterAsync(TimeSpan dueTime, TimeSpan period)
    {
        await this.RegisterOrUpdateReminder(ReminderName, dueTime, period);
    }

    public async Task UnregisterAsync()
    {
        var reminder = await this.GetReminder(ReminderName);
        if (reminder is not null)
        {
            await this.UnregisterReminder(reminder);
        }
    }

    public Task<int> GetTickCountAsync()
    {
        return Task.FromResult(_tickCount);
    }

    public Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (reminderName == ReminderName)
        {
            Interlocked.Increment(ref _tickCount);
        }

        return Task.CompletedTask;
    }
}

[Collection("Elyfe Marten Reminder Tests")]
public sealed class ReminderProbeGrainTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgreSqlContainer = new PostgreSqlBuilder("timescale/timescaledb:latest-pg17")
        .WithDatabase("reminder_probe_tests")
        .WithUsername("testuser")
        .WithPassword("testpass")
        .Build();

    private TestCluster? _cluster;

    public async Task InitializeAsync()
    {
        await _postgreSqlContainer.StartAsync();
        ReminderProbeSiloConfigurator.ConnectionString = _postgreSqlContainer.GetConnectionString();

        var builder = new TestClusterBuilder();
        builder.Options.InitialSilosCount = 1;
        builder.AddSiloBuilderConfigurator<ReminderProbeSiloConfigurator>();
        _cluster = builder.Build();
        _cluster.Deploy();
    }

    public async Task DisposeAsync()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            _cluster.Dispose();
        }

        await _postgreSqlContainer.DisposeAsync();
    }

    [Fact]
    public async Task RegisteredReminder_TicksThroughElyfeMartenReminderProvider()
    {
        var grain = Cluster.GrainFactory.GetGrain<IReminderProbeGrain>($"probe-{Guid.NewGuid():N}");

        await grain.RegisterAsync(TimeSpan.FromMilliseconds(200), TimeSpan.FromMinutes(1));
        var ticked = await WaitUntilAsync(async () => await grain.GetTickCountAsync() > 0, TimeSpan.FromSeconds(60));
        await grain.UnregisterAsync();

        Assert.True(ticked, "The Orleans reminder service should load and tick the probe reminder through the Elyfe Marten provider.");
    }

    private TestCluster Cluster => _cluster ?? throw new InvalidOperationException("Test cluster was not initialized.");

    private static async Task<bool> WaitUntilAsync(Func<Task<bool>> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
            {
                return true;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(100));
        }

        return false;
    }
}

public sealed class ReminderProbeSiloConfigurator : ISiloConfigurator
{
    public static string ConnectionString { get; set; } = string.Empty;

    public void Configure(ISiloBuilder siloBuilder)
    {
        siloBuilder.Services.AddMarten(options =>
        {
            options.Connection(ConnectionString);
            options.UseSystemTextJsonForSerialization(EnumStorage.AsString);
            options.AutoCreateSchemaObjects = AutoCreate.CreateOrUpdate;
        });

        siloBuilder.UseElyfeMartenReminderService(options =>
        {
            options.ConnectionString = ConnectionString;
            options.AutoCreateSchema = true;
            options.PreferTimescale = false;
        });
    }
}
