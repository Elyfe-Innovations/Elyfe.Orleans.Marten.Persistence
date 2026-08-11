using System.Net;
using AwesomeAssertions;
using Marten;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Configuration;
using Orleans.Runtime;
using Testcontainers.PostgreSql;
using JasperFx;
using Xunit;

namespace Elyfe.Orleans.Marten.Clustering.Tests;

public sealed class ElyfeMartenMembershipTableTests : IAsyncLifetime
{
    private const string ClusterId = "cluster-under-test";

    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("timescale/timescaledb:2.29.1-pg18")
        .WithDatabase("clustering_test")
        .WithUsername("postgres")
        .WithPassword("postgres")
        .Build();

    private DocumentStore _store = null!;

    public async Task InitializeAsync()
    {
        await _postgres.StartAsync();
        _store = DocumentStore.For(options =>
        {
            options.Connection(_postgres.GetConnectionString());
            options.AutoCreateSchemaObjects = AutoCreate.All;
            new ElyfeMartenClusteringMartenConfiguration(
                    Options.Create(new ElyfeMartenClusteringOptions()))
                .Configure(null!, options);
        });
        await _store.Storage.ApplyAllConfiguredChangesToDatabaseAsync();
    }

    public async Task DisposeAsync()
    {
        _store.Dispose();
        await _postgres.DisposeAsync();
    }

    [Fact]
    public async Task Initializes_the_cluster_version_once()
    {
        var table = CreateTable();

        await table.InitializeMembershipTable(true);
        await table.InitializeMembershipTable(true);

        var data = await table.ReadAll();
        data.Version.Version.Should().Be(0);
        data.Members.Should().BeEmpty();
    }

    [Fact]
    public async Task Inserts_a_silo_and_rejects_a_stale_table_version()
    {
        var table = CreateTable();
        await table.InitializeMembershipTable(true);
        var initial = await table.ReadAll();

        var entry = CreateEntry(11111);
        (await table.InsertRow(entry, initial.Version.Next())).Should().BeTrue();

        // Replaying the same table version is exactly the lost-race case Orleans must observe as false.
        (await table.InsertRow(CreateEntry(22222), initial.Version.Next())).Should().BeFalse();

        var afterInsert = await table.ReadAll();
        afterInsert.Members.Should().ContainSingle();
        afterInsert.Version.Version.Should().Be(1);
    }

    [Fact]
    public async Task Rejects_duplicate_silo_insert()
    {
        var table = CreateTable();
        await table.InitializeMembershipTable(true);
        var entry = CreateEntry(11111);

        var first = await table.ReadAll();
        (await table.InsertRow(entry, first.Version.Next())).Should().BeTrue();

        var second = await table.ReadAll();
        (await table.InsertRow(entry, second.Version.Next())).Should().BeFalse();
    }

    [Fact]
    public async Task Updates_a_row_and_rejects_stale_row_etag_and_table_version()
    {
        var table = CreateTable();
        await table.InitializeMembershipTable(true);
        var entry = CreateEntry(11111);
        var initial = await table.ReadAll();
        await table.InsertRow(entry, initial.Version.Next());

        var current = await table.ReadAll();
        var (storedEntry, etag) = (current.Members[0].Item1, current.Members[0].Item2);
        storedEntry.Status = SiloStatus.Active;

        (await table.UpdateRow(storedEntry, etag, current.Version.Next())).Should().BeTrue();

        var updated = await table.ReadAll();
        updated.Members[0].Item1.Status.Should().Be(SiloStatus.Active);

        // Stale row etag.
        (await table.UpdateRow(storedEntry, etag, updated.Version.Next())).Should().BeFalse();
        // Stale table version.
        (await table.UpdateRow(storedEntry, updated.Members[0].Item2, current.Version.Next()))
            .Should().BeFalse();
    }

    [Fact]
    public async Task UpdateIAmAlive_does_not_move_the_table_version()
    {
        var table = CreateTable();
        await table.InitializeMembershipTable(true);
        var entry = CreateEntry(11111);
        var initial = await table.ReadAll();
        await table.InsertRow(entry, initial.Version.Next());

        var beforeBeat = await table.ReadAll();
        var liveness = new DateTime(2026, 8, 11, 12, 0, 0, DateTimeKind.Utc);
        entry.IAmAliveTime = liveness;

        await table.UpdateIAmAlive(entry);

        var afterBeat = await table.ReadAll();
        afterBeat.Version.Version.Should().Be(beforeBeat.Version.Version);
        afterBeat.Version.VersionEtag.Should().Be(beforeBeat.Version.VersionEtag);
        afterBeat.Members[0].Item1.IAmAliveTime.Should().Be(liveness);
    }

    [Fact]
    public async Task Round_trips_entry_details_including_suspect_times()
    {
        var table = CreateTable();
        await table.InitializeMembershipTable(true);
        var entry = CreateEntry(11111);
        entry.Status = SiloStatus.ShuttingDown;
        entry.SuspectTimes =
        [
            Tuple.Create(SiloAddress.New(new IPEndPoint(IPAddress.Loopback, 22222), 7),
                new DateTime(2026, 8, 11, 10, 0, 0, DateTimeKind.Utc))
        ];

        var initial = await table.ReadAll();
        await table.InsertRow(entry, initial.Version.Next());

        var readBack = await table.ReadRow(entry.SiloAddress);
        var stored = readBack.Members.Should().ContainSingle().Subject.Item1;
        stored.SiloName.Should().Be(entry.SiloName);
        stored.HostName.Should().Be(entry.HostName);
        stored.ProxyPort.Should().Be(entry.ProxyPort);
        stored.Status.Should().Be(SiloStatus.ShuttingDown);
        stored.SiloAddress.Generation.Should().Be(entry.SiloAddress.Generation);
        stored.SuspectTimes.Should().ContainSingle()
            .Which.Item1.Endpoint.Port.Should().Be(22222);
    }

    [Fact]
    public async Task Cleans_up_only_defunct_entries_older_than_the_cutoff()
    {
        var table = CreateTable();
        await table.InitializeMembershipTable(true);

        var dead = CreateEntry(11111);
        dead.Status = SiloStatus.Dead;
        dead.IAmAliveTime = new DateTime(2026, 8, 1, 0, 0, 0, DateTimeKind.Utc);

        var active = CreateEntry(22222);
        active.Status = SiloStatus.Active;
        active.IAmAliveTime = new DateTime(2026, 8, 1, 0, 0, 0, DateTimeKind.Utc);

        var recentlyDead = CreateEntry(33333);
        recentlyDead.Status = SiloStatus.Dead;
        recentlyDead.IAmAliveTime = new DateTime(2026, 8, 10, 0, 0, 0, DateTimeKind.Utc);

        foreach (var entry in new[] { dead, active, recentlyDead })
        {
            var version = (await table.ReadAll()).Version;
            await table.InsertRow(entry, version.Next());
        }

        await table.CleanupDefunctSiloEntries(new DateTimeOffset(2026, 8, 5, 0, 0, 0, TimeSpan.Zero));

        var remaining = await table.ReadAll();
        remaining.Members.Select(member => member.Item1.SiloAddress.Endpoint.Port)
            .Should().BeEquivalentTo([22222, 33333]);
    }

    [Fact]
    public async Task Deletes_only_the_requested_cluster()
    {
        var table = CreateTable();
        var otherTable = CreateTable("other-cluster");
        await table.InitializeMembershipTable(true);
        await otherTable.InitializeMembershipTable(true);

        await table.InsertRow(CreateEntry(11111), (await table.ReadAll()).Version.Next());
        await otherTable.InsertRow(CreateEntry(11111), (await otherTable.ReadAll()).Version.Next());

        await table.DeleteMembershipTableEntries(ClusterId);

        (await table.ReadAll()).Members.Should().BeEmpty();
        (await otherTable.ReadAll()).Members.Should().ContainSingle();
    }

    [Fact]
    public async Task Concurrent_inserts_produce_exactly_one_winner()
    {
        var table = CreateTable();
        await table.InitializeMembershipTable(true);
        var version = (await table.ReadAll()).Version;

        var results = await Task.WhenAll(
            Enumerable.Range(0, 5)
                .Select(index => CreateTable().InsertRow(CreateEntry(40000 + index), version.Next())));

        results.Count(succeeded => succeeded).Should().Be(1);
        (await table.ReadAll()).Members.Should().ContainSingle();
    }

    [Fact]
    public async Task Gateway_provider_returns_only_active_silos_with_a_proxy_port()
    {
        var table = CreateTable();
        await table.InitializeMembershipTable(true);

        var active = CreateEntry(11111, proxyPort: 30000);
        active.Status = SiloStatus.Active;
        var activeWithoutProxy = CreateEntry(22222, proxyPort: 0);
        activeWithoutProxy.Status = SiloStatus.Active;
        var dead = CreateEntry(33333, proxyPort: 30001);
        dead.Status = SiloStatus.Dead;

        foreach (var entry in new[] { active, activeWithoutProxy, dead })
        {
            var version = (await table.ReadAll()).Version;
            await table.InsertRow(entry, version.Next());
        }

        var gateways = await CreateGatewayProvider().GetGateways();

        gateways.Should().ContainSingle().Which.Port.Should().Be(30000);
    }

    private ElyfeMartenMembershipTable CreateTable(string clusterId = ClusterId) =>
        new(
            new ElyfeMartenClusteringDefaultStore(_store),
            Options.Create(new ClusterOptions { ClusterId = clusterId, ServiceId = "test-service" }),
            NullLogger<ElyfeMartenMembershipTable>.Instance);

    private ElyfeMartenGatewayListProvider CreateGatewayProvider(string clusterId = ClusterId) =>
        new(
            new ElyfeMartenClusteringDefaultStore(_store),
            Options.Create(new ElyfeMartenClusteringOptions()),
            Options.Create(new ClusterOptions { ClusterId = clusterId, ServiceId = "test-service" }));

    private static MembershipEntry CreateEntry(int port, int proxyPort = 30000) => new()
    {
        SiloAddress = SiloAddress.New(new IPEndPoint(IPAddress.Loopback, port), 1),
        Status = SiloStatus.Joining,
        ProxyPort = proxyPort,
        HostName = "test-host",
        SiloName = $"silo-{port}",
        RoleName = "worker",
        UpdateZone = 0,
        FaultZone = 0,
        StartTime = new DateTime(2026, 8, 11, 9, 0, 0, DateTimeKind.Utc),
        IAmAliveTime = new DateTime(2026, 8, 11, 9, 0, 0, DateTimeKind.Utc)
    };
}
