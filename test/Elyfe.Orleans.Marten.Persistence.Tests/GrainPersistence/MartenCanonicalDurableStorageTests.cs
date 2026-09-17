using System.Collections.Concurrent;
using Elyfe.Orleans.Marten.Persistence.Abstractions;
using Elyfe.Orleans.Marten.Persistence.GrainPersistence;
using Elyfe.Orleans.Marten.Persistence.Options;
using Marten;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Orleans;
using Orleans.Configuration;
using Orleans.Runtime;
using Orleans.Storage;
using Testcontainers.PostgreSql;
using Xunit;

namespace Elyfe.Orleans.Marten.Persistence.Tests.GrainPersistence;

[Collection("Marten Storage Tests")]
public sealed class MartenCanonicalDurableStorageTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:17-alpine")
        .WithDatabase("canonical_storage").WithUsername("testuser").WithPassword("testpass").Build();
    private readonly ConcurrentQueue<IQuerySession> _sessions = new();
    // Any cache access fails: canonical correctness must not depend on cache availability.
    private readonly Mock<IGrainStateCache> _cache = new(MockBehavior.Strict);
    private IDocumentStore _store = null!;
    private MartenGrainStorage _storage = null!;

    public async Task InitializeAsync()
    {
        await _postgres.StartAsync();
        var logger = new Mock<IMartenLogger>();
        logger.Setup(x => x.StartSession(It.IsAny<IQuerySession>()))
            .Callback<IQuerySession>(_sessions.Enqueue).Returns(Mock.Of<IMartenSessionLogger>());
        _store = DocumentStore.For(options =>
        {
            options.Connection(_postgres.GetConnectionString());
            options.Schema.For<MartenGrainData<TestState>>()
                .DocumentAlias("canonical_states").UseOptimisticConcurrency(true);
            options.Logger(logger.Object);
        });
        await _store.Storage.ApplyAllConfiguredChangesToDatabaseAsync();
        _storage = CreateStorage();
        _sessions.Clear();
    }

    public async Task DisposeAsync()
    {
        _store.Dispose();
        await _postgres.DisposeAsync();
    }

    [Fact]
    public async Task Missing_activation_ignores_legacy_rows_and_initial_write_is_durable_without_cache()
    {
        var id = GrainId.Parse("TestState/canonical-missing");
        await SeedLegacyRows(id);
        _sessions.Clear();
        var state = new GrainState<TestState>();

        await _storage.ReadStateAsync("state", id, state);

        Assert.False(state.RecordExists);
        Assert.Null(state.ETag);
        Assert.Equal(string.Empty, state.State.Name);
        Assert.Equal(1, _sessions.Sum(session => session.RequestCount));
        _sessions.Clear();
        state.State.Name = "accepted";
        await _storage.WriteStateAsync("state", id, state);
        Assert.Equal(0, _sessions.Sum(session => session.RequestCount));

        // A new provider has no activation metadata or cache and must see the committed acceptance.
        var reactivated = new GrainState<TestState>();
        await CreateStorage().ReadStateAsync("state", id, reactivated);
        Assert.True(reactivated.RecordExists);
        Assert.Equal("accepted", reactivated.State.Name);
        Assert.Equal(state.ETag, reactivated.ETag);
        await AssertLegacyRowsRemain(id);
    }

    [Fact]
    public async Task Creation_metadata_survives_reactivation_and_repeated_updates_without_preread()
    {
        var id = GrainId.Parse("TestState/metadata");
        var createdAt = new DateTimeOffset(2020, 2, 3, 4, 5, 6, TimeSpan.Zero);
        await using (var seed = _store.LightweightSession())
        {
            var document = MartenGrainData<TestState>.Create(new TestState { Name = "migrated" }, CanonicalId(id));
            document.CreatedAt = createdAt;
            seed.Insert(document);
            await seed.SaveChangesAsync();
        }
        var state = new GrainState<TestState>();
        await _storage.ReadStateAsync("state", id, state);
        var firstEtag = state.ETag;
        state.State = new TestState { Name = "updated" };
        _sessions.Clear();
        await _storage.WriteStateAsync("state", id, state);
        Assert.Equal(0, _sessions.Sum(session => session.RequestCount));
        Assert.NotEqual(firstEtag, state.ETag);
        await _storage.WriteStateAsync("state", id, state);

        var newProvider = CreateStorage();
        var reactivated = new GrainState<TestState>();
        await newProvider.ReadStateAsync("state", id, reactivated);
        reactivated.State.Name = "reactivated";
        await newProvider.WriteStateAsync("state", id, reactivated);
        await using var query = _store.QuerySession();
        var stored = await query.LoadAsync<MartenGrainData<TestState>>(CanonicalId(id));
        Assert.Equal(createdAt, stored!.CreatedAt);
        Assert.Equal("reactivated", stored.Data.Name);
    }

    [Fact]
    public async Task Concurrent_creators_cannot_overwrite_the_winner()
    {
        var id = GrainId.Parse("TestState/create-race");
        var first = new GrainState<TestState> { State = new TestState { Name = "first" } };
        var second = new GrainState<TestState> { State = new TestState { Name = "second" } };
        var results = await Task.WhenAll(
            Record.ExceptionAsync(() => _storage.WriteStateAsync("state", id, first)),
            Record.ExceptionAsync(() => CreateStorage().WriteStateAsync("state", id, second)));
        Assert.Single(results, error => error is null);
        Assert.IsType<InconsistentStateException>(Assert.Single(results, error => error is not null));
        var winner = results[0] is null ? first : second;
        var stored = new GrainState<TestState>();
        await _storage.ReadStateAsync("state", id, stored);
        Assert.Equal(winner.State.Name, stored.State.Name);
        Assert.Equal(winner.ETag, stored.ETag);
    }

    [Fact]
    public async Task Concurrent_updates_have_exactly_one_winner_and_preserve_loser_token()
    {
        var id = GrainId.Parse("TestState/update-race");
        await _storage.WriteStateAsync("state", id, new GrainState<TestState> { State = new TestState() });
        var otherProvider = CreateStorage();
        var first = new GrainState<TestState>();
        var second = new GrainState<TestState>();
        await _storage.ReadStateAsync("state", id, first);
        await otherProvider.ReadStateAsync("state", id, second);
        var oldEtag = first.ETag;
        first.State.Name = "first";
        second.State.Name = "second";

        var results = await Task.WhenAll(
            Record.ExceptionAsync(() => _storage.WriteStateAsync("state", id, first)),
            Record.ExceptionAsync(() => otherProvider.WriteStateAsync("state", id, second)));

        Assert.Single(results, error => error is null);
        Assert.IsType<InconsistentStateException>(Assert.Single(results, error => error is not null));
        var winner = results[0] is null ? first : second;
        var loser = results[0] is null ? second : first;
        Assert.Equal(oldEtag, loser.ETag);
        var stored = new GrainState<TestState>();
        await _storage.ReadStateAsync("state", id, stored);
        Assert.Equal(winner.State.Name, stored.State.Name);
        Assert.Equal(winner.ETag, stored.ETag);
    }

    [Fact]
    public async Task Stale_clear_fails_without_deleting_newer_state()
    {
        var id = GrainId.Parse("TestState/stale-clear");
        var current = new GrainState<TestState> { State = new TestState { Name = "initial" } };
        await _storage.WriteStateAsync("state", id, current);
        var stale = new GrainState<TestState>();
        await _storage.ReadStateAsync("state", id, stale);
        current.State.Name = "newer";
        await _storage.WriteStateAsync("state", id, current);
        var staleEtag = stale.ETag;

        await Assert.ThrowsAsync<InconsistentStateException>(() => _storage.ClearStateAsync("state", id, stale));

        Assert.True(stale.RecordExists);
        Assert.Equal(staleEtag, stale.ETag);
        var stored = new GrainState<TestState>();
        await _storage.ReadStateAsync("state", id, stored);
        Assert.Equal("newer", stored.State.Name);
        Assert.Equal(current.ETag, stored.ETag);
    }

    [Fact]
    public async Task Clear_cannot_be_resurrected_by_stale_writer_or_legacy_rows()
    {
        var id = GrainId.Parse("TestState/no-resurrection");
        await SeedLegacyRows(id);
        var current = new GrainState<TestState> { State = new TestState { Name = "current" } };
        await _storage.WriteStateAsync("state", id, current);
        var stale = new GrainState<TestState>();
        await _storage.ReadStateAsync("state", id, stale);
        await _storage.ClearStateAsync("state", id, current);
        Assert.False(current.RecordExists);
        Assert.Null(current.ETag);
        Assert.Equal(string.Empty, current.State.Name);

        stale.State.Name = "resurrection";
        await Assert.ThrowsAsync<InconsistentStateException>(() => _storage.WriteStateAsync("state", id, stale));
        await Assert.ThrowsAsync<InconsistentStateException>(() => _storage.ClearStateAsync("state", id, stale));
        var reactivated = new GrainState<TestState>();
        await CreateStorage().ReadStateAsync("state", id, reactivated);
        Assert.False(reactivated.RecordExists);
        await AssertLegacyRowsRemain(id);

        // A deliberate fresh lifecycle may insert, but the old lifecycle still cannot modify it.
        current.State.Name = "new lifecycle";
        await _storage.WriteStateAsync("state", id, current);
        await Assert.ThrowsAsync<InconsistentStateException>(() => _storage.ClearStateAsync("state", id, stale));
        await _storage.ReadStateAsync("state", id, reactivated);
        Assert.Equal("new lifecycle", reactivated.State.Name);
    }

    [Fact]
    public async Task Existing_state_without_activation_metadata_must_reload_before_mutation()
    {
        var id = GrainId.Parse("TestState/missing-metadata");
        var original = new GrainState<TestState> { State = new TestState { Name = "original" } };
        await _storage.WriteStateAsync("state", id, original);
        var reconstructed = new GrainState<TestState>
        {
            State = new TestState { Name = "replacement" }, RecordExists = true, ETag = original.ETag
        };
        await Assert.ThrowsAsync<InconsistentStateException>(() => _storage.WriteStateAsync("state", id, reconstructed));
        await Assert.ThrowsAsync<InconsistentStateException>(() => _storage.ClearStateAsync("state", id, reconstructed));
        await _storage.ReadStateAsync("state", id, reconstructed);
        Assert.Equal("original", reconstructed.State.Name);
        reconstructed.State.Name = "reloaded";
        await _storage.WriteStateAsync("state", id, reconstructed);
        var read = new GrainState<TestState>();
        await _storage.ReadStateAsync("state", id, read);
        Assert.Equal("reloaded", read.State.Name);
    }

    [Fact]
    public async Task Clearing_a_missing_activation_does_not_delete_another_activations_insert()
    {
        var id = GrainId.Parse("TestState/missing-clear");
        var missing = new GrainState<TestState>();
        await _storage.ReadStateAsync("state", id, missing);
        var creator = new GrainState<TestState> { State = new TestState { Name = "created later" } };
        await _storage.WriteStateAsync("state", id, creator);
        await _storage.ClearStateAsync("state", id, missing);
        var read = new GrainState<TestState>();
        await _storage.ReadStateAsync("state", id, read);
        Assert.Equal("created later", read.State.Name);
    }

    [Fact]
    public void Opaque_state_cannot_opt_into_canonical_storage()
    {
        var options = new MartenStorageOptions();
        Assert.Throws<ArgumentException>(() => options.EnableCanonicalDurableState<OpaqueState>());
    }

    [Fact]
    public async Task Canonical_read_rejects_unmigrated_creation_metadata()
    {
        var id = GrainId.Parse("TestState/unmigrated-metadata");
        await using (var seed = _store.LightweightSession())
        {
            seed.Insert(new MartenGrainData<TestState>
            {
                Id = CanonicalId(id),
                Data = new TestState { Name = "incomplete migration" },
                LastModified = DateTimeOffset.UtcNow
            });
            await seed.SaveChangesAsync();
        }

        var state = new GrainState<TestState>();
        await Assert.ThrowsAsync<InvalidOperationException>(() => _storage.ReadStateAsync("state", id, state));
    }

    private MartenGrainStorage CreateStorage()
    {
        var options = new MartenStorageOptions
        {
            CheckConcurrency = false,
            WriteBehind = new WriteBehindOptions { EnableReadThrough = true, EnableWriteBehind = true, Threshold = 0 }
        };
        options.EnableCanonicalDurableState<TestState>();
        var services = new Mock<IServiceProvider>();
        services.Setup(x => x.GetService(typeof(IOptions<MartenStorageOptions>))).Returns(OptionsHelper.Create(options));
        services.Setup(x => x.GetService(typeof(IGrainStateCache))).Returns(_cache.Object);
        var environment = new Mock<IHostEnvironment>();
        environment.SetupGet(x => x.EnvironmentName).Returns("Development");
        return new MartenGrainStorage("test", _store, services.Object, NullLogger<MartenGrainStorage>.Instance,
            OptionsHelper.Create(new ClusterOptions { ServiceId = "test-cluster" }), environment.Object);
    }

    private async Task SeedLegacyRows(GrainId id)
    {
        await using var session = _store.LightweightSession();
        session.Insert(MartenGrainData<TestState>.Create(new TestState { Name = "raw legacy" }, id.ToString()));
        session.Insert(MartenGrainData<TestState>.Create(new TestState { Name = "lossy legacy" }, LegacyId(id)));
        await session.SaveChangesAsync();
    }

    private async Task AssertLegacyRowsRemain(GrainId id)
    {
        await using var session = _store.QuerySession();
        Assert.Equal("raw legacy", (await session.LoadAsync<MartenGrainData<TestState>>(id.ToString()))!.Data.Name);
        Assert.Equal("lossy legacy", (await session.LoadAsync<MartenGrainData<TestState>>(LegacyId(id)))!.Data.Name);
    }

    private static string CanonicalId(GrainId id) => $"test-cluster_{GrainKeyEncoding.Encode(id)}";
    private static string LegacyId(GrainId id) => $"test-cluster_{GrainKeyEncoding.LegacyEncode(id.ToString())}";
    private sealed class OpaqueState { }
}
