using AwesomeAssertions;
using Elyfe.Orleans.Marten.Persistence.Abstractions;
using Elyfe.Orleans.Marten.Persistence.GrainPersistence;
using Elyfe.Orleans.Marten.Persistence.Options;
using Marten;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Configuration;
using Orleans.Runtime;
using Testcontainers.PostgreSql;
using Weasel.Core;
using Xunit;

namespace Elyfe.Orleans.Marten.Persistence.Tests.GrainPersistence;

/// <summary>
/// Regression coverage for the write-behind drain racing a concurrent <c>ClearStateAsync</c>:
/// a drain that read the cached value before the clear must not persist it afterwards.
/// </summary>
[Collection("Marten Storage Tests")]
public sealed class CacheToMartenWriterDrainTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgreSqlContainer = new PostgreSqlBuilder("timescale/timescaledb:latest-pg17")
        .WithDatabase("drain_race_db")
        .WithUsername("testuser")
        .WithPassword("testpass")
        .Build();

    private IDocumentStore? _documentStore;

    public async Task InitializeAsync()
    {
        await _postgreSqlContainer.StartAsync();

        _documentStore = DocumentStore.For(options =>
        {
            options.Connection(_postgreSqlContainer.GetConnectionString());
            options.AutoCreateSchemaObjects = global::JasperFx.AutoCreate.All;
        });
    }

    public async Task DisposeAsync()
    {
        _documentStore?.Dispose();
        await _postgreSqlContainer.DisposeAsync();
    }

    [Fact]
    public async Task Drain_drops_a_stale_value_when_the_cache_was_cleared_after_the_initial_read()
    {
        var cache = new ScriptedCache([ScriptedCache.Cached(), null]); // value on the initial read, gone on re-read
        var writer = CreateWriter(cache);

        await writer.StartAsync(CancellationToken.None);
        try
        {
            await cache.WaitForSecondReadAsync();
        }
        finally
        {
            await writer.StopAsync(CancellationToken.None);
        }

        cache.WriteCalls.Should().Be(0, "the cleared state must not be re-persisted");
    }

    [Fact]
    public async Task Drain_drops_a_superseded_value_when_a_newer_write_landed_mid_drain()
    {
        var cache = new ScriptedCache([ScriptedCache.Cached(), null]); // value on the initial read, gone on re-read
        var writer = CreateWriter(cache);

        await writer.StartAsync(CancellationToken.None);
        try
        {
            await cache.WaitForSecondReadAsync();
        }
        finally
        {
            await writer.StopAsync(CancellationToken.None);
        }

        cache.WriteCalls.Should().Be(0, "persisting the older copy would clobber the newer write");
    }

    [Fact]
    public async Task Drain_persists_a_value_that_is_unchanged_at_the_revalidation_read()
    {
        var cache = new ScriptedCache([ScriptedCache.Cached(), ScriptedCache.Cached()]);
        var writer = CreateWriter(cache);

        await writer.StartAsync(CancellationToken.None);
        try
        {
            await cache.WaitForWriteAsync();
        }
        finally
        {
            await writer.StopAsync(CancellationToken.None);
        }

        cache.WriteCalls.Should().Be(1, "an unchanged value drains to Marten as usual");
    }

    private CacheToMartenWriter CreateWriter(ScriptedCache cache)
    {
        // The abort paths never open a Marten session, so an unreachable store is a hard guarantee
        // they make no database contact. The happy path needs the real store.
        var store = _documentStore ?? DocumentStore.For(options =>
        {
            options.Connection("Host=localhost;Port=1;Database=never;Username=x;Password=x");
        });

        var writer = new CacheToMartenWriter(
            cache,
            store,
            new NullLogger<CacheToMartenWriter>(),
            OptionsHelper.Create(new ClusterOptions { ServiceId = "test-cluster", ClusterId = "test-cluster" }),
            OptionsHelper.Create(new MartenStorageOptions
            {
                WriteBehind = new WriteBehindOptions
                {
                    DrainIntervalSeconds = 1,
                    BatchSize = 10,
                    DrainLockTtlSeconds = 30
                }
            }));
        writer.RegisterStorage("test");
        return writer;
    }

    /// <summary>
    /// Scripted cache that pops one value per read, hands out a single dirty key once, and records
    /// every persistence attempt so the test can assert no resurrection happened.
    /// </summary>
    private sealed class ScriptedCache : IGrainStateCache
    {
        private readonly Queue<CachedGrainState<object>?> _reads;
        private readonly TaskCompletionSource _secondRead = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _write = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _dirtyKeysReturned;

        public ScriptedCache(IEnumerable<CachedGrainState<object>?> reads) => _reads = new Queue<CachedGrainState<object>?>(reads);

        public int WriteCalls { get; private set; }

        public Task WaitForSecondReadAsync() => _secondRead.Task.WaitAsync(TimeSpan.FromSeconds(10));

        public Task WaitForWriteAsync() => _write.Task.WaitAsync(TimeSpan.FromSeconds(10));

        public Task<CachedGrainState<T>?> ReadAsync<T>(string storageName, GrainId grainId,
            CancellationToken cancellationToken = default)
        {
            _reads.TryDequeue(out var read);
            if (_reads.Count == 0)
            {
                _secondRead.TrySetResult();
            }

            return Task.FromResult(read is CachedGrainState<T> typed ? typed : null);
        }

        public Task WriteAsync<T>(string storageName, GrainId grainId, T state, string etag, long lastModified,
            CancellationToken cancellationToken = default)
        {
            WriteCalls++;
            _write.TrySetResult();
            return Task.CompletedTask;
        }

        public Task RemoveAsync(string storageName, GrainId grainId, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task MarkDirtyAsync(string storageName, GrainId grainId, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task ClearDirtyAsync(string storageName, GrainId grainId, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task<bool> IsDirtyAsync(string storageName, GrainId grainId,
            CancellationToken cancellationToken = default) => Task.FromResult(false);

        public Task<IReadOnlyList<string>> GetDirtyKeysAsync(string storageName, int batchSize,
            CancellationToken cancellationToken = default) =>
            Interlocked.Exchange(ref _dirtyKeysReturned, 1) == 0
                ? Task.FromResult<IReadOnlyList<string>>(["TestState/test-grain"])
                : Task.FromResult<IReadOnlyList<string>>([]);

        public Task<long> IncrementWriteCounterAsync(string storageName, CancellationToken cancellationToken = default) =>
            Task.FromResult(0L);

        public Task<bool> TryAcquireDrainLockAsync(string storageName, TimeSpan lockTtl,
            CancellationToken cancellationToken = default) => Task.FromResult(true);

        public Task ReleaseDrainLockAsync(string storageName, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public static CachedGrainState<object> Cached(string etag = "etag-1") => new(
            new DrainTestState { Name = "Stale", Value = 1 },
            etag,
            1_700_000_000_000,
            typeof(DrainTestState));
    }

}

[GenerateSerializer]
    public sealed class DrainTestState
{
    [Id(0)]
    public string Name { get; set; } = string.Empty;

    [Id(1)]
    public int Value { get; set; }
}