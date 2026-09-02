using AwesomeAssertions;
using Elyfe.Orleans.Marten.Persistence.Abstractions;
using Elyfe.Orleans.Marten.Persistence.GrainPersistence;
using Elyfe.Orleans.Marten.Persistence.Options;
using Marten;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Configuration;
using Orleans.Runtime;
using Testcontainers.PostgreSql;
using Xunit;

namespace Elyfe.Orleans.Marten.Persistence.Tests.GrainPersistence;

/// <summary>
/// Regression coverage for the write-behind drain racing a concurrent <c>ClearStateAsync</c>:
/// the per-grain lock serializes them, and the drain's re-validation drops superseded values.
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
    public async Task Drain_skips_when_the_grain_lock_is_held_by_a_clear()
    {
        // The lock is what stops the resurrection: if a clear holds (or waits on) the per-grain lock,
        // the drain must not proceed; it re-marks the grain dirty and leaves the value for next cycle.
        var cache = new ScriptedCache([ScriptedCache.Cached(), ScriptedCache.Cached(), ScriptedCache.Cached()])
        {
            LockAvailable = false
        };
        var writer = CreateWriter(cache);

        await writer.StartAsync(CancellationToken.None);
        try
        {
            await cache.WaitForLockAttemptAsync();
        }
        finally
        {
            await writer.StopAsync(CancellationToken.None);
        }

        cache.MarkDirtyCalls.Should().Be(1, "the skipped drain must be re-queued for the next cycle");
        cache.ReleaseCalls.Should().Be(0, "a lock that was never acquired must not be released");
        cache.WriteCalls.Should().Be(0, "nothing may be persisted while the lock is held");
    }

    [Fact]
    public async Task Drain_drops_a_superseded_value_when_a_newer_write_landed_mid_drain()
    {
        // A newer write during the drain wins: persisting the older copy would clobber it.
        var cache = new ScriptedCache(
            [ScriptedCache.Cached(), ScriptedCache.Cached(etag: "newer-etag"), ScriptedCache.Cached("newer-etag")]);
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

        cache.ReleaseCalls.Should().Be(1, "the acquired lock must always be released");
        cache.WriteCalls.Should().Be(0, "persisting the older copy would clobber the newer write");
    }

    [Fact]
    public async Task Drain_drops_a_value_cleared_after_the_initial_read()
    {
        // Defense in depth: even without the lock, a drain that reads the value and then finds the
        // cache empty at the re-validation read must drop the stale copy.
        var cache = new ScriptedCache([ScriptedCache.Cached(), null, null]);
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

        cache.ReleaseCalls.Should().Be(1);
        cache.WriteCalls.Should().Be(0, "the cleared state must not be re-persisted");
    }

    [Fact]
    public async Task Drain_persists_a_value_that_is_unchanged_at_the_revalidation_read()
    {
        var cache = new ScriptedCache([ScriptedCache.Cached(), ScriptedCache.Cached(), ScriptedCache.Cached()]);
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

        cache.ReleaseCalls.Should().Be(1);
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
    /// persistence and lock traffic so the tests can assert exactly what happened.
    /// </summary>
    private sealed class ScriptedCache : IGrainStateCache
    {
        private readonly Queue<CachedGrainState<object>?> _reads;
        private readonly TaskCompletionSource _secondRead = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _write = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _lockAttempt = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _dirtyKeysReturned;

        public ScriptedCache(IEnumerable<CachedGrainState<object>?> reads) => _reads = new Queue<CachedGrainState<object>?>(reads);

        public bool LockAvailable { get; set; } = true;

        public int WriteCalls { get; private set; }

        public int MarkDirtyCalls { get; private set; }

        public int ReleaseCalls { get; private set; }

        public Task WaitForSecondReadAsync() => _secondRead.Task.WaitAsync(TimeSpan.FromSeconds(10));

        public Task WaitForWriteAsync() => _write.Task.WaitAsync(TimeSpan.FromSeconds(10));

        public Task WaitForLockAttemptAsync() => _lockAttempt.Task.WaitAsync(TimeSpan.FromSeconds(10));

        private int _readCount;

        public Task<CachedGrainState<T>?> ReadAsync<T>(string storageName, GrainId grainId,
            CancellationToken cancellationToken = default)
        {
            _reads.TryDequeue(out var read);
            if (Interlocked.Increment(ref _readCount) > 1)
            {
                _secondRead.TrySetResult();
            }

            return Task.FromResult(read is CachedGrainState<T> typed ? typed : null);
        }

        public Task WriteAsync<T>(string storageName, GrainId grainId, T state, string etag, long lastModified,
            long createdAt, CancellationToken cancellationToken = default)
        {
            WriteCalls++;
            _write.TrySetResult();
            return Task.CompletedTask;
        }

        public Task RemoveAsync(string storageName, GrainId grainId, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task MarkDirtyAsync(string storageName, GrainId grainId, CancellationToken cancellationToken = default)
        {
            MarkDirtyCalls++;
            return Task.CompletedTask;
        }

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

        public Task<bool> TryAcquireGrainLockAsync(string storageName, GrainId grainId, TimeSpan ttl,
            CancellationToken cancellationToken = default)
        {
            if (!LockAvailable)
            {
                _lockAttempt.TrySetResult();
                return Task.FromResult(false);
            }

            return Task.FromResult(true);
        }

        public Task ReleaseGrainLockAsync(string storageName, GrainId grainId,
            CancellationToken cancellationToken = default)
        {
            ReleaseCalls++;
            return Task.CompletedTask;
        }

        public Task<bool> TryAcquireDrainLockAsync(string storageName, TimeSpan lockTtl,
            CancellationToken cancellationToken = default) => Task.FromResult(true);

        public Task ReleaseDrainLockAsync(string storageName, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public static CachedGrainState<object> Cached(string etag = "etag-1") => new(
            new DrainTestState { Name = "Stale", Value = 1 },
            etag,
            1_700_000_000_000,
            1_699_000_000_000,
            typeof(DrainTestState));
    }

    [GenerateSerializer]
    public sealed class DrainTestState
    {
        [Id(0)]
        public string Name { get; set; } = string.Empty;

        [Id(1)]
        public int Value { get; set; }
    }
}