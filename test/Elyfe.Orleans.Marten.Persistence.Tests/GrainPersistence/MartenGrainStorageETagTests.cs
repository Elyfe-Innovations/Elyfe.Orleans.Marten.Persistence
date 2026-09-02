using AwesomeAssertions;
using Elyfe.Orleans.Marten.Persistence.Abstractions;
using Elyfe.Orleans.Marten.Persistence.GrainPersistence;
using Elyfe.Orleans.Marten.Persistence.Options;
using Marten;
using Marten.TimescaleDB;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Orleans;
using Orleans.Configuration;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Serialization.Serializers;
using Orleans.Storage;
using Testcontainers.PostgreSql;
using Xunit;

namespace Elyfe.Orleans.Marten.Persistence.Tests.GrainPersistence;

[CollectionDefinition("Marten Storage Tests", DisableParallelization = true)]
public class MartenStorageTestCollection {}

[Collection("Marten Storage Tests")]
public class MartenGrainStorageETagTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgreSqlContainer;
    private IDocumentStore? _documentStore;
    private MartenGrainStorage? _storage;

    public MartenGrainStorageETagTests()
    {
        _postgreSqlContainer = new PostgreSqlBuilder("timescale/timescaledb:latest-pg17")
            .WithDatabase("test_etag_db")
            .WithUsername("testuser")
            .WithPassword("testpass")
            .Build();
    }

    public async Task InitializeAsync()
    {
        await _postgreSqlContainer.StartAsync();
        
        _documentStore = DocumentStore.For(options =>
        {
            options.Connection(_postgreSqlContainer.GetConnectionString());
            options.Schema.For<MartenGrainData<byte[]>>()
                .DocumentAlias("opaque_grain_states");
            options.Schema.For<MartenGrainData<HypertableTestState>>()
                .DocumentAlias("hypertable_grain_states");
            options.UseTimescaleDB(timescale =>
                timescale.DocumentAsHypertable<MartenGrainData<HypertableTestState>>(x => x.CreatedAt));
        });

        var logger = new NullLogger<MartenGrainStorage>();
        var clusterOptions = OptionsHelper.Create(new ClusterOptions { ServiceId = "test-cluster" });
        var hostEnvironment = new Mock<IHostEnvironment>();
        hostEnvironment.Setup(h => h.EnvironmentName).Returns("Development");
        
        // Create mock service provider (no cache)
        var serviceProvider = new Mock<IServiceProvider>();
        serviceProvider.Setup(sp => sp.GetService(typeof(IDocumentStore))).Returns(_documentStore);
        serviceProvider.Setup(sp => sp.GetService(typeof(IOptions<MartenStorageOptions>)))
            .Returns(OptionsHelper.Create(new MartenStorageOptions { CheckConcurrency = true }));
        var serializer = new ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider()
            .GetRequiredService<Serializer>();
        serviceProvider.Setup(sp => sp.GetService(typeof(Serializer))).Returns(serializer);

        _storage = new MartenGrainStorage("test", _documentStore, serviceProvider.Object, logger, clusterOptions, hostEnvironment.Object);
    }

    public async Task DisposeAsync()
    {
        _documentStore?.Dispose();
        await _postgreSqlContainer.DisposeAsync();
    }

    [Fact]
    public async Task ReadStateAsync_NewGrain_ShouldHaveNullETagAndEmptyState()
    {
        // Arrange
        ArgumentNullException.ThrowIfNull(_storage);
        var grainId = GrainId.Parse("TestState/test-grain-1");
        var grainState = new GrainState<TestState>();

        // Act
        await _storage.ReadStateAsync("TestState", grainId, grainState);

        // Assert
        grainState.RecordExists.Should().BeFalse();
        grainState.ETag.Should().BeNull();
        grainState.State.Should().NotBeNull("because Orleans grains must observe an empty state, not null");
        grainState.State.Name.Should().BeEmpty();
    }

    [Fact]
    public async Task WriteAndReadStateAsync_ShouldGenerateETag()
    {
        // Arrange
        ArgumentNullException.ThrowIfNull(_storage);
        var grainId = GrainId.Parse("TestState/test-grain-2");
        var grainState = new GrainState<TestState>
        {
            State = new TestState { Name = "Test", Value = 42 }
        };

        // Act - Write
        await _storage.WriteStateAsync("TestState", grainId, grainState);

        // Assert - Write should set ETag
        grainState.RecordExists.Should().BeTrue();
        grainState.ETag.Should().NotBeNull();
        var originalETag = grainState.ETag;

        // Act - Read
        var readGrainState = new GrainState<TestState>();
        ArgumentNullException.ThrowIfNull(_storage);
        await _storage.ReadStateAsync("TestState", grainId, readGrainState);

        // Assert - Read should have same ETag
        readGrainState.RecordExists.Should().BeTrue();
        readGrainState.ETag.Should().Be(originalETag);
        readGrainState.State!.Name.Should().Be("Test");
        readGrainState.State.Value.Should().Be(42);
    }

    [Fact]
    public async Task WriteStateAsync_Preserves_native_Timescale_partition_identity_across_updates()
    {
        ArgumentNullException.ThrowIfNull(_storage);
        ArgumentNullException.ThrowIfNull(_documentStore);
        var grainState = new GrainState<HypertableTestState>
        {
            State = new HypertableTestState { Name = "Created", Value = 1 }
        };
        var grainId = GrainId.Parse("HypertableTestState/timescale-grain");

        await _storage.WriteStateAsync(nameof(HypertableTestState), grainId, grainState);

        DateTimeOffset createdAt;
        await using (var firstRead = _documentStore.QuerySession())
        {
            var first = await firstRead.Query<MartenGrainData<HypertableTestState>>().SingleAsync();
            createdAt = first.CreatedAt;
        }

        grainState.State = new HypertableTestState { Name = "Updated", Value = 2 };
        await _storage.WriteStateAsync(nameof(HypertableTestState), grainId, grainState);

        await using var secondRead = _documentStore.QuerySession();
        var documents = await secondRead.Query<MartenGrainData<HypertableTestState>>().ToListAsync();
        documents.Should().ContainSingle();
        documents[0].CreatedAt.Should().Be(createdAt);
        documents[0].Data.Name.Should().Be("Updated");
        documents[0].Data.Value.Should().Be(2);
    }

    [Fact]
    public async Task WriteAndReadStateAsync_NonPublicState_ShouldUseOpaquePersistence()
    {
        ArgumentNullException.ThrowIfNull(_storage);
        var grainId = GrainId.Parse("InternalTestState/internal-grain");
        var grainState = new GrainState<InternalTestState>
        {
            State = new InternalTestState { Name = "Internal", Value = 73 }
        };

        await _storage.WriteStateAsync("InternalTestState", grainId, grainState);

        var readGrainState = new GrainState<InternalTestState>
        {
            State = new InternalTestState()
        };
        await _storage.ReadStateAsync("InternalTestState", grainId, readGrainState);

        readGrainState.RecordExists.Should().BeTrue();
        readGrainState.ETag.Should().Be(grainState.ETag);
        readGrainState.State.Name.Should().Be("Internal");
        readGrainState.State.Value.Should().Be(73);
    }

    [Fact]
    public async Task WriteStateAsync_WithDifferentData_ShouldGenerateDifferentETag()
    {
        // Arrange
        ArgumentNullException.ThrowIfNull(_storage);
        var grainId = GrainId.Parse("TestState/test-grain-3");
        var grainState1 = new GrainState<TestState>
        {
            State = new TestState { Name = "Test1", Value = 1 }
        };
        var grainState2 = new GrainState<TestState>
        {
            State = new TestState { Name = "Test2", Value = 2 }
        };

        // Act
        await _storage.WriteStateAsync("TestState", grainId, grainState1);
        var etag1 = grainState1.ETag;

        await Task.Delay(10); // Ensure different timestamp
        ArgumentNullException.ThrowIfNull(_storage);
        await _storage.WriteStateAsync("TestState", grainId, grainState2);
        var etag2 = grainState2.ETag;

        // Assert
        etag1.Should().NotBe(etag2);
    }

    [Fact]
    public async Task WriteStateAsync_WithValidETag_ShouldSucceed()
    {
        // Arrange
        ArgumentNullException.ThrowIfNull(_storage);
        var grainId = GrainId.Parse("TestState/test-grain-4");
        var grainState = new GrainState<TestState>
        {
            State = new TestState { Name = "Initial", Value = 1 }
        };

        // Act - Initial write
        await _storage.WriteStateAsync("TestState", grainId, grainState);
        var originalETag = grainState.ETag;

        // Update state with correct ETag
        grainState.State.Name = "Updated";
        grainState.State.Value = 2;

        // Act - Update with valid ETag
        ArgumentNullException.ThrowIfNull(_storage);
        await _storage.WriteStateAsync("TestState", grainId, grainState);

        // Assert
        grainState.ETag.Should().NotBe(originalETag);
        grainState.RecordExists.Should().BeTrue();
    }

    [Fact]
    public async Task WriteStateAsync_WithInvalidETag_ShouldThrowInconsistentStateException()
    {
        // Arrange
        ArgumentNullException.ThrowIfNull(_storage);
        ArgumentNullException.ThrowIfNull(_documentStore);
        var grainId = GrainId.Parse("TestState/test-grain-5");
        var grainState = new GrainState<TestState>
        {
            State = new TestState { Name = "Initial", Value = 1 }
        };

        // Act - Initial write
        await _storage.WriteStateAsync("TestState", grainId, grainState);

        // Simulate concurrent update by modifying the state directly in the database
        await using var session = _documentStore.LightweightSession();
        var id = $"test-cluster_{grainId.ToString().Replace('/', '_')}";
        var document = await session.LoadAsync<MartenGrainData<TestState>>(id);
        if (document != null)
        {
            document.Data.Name = "Modified by someone else";
            session.Store(document);
            await session.SaveChangesAsync();
        }

        // Try to update with a stale ETag. The state may have been changed by another
        // writer whose ETag differs from this caller's cached value.
        grainState.State.Name = "My Update";
        grainState.ETag = "stale-etag";
        var call = async () => await _storage.WriteStateAsync("TestState", grainId, grainState);

        // Assert
        await call.Should().ThrowAsync<InconsistentStateException>()
            .WithMessage("ETag mismatch for grain *");
    }

    [Fact]
    public async Task WriteStateAsync_NewGrainWithoutETag_ShouldSucceed()
    {
        // Arrange
        ArgumentNullException.ThrowIfNull(_storage);
        var grainId = GrainId.Parse("TestState/test-grain-6");
        var grainState = new GrainState<TestState>
        {
            State = new TestState { Name = "New", Value = 42 },
            RecordExists = false,
            ETag = null
        };

        // Act
        await _storage.WriteStateAsync("TestState", grainId, grainState);

        // Assert
        grainState.RecordExists.Should().BeTrue();
        grainState.ETag.Should().NotBeNull();
    }

    [Fact]
    public async Task ClearStateAsync_ShouldRemoveState()
    {
        // Arrange
        ArgumentNullException.ThrowIfNull(_storage);
        var grainId = GrainId.Parse("TestState/test-grain-7");
        var grainState = new GrainState<TestState>
        {
            State = new TestState { Name = "ToBeDeleted", Value = 99 }
        };

        // Act - Write then clear
        await _storage.WriteStateAsync("TestState", grainId, grainState);
        await _storage.ClearStateAsync("TestState", grainId, grainState);

        // Assert - the cleared grain state is reset in place
        grainState.RecordExists.Should().BeFalse();
        grainState.ETag.Should().BeNull();
        grainState.State.Should().NotBeNull();
        grainState.State.Name.Should().BeEmpty();

        // Verify deletion
        var readGrainState = new GrainState<TestState>();
        ArgumentNullException.ThrowIfNull(_storage);
        await _storage.ReadStateAsync("TestState", grainId, readGrainState);

        // Assert
        readGrainState.RecordExists.Should().BeFalse();
        readGrainState.ETag.Should().BeNull();
        readGrainState.State.Should().NotBeNull();
    }

    [Fact]
    public async Task ReadStateAsync_StateWithoutParameterlessConstructor_StillReturnsInstance()
    {
        ArgumentNullException.ThrowIfNull(_storage);
        var grainId = GrainId.Parse("ConstructedState/test-grain-8");

        // Fallback path: no IActivatorProvider registered.
        var fallbackState = new GrainState<ConstructedTestState>();
        await _storage.ReadStateAsync("ConstructedState", grainId, fallbackState);

        // Production path: delegates to the same activator StateStorageBridge uses.
        var activatedState = new GrainState<ConstructedTestState>();
        await CreateStorage(cache: null, withActivatorProvider: true)
            .ReadStateAsync("ConstructedState", grainId, activatedState);

        fallbackState.RecordExists.Should().BeFalse();
        fallbackState.State.Should()
            .NotBeNull("because a state type without a public parameterless constructor must not degrade to null");
        activatedState.State.Should().NotBeNull();
    }

    [Fact]
    public async Task ReadStateAsync_WhenCacheIsUnavailable_StillReturnsMartenState()
    {
        ArgumentNullException.ThrowIfNull(_documentStore);
        ArgumentNullException.ThrowIfNull(_storage);

        var grainId = GrainId.Parse("TestState/test-grain-9");
        var written = new GrainState<TestState>
        {
            State = new TestState { Name = "Durable", Value = 7 }
        };
        await _storage.WriteStateAsync("TestState", grainId, written);

        var faultyCacheStorage = CreateStorage(new ThrowingGrainStateCache());
        var read = new GrainState<TestState>();

        await faultyCacheStorage.ReadStateAsync("TestState", grainId, read);

        read.RecordExists.Should().BeTrue("because Marten is authoritative and the cache is only an optimisation");
        read.State!.Name.Should().Be("Durable");
        read.State.Value.Should().Be(7);
    }

    [Fact]
    public async Task ReadStateAsync_WhenCacheFailsAndGrainHasPendingWrite_ThrowsInsteadOfStaleMarten()
    {
        ArgumentNullException.ThrowIfNull(_documentStore);
        ArgumentNullException.ThrowIfNull(_storage);

        var grainId = GrainId.Parse("TestState/test-grain-10");
        var written = new GrainState<TestState>
        {
            State = new TestState { Name = "Durable", Value = 7 }
        };
        await _storage.WriteStateAsync("TestState", grainId, written);

        // Marten holds an older value and the grain has a pending write-behind write we can no
        // longer read: serving the Marten document would let the grain clobber a newer accepted write.
        var faultyCacheStorage = CreateStorage(new DirtyThrowingGrainStateCache());
        var read = new GrainState<TestState>();

        var act = async () => await faultyCacheStorage.ReadStateAsync("TestState", grainId, read);

        await act.Should().ThrowAsync<OrleansException>();
    }

    [Fact]
    public async Task ReadStateAsync_WhenCacheFailsAndDirtyStateCannotBeDetermined_Throws()
    {
        ArgumentNullException.ThrowIfNull(_documentStore);
        ArgumentNullException.ThrowIfNull(_storage);

        var grainId = GrainId.Parse("TestState/test-grain-11");
        var written = new GrainState<TestState>
        {
            State = new TestState { Name = "Durable", Value = 7 }
        };
        await _storage.WriteStateAsync("TestState", grainId, written);

        var faultyCacheStorage = CreateStorage(new UnknownThrowingGrainStateCache());
        var read = new GrainState<TestState>();

        var act = async () => await faultyCacheStorage.ReadStateAsync("TestState", grainId, read);

        await act.Should().ThrowAsync<OrleansException>();
    }

    [Fact]
    public async Task ClearStateAsync_WhenCacheEvictionFails_AbortsBeforeDeletingMartenState()
    {
        ArgumentNullException.ThrowIfNull(_documentStore);
        ArgumentNullException.ThrowIfNull(_storage);

        var grainId = GrainId.Parse("TestState/test-grain-12");
        var written = new GrainState<TestState>
        {
            State = new TestState { Name = "Durable", Value = 7 }
        };
        await _storage.WriteStateAsync("TestState", grainId, written);

        var failingEvictCacheStorage = CreateStorage(new EvictFailingGrainStateCache());
        var clear = new GrainState<TestState> { State = new TestState() };

        var act = async () => await failingEvictCacheStorage.ClearStateAsync("TestState", grainId, clear);

        await act.Should().ThrowAsync<InvalidOperationException>();

        // The Marten document must survive: clearing it while the cache still holds the value would
        // let a read-through resurrect the "cleared" state.
        var read = new GrainState<TestState>();
        await _storage.ReadStateAsync("TestState", grainId, read);
        read.RecordExists.Should().BeTrue("because the cache eviction failed and the clear aborted");
        read.State!.Value.Should().Be(7);
    }

    [Fact]
    public async Task ClearStateAsync_WhenDrainHoldsGrainLock_AbortsWithoutTouchingCacheOrMarten()
    {
        ArgumentNullException.ThrowIfNull(_documentStore);
        ArgumentNullException.ThrowIfNull(_storage);

        var grainId = GrainId.Parse("TestState/test-grain-13");
        var written = new GrainState<TestState>
        {
            State = new TestState { Name = "Durable", Value = 7 }
        };
        await _storage.WriteStateAsync("TestState", grainId, written);

        // The write-behind drain holds the per-grain lock (as it does across read-store-writeback).
        // The clear must not evict or delete underneath it, or the drain would resurrect the state.
        var cache = new LockBusyGrainStateCache();
        var lockBusyStorage = CreateStorage(cache);
        var clear = new GrainState<TestState> { State = new TestState() };

        var act = async () => await lockBusyStorage.ClearStateAsync("TestState", grainId, clear);

        await act.Should().ThrowAsync<InvalidOperationException>();
        cache.EvictionAttempts.Should().Be(0, "the clear must not touch the cache while a drain holds the grain lock");

        var read = new GrainState<TestState>();
        await _storage.ReadStateAsync("TestState", grainId, read);
        read.RecordExists.Should().BeTrue("because the clear aborted before deleting the Marten document");
        read.State!.Value.Should().Be(7);
    }

    private MartenGrainStorage CreateStorage(IGrainStateCache? cache, bool withActivatorProvider = false)
    {
        var serviceProvider = new Mock<IServiceProvider>();
        serviceProvider.Setup(sp => sp.GetService(typeof(IDocumentStore))).Returns(_documentStore);
        serviceProvider.Setup(sp => sp.GetService(typeof(IOptions<MartenStorageOptions>)))
            .Returns(OptionsHelper.Create(new MartenStorageOptions
            {
                CheckConcurrency = true,
                WriteBehind = new WriteBehindOptions { EnableReadThrough = true }
            }));
        serviceProvider.Setup(sp => sp.GetService(typeof(IGrainStateCache))).Returns(cache);

        if (withActivatorProvider)
        {
            var orleansServices = new ServiceCollection().AddSerializer().BuildServiceProvider();
            serviceProvider.Setup(sp => sp.GetService(typeof(IActivatorProvider)))
                .Returns(orleansServices.GetRequiredService<IActivatorProvider>());
        }

        var hostEnvironment = new Mock<IHostEnvironment>();
        hostEnvironment.Setup(h => h.EnvironmentName).Returns("Development");

        return new MartenGrainStorage(
            "test",
            _documentStore!,
            serviceProvider.Object,
            new NullLogger<MartenGrainStorage>(),
            OptionsHelper.Create(new ClusterOptions { ServiceId = "test-cluster" }),
            hostEnvironment.Object);
    }
}

public sealed class ConstructedTestState(string name)
{
    public string Name { get; set; } = name;
}

/// <summary>
///     Cache that is unavailable for both reads and writes.
/// </summary>
internal class ThrowingGrainStateCache : IGrainStateCache
{
    public Task<CachedGrainState<T>?> ReadAsync<T>(string storageName, GrainId grainId,
        CancellationToken cancellationToken = default) => throw new InvalidOperationException("cache down");

    public Task WriteAsync<T>(string storageName, GrainId grainId, T state, string etag, long lastModified,
        long createdAt, CancellationToken cancellationToken = default) => throw new InvalidOperationException("cache down");

    public virtual Task RemoveAsync(string storageName, GrainId grainId, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;

    public virtual Task MarkDirtyAsync(string storageName, GrainId grainId, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;

    public virtual Task ClearDirtyAsync(string storageName, GrainId grainId, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;

    public virtual Task<bool> IsDirtyAsync(string storageName, GrainId grainId,
        CancellationToken cancellationToken = default) => Task.FromResult(false);

    public virtual Task<bool> TryAcquireGrainLockAsync(string storageName, GrainId grainId, TimeSpan ttl,
        CancellationToken cancellationToken = default) => Task.FromResult(true);

    public virtual Task ReleaseGrainLockAsync(string storageName, GrainId grainId,
        CancellationToken cancellationToken = default) => Task.CompletedTask;

    public Task<IReadOnlyList<string>> GetDirtyKeysAsync(string storageName, int batchSize,
        CancellationToken cancellationToken = default) => Task.FromResult<IReadOnlyList<string>>([]);

    public Task<long> IncrementWriteCounterAsync(string storageName, CancellationToken cancellationToken = default) =>
        Task.FromResult(0L);

    public Task<bool> TryAcquireDrainLockAsync(string storageName, TimeSpan lockTtl,
        CancellationToken cancellationToken = default) => Task.FromResult(false);

    public Task ReleaseDrainLockAsync(string storageName, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;
}

/// <summary>
///     Cache whose reads fail while the grain reports a pending write-behind write.
/// </summary>
internal sealed class DirtyThrowingGrainStateCache : ThrowingGrainStateCache
{
    public override Task<bool> IsDirtyAsync(string storageName, GrainId grainId,
        CancellationToken cancellationToken = default) => Task.FromResult(true);
}

/// <summary>
///     Cache whose reads fail and whose dirty marker cannot be checked either.
/// </summary>
internal sealed class UnknownThrowingGrainStateCache : ThrowingGrainStateCache
{
    public override Task<bool> IsDirtyAsync(string storageName, GrainId grainId,
        CancellationToken cancellationToken = default) => throw new InvalidOperationException("dirty check down");
}

/// <summary>
///     Cache whose eviction fails, as when Redis is unreachable during a clear.
/// </summary>
internal sealed class EvictFailingGrainStateCache : ThrowingGrainStateCache
{
    public override Task RemoveAsync(string storageName, GrainId grainId,
        CancellationToken cancellationToken = default) => throw new InvalidOperationException("eviction down");
}

/// <summary>
///     Cache whose per-grain lock is held by an in-flight drain, and which records every eviction
///     attempt so the test can prove the clear never touched the cache or Marten.
/// </summary>
internal sealed class LockBusyGrainStateCache : ThrowingGrainStateCache
{
    public int EvictionAttempts { get; private set; }

    public override Task<bool> TryAcquireGrainLockAsync(string storageName, GrainId grainId, TimeSpan ttl,
        CancellationToken cancellationToken = default) => Task.FromResult(false);

    public override Task RemoveAsync(string storageName, GrainId grainId,
        CancellationToken cancellationToken = default)
    {
        EvictionAttempts++;
        return Task.CompletedTask;
    }
}


public sealed class HypertableTestState
{
    public string Name { get; set; } = string.Empty;
    public int Value { get; set; }
}
[GenerateSerializer]
internal sealed class InternalTestState
{
    [Id(0)]
    public string Name { get; set; } = string.Empty;

    [Id(1)]
    public int Value { get; set; }
}
