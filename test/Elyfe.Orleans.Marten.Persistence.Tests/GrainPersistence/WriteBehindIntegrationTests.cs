using AwesomeAssertions;
using Elyfe.Orleans.Marten.Persistence.Abstractions;
using Elyfe.Orleans.Marten.Persistence.GrainPersistence;
using JasperFx;
using Marten;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Configuration;
using Orleans.Runtime;
using StackExchange.Redis;
using Testcontainers.PostgreSql;
using Testcontainers.Redis;
using Xunit;

namespace Elyfe.Orleans.Marten.Persistence.Tests.GrainPersistence;

public class WriteBehindIntegrationTests : IAsyncLifetime
{
    private PostgreSqlContainer? _postgresContainer;
    private RedisContainer? _redisContainer;
    private IDocumentStore? _documentStore;
    private IConnectionMultiplexer? _redis;

    public async Task InitializeAsync()
    {
        _postgresContainer = new PostgreSqlBuilder()
            .WithImage("postgres:16-alpine")
            .Build();

        _redisContainer = new RedisBuilder()
            .WithImage("redis:7-alpine")
            .Build();

        await _postgresContainer.StartAsync();
        await _redisContainer.StartAsync();

        // Setup Marten
        _documentStore = DocumentStore.For(opts =>
        {
            opts.Connection(_postgresContainer.GetConnectionString());
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.DatabaseSchemaName = "test";
        });

        // Setup Redis
        _redis = await ConnectionMultiplexer.ConnectAsync(_redisContainer.GetConnectionString());
    }

    public async Task DisposeAsync()
    {
        _redis?.Dispose();
        _documentStore?.Dispose();

        if (_postgresContainer != null)
            await _postgresContainer.DisposeAsync();

        if (_redisContainer != null)
            await _redisContainer.DisposeAsync();
    }

    [Fact]
    public async Task WriteStateAsync_WithOverflow_ShouldCacheAndMarkDirty()
    {
        if (_documentStore == null || _redis == null)
            throw new InvalidOperationException("Test containers not initialized");

        var options = Options.Create(new WriteBehindOptions
        {
            Threshold = 0, // Force overflow immediately
            EnableWriteBehind = true,
            EnableReadThrough = true
        });

        var clusterOptions = Options.Create(new ClusterOptions { ServiceId = "test-cluster" });
        var logger = new LoggerFactory().CreateLogger<MartenGrainStorage>();
        var hostEnv = new MockHostEnvironment();

        var serviceProvider = new MockServiceProvider(_documentStore, _redis, options, clusterOptions);

        var storage = new MartenGrainStorage(
            "TestStorage",
            _documentStore,
            serviceProvider,
            logger,
            clusterOptions,
            hostEnv
        );

        var grainId = GrainId.Parse("test/grain/overflow");
        var grainState = new GrainState<TestState>(new TestState { TextValue = "overflow-test" });

        // Write should go to cache due to overflow
        await storage.WriteStateAsync("TestState", grainId, grainState);

        grainState.ETag.Should().NotBeNull();
        grainState.RecordExists.Should().BeTrue();

        // Verify it's in cache
        var cache = (RedisGrainStateCache?)serviceProvider.GetService(typeof(RedisGrainStateCache));
        var cached = await cache!.ReadAsync<TestState>("TestStorage", grainId);

        cached.Should().NotBeNull();
        cached!.Data.TextValue.Should().Be("overflow-test");

        // Verify it's marked dirty
        var db = _redis.GetDatabase(1);
        var dirtySetKey = $"mgs:test-cluster:TestStorage:dirty";
        var isDirty = await db.SetContainsAsync(dirtySetKey, "test_grain_overflow");

        isDirty.Should().BeTrue();
    }

    [Fact]
    public async Task ReadStateAsync_WithCache_ShouldReturnCachedValue()
    {
        if (_documentStore == null || _redis == null)
            throw new InvalidOperationException("Test containers not initialized");

        var options = Options.Create(new WriteBehindOptions
        {
            EnableReadThrough = true,
            EnableWriteBehind = false
        });

        var clusterOptions = Options.Create(new ClusterOptions { ServiceId = "test-cluster" });
        var logger = new LoggerFactory().CreateLogger<MartenGrainStorage>();
        var cacheLogger = new LoggerFactory().CreateLogger<RedisGrainStateCache>();
        var hostEnv = new MockHostEnvironment();

        var cache = new RedisGrainStateCache(_redis, cacheLogger, options, "test-cluster");
        var serviceProvider = new MockServiceProvider(_documentStore, _redis, options, clusterOptions, cache);

        var storage = new MartenGrainStorage(
            "TestStorage",
            _documentStore,
            serviceProvider,
            logger,
            clusterOptions,
            hostEnv
        );

        var grainId = GrainId.Parse("test/grain/cached");
        var testState = new TestState { TextValue = "cached-value" };

        // Write to cache directly
        await cache.WriteAsync("TestStorage", grainId, testState, "test-etag",
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

        // Read should hit cache
        var grainState = new GrainState<TestState>();
        await storage.ReadStateAsync("TestState", grainId, grainState);

        grainState.State.Should().NotBeNull();
        grainState.State!.TextValue.Should().Be("cached-value");
        grainState.ETag.Should().Be("test-etag");
    }

    [Fact]
    public async Task Drainer_ShouldFlushDirtyStatesToMarten()
    {
        if (_documentStore == null || _redis == null)
            throw new InvalidOperationException("Test containers not initialized");

        var options = Options.Create(new WriteBehindOptions
        {
            BatchSize = 10,
            DrainIntervalSeconds = 1
        });

        var clusterOptions = Options.Create(new ClusterOptions { ServiceId = "test-cluster" });
        var logger = new LoggerFactory().CreateLogger<CacheToMartenWriter>();
        var cacheLogger = new LoggerFactory().CreateLogger<RedisGrainStateCache>();

        var cache = new RedisGrainStateCache(_redis, cacheLogger, options, "test-cluster");

        // Add a dirty grain to cache
        var grainId = GrainId.Parse("test/grain/drain");
        var testState = new TestState { TextValue = "drain-test" };
        var etag = "drain-etag";
        var lastModified = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        await cache.WriteAsync("Default", grainId, testState, etag, lastModified);
        await cache.MarkDirtyAsync("Default", grainId);

        // Create drainer
        var drainer = new CacheToMartenWriter(
            cache,
            _documentStore,
            logger,
            options,
            clusterOptions
        );
        drainer.RegisterStorage("Default");
        // Run drain manually
        var cts = new CancellationTokenSource();
        var drainTask = drainer.StartAsync(cts.Token);

        // Wait a bit for drain to happen
        await Task.Delay(TimeSpan.FromSeconds(5));

        // Stop drainer

        cts.Cancel();
        await drainer.StopAsync(CancellationToken.None);

        // Verify state was persisted to Marten
        await using var session = _documentStore.QuerySession();
        var martenId = "test-cluster_test_grain_drain";
        var document = await session.LoadAsync<MartenGrainData<TestState>>(martenId);

        document.Should().NotBeNull();
        document!.Data.TextValue.Should().Be("drain-test");

        // Verify dirty marker was cleared
        var db = _redis.GetDatabase();
        var dirtySetKey = "mgs:test-cluster:Default:dirty";
        var isDirty = await db.SetContainsAsync(dirtySetKey, "test_grain_drain");

        isDirty.Should().BeFalse();
    }

    private class MockHostEnvironment : IHostEnvironment
    {
        public string EnvironmentName { get; set; } = "Development";
        public string ApplicationName { get; set; } = "Test";
        public string ContentRootPath { get; set; } = string.Empty;
        public IFileProvider ContentRootFileProvider { get; set; } = null!;
    }

    private class MockServiceProvider : IServiceProvider
    {
        private readonly IDocumentStore _documentStore;
        private readonly IConnectionMultiplexer _redis;
        private readonly IOptions<WriteBehindOptions> _options;
        private readonly IOptions<ClusterOptions> _clusterOptions;
        private readonly RedisGrainStateCache? _cache;

        public MockServiceProvider(
            IDocumentStore documentStore,
            IConnectionMultiplexer redis,
            IOptions<WriteBehindOptions> options,
            IOptions<ClusterOptions> clusterOptions,
            RedisGrainStateCache? cache = null)
        {
            _documentStore = documentStore;
            _redis = redis;
            _options = options;
            _clusterOptions = clusterOptions;
            _cache = cache ?? new RedisGrainStateCache(
                redis,
                new LoggerFactory().CreateLogger<RedisGrainStateCache>(),
                options,
                clusterOptions.Value.ServiceId
            );
        }

        public object? GetService(Type serviceType)
        {
            if (serviceType == typeof(IDocumentStore))
                return _documentStore;
            if (serviceType == typeof(IConnectionMultiplexer))
                return _redis;
            if (serviceType == typeof(IOptions<WriteBehindOptions>))
                return _options;
            if (serviceType == typeof(IOptions<ClusterOptions>))
                return _clusterOptions;
            if (serviceType == typeof(RedisGrainStateCache))
                return _cache;
            if (serviceType == typeof(IGrainStateCache))
                return _cache;

            return null;
        }
    }
}