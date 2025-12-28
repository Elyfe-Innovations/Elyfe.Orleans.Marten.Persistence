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

/// <summary>
/// Integration tests for write-behind functionality with Marten multi-tenancy.
/// </summary>
public class WriteBehindWithMultiTenancyTests : IAsyncLifetime
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

        // Setup Marten with multi-tenancy
        _documentStore = DocumentStore.For(opts =>
        {
            opts.Connection(_postgresContainer.GetConnectionString());
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.Policies.AllDocumentsAreMultiTenanted();
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
    public async Task Drainer_WithMultiTenancy_ShouldPersistToCorrectTenant()
    {
        if (_documentStore == null || _redis == null)
            throw new InvalidOperationException("Test containers not initialized");

        // Arrange
        var martenOptions = OptionsHelper.Create(new MartenStorageOptions
        {
            UseTenantPerStorage = true,
            WriteBehind = new WriteBehindOptions
            {
                BatchSize = 10,
                DrainIntervalSeconds = 1,
                Threshold = 0 // Force overflow
            }
        });
        var clusterOptions = OptionsHelper.Create(new ClusterOptions { ServiceId = "test-cluster" });
        var logger = new LoggerFactory().CreateLogger<CacheToMartenWriter>();
        var cacheLogger = new LoggerFactory().CreateLogger<RedisGrainStateCache>();

        var cache = new RedisGrainStateCache(_redis, cacheLogger, martenOptions, "test-cluster");

        // Create drainer
        var drainer = new CacheToMartenWriter(
            cache,
            _documentStore,
            logger,
            clusterOptions,
            martenOptions
        );
        drainer.RegisterStorage("sms");
        drainer.RegisterStorage("events");

        // Add dirty grains to cache for both storage names
        var smsGrainId = GrainId.Parse("message/101");
        var eventsGrainId = GrainId.Parse("ticket/202");

        var smsState = new TestState { TextValue = "SMS Message", Value = 101 };
        var eventsState = new TestState { TextValue = "Event Ticket", Value = 202 };

        var smsEtag = "sms-etag-123";
        var eventsEtag = "events-etag-456";
        var lastModified = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        await cache.WriteAsync("sms", smsGrainId, smsState, smsEtag, lastModified);
        await cache.MarkDirtyAsync("sms", smsGrainId);

        await cache.WriteAsync("events", eventsGrainId, eventsState, eventsEtag, lastModified);
        await cache.MarkDirtyAsync("events", eventsGrainId);

        // Act - Run drainer
        var cts = new CancellationTokenSource();
        var drainTask = drainer.StartAsync(cts.Token);

        // Wait for drain to complete
        await Task.Delay(TimeSpan.FromSeconds(5));

        cts.Cancel();
        await drainer.StopAsync(CancellationToken.None);

        // Assert - Verify SMS data is in SMS tenant
        await using var smsSession = _documentStore.QuerySession("sms");
        var smsDocument = await smsSession.LoadAsync<MartenGrainData<TestState>>("test-cluster_message_101");

        smsDocument.Should().NotBeNull();
        smsDocument!.Data.TextValue.Should().Be("SMS Message");
        smsDocument.Data.Value.Should().Be(101);

        // Assert - Verify Events data is in Events tenant
        await using var eventsSession = _documentStore.QuerySession("events");
        var eventsDocument = await eventsSession.LoadAsync<MartenGrainData<TestState>>("test-cluster_ticket_202");

        eventsDocument.Should().NotBeNull();
        eventsDocument!.Data.TextValue.Should().Be("Event Ticket");
        eventsDocument.Data.Value.Should().Be(202);

        // Assert - Verify data isolation (SMS tenant shouldn't have events data)
        var crossCheck = await smsSession.LoadAsync<MartenGrainData<TestState>>("test-cluster_ticket_202");
        crossCheck.Should().BeNull();
    }

    [Fact]
    public async Task WriteStateAsync_WithOverflowAndMultiTenancy_ShouldCacheAndDrainToCorrectTenant()
    {
        if (_documentStore == null || _redis == null)
            throw new InvalidOperationException("Test containers not initialized");

        // Arrange
        var martenOptions = OptionsHelper.Create(new MartenStorageOptions
        {
            UseTenantPerStorage = true,
            WriteBehind = new WriteBehindOptions
            {
                Threshold = 0, // Force overflow
                EnableWriteBehind = true,
                EnableReadThrough = true,
                BatchSize = 10,
                DrainIntervalSeconds = 1
            }
        });
        var clusterOptions = OptionsHelper.Create(new ClusterOptions { ServiceId = "test-cluster" });
        var hostEnv = new MockHostEnvironment();
        var storageLogger = new LoggerFactory().CreateLogger<MartenGrainStorage>();
        var drainerLogger = new LoggerFactory().CreateLogger<CacheToMartenWriter>();
        var cacheLogger = new LoggerFactory().CreateLogger<RedisGrainStateCache>();

        var cache = new RedisGrainStateCache(_redis, cacheLogger,martenOptions, "test-cluster");
        var serviceProvider =
            new MockServiceProvider(_documentStore, _redis, clusterOptions, martenOptions, cache);

        // Create storage instances
        var financeStorage = new MartenGrainStorage(
            "finance",
            _documentStore,
            serviceProvider,
            storageLogger,
            clusterOptions,
            hostEnv
        );

        var ussdStorage = new MartenGrainStorage(
            "ussd",
            _documentStore,
            serviceProvider,
            storageLogger,
            clusterOptions,
            hostEnv
        );

        // Create and start drainer
        var drainer =
            new CacheToMartenWriter(cache, _documentStore, drainerLogger, clusterOptions, martenOptions);
        drainer.RegisterStorage("finance");
        drainer.RegisterStorage("ussd");

        var cts = new CancellationTokenSource();
        var drainTask = drainer.StartAsync(cts.Token);

        // Act - Write to both storages (should overflow to cache)
        var financeGrainId = GrainId.Parse("payment/301");
        var financeState = new GrainState<TestState>(new TestState { TextValue = "Payment Record", Value = 301 });
        await financeStorage.WriteStateAsync("TestState", financeGrainId, financeState);

        var ussdGrainId = GrainId.Parse("session/402");
        var ussdState = new GrainState<TestState>(new TestState { TextValue = "USSD Session", Value = 402 });
        await ussdStorage.WriteStateAsync("TestState", ussdGrainId, ussdState);

        // Wait for drain
        await Task.Delay(TimeSpan.FromSeconds(5));

        cts.Cancel();
        await drainer.StopAsync(CancellationToken.None);

        // Assert - Verify data is persisted to correct tenants
        await using var financeSession = _documentStore.QuerySession("finance");
        var financeDocument = await financeSession.LoadAsync<MartenGrainData<TestState>>("test-cluster_payment_301");
        financeDocument.Should().NotBeNull();
        financeDocument!.Data.TextValue.Should().Be("Payment Record");

        await using var ussdSession = _documentStore.QuerySession("ussd");
        var ussdDocument = await ussdSession.LoadAsync<MartenGrainData<TestState>>("test-cluster_session_402");
        ussdDocument.Should().NotBeNull();
        ussdDocument!.Data.TextValue.Should().Be("USSD Session");
    }

    [Fact]
    public async Task ReadStateAsync_AfterDrain_WithMultiTenancy_ShouldReadFromCorrectTenant()
    {
        if (_documentStore == null || _redis == null)
            throw new InvalidOperationException("Test containers not initialized");

        // Arrange
        var options = OptionsHelper.Create(new WriteBehindOptions
        {
            Threshold = 0,
            EnableWriteBehind = true,
            EnableReadThrough = true,
            BatchSize = 10,
            DrainIntervalSeconds = 1
        });

        var martenOptions = OptionsHelper.Create(new MartenStorageOptions { UseTenantPerStorage = true });
        var clusterOptions = OptionsHelper.Create(new ClusterOptions { ServiceId = "test-cluster" });
        var hostEnv = new MockHostEnvironment();
        var storageLogger = new LoggerFactory().CreateLogger<MartenGrainStorage>();
        var drainerLogger = new LoggerFactory().CreateLogger<CacheToMartenWriter>();
        var cacheLogger = new LoggerFactory().CreateLogger<RedisGrainStateCache>();

        var cache = new RedisGrainStateCache(_redis, cacheLogger, martenOptions, "test-cluster");
        var serviceProvider =
            new MockServiceProvider(_documentStore, _redis, clusterOptions, martenOptions, cache);

        var storage = new MartenGrainStorage("sms", _documentStore, serviceProvider, storageLogger, clusterOptions,
            hostEnv);

        var drainer =
            new CacheToMartenWriter(cache, _documentStore, drainerLogger, clusterOptions, martenOptions);
        drainer.RegisterStorage("sms");

        var cts = new CancellationTokenSource();
        var drainTask = drainer.StartAsync(cts.Token);

        // Act - Write (will overflow to cache)
        var grainId = GrainId.Parse("broadcast/555");
        var writeState = new GrainState<TestState>(new TestState { TextValue = "Broadcast Message", Value = 555 });
        await storage.WriteStateAsync("TestState", grainId, writeState);

        // Wait for drain
        await Task.Delay(TimeSpan.FromSeconds(5));

        // Clear cache to force read from Marten
        await cache.RemoveAsync("sms", grainId);

        // Read state
        var readState = new GrainState<TestState>();
        await storage.ReadStateAsync("TestState", grainId, readState);

        cts.Cancel();
        await drainer.StopAsync(CancellationToken.None);

        // Assert
        readState.RecordExists.Should().BeTrue();
        readState.State!.TextValue.Should().Be("Broadcast Message");
        readState.State.Value.Should().Be(555);
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
        private readonly IOptions<ClusterOptions> _clusterOptions;
        private readonly IOptions<MartenStorageOptions> _martenOptions;
        private readonly RedisGrainStateCache _cache;

        public MockServiceProvider(
            IDocumentStore documentStore,
            IConnectionMultiplexer redis,
            IOptions<ClusterOptions> clusterOptions,
            IOptions<MartenStorageOptions> martenOptions,
            RedisGrainStateCache cache)
        {
            _documentStore = documentStore;
            _redis = redis;
            _clusterOptions = clusterOptions;
            _martenOptions = martenOptions;
            _cache = cache;
        }

        public object? GetService(Type serviceType)
        {
            if (serviceType == typeof(IDocumentStore))
                return _documentStore;
            if (serviceType == typeof(IConnectionMultiplexer))
                return _redis;
            if (serviceType == typeof(IOptions<ClusterOptions>))
                return _clusterOptions;
            if (serviceType == typeof(IOptions<MartenStorageOptions>))
                return _martenOptions;
            if (serviceType == typeof(RedisGrainStateCache))
                return _cache;
            if (serviceType == typeof(IGrainStateCache))
                return _cache;
            if (serviceType == typeof(CacheToMartenWriter))
            {
                var logger = new LoggerFactory().CreateLogger<CacheToMartenWriter>();
                var drainer = new CacheToMartenWriter(_cache, _documentStore, logger, _clusterOptions,
                    _martenOptions);
                return drainer;
            }

            return null;
        }
    }
}