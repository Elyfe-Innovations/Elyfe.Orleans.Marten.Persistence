using AwesomeAssertions;
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
using Testcontainers.PostgreSql;
using Xunit;

namespace Elyfe.Orleans.Marten.Persistence.Tests.GrainPersistence;

/// <summary>
/// Tests for Marten multi-tenancy feature where each storage name uses a different Marten tenant.
/// </summary>
public class MartenMultiTenancyTests : IAsyncLifetime
{
    private PostgreSqlContainer? _postgresContainer;
    private IDocumentStore? _documentStore;

    public async Task InitializeAsync()
    {
        _postgresContainer = new PostgreSqlBuilder()
            .WithImage("postgres:16-alpine")
            .Build();

        await _postgresContainer.StartAsync();

        // Setup Marten with multi-tenancy enabled
        _documentStore = DocumentStore.For(opts =>
        {
            opts.Connection(_postgresContainer.GetConnectionString());
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            
            // Enable multi-tenancy for all documents
            opts.Policies.AllDocumentsAreMultiTenanted();
        });
    }

    public async Task DisposeAsync()
    {
        _documentStore?.Dispose();

        if (_postgresContainer != null)
            await _postgresContainer.DisposeAsync();
    }

    [Fact]
    public async Task WriteStateAsync_WithUseTenantPerStorage_ShouldIsolateDataByStorageName()
    {
        if (_documentStore == null)
            throw new InvalidOperationException("Test containers not initialized");

        // Arrange
        var clusterOptions = OptionsHelper.Create(new ClusterOptions { ServiceId = "test-cluster" });
        var martenOptions = OptionsHelper.Create(new MartenStorageOptions { UseTenantPerStorage = true });
        var logger = new LoggerFactory().CreateLogger<MartenGrainStorage>();
        var hostEnv = new MockHostEnvironment();

        var serviceProvider = new MockServiceProvider(_documentStore, clusterOptions, martenOptions);

        // Create two storage instances with different names
        var smsStorage = new MartenGrainStorage(
            "sms",
            _documentStore,
            serviceProvider,
            logger,
            clusterOptions,
            hostEnv
        );

        var eventsStorage = new MartenGrainStorage(
            "events",
            _documentStore,
            serviceProvider,
            logger,
            clusterOptions,
            hostEnv
        );

        // Act - Write to SMS storage
        var smsGrainId = GrainId.Parse("user/123");
        var smsState = new GrainState<TestState>(new TestState { TextValue = "SMS Data", Value = 100 });
        await smsStorage.WriteStateAsync("TestState", smsGrainId, smsState);

        // Act - Write to Events storage with same grain ID
        var eventsGrainId = GrainId.Parse("user/123");
        var eventsState = new GrainState<TestState>(new TestState { TextValue = "Events Data", Value = 200 });
        await eventsStorage.WriteStateAsync("TestState", eventsGrainId, eventsState);

        // Assert - Verify data is isolated in different tenants
        await using var smsSession = _documentStore.QuerySession("sms");
        var smsId = "test-cluster_user_123";
        var smsDocument = await smsSession.LoadAsync<MartenGrainData<TestState>>(smsId);

        smsDocument.Should().NotBeNull();
        smsDocument!.Data.TextValue.Should().Be("SMS Data");
        smsDocument.Data.Value.Should().Be(100);

        await using var eventsSession = _documentStore.QuerySession("events");
        var eventsId = "test-cluster_user_123";
        var eventsDocument = await eventsSession.LoadAsync<MartenGrainData<TestState>>(eventsId);

        eventsDocument.Should().NotBeNull();
        eventsDocument!.Data.TextValue.Should().Be("Events Data");
        eventsDocument.Data.Value.Should().Be(200);

        // Assert - Verify cross-tenant queries don't leak data
        var smsEventsQuery = await smsSession.LoadAsync<MartenGrainData<TestState>>(eventsId);
        smsEventsQuery.Should().NotBeNull(); // Same ID exists in both tenants
        smsEventsQuery!.Data.TextValue.Should().Be("SMS Data"); // But it's the SMS data, not events
    }

    [Fact]
    public async Task ReadStateAsync_WithUseTenantPerStorage_ShouldReadFromCorrectTenant()
    {
        if (_documentStore == null)
            throw new InvalidOperationException("Test containers not initialized");

        // Arrange
        var clusterOptions = OptionsHelper.Create(new ClusterOptions { ServiceId = "test-cluster" });
        var martenOptions = OptionsHelper.Create(new MartenStorageOptions { UseTenantPerStorage = true });
        var logger = new LoggerFactory().CreateLogger<MartenGrainStorage>();
        var hostEnv = new MockHostEnvironment();

        var serviceProvider = new MockServiceProvider(_documentStore, clusterOptions, martenOptions);

        // Pre-populate data directly in different tenants
        var grainId = "test-cluster_order_456";
        
        await using (var smsSession = _documentStore.LightweightSession("sms"))
        {
            var smsData = MartenGrainData<TestState>.Create(
                new TestState { TextValue = "SMS Order", Value = 456 },
                grainId
            );
            smsSession.Store(smsData);
            await smsSession.SaveChangesAsync();
        }

        await using (var financeSession = _documentStore.LightweightSession("finance"))
        {
            var financeData = MartenGrainData<TestState>.Create(
                new TestState { TextValue = "Finance Order", Value = 789 },
                grainId
            );
            financeSession.Store(financeData);
            await financeSession.SaveChangesAsync();
        }

        // Act - Read from SMS storage
        var smsStorage = new MartenGrainStorage(
            "sms",
            _documentStore,
            serviceProvider,
            logger,
            clusterOptions,
            hostEnv
        );

        var smsGrainState = new GrainState<TestState>();
        await smsStorage.ReadStateAsync("TestState", GrainId.Parse("order/456"), smsGrainState);

        // Act - Read from Finance storage
        var financeStorage = new MartenGrainStorage(
            "finance",
            _documentStore,
            serviceProvider,
            logger,
            clusterOptions,
            hostEnv
        );

        var financeGrainState = new GrainState<TestState>();
        await financeStorage.ReadStateAsync("TestState", GrainId.Parse("order/456"), financeGrainState);

        // Assert
        smsGrainState.RecordExists.Should().BeTrue();
        smsGrainState.State!.TextValue.Should().Be("SMS Order");
        smsGrainState.State.Value.Should().Be(456);

        financeGrainState.RecordExists.Should().BeTrue();
        financeGrainState.State!.TextValue.Should().Be("Finance Order");
        financeGrainState.State.Value.Should().Be(789);
    }

    [Fact]
    public async Task ClearStateAsync_WithUseTenantPerStorage_ShouldClearOnlyFromCorrectTenant()
    {
        if (_documentStore == null)
            throw new InvalidOperationException("Test containers not initialized");

        // Arrange
        var clusterOptions = OptionsHelper.Create(new ClusterOptions { ServiceId = "test-cluster" });
        var martenOptions = OptionsHelper.Create(new MartenStorageOptions { UseTenantPerStorage = true });
        var logger = new LoggerFactory().CreateLogger<MartenGrainStorage>();
        var hostEnv = new MockHostEnvironment();

        var serviceProvider = new MockServiceProvider(_documentStore, clusterOptions, martenOptions);

        var smsStorage = new MartenGrainStorage("sms", _documentStore, serviceProvider, logger, clusterOptions, hostEnv);
        var eventsStorage = new MartenGrainStorage("events", _documentStore, serviceProvider, logger, clusterOptions, hostEnv);

        // Write to both storages
        var grainId = GrainId.Parse("temp/999");
        var state = new GrainState<TestState>(new TestState { TextValue = "Temporary", Value = 999 });

        await smsStorage.WriteStateAsync("TestState", grainId, state);
        await eventsStorage.WriteStateAsync("TestState", grainId, state);

        // Act - Clear from SMS storage only
        await smsStorage.ClearStateAsync("TestState", grainId, state);

        // Assert - SMS tenant should be cleared
        await using var smsSession = _documentStore.QuerySession("sms");
        var smsDocument = await smsSession.LoadAsync<MartenGrainData<TestState>>("test-cluster_temp_999");
        smsDocument.Should().BeNull();

        // Assert - Events tenant should still have data
        await using var eventsSession = _documentStore.QuerySession("events");
        var eventsDocument = await eventsSession.LoadAsync<MartenGrainData<TestState>>("test-cluster_temp_999");
        eventsDocument.Should().NotBeNull();
        eventsDocument!.Data.TextValue.Should().Be("Temporary");
    }

    [Fact]
    public async Task WriteStateAsync_WithoutUseTenantPerStorage_ShouldUseDefaultTenant()
    {
        if (_documentStore == null)
            throw new InvalidOperationException("Test containers not initialized");

        // Arrange - UseTenantPerStorage = false (default)
        var clusterOptions = OptionsHelper.Create(new ClusterOptions { ServiceId = "test-cluster" });
        var martenOptions = OptionsHelper.Create(new MartenStorageOptions { UseTenantPerStorage = false });
        var logger = new LoggerFactory().CreateLogger<MartenGrainStorage>();
        var hostEnv = new MockHostEnvironment();

        var serviceProvider = new MockServiceProvider(_documentStore, clusterOptions, martenOptions);

        var storage = new MartenGrainStorage(
            "default",
            _documentStore,
            serviceProvider,
            logger,
            clusterOptions,
            hostEnv
        );

        // Act
        var grainId = GrainId.Parse("product/777");
        var grainState = new GrainState<TestState>(new TestState { TextValue = "Product Data", Value = 777 });
        await storage.WriteStateAsync("TestState", grainId, grainState);

        // Assert - Should be stored in default tenant (no tenant specified)
        await using var session = _documentStore.QuerySession();
        var document = await session.LoadAsync<MartenGrainData<TestState>>("test-cluster_product_777");
        
        document.Should().NotBeNull();
        document!.Data.TextValue.Should().Be("Product Data");
    }

    [Fact]
    public async Task MultipleStorages_WithDifferentTenants_ShouldMaintainIndependentSchemas()
    {
        if (_documentStore == null)
            throw new InvalidOperationException("Test containers not initialized");

        // Arrange
        var clusterOptions = OptionsHelper.Create(new ClusterOptions { ServiceId = "test-cluster" });
        var martenOptions = OptionsHelper.Create(new MartenStorageOptions { UseTenantPerStorage = true });
        var logger = new LoggerFactory().CreateLogger<MartenGrainStorage>();
        var hostEnv = new MockHostEnvironment();

        var serviceProvider = new MockServiceProvider(_documentStore, clusterOptions, martenOptions);

        var storageNames = new[] { "sms", "events", "finance", "ussd" };
        var storages = storageNames.Select(name => 
            new MartenGrainStorage(name, _documentStore, serviceProvider, logger, clusterOptions, hostEnv)
        ).ToList();

        // Act - Write unique data to each storage
        for (int i = 0; i < storageNames.Length; i++)
        {
            var grainId = GrainId.Parse($"item/{i}");
            var state = new GrainState<TestState>(new TestState 
            { 
                TextValue = $"{storageNames[i]} Data", 
                Value = i * 100 
            });
            await storages[i].WriteStateAsync("TestState", grainId, state);
        }

        // Assert - Each tenant should have only its own data
        for (int i = 0; i < storageNames.Length; i++)
        {
            await using var session = _documentStore.QuerySession(storageNames[i]);
            var document = await session.LoadAsync<MartenGrainData<TestState>>($"test-cluster_item_{i}");
            
            document.Should().NotBeNull();
            document!.Data.TextValue.Should().Be($"{storageNames[i]} Data");
            document.Data.Value.Should().Be(i * 100);
        }
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
        private readonly IOptions<ClusterOptions> _clusterOptions;
        private readonly IOptions<MartenStorageOptions> _martenOptions;

        public MockServiceProvider(
            IDocumentStore documentStore,
            IOptions<ClusterOptions> clusterOptions,
            IOptions<MartenStorageOptions> martenOptions)
        {
            _documentStore = documentStore;
            _clusterOptions = clusterOptions;
            _martenOptions = martenOptions;
        }

        public object? GetService(Type serviceType)
        {
            if (serviceType == typeof(IDocumentStore))
                return _documentStore;
            if (serviceType == typeof(IOptions<ClusterOptions>))
                return _clusterOptions;
            if (serviceType == typeof(IOptions<MartenStorageOptions>))
                return _martenOptions;
            if (serviceType == typeof(IOptions<WriteBehindOptions>))
                return OptionsHelper.Create(new WriteBehindOptions());

            return null;
        }
    }
}
