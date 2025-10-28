using AwesomeAssertions;
using Elyfe.Orleans.Marten.Persistence.GrainPersistence;
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
public class MartenGrainStorageMigrationTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgreSqlContainer;
    private IDocumentStore? _documentStore;
    private MartenGrainStorage? _storage;

    public MartenGrainStorageMigrationTests()
    {
        _postgreSqlContainer = new PostgreSqlBuilder()
            .WithImage("timescaledb:latest-pg17")
            .WithDatabase("test_migration_db")
            .WithUsername("testuser")
            .WithPassword("testpass")
            .Build();
    }

    public async Task InitializeAsync()
    {
        await _postgreSqlContainer.StartAsync();
        
        _documentStore = DocumentStore.For(_postgreSqlContainer.GetConnectionString());

        var logger = new NullLogger<MartenGrainStorage>();
        var clusterOptions = OptionsHelper.Create(new ClusterOptions { ServiceId = "test-cluster" });
        var hostEnvironment = new Mock<IHostEnvironment>();
        var serviceProvider = new Mock<IServiceProvider>();
        hostEnvironment.Setup(h => h.EnvironmentName).Returns("Development");

        _storage = new MartenGrainStorage("test", _documentStore, serviceProvider.Object, logger, clusterOptions, hostEnvironment.Object);
    }

    public async Task DisposeAsync()
    {
        _documentStore?.Dispose();
        await _postgreSqlContainer.DisposeAsync();
    }

    [Fact]
    public async Task ReadStateAsync_WhenDataExistsWithOldId_ShouldMigrateToNewId()
    {
        // Arrange
        ArgumentNullException.ThrowIfNull(_documentStore);
        ArgumentNullException.ThrowIfNull(_storage);

        var grainId = GrainId.Parse("TestState/migration-grain-1");
        var oldId = grainId.ToString(); // The old ID format
        var newId = $"test-cluster_{grainId.ToString().Replace('/', '_')}"; // The new ID format

        // Create a grain document with the old ID directly in the database
        var originalState = new TestState { Name = "Original", Value = 42 };
        var originalDocument = MartenGrainData<TestState>.Create(originalState, oldId);
        
        await using (var session = _documentStore.LightweightSession())
        {
            session.Store(originalDocument);
            await session.SaveChangesAsync();
        }

        // Act - Read which should trigger migration
        var grainState = new GrainState<TestState>();
        await _storage.ReadStateAsync("TestState", grainId, grainState);

        // Assert - Data should be migrated
        grainState.RecordExists.Should().BeTrue();
        grainState.State.Should().NotBeNull();
        grainState.State!.Name.Should().Be("Original");
        grainState.State!.Value.Should().Be(42);
        grainState.ETag.Should().NotBeNull();

        // Verify old document is gone and new document exists
        await using (var verifySession = _documentStore.QuerySession())
        {
            var oldDocument = await verifySession.LoadAsync<MartenGrainData<TestState>>(oldId);
            var newDocument = await verifySession.LoadAsync<MartenGrainData<TestState>>(newId);

            oldDocument.Should().BeNull("because the old document should be deleted after migration");
            newDocument.Should().NotBeNull("because data should be stored with the new ID format");
            newDocument!.Data.Name.Should().Be("Original");
            newDocument!.Data.Value.Should().Be(42);
        }
    }

    [Fact]
    public async Task ReadStateAsync_AfterMigration_ShouldUseNewIdFormat()
    {
        // Arrange
        ArgumentNullException.ThrowIfNull(_documentStore);
        ArgumentNullException.ThrowIfNull(_storage);

        var grainId = GrainId.Parse("TestState/migration-grain-2");
        var oldId = grainId.ToString(); // The old ID format

        // Create a grain document with the old ID directly in the database
        var originalState = new TestState { Name = "OriginalData", Value = 100 };
        var originalDocument = MartenGrainData<TestState>.Create(originalState, oldId);
        
        await using (var session = _documentStore.LightweightSession())
        {
            session.Store(originalDocument);
            await session.SaveChangesAsync();
        }

        // Act 1 - First read to trigger migration
        var grainState1 = new GrainState<TestState>();
        await _storage.ReadStateAsync("TestState", grainId, grainState1);
        
        // Act 2 - Second read to confirm using new format
        var grainState2 = new GrainState<TestState>();
        await _storage.ReadStateAsync("TestState", grainId, grainState2);

        // Assert - Both reads should have same data and ETag
        grainState1.State!.Name.Should().Be("OriginalData");
        grainState1.State!.Value.Should().Be(100);
        grainState1.ETag.Should().Be(grainState2.ETag, "because both reads should produce the same ETag");
        grainState2.State!.Name.Should().Be("OriginalData");
    }

    [Fact]
    public async Task WriteStateAsync_AfterMigration_ShouldUpdateWithNewIdFormat()
    {
        // Arrange
        ArgumentNullException.ThrowIfNull(_documentStore);
        ArgumentNullException.ThrowIfNull(_storage);

        var grainId = GrainId.Parse("TestState/migration-grain-3");
        var oldId = grainId.ToString(); // The old ID format
        var newId = $"test-cluster_{grainId.ToString().Replace('/', '_')}"; // The new ID format

        // Create a grain document with the old ID directly in the database
        var originalState = new TestState { Name = "BeforeUpdate", Value = 200 };
        var originalDocument = MartenGrainData<TestState>.Create(originalState, oldId);
        
        await using (var session = _documentStore.LightweightSession())
        {
            session.Store(originalDocument);
            await session.SaveChangesAsync();
        }

        // Act 1 - Read to trigger migration
        var grainState = new GrainState<TestState>();
        await _storage.ReadStateAsync("TestState", grainId, grainState);
        
        // Act 2 - Update the state after migration
        grainState.State!.Name = "AfterUpdate";
        grainState.State!.Value = 300;
        await _storage.WriteStateAsync("TestState", grainId, grainState);
        
        // Assert - State should be updated with new ID format
        await using (var verifySession = _documentStore.QuerySession())
        {
            var oldDocument = await verifySession.LoadAsync<MartenGrainData<TestState>>(oldId);
            var newDocument = await verifySession.LoadAsync<MartenGrainData<TestState>>(newId);

            oldDocument.Should().BeNull("because the old document should be deleted after migration");
            newDocument.Should().NotBeNull("because data should be stored with the new ID format");
            newDocument!.Data.Name.Should().Be("AfterUpdate", "because the update should have changed the name");
            newDocument!.Data.Value.Should().Be(300, "because the update should have changed the value");
        }
    }

    [Fact]
    public async Task ClearStateAsync_AfterMigration_ShouldDeleteWithNewIdFormat()
    {
        // Arrange
        ArgumentNullException.ThrowIfNull(_documentStore);
        ArgumentNullException.ThrowIfNull(_storage);

        var grainId = GrainId.Parse("TestState/migration-grain-4");
        var oldId = grainId.ToString(); // The old ID format
        var newId = $"test-cluster_{grainId.ToString().Replace('/', '_')}"; // The new ID format

        // Create a grain document with the old ID directly in the database
        var originalState = new TestState { Name = "ToBeDeleted", Value = 999 };
        var originalDocument = MartenGrainData<TestState>.Create(originalState, oldId);
        
        await using (var session = _documentStore.LightweightSession())
        {
            session.Store(originalDocument);
            await session.SaveChangesAsync();
        }

        // Act 1 - Read to trigger migration
        var grainState = new GrainState<TestState>();
        await _storage.ReadStateAsync("TestState", grainId, grainState);
        
        // Act 2 - Clear the state after migration
        await _storage.ClearStateAsync("TestState", grainId, grainState);
        
        // Assert - Document should be cleared with new ID format
        await using (var verifySession = _documentStore.QuerySession())
        {
            var oldDocument = await verifySession.LoadAsync<MartenGrainData<TestState>>(oldId);
            var newDocument = await verifySession.LoadAsync<MartenGrainData<TestState>>(newId);

            oldDocument.Should().BeNull("because the old document should be deleted after migration");
            newDocument.Should().BeNull("because the new document should be deleted after ClearState");
        }

        // Also verify through the storage API
        var verifyState = new GrainState<TestState>();
        await _storage.ReadStateAsync("TestState", grainId, verifyState);
        verifyState.RecordExists.Should().BeFalse("because the state should have been cleared");
        verifyState.State.Should().BeNull();
    }

    [Fact]
    public async Task MigratedState_ShouldMaintainETagConsistency()
    {
        // Arrange
        ArgumentNullException.ThrowIfNull(_documentStore);
        ArgumentNullException.ThrowIfNull(_storage);

        var grainId = GrainId.Parse("TestState/migration-grain-5");
        var oldId = grainId.ToString(); // The old ID format

        // Create a grain document with the old ID directly in the database
        var originalState = new TestState { Name = "ETagTest", Value = 777 };
        var originalDocument = MartenGrainData<TestState>.Create(originalState, oldId);
        
        await using (var session = _documentStore.LightweightSession())
        {
            session.Store(originalDocument);
            await session.SaveChangesAsync();
        }

        // Act 1 - Read to trigger migration
        var grainState1 = new GrainState<TestState>();
        await _storage.ReadStateAsync("TestState", grainId, grainState1);
        var etagAfterMigration = grainState1.ETag;
        
        // Act 2 - Update with valid ETag
        grainState1.State!.Value = 888;
        await _storage.WriteStateAsync("TestState", grainId, grainState1);
        var etagAfterUpdate = grainState1.ETag;
        
        // Assert - ETags should change appropriately
        etagAfterMigration.Should().NotBeNull("because migration should generate an ETag");
        etagAfterUpdate.Should().NotBeNull("because update should generate a new ETag");
        etagAfterUpdate.Should().NotBe(etagAfterMigration, "because updating the state should change the ETag");
        
        /*
        // Act 3 - Create a new state with incorrect ETag
        var grainStateWithInvalidETag = new GrainState<TestState>
        {
            State = new TestState { Name = "BadETag", Value = 999 },
            RecordExists = true,
            ETag = "Invalid-ETag-Value"
        };
        
        // Assert - Should throw InconsistentStateException due to ETag mismatch
        var action = async () => await _storage.WriteStateAsync("TestState", grainId, grainStateWithInvalidETag);
        await action.Should().ThrowAsync<InconsistentStateException>()
            .WithMessage("*ETag mismatch*");*/
    }
}

