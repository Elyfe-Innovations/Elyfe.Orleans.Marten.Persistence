using System;
using System.Threading.Tasks;
using Elyfe.Orleans.Marten.Persistence.GrainPersistence;
using Marten;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Orleans;
using Orleans.Configuration;
using Orleans.Runtime;
using Orleans.Storage;
using Testcontainers.PostgreSql;
using Xunit;

namespace Elyfe.Orleans.Marten.Persistence.Tests.GrainPersistence;

public class MartenGrainStorageETagTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgresContainer;
    private IDocumentStore _documentStore = null!;
    private MartenGrainStorage _storage = null!;
    private readonly Mock<ILogger<MartenGrainStorage>> _loggerMock;

    public MartenGrainStorageETagTests()
    {
        _postgresContainer = new PostgreSqlBuilder()
            .WithImage("postgres:15")
            .WithDatabase("test_etag_db")
            .WithUsername("testuser")
            .WithPassword("testpass")
            .Build();

        _loggerMock = new Mock<ILogger<MartenGrainStorage>>();
    }

    public async Task InitializeAsync()
    {
        await _postgresContainer.StartAsync();

        _documentStore = DocumentStore.For(opts =>
        {
            opts.Connection(_postgresContainer.GetConnectionString());
            opts.AutoCreateSchemaObjects = Weasel.Core.AutoCreate.All;
        });

        var clusterOptions = Options.Create(new ClusterOptions { ServiceId = "test-cluster" });
        var environment = Mock.Of<IHostEnvironment>(env => env.EnvironmentName == "Development");

        _storage = new MartenGrainStorage(
            "test-storage",
            _documentStore,
            _loggerMock.Object,
            clusterOptions,
            environment);
    }

    public async Task DisposeAsync()
    {
        _documentStore?.Dispose();
        await _postgresContainer.DisposeAsync();
    }

    [Fact]
    public async Task ReadStateAsync_NewGrain_ShouldHaveNullETag()
    {
        // Arrange
        var grainId = GrainId.Parse("test-grain-1");
        var grainState = new GrainState<TestState>();

        // Act
        await _storage.ReadStateAsync("test", grainId, grainState);

        // Assert
        Assert.False(grainState.RecordExists);
        Assert.Null(grainState.ETag);
        Assert.Null(grainState.State);
    }

    [Fact]
    public async Task WriteAndReadStateAsync_ShouldGenerateETag()
    {
        // Arrange
        var grainId = GrainId.Parse("test-grain-2");
        var grainState = new GrainState<TestState>
        {
            State = new TestState { Name = "Test", Value = 42 }
        };

        // Act - Write
        await _storage.WriteStateAsync("TestState", grainId, grainState);

        // Assert - Write should set ETag
        Assert.True(grainState.RecordExists);
        Assert.NotNull(grainState.ETag);
        var originalETag = grainState.ETag;

        // Act - Read
        var readGrainState = new GrainState<TestState>();
        await _storage.ReadStateAsync("test", grainId, readGrainState);

        // Assert - Read should have same ETag
        Assert.True(readGrainState.RecordExists);
        Assert.Equal(originalETag, readGrainState.ETag);
        Assert.Equal("Test", readGrainState.State!.Name);
        Assert.Equal(42, readGrainState.State.Value);
    }

    [Fact]
    public async Task WriteStateAsync_WithDifferentData_ShouldGenerateDifferentETag()
    {
        // Arrange
        var grainId = GrainId.Parse("test-grain-3");
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
        await _storage.WriteStateAsync("TestState", grainId, grainState2);
        var etag2 = grainState2.ETag;

        // Assert
        Assert.NotEqual(etag1, etag2);
    }

    [Fact]
    public async Task WriteStateAsync_WithValidETag_ShouldSucceed()
    {
        // Arrange
        var grainId = GrainId.Parse("test-grain-4");
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
        await _storage.WriteStateAsync("TestState", grainId, grainState);

        // Assert
        Assert.NotEqual(originalETag, grainState.ETag);
        Assert.True(grainState.RecordExists);
    }

    [Fact]
    public async Task WriteStateAsync_WithInvalidETag_ShouldThrowInconsistentStateException()
    {
        // Arrange
        var grainId = GrainId.Parse("test-grain-5");
        var grainState = new GrainState<TestState>
        {
            State = new TestState { Name = "Initial", Value = 1 }
        };

        // Act - Initial write
        await _storage.WriteStateAsync("TestState", grainId, grainState);

        // Simulate concurrent update by modifying the state directly in the database
        await using var session = _documentStore.LightweightSession();
        var id = $"test-cluster_test-grain-5";
        var document = await session.LoadAsync<MartenGrainData<TestState>>(id);
        if (document != null)
        {
            document.Data.Name = "Modified by someone else";
            session.Store(document);
            await session.SaveChangesAsync();
        }

        // Try to update with stale ETag
        grainState.State.Name = "My Update";

        // Assert
        await Assert.ThrowsAsync<InconsistentStateException>(
            () => _storage.WriteStateAsync("TestState", grainId, grainState));
    }

    [Fact]
    public async Task WriteStateAsync_NewGrainWithoutETag_ShouldSucceed()
    {
        // Arrange
        var grainId = GrainId.Parse("test-grain-6");
        var grainState = new GrainState<TestState>
        {
            State = new TestState { Name = "New", Value = 42 },
            RecordExists = false,
            ETag = null
        };

        // Act
        await _storage.WriteStateAsync("TestState", grainId, grainState);

        // Assert
        Assert.True(grainState.RecordExists);
        Assert.NotNull(grainState.ETag);
    }

    [Fact]
    public async Task ClearStateAsync_ShouldRemoveState()
    {
        // Arrange
        var grainId = GrainId.Parse("test-grain-7");
        var grainState = new GrainState<TestState>
        {
            State = new TestState { Name = "ToBeDeleted", Value = 99 }
        };

        // Act - Write then clear
        await _storage.WriteStateAsync("TestState", grainId, grainState);
        await _storage.ClearStateAsync("TestState", grainId, grainState);

        // Verify deletion
        var readGrainState = new GrainState<TestState>();
        await _storage.ReadStateAsync("test", grainId, readGrainState);

        // Assert
        Assert.False(readGrainState.RecordExists);
        Assert.Null(readGrainState.ETag);
    }
}

public class TestState
{
    public string Name { get; set; } = string.Empty;
    public int Value { get; set; }
}
