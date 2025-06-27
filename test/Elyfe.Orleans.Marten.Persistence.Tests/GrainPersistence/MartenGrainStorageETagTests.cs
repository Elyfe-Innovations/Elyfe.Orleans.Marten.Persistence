using System;
using System.Threading.Tasks;
using Elyfe.Orleans.Marten.Persistence.GrainPersistence;
using JasperFx;
using JasperFx.Core;
using Marten;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Orleans;
using Orleans.Configuration;
using Orleans.Runtime;
using Orleans.Storage;
using Testcontainers.PostgreSql;
using Xunit;
using AwesomeAssertions;

namespace Elyfe.Orleans.Marten.Persistence.Tests.GrainPersistence;

[CollectionDefinition("Marten Storage Tests", DisableParallelization = true)]
public class MartenStorageTestCollection { }

[Collection("Marten Storage Tests")]
public class MartenGrainStorageETagTests : IAsyncLifetime
{
    private IDocumentStore? _documentStore;
    private MartenGrainStorage? _storage;
    private readonly PostgreSqlContainer _postgreSqlContainer;

    public MartenGrainStorageETagTests()
    {
        _postgreSqlContainer = new PostgreSqlBuilder()
            .WithImage("postgres:15")
            .WithDatabase("test_etag_db")
            .WithUsername("testuser")
            .WithPassword("testpass")
            .Build();
    }

    public async Task InitializeAsync()
    {
        await _postgreSqlContainer.StartAsync();
        
        _documentStore = DocumentStore.For(_postgreSqlContainer.GetConnectionString());

        var logger = new NullLogger<MartenGrainStorage>();
        var clusterOptions = Options.Create(new ClusterOptions { ServiceId = "test-cluster" });
        var hostEnvironment = new Mock<IHostEnvironment>();
        hostEnvironment.Setup(h => h.EnvironmentName).Returns("Development");

        _storage = new MartenGrainStorage("test", _documentStore, logger, clusterOptions, hostEnvironment.Object);
    }

    public async Task DisposeAsync()
    {
        _documentStore?.Dispose();
        await _postgreSqlContainer.DisposeAsync();
    }

    [Fact]
    public async Task ReadStateAsync_NewGrain_ShouldHaveNullETag()
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
        grainState.State.Should().BeNull();
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

        // Try to update with stale ETag
        grainState.State.Name = "My Update";
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

        // Verify deletion
        var readGrainState = new GrainState<TestState>();
        ArgumentNullException.ThrowIfNull(_storage);
        await _storage.ReadStateAsync("TestState", grainId, readGrainState);

        // Assert
        readGrainState.RecordExists.Should().BeFalse();
        readGrainState.ETag.Should().BeNull();
        readGrainState.State.Should().BeNull();
    }
}

public class TestState
{
    public string Name { get; set; } = string.Empty;
    public int Value { get; set; }
}
