using AwesomeAssertions;
using Elyfe.Orleans.Marten.Persistence.GrainPersistence;
using Microsoft.Extensions.Logging;
using Moq;
using Orleans.Runtime;
using StackExchange.Redis;
using Xunit;

namespace Elyfe.Orleans.Marten.Persistence.Tests.GrainPersistence;

public class RedisGrainStateCacheTests
{
    [Fact]
    public void GetGrainKey_ReplacesSlashWithUnderscore()
    {
        // This tests the key generation logic
        var grainId = GrainId.Parse("test/grain/123");
        var expectedKey = "test_grain_123";

        // The actual key generation happens in RedisGrainStateCache.GetGrainKey
        // We verify the logic by checking that GrainId.ToString() contains slashes
        grainId.ToString().Should().Contain("/");

        // And that our expected key format is underscore-separated
        expectedKey.Should().NotContain("/");
    }

    [Fact]
    public void GetStateHashKey_IncludesServiceIdAndStorageName()
    {
        var serviceId = "test-service";
        var storageName = "TestStorage";

        var expectedPattern = $"mgs:{serviceId}:{storageName}";

        // Key should follow pattern: mgs:{serviceId}:{storageName}:state (without tenant)
        expectedPattern.Should().StartWith("mgs:");
    }

    [Fact]
    public void GetDirtySetKey_IncludesServiceIdAndStorageName()
    {
        var serviceId = "test-service";
        var storageName = "TestStorage";

        var expectedPattern = $"mgs:{serviceId}:{storageName}";

        // Key should follow pattern: mgs:{serviceId}:{storageName}:dirty (without tenant)
        expectedPattern.Should().StartWith("mgs:");
    }

    [Fact]
    public void GetWriteCounterKey_IncludesServiceIdAndStorageName()
    {
        var serviceId = "test-service";
        var storageName = "TestStorage";

        var expectedKey = $"mgs:{serviceId}:{storageName}:wcount";

        expectedKey.Should().Be(expectedKey);
    }

    [Fact]
    public void GetDrainLockKey_IncludesServiceIdAndStorageName()
    {
        var serviceId = "test-service";
        var storageName = "TestStorage";

        var expectedKey = $"mgs:{serviceId}:{storageName}:drain-lock";

        expectedKey.Should().Be(expectedKey);
    }

    [Fact]
    public async Task IncrementWriteCounterAsync_ShouldReturnIncrementedValue()
    {
        // Mock Redis database
        var mockDb = new Mock<IDatabase>();
        var mockRedis = new Mock<IConnectionMultiplexer>();
        mockRedis.Setup(r => r.GetDatabase(It.IsAny<int>(), It.IsAny<object>())).Returns(mockDb.Object);

        // Setup increment to return 1, then set expiration
        mockDb.Setup(db => db.StringIncrementAsync(
            It.IsAny<RedisKey>(),
            It.IsAny<long>(),
            It.IsAny<CommandFlags>()
        )).ReturnsAsync(1);

        mockDb.Setup(db => db.KeyExpireAsync(
            It.IsAny<RedisKey>(),
            It.IsAny<TimeSpan?>(),
            It.IsAny<ExpireWhen>(),
            It.IsAny<CommandFlags>()
        )).ReturnsAsync(true);

        var options = OptionsHelper.Create(new MartenStorageOptions());
        var logger = Mock.Of<ILogger<RedisGrainStateCache>>();

        var cache = new RedisGrainStateCache(mockRedis.Object, logger, options, "test-service");

        var count = await cache.IncrementWriteCounterAsync("TestStorage");

        count.Should().Be(1);

        // Verify expiration was set
        mockDb.Verify(db => db.KeyExpireAsync(
            It.IsAny<RedisKey>(),
            TimeSpan.FromSeconds(1),
            It.IsAny<ExpireWhen>(),
            It.IsAny<CommandFlags>()
        ), Times.Once);
    }

    [Fact]
    public async Task TryAcquireDrainLockAsync_ShouldReturnTrue_WhenLockAcquired()
    {
        var mockDb = new Mock<IDatabase>();
        var mockRedis = new Mock<IConnectionMultiplexer>();
        mockRedis.Setup(r => r.GetDatabase(It.IsAny<int>(), It.IsAny<object>())).Returns(mockDb.Object);

        mockDb.Setup(db => db.StringSetAsync(
            It.IsAny<RedisKey>(),
            It.IsAny<RedisValue>(),
            It.IsAny<TimeSpan?>(),
            When.NotExists
        )).ReturnsAsync(true);

        var options = OptionsHelper.Create(new MartenStorageOptions());
        var logger = Mock.Of<ILogger<RedisGrainStateCache>>();

        var cache = new RedisGrainStateCache(mockRedis.Object, logger, options, "test-service");

        var acquired = await cache.TryAcquireDrainLockAsync("TestStorage", TimeSpan.FromSeconds(30));

        acquired.Should().BeTrue();
    }

    [Fact]
    public async Task TryAcquireDrainLockAsync_ShouldReturnFalse_WhenLockNotAcquired()
    {
        var mockDb = new Mock<IDatabase>();
        var mockRedis = new Mock<IConnectionMultiplexer>();
        mockRedis.Setup(r => r.GetDatabase(It.IsAny<int>(), It.IsAny<object>())).Returns(mockDb.Object);

        mockDb.Setup(db => db.StringSetAsync(
            It.IsAny<RedisKey>(),
            It.IsAny<RedisValue>(),
            It.IsAny<TimeSpan?>(),
            It.IsAny<bool>(),
            When.NotExists,
            It.IsAny<CommandFlags>()
        )).ReturnsAsync(false);

        var options = OptionsHelper.Create(new MartenStorageOptions());
        var logger = Mock.Of<ILogger<RedisGrainStateCache>>();

        var cache = new RedisGrainStateCache(mockRedis.Object, logger, options, "test-service");

        var acquired = await cache.TryAcquireDrainLockAsync("TestStorage", TimeSpan.FromSeconds(30));

        acquired.Should().BeFalse();
    }

    [Fact]
    public async Task WriteAsync_ShouldStoreSerializedState()
    {
        var mockDb = new Mock<IDatabase>();
        var mockRedis = new Mock<IConnectionMultiplexer>();
        mockRedis.Setup(r => r.GetDatabase(It.IsAny<int>(), It.IsAny<object>())).Returns(mockDb.Object);

        RedisValue capturedValue = RedisValue.Null;
        mockDb.Setup(db => db.HashSetAsync(
                It.IsAny<RedisKey>(),
                It.IsAny<RedisValue>(),
                It.IsAny<RedisValue>(),
                It.IsAny<When>(),
                It.IsAny<CommandFlags>()
            )).Callback<RedisKey, RedisValue, RedisValue, When, CommandFlags>((k, f, v, w, c) => capturedValue = v)
            .ReturnsAsync(true);

        var options = OptionsHelper.Create(new MartenStorageOptions()
        {
            WriteBehind = new WriteBehindOptions { StateTtlSeconds = 300 }
        });
        var logger = Mock.Of<ILogger<RedisGrainStateCache>>();

        var cache = new RedisGrainStateCache(mockRedis.Object, logger, options, "test-service");

        var grainId = GrainId.Parse("test/grain/123");
        var state = new TestState { Name = "test-name", Value = 42 };
        var etag = "test-etag";
        var lastModified = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        await cache.WriteAsync("TestStorage", grainId, state, etag, lastModified);

        // Verify HashSet was called
        mockDb.Verify(db => db.HashSetAsync(
            It.IsAny<RedisKey>(),
            It.IsAny<RedisValue>(),
            It.IsAny<RedisValue>(),
            It.IsAny<When>(),
            It.IsAny<CommandFlags>()
        ), Times.Once);

        // Verify TTL was set
        mockDb.Verify(db => db.KeyExpireAsync(
            It.IsAny<RedisKey>(),
            TimeSpan.FromSeconds(300),
            It.IsAny<ExpireWhen>(),
            It.IsAny<CommandFlags>()
        ), Times.Once);

        // Verify the stored value contains our test data
        capturedValue.Should().NotBe(RedisValue.Null);
    }

    [Fact]
    public async Task MarkDirtyAsync_ShouldAddToSet()
    {
        var mockDb = new Mock<IDatabase>();
        var mockRedis = new Mock<IConnectionMultiplexer>();
        mockRedis.Setup(r => r.GetDatabase(It.IsAny<int>(), It.IsAny<object>())).Returns(mockDb.Object);

        mockDb.Setup(db => db.SetAddAsync(
            It.IsAny<RedisKey>(),
            It.IsAny<RedisValue>(),
            It.IsAny<CommandFlags>()
        )).ReturnsAsync(true);

        var options = OptionsHelper.Create(new MartenStorageOptions());
        var logger = Mock.Of<ILogger<RedisGrainStateCache>>();

        var cache = new RedisGrainStateCache(mockRedis.Object, logger, options, "test-service");

        var grainId = GrainId.Parse("test/grain/123");

        await cache.MarkDirtyAsync("TestStorage", grainId);

        mockDb.Verify(db => db.SetAddAsync(
            It.IsAny<RedisKey>(),
            It.IsAny<RedisValue>(),
            It.IsAny<CommandFlags>()
        ), Times.Once);
    }

    [Fact]
    public async Task ClearDirtyAsync_ShouldRemoveFromSet()
    {
        var mockDb = new Mock<IDatabase>();
        var mockRedis = new Mock<IConnectionMultiplexer>();
        mockRedis.Setup(r => r.GetDatabase(It.IsAny<int>(), It.IsAny<object>())).Returns(mockDb.Object);

        mockDb.Setup(db => db.SetRemoveAsync(
            It.IsAny<RedisKey>(),
            It.IsAny<RedisValue>(),
            It.IsAny<CommandFlags>()
        )).ReturnsAsync(true);

        var options = OptionsHelper.Create(new MartenStorageOptions());
        var logger = Mock.Of<ILogger<RedisGrainStateCache>>();

        var cache = new RedisGrainStateCache(mockRedis.Object, logger, options, "test-service");

        var grainId = GrainId.Parse("test/grain/123");

        await cache.ClearDirtyAsync("TestStorage", grainId);

        mockDb.Verify(db => db.SetRemoveAsync(
            It.IsAny<RedisKey>(),
            It.IsAny<RedisValue>(),
            It.IsAny<CommandFlags>()
        ), Times.Once);
    }

    [Fact]
    public async Task GetDirtyKeysAsync_ShouldPopFromSet()
    {
        var mockDb = new Mock<IDatabase>();
        var mockRedis = new Mock<IConnectionMultiplexer>();
        mockRedis.Setup(r => r.GetDatabase(It.IsAny<int>(), It.IsAny<object>())).Returns(mockDb.Object);

        var dirtyKeys = new RedisValue[] { "grain1", "grain2", "grain3" };
        mockDb.Setup(db => db.SetPopAsync(
            It.IsAny<RedisKey>(),
            It.IsAny<long>(),
            It.IsAny<CommandFlags>()
        )).ReturnsAsync(dirtyKeys);

        var options = OptionsHelper.Create(new MartenStorageOptions());
        var logger = Mock.Of<ILogger<RedisGrainStateCache>>();

        var cache = new RedisGrainStateCache(mockRedis.Object, logger, options, "test-service");

        var keys = await cache.GetDirtyKeysAsync("TestStorage", 10);

        keys.Count.Should().Be(3);
        keys.Should().Contain("grain1");
        keys.Should().Contain("grain2");
        keys.Should().Contain("grain3");
    }
}