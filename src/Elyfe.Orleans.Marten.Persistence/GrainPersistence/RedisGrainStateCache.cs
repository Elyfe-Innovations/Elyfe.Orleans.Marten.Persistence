using System.Text.Json;
using Elyfe.Orleans.Marten.Persistence.Abstractions;
using Elyfe.Orleans.Marten.Persistence.Options;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Runtime;
using StackExchange.Redis;

namespace Elyfe.Orleans.Marten.Persistence.GrainPersistence;

/// <summary>
/// Redis-backed grain state cache with Hash+Set coalescing pattern.
/// </summary>
public class RedisGrainStateCache(
    IConnectionMultiplexer redis,
    ILogger<RedisGrainStateCache> logger,
    IOptions<MartenStorageOptions> options,
    string serviceId)
    : IGrainStateCache
{
    private readonly MartenStorageOptions _storageOptions = options.Value;

    private readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    public async Task<CachedGrainState<T>?> ReadAsync<T>(string storageName, GrainId grainId,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var db = redis.GetDatabase(_storageOptions.WriteBehind.CacheDatabase);
            var grainKey = GetGrainKey(grainId);
            var stateHashKey = GetStateHashKey(storageName);

            var json = await db.HashGetAsync(stateHashKey, grainKey);
            if (!json.HasValue)
            {
                logger.LogTrace("Cache Miss for grain {GrainId} from storage {StorageName}.", grainId, storageName);
                return null;
            }

            var dto = JsonSerializer.Deserialize<CacheDto<T?>?>(json.ToString()!, _jsonOptions);
            if (dto == null)
            {
                logger.LogWarning("Failed to deserialize cached state for grain {GrainId} from storage {StorageName}.", grainId, storageName);
                return null;
            }

            var createdAt = dto.CreatedAt == 0 ? dto.LastModified : dto.CreatedAt;
            return new CachedGrainState<T>(dto.GetData()!, dto.ETag, dto.LastModified, createdAt, dto.CacheType);
        }
        catch (Exception ex)
        {
            // Propagate: a read failure is NOT a cache miss. Callers rely on the distinction to avoid
            // falling back to Marten when a newer write-behind write may be sitting in this cache.
            logger.LogError(ex, "Failed to read cached state for grain {GrainId} from storage {StorageName}", grainId,
                storageName);
            throw;
        }
    }

    public async Task WriteAsync<T>(string storageName, GrainId grainId, T state, string etag, long lastModified,
        long createdAt, CancellationToken cancellationToken = default)
    {
        try
        {
            var db = redis.GetDatabase(_storageOptions.WriteBehind.CacheDatabase);
            var grainKey = GetGrainKey(grainId);
            var stateHashKey = GetStateHashKey(storageName);

            var dto = new CacheDto<T>(state, etag, lastModified, createdAt);
            var json = JsonSerializer.Serialize(dto, _jsonOptions);

            await db.HashSetAsync(stateHashKey, grainKey, json);

            // Apply TTL if configured
            if (_storageOptions.WriteBehind.StateTtlSeconds > 0)
            {
                await db.KeyExpireAsync(stateHashKey,
                    TimeSpan.FromSeconds(_storageOptions.WriteBehind.StateTtlSeconds));
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to write cached state for grain {GrainId} to storage {StorageName}", grainId,
                storageName);
            throw;
        }
    }

    public async Task RemoveAsync(string storageName, GrainId grainId, CancellationToken cancellationToken = default)
    {
        try
        {
            var db = redis.GetDatabase(_storageOptions.WriteBehind.CacheDatabase);
            var grainKey = GetGrainKey(grainId);
            var stateHashKey = GetStateHashKey(storageName);

            await db.HashDeleteAsync(stateHashKey, grainKey);
        }
        catch (Exception ex)
        {
            // Propagate: a failed eviction during a clear would otherwise leave the value in the
            // cache, where a later read-through would resurrect a state that was just deleted.
            logger.LogError(ex, "Failed to remove cached state for grain {GrainId} from storage {StorageName}",
                grainId, storageName);
            throw;
        }
    }

    public async Task MarkDirtyAsync(string storageName, GrainId grainId, CancellationToken cancellationToken = default)
    {
        try
        {
            var db = redis.GetDatabase(_storageOptions.WriteBehind.CacheDatabase);
            var grainKey = GetGrainKey(grainId);
            var dirtySetKey = GetDirtySetKey(storageName);

            await db.SetAddAsync(dirtySetKey, grainKey);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to mark grain {GrainId} as dirty in storage {StorageName}", grainId,
                storageName);
            throw;
        }
    }

    public async Task ClearDirtyAsync(string storageName, GrainId grainId,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var db = redis.GetDatabase(_storageOptions.WriteBehind.CacheDatabase);
            var grainKey = GetGrainKey(grainId);
            var dirtySetKey = GetDirtySetKey(storageName);

            await db.SetRemoveAsync(dirtySetKey, grainKey);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to clear dirty marker for grain {GrainId} in storage {StorageName}", grainId,
                storageName);
        }
    }

    public async Task<bool> IsDirtyAsync(string storageName, GrainId grainId,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var db = redis.GetDatabase(_storageOptions.WriteBehind.CacheDatabase);
            var grainKey = GetGrainKey(grainId);
            var dirtySetKey = GetDirtySetKey(storageName);

            return await db.SetContainsAsync(dirtySetKey, grainKey);
        }
        catch (Exception ex)
        {
            // Propagate: an error means "unknown", which callers must treat differently from "clean".
            logger.LogError(ex, "Failed to check dirty marker for grain {GrainId} in storage {StorageName}",
                grainId, storageName);
            throw;
        }
    }

    public async Task<bool> TryAcquireGrainLockAsync(string storageName, GrainId grainId, TimeSpan ttl,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var db = redis.GetDatabase(_storageOptions.WriteBehind.CacheDatabase);
            var key = GetGrainLockKey(storageName, grainId);

            return await db.StringSetAsync(key, "1", ttl, When.NotExists);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to acquire grain lock for {GrainId} in storage {StorageName}", grainId,
                storageName);
            throw;
        }
    }

    public async Task ReleaseGrainLockAsync(string storageName, GrainId grainId,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var db = redis.GetDatabase(_storageOptions.WriteBehind.CacheDatabase);
            var key = GetGrainLockKey(storageName, grainId);

            // Plain DEL is safe despite the TTL: the holder reaches this call within TTL or the lock
            // has already expired and any new holder would itself release after its own operation.
            await db.KeyDeleteAsync(key);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to release grain lock for {GrainId} in storage {StorageName}", grainId,
                storageName);
        }
    }

    public async Task<IReadOnlyList<string>> GetDirtyKeysAsync(string storageName, int batchSize,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var db = redis.GetDatabase(_storageOptions.WriteBehind.CacheDatabase);
            var dirtySetKey = GetDirtySetKey(storageName);

            var members = await db.SetPopAsync(dirtySetKey, batchSize);
            return members.Select(m => m.ToString()).ToList();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get dirty keys from storage {StorageName}", storageName);
            return [];
        }
    }

    public async Task<long> IncrementWriteCounterAsync(string storageName,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var db = redis.GetDatabase(_storageOptions.WriteBehind.CacheDatabase);
            var wcountKey = GetWriteCounterKey(storageName);

            var count = await db.StringIncrementAsync(wcountKey);

            // Set expiration on first increment
            if (count == 1)
            {
                await db.KeyExpireAsync(wcountKey, TimeSpan.FromSeconds(1));
            }

            return count;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to increment write counter for storage {StorageName}", storageName);
            return 0;
        }
    }

    public async Task<bool> TryAcquireDrainLockAsync(string storageName, TimeSpan lockTtl,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var db = redis.GetDatabase(_storageOptions.WriteBehind.CacheDatabase);
            var lockKey = GetDrainLockKey(storageName);

            var drainLockObject = await db.StringSetAsync(lockKey, "locked", lockTtl, When.NotExists);
            return drainLockObject;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to acquire drain lock for storage {StorageName}", storageName);
            return false;
        }
    }

    public async Task ReleaseDrainLockAsync(string storageName, CancellationToken cancellationToken = default)
    {
        try
        {
            var db = redis.GetDatabase(_storageOptions.WriteBehind.CacheDatabase);
            var lockKey = GetDrainLockKey(storageName);

            await db.KeyDeleteAsync(lockKey);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to release drain lock for storage {StorageName}", storageName);
        }
    }

    private string GetGrainKey(GrainId grainId)
    {
        // MARTEN-01: reversible encoding; the lossy '/'->'_' collapse merged
        // distinct grains onto one cache entry. Old cache entries simply miss
        // after deploy and repopulate from Marten.
        return GrainKeyEncoding.Encode(grainId);
    }

    private string GetStateHashKey(string storageName)
    {
        // Note: Cache keys are NOT tenant-scoped because RequestContext is not available during grain activation.
        // The grain ID itself is unique per message/entity, and tenant authorization is handled at the grain level.
        return $"mgs:{serviceId}:{storageName}:state";
    }

    private string GetDirtySetKey(string storageName)
    {
        return $"mgs:{serviceId}:{storageName}:dirty";
    }

    private string GetGrainLockKey(string storageName, GrainId grainId)
    {
        return $"mgs:{serviceId}:{storageName}:grain-lock:{GetGrainKey(grainId)}";
    }

    private string GetWriteCounterKey(string storageName)
    {
        return $"mgs:{serviceId}:{storageName}:wcount";
    }

    private string GetDrainLockKey(string storageName)
    {
        return $"mgs:{serviceId}:{storageName}:drain-lock";
    }
}