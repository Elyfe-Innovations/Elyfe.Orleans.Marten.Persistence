using System.Text.Json;
using System.Text.Json.Serialization;
using Elyfe.Orleans.Marten.Persistence.Abstractions;
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
    IOptions<WriteBehindOptions> options,
    string serviceId)
    : IGrainStateCache
{
    private readonly WriteBehindOptions _options = options.Value;

    private readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    public async Task<CachedGrainState<T>?> ReadAsync<T>(string storageName, GrainId grainId,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var db = redis.GetDatabase(1);
            var grainKey = GetGrainKey(grainId);
            var stateHashKey = GetStateHashKey(storageName);
            
            var json = await db.HashGetAsync(stateHashKey, grainKey);
            if (!json.HasValue)
            {
                return null;
            }

            var dto = JsonSerializer.Deserialize<CacheDto<T>>(json.ToString()!, _jsonOptions);
            if (dto == null)
            {
                return null;
            }

            return new CachedGrainState<T>(dto.GetData()!, dto.ETag, dto.LastModified, dto.CacheType);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to read cached state for grain {GrainId} from storage {StorageName}", grainId,
                storageName);
            return null;
        }
    }

    public async Task WriteAsync<T>(string storageName, GrainId grainId, T state, string etag, long lastModified,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var db = redis.GetDatabase(1);
            var grainKey = GetGrainKey(grainId);
            var stateHashKey = GetStateHashKey(storageName);

            var dto = new CacheDto<T>(state, etag, lastModified);
            var json = JsonSerializer.Serialize(dto, _jsonOptions);

            await db.HashSetAsync(stateHashKey, grainKey, json);

            // Apply TTL if configured
            if (_options.StateTtlSeconds > 0)
            {
                await db.KeyExpireAsync(stateHashKey, TimeSpan.FromSeconds(_options.StateTtlSeconds));
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
            var db = redis.GetDatabase(1);
            var grainKey = GetGrainKey(grainId);
            var stateHashKey = GetStateHashKey(storageName);

            await db.HashDeleteAsync(stateHashKey, grainKey);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to remove cached state for grain {GrainId} from storage {StorageName}",
                grainId, storageName);
        }
    }

    public async Task MarkDirtyAsync(string storageName, GrainId grainId, CancellationToken cancellationToken = default)
    {
        try
        {
            var db = redis.GetDatabase(1);
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
            var db = redis.GetDatabase(1);
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

    public async Task<IReadOnlyList<string>> GetDirtyKeysAsync(string storageName, int batchSize,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var db = redis.GetDatabase(1);
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
            var db = redis.GetDatabase(1);
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
            var db = redis.GetDatabase(1);
            var lockKey = GetDrainLockKey(storageName);

            return await db.StringSetAsync(lockKey, "locked", lockTtl, When.NotExists);
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
            var db = redis.GetDatabase(1);
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
        return grainId.ToString().Replace('/', '_');
    }

    private string GetStateHashKey(string storageName)
    {
        var tenantPart = GetTenantPart();
        return $"mgs:{serviceId}:{storageName}{tenantPart}:state";
    }

    private string GetDirtySetKey(string storageName)
    {
        var tenantPart = GetTenantPart();
        return $"mgs:{serviceId}:{storageName}{tenantPart}:dirty";
    }

    private string GetWriteCounterKey(string storageName)
    {
        return $"mgs:{serviceId}:{storageName}:wcount";
    }

    private string GetDrainLockKey(string storageName)
    {
        return $"mgs:{serviceId}:{storageName}:drain-lock";
    }

    private string GetTenantPart()
    {
        // Try to get tenant from Orleans RequestContext
        var tenantId = RequestContext.Get("tenantId") as string;
        return string.IsNullOrEmpty(tenantId) ? string.Empty : $":tenant:{tenantId}";
    }
}

public class CacheDto<T>
{
    [JsonIgnore]
    public Type CacheType { get; set; } = null!;

    public string? TypeString
    {
        get => CacheType.AssemblyQualifiedName;
        set
        {
            if(value is null)
                return;
            CacheType = Type.GetType(value!)!;
        }
    }
    public string SerializedData { get; set; } = null!;

    public T? GetData()
    {
        var data = JsonSerializer.Deserialize(SerializedData, CacheType, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });
        return data is T typedData ? typedData : default;
    }

    public CacheDto()
    {
        
    }

    public CacheDto(T value, string etag, long lastModified)
    {
        CacheType = typeof(T);
        SerializedData = JsonSerializer.Serialize(value, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });
        ETag = etag;
        LastModified = lastModified;
    }


    public string ETag { get; init; } = null!;
    public long LastModified { get; init; }
}