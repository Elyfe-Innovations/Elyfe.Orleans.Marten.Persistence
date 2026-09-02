using System.Collections.Concurrent;
using System.Reflection;
using Elyfe.Orleans.Marten.Persistence.Abstractions;
using Elyfe.Orleans.Marten.Persistence.Options;
using Marten;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Runtime;

namespace Elyfe.Orleans.Marten.Persistence.GrainPersistence;

/// <summary>
/// Background service that drains dirty grain states from Redis to Marten.
/// </summary>
public class CacheToMartenWriter : BackgroundService
{
    private readonly IGrainStateCache _cache;
    private readonly IDocumentStore _defaultDocumentStore;
    private readonly ILogger<CacheToMartenWriter> _logger;
    private readonly MartenStorageOptions _martenOptions;
    private readonly string _serviceId;
    private readonly ConcurrentDictionary<string, IDocumentStore> _storageStores = new();

    public CacheToMartenWriter(
        IGrainStateCache cache,
        IDocumentStore documentStore,
        ILogger<CacheToMartenWriter> logger,
        IOptions<ClusterOptions> clusterOptions,
        IOptions<MartenStorageOptions> martenOptions)
    {
        _cache = cache;
        _defaultDocumentStore = documentStore;
        _logger = logger;
        _martenOptions = martenOptions.Value;
        _serviceId = clusterOptions.Value.ServiceId;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "MartenWriteBehindDrainer starting with interval {IntervalSeconds}s, batch size {BatchSize}",
            _martenOptions.WriteBehind.DrainIntervalSeconds, _martenOptions.WriteBehind.BatchSize);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await DrainAsync(stoppingToken);
                await Task.Delay(TimeSpan.FromSeconds(_martenOptions.WriteBehind.DrainIntervalSeconds), stoppingToken);
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during drain cycle");
            }
        }

        _logger.LogInformation("MartenWriteBehindDrainer stopped");
    }

    private async Task DrainAsync(CancellationToken cancellationToken)
    {
        foreach (var (storageName, documentStore) in _storageStores)
        {
            if (cancellationToken.IsCancellationRequested)
                break;
            await DrainStorageAsync(storageName, documentStore, cancellationToken);
        }
    }

    private async Task DrainStorageAsync(
        string storageName,
        IDocumentStore documentStore,
        CancellationToken cancellationToken)
    {
        // Try to acquire lock
        var lockTtl = TimeSpan.FromSeconds(_martenOptions.WriteBehind.DrainLockTtlSeconds);
        var acquired = await _cache.TryAcquireDrainLockAsync(storageName, lockTtl, cancellationToken);

        if (!acquired)
        {
            if (_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug("Could not acquire drain lock for storage {StorageName}, skipping", storageName);
            return;
        }

        try
        {
            var dirtyKeys =
                await _cache.GetDirtyKeysAsync(storageName, _martenOptions.WriteBehind.BatchSize, cancellationToken);
            if (dirtyKeys.Count == 0)
            {
                return;
            }

            _logger.LogInformation("Draining {Count} dirty grain states from storage {StorageName}", dirtyKeys.Count,
                storageName);

            var drained = 0;
            var failed = 0;

            foreach (var grainKey in dirtyKeys)
            {
                if (cancellationToken.IsCancellationRequested)
                    break;

                try
                {
                    await DrainGrainAsync(storageName, documentStore, grainKey, cancellationToken);
                    drained++;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to drain grain {GrainKey} from storage {StorageName}", grainKey,
                        storageName);
                    failed++;

                    // Re-add to dirty set for retry
                    try
                    {
                        // Reconstruct GrainId from key (reverse of GetGrainKey);
                        // MARTEN-01 keys are Base64Url, legacy keys are the
                        // lossy '/'->'_' collapse.
                        if (!GrainKeyEncoding.TryDecodeToGrainId(grainKey, out var grainId))
                            grainId = GrainId.Parse(grainKey.Replace('_', '/'));
                        await _cache.MarkDirtyAsync(storageName, grainId, cancellationToken);
                    }
                    catch (Exception markEx)
                    {
                        _logger.LogError(markEx, "Failed to re-mark grain {GrainKey} as dirty", grainKey);
                    }
                }
            }

            _logger.LogInformation(
                "Drain cycle completed for storage {StorageName}: {Drained} succeeded, {Failed} failed",
                storageName, drained, failed);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Error during drain cycle for storage {StorageName}", storageName);
        }
        finally
        {
            await _cache.ReleaseDrainLockAsync(storageName, cancellationToken);
        }
    }

    private async Task DrainGrainAsync(
        string storageName,
        IDocumentStore documentStore,
        string grainKey,
        CancellationToken cancellationToken)
    {
        // Parse grain ID from key; MARTEN-01 keys are Base64Url, legacy keys
        // use the lossy '/'->'_' collapse.
        if (!GrainKeyEncoding.TryDecodeToGrainId(grainKey, out var grainId))
            grainId = GrainId.Parse(grainKey.Replace('_', '/'));

        // Serialize with ClearStateAsync on the same grain: holding the per-grain lock across the
        // read-store-writeback window is what stops a clear from landing between the re-validation
        // read and the Marten commit and resurrecting cleared state.
        var lockTtl = TimeSpan.FromSeconds(_martenOptions.WriteBehind.DrainLockTtlSeconds);
        if (!await _cache.TryAcquireGrainLockAsync(storageName, grainId, lockTtl, cancellationToken))
        {
            // The outer drain loop treats this as a failure and re-marks the grain dirty for the next
            // cycle; never drain a grain whose clear may be waiting on the lock.
            throw new InvalidOperationException($"Grain {grainId} drain lock is held.");
        }

        try
        {
            await DrainGrainCoreAsync(storageName, documentStore, grainId, grainKey, cancellationToken);
        }
        finally
        {
            await _cache.ReleaseGrainLockAsync(storageName, grainId, cancellationToken);
        }
    }

    private async Task DrainGrainCoreAsync(string storageName, IDocumentStore documentStore, GrainId grainId,
        string grainKey, CancellationToken cancellationToken)
    {
        // Read cached state (type-erased, we'll use object)
        var cached = await _cache.ReadAsync<object>(storageName, grainId, cancellationToken);
        if (cached == null)
        {
            // State no longer in cache, clear dirty marker
            await _cache.ClearDirtyAsync(storageName, grainId, cancellationToken);
            return;
        }

        // Generate Marten ID
        var martenId = $"{_serviceId}_{grainKey}";
        var genericType = typeof(MartenGrainData<>).MakeGenericType(cached.stateType);
        var document = genericType
            .GetMethod("Create")?
            .Invoke(null, new object?[] { cached.Data, martenId })!;
        var createdAt = cached.CreatedAt == 0 ? cached.LastModified : cached.CreatedAt;
        document.GetType()
            .GetProperty(nameof(MartenGrainData<object>.CreatedAt))!
            .SetValue(document, DateTimeOffset.FromUnixTimeMilliseconds(createdAt));

        // Create MartenGrainData document
        // var document = MartenGrainData.Create(cached.Data, martenId);

        // A concurrent ClearStateAsync (or a newer write) between the initial read and now must win.
        // Re-read the cached value and persist only if it still holds exactly the state we copied;
        // otherwise drop the stale copy — a fresh drain cycle will pick up any newer dirty write.
        // (With the grain lock held, a clear cannot land between this read and the commit; the read
        // still guards against a concurrent newer write.)
        var latest = await _cache.ReadAsync<object>(storageName, grainId, cancellationToken);
        if (latest is null || !string.Equals(latest.ETag, cached.ETag, StringComparison.Ordinal))
        {
            _logger.LogInformation(
                "Dropping stale drain for grain {GrainId} in storage {StorageName}: the cached state changed or was cleared while draining",
                grainId, storageName);
            return;
        }

        // Upsert to Marten
        await using var session = _martenOptions.UseTenantPerStorage
            ? documentStore.LightweightSession(storageName)
            : documentStore.LightweightSession();
        session.Store(document);
        await session.SaveChangesAsync(cancellationToken);

        // Update cache with new lastModified and etag
        var newETag = document.GetType()
                          .GetMethod("GenerateETag", BindingFlags.Instance | BindingFlags.Public)!
                          .Invoke(document, new object?[] { }) as string
                      ?? throw new InvalidOperationException("Failed to generate ETag for cached grain state");
        var newModified = DateTimeOffset.Parse(document.GetType()
            .GetProperty("LastModified")!.GetValue(document)!.ToString()!);

        // A write that landed between the re-validation read and this point must win: write back only
        // if the cache still holds the value we drained, otherwise leave the newer (still dirty) value
        // in place for the next drain cycle.
        var beforeWriteBack = await _cache.ReadAsync<object>(storageName, grainId, cancellationToken);
        if (beforeWriteBack is null ||
            !string.Equals(beforeWriteBack.ETag, cached.ETag, StringComparison.Ordinal))
        {
            _logger.LogInformation(
                "Dropping cache write-back for grain {GrainId} in storage {StorageName}: a newer write landed while draining",
                grainId, storageName);
            return;
        }

        await _cache.WriteAsync(
            storageName,
            grainId,
            cached.Data,
            newETag,
            newModified.ToUnixTimeMilliseconds(),
            createdAt,
            cancellationToken);

        // Clear dirty marker
        await _cache.ClearDirtyAsync(storageName, grainId, cancellationToken);

        if (_logger.IsEnabled(LogLevel.Trace))
            _logger.LogTrace("Successfully drained grain {GrainId} to Marten", grainId);
    }

    private static string GenerateETag<T>(MartenGrainData<T> state)
    {
        // Delegate to the model method which uses streaming hash to avoid OOM on large states
        return state.GenerateETag();
    }

    public void RegisterStorage(string storageName) =>
        RegisterStorage(storageName, _defaultDocumentStore);

    public void RegisterStorage(string storageName, IDocumentStore documentStore)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(storageName);
        ArgumentNullException.ThrowIfNull(documentStore);
        _storageStores[storageName] = documentStore;
    }
}