using System.Reflection;
using System.Text.Json;
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
    private readonly IDocumentStore _documentStore;
    private readonly ILogger<CacheToMartenWriter> _logger;
    private readonly MartenStorageOptions _martenOptions;
    private readonly string _serviceId;
    private readonly JsonSerializerOptions _jsonOptions;
    private HashSet<string> _storageNames = new();

    public CacheToMartenWriter(
        IGrainStateCache cache,
        IDocumentStore documentStore,
        ILogger<CacheToMartenWriter> logger,
        IOptions<ClusterOptions> clusterOptions,
        IOptions<MartenStorageOptions> martenOptions)
    {
        _cache = cache;
        _documentStore = documentStore;
        _logger = logger;
        _martenOptions = martenOptions.Value;
        _serviceId = clusterOptions.Value.ServiceId;
        _jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };
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
        foreach (var storageName in _storageNames)
        {
            if (cancellationToken.IsCancellationRequested)
                break;
            await DrainStorageAsync(storageName, cancellationToken);
        }
    }

    private async Task DrainStorageAsync(string storageName, CancellationToken cancellationToken)
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
                    await DrainGrainAsync(storageName, grainKey, cancellationToken);
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
                        // Reconstruct GrainId from key (reverse of GetGrainKey)
                        var grainId = GrainId.Parse(grainKey.Replace('_', '/'));
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

    private async Task DrainGrainAsync(string storageName, string grainKey, CancellationToken cancellationToken)
    {
        // Parse grain ID from key
        var grainId = GrainId.Parse(grainKey.Replace('_', '/'));

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

        // Create MartenGrainData document
        // var document = MartenGrainData.Create(cached.Data, martenId);

        // Upsert to Marten
        await using var session = _martenOptions.UseTenantPerStorage
            ? _documentStore.LightweightSession(storageName)
            : _documentStore.LightweightSession();
        session.Store(document);
        await session.SaveChangesAsync(cancellationToken);

        // Update cache with new lastModified and etag
        var newETag = document.GetType()
                          .GetMethod("GenerateETag", BindingFlags.Instance | BindingFlags.Public)!
                          .Invoke(document, new object?[] { }) as string
                      ?? throw new InvalidOperationException("Failed to generate ETag for cached grain state");
        var newModified = DateTimeOffset.Parse(document.GetType()
            .GetProperty("LastModified")!.GetValue(document)!.ToString()!);
        await _cache.WriteAsync(storageName, grainId, cached.Data, newETag, newModified.ToUnixTimeMilliseconds(),
            cancellationToken);

        // Clear dirty marker
        await _cache.ClearDirtyAsync(storageName, grainId, cancellationToken);

        if (_logger.IsEnabled(LogLevel.Trace))
            _logger.LogTrace("Successfully drained grain {GrainId} to Marten", grainId);
    }

    private static string GenerateETag<T>(MartenGrainData<T> state)
    {
        var lastModified = state.LastModified.ToUnixTimeMilliseconds();
        var dataJson = JsonSerializer.Serialize(state.Data);
        var combined = $"{lastModified}_{dataJson}";

        using var hash = System.Security.Cryptography.SHA256.Create();
        var hashBytes = hash.ComputeHash(System.Text.Encoding.UTF8.GetBytes(combined));
        return Convert.ToBase64String(hashBytes);
    }

    public void RegisterStorage(string storageName)
    {
        _storageNames.Add(storageName);
    }
}