using Elyfe.Orleans.Marten.Persistence.Abstractions;
using Elyfe.Orleans.Marten.Persistence.Options;
using JasperFx;
using Marten;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Configuration;
using Orleans.Runtime;
using Orleans.Storage;
using Orleans.Serialization;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Orleans.Serialization.Serializers;

namespace Elyfe.Orleans.Marten.Persistence.GrainPersistence;

public class MartenGrainStorage : IGrainStorage, ILifecycleParticipant<ISiloLifecycle>
{
    private readonly string _clusterService;
    private static readonly ConcurrentDictionary<Type, bool> DefaultConstructible = new();


    // private readonly IDocumentStore _documentStore = services.GetKeyedService<IDocumentStore>(storageName) ?? documentStore;
    private readonly IGrainStateCache? _cache;
    private readonly string _storageName;
    private readonly IDocumentStore _documentStore;
    private readonly ILogger<MartenGrainStorage> _logger;
    private readonly IHostEnvironment _environment;
    private readonly MartenStorageOptions _martenOptions;
    private readonly Serializer? _serializer;
    private readonly IActivatorProvider? _activatorProvider;
    private readonly ActivitySource _activitySource = new("Elyfe.Orleans.Marten.Persistence");

    public MartenGrainStorage(string storageName,
        IDocumentStore documentStore,
        IServiceProvider services,
        ILogger<MartenGrainStorage> logger,
        IOptions<ClusterOptions> clusterOptions,
        IHostEnvironment environment)
    {
        _storageName = storageName;
        _documentStore = documentStore;
        _logger = logger;
        _environment = environment;
        _clusterService = clusterOptions.Value.ServiceId;
        _cache = services.GetService<IGrainStateCache>();
        _martenOptions = services.GetService<IOptions<MartenStorageOptions>>()?.Value ?? new MartenStorageOptions();
        _serializer = services.GetService<Serializer>();
        _activatorProvider = services.GetService<IActivatorProvider>();
        services.GetService<CacheToMartenWriter>()?.RegisterStorage(_storageName);
    }

    public async Task ClearStateAsync<T>(string grainType, GrainId grainId, IGrainState<T> grainState)
    {
        using var activity = CreateLinkedDbActivity($"{_storageName}.ClearStateAsync", grainType);
        _logger.LogTrace($"Clearing state for grain {grainId} of type {grainType}.");

        var id = GenerateId(grainId);

        // Evict before deleting: a read-through read would otherwise resurrect the document, and
        // a write-behind drain cycle landing between the delete and the eviction would re-store it.
        // The drainer clears the dirty marker itself once the cached value is gone.
        if (_cache != null)
        {
            await _cache.RemoveAsync(_storageName, grainId);
            await _cache.ClearDirtyAsync(_storageName, grainId);
        }

        await using var session = _martenOptions.UseTenantPerStorage
            ? _documentStore.LightweightSession(_storageName)
            : _documentStore.LightweightSession();
        if (!typeof(T).IsVisible)
            session.Delete<MartenGrainData<byte[]>>(id);
        else
            session.Delete<MartenGrainData<T>>(id);

        await session.SaveChangesAsync();

        // Orleans storage contract: after a clear the grain observes a fresh, empty state.
        grainState.State = CreateDefaultState<T>();
        grainState.RecordExists = false;
        grainState.ETag = null;
    }

    public async Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        using var activity = CreateLinkedDbActivity($"{_storageName}.ReadStateAsync", stateName);
        try
        {
            if (_logger.IsEnabled(LogLevel.Trace))
                _logger.LogTrace($"Reading state for grain {grainId} of type {typeof(T).Name}.");

            if (!typeof(T).IsVisible)
            {
                await using var opaqueSession = _martenOptions.UseTenantPerStorage
                    ? _documentStore.QuerySession(_storageName)
                    : _documentStore.QuerySession();
                await ReadOpaqueStateAsync(opaqueSession, grainId, grainState, GenerateId(grainId));
                return;
            }

            // Read-through cache: check cache first if enabled. Marten stays authoritative, so a
            // cache outage degrades to a slower read instead of failing the activation.
            if (_cache != null && _martenOptions.WriteBehind.EnableReadThrough)
            {
                _logger.LogTrace("Checking cache for grain {GrainId} in storage {StorageName}", grainId, _storageName);
                var cached = await TryReadCacheAsync<T>(grainId);
                if (cached != null)
                {
                    grainState.State = cached.Data;
                    grainState.ETag = cached.ETag;
                    grainState.RecordExists = true;

                    _logger.LogDebug("Cache hit for grain {GrainId} in storage {StorageName}", grainId,
                        _storageName);

                    return;
                }
            }

            await using var session = _martenOptions.UseTenantPerStorage
                ? _documentStore.QuerySession(_storageName)
                : _documentStore.QuerySession();
            var id = GenerateId(grainId);
            var document = await session.LoadAsync<MartenGrainData<T>>(id);

            if (document != null)
            {
                grainState.State = document.Data;
                grainState.RecordExists = true;
                grainState.ETag = document.Etag; // Generate the ETag from the state.

                // Warm cache after Marten read; failures must not discard an authoritative read.
                if (_cache != null && _martenOptions.WriteBehind.EnableReadThrough)
                {
                    await TryWarmCacheAsync(grainId, document.Data, grainState.ETag,
                        document.LastModified.ToUnixTimeMilliseconds());
                }
            }
            else
            {
                //Try with the old Id for Backward compatibility
                var oldId = grainId.ToString();
                document = await session.LoadAsync<MartenGrainData<T>>(oldId);
                if (document != null)
                {
                    //Migrate to new ID
                    await MigrateGrainStateAsync(grainState, document, id, oldId);
                }
                else
                {
                    // Orleans storage contract: State must be a usable instance even when no
                    // record exists, otherwise every grain has to null-guard on first activation.
                    grainState.State = CreateDefaultState<T>();
                    grainState.RecordExists = false;
                    grainState.ETag = null;
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "An error occurred executing {Method}- Error {Message}", nameof(ReadStateAsync),
                ex.Message);
            activity?.AddException(ex);
            // Never swallow: reporting "no state" on a failed read makes the grain overwrite live data.
            throw;
        }
        finally
        {
            activity?.Stop();
        }
    }

    private async Task MigrateGrainStateAsync<T>(IGrainState<T> grainState, MartenGrainData<T> document, string id,
        string oldId)
    {
        var newState = MartenGrainData<T>.Create(document.Data, id);
        await using var migrationSession = _martenOptions.UseTenantPerStorage
            ? _documentStore.LightweightSession(_storageName)
            : _documentStore.LightweightSession();
        migrationSession.Store(newState);
        await migrationSession.SaveChangesAsync();
        //Delete old document
        migrationSession.Delete<MartenGrainData<T>>(oldId);
        await migrationSession.SaveChangesAsync();
        grainState.State = newState.Data;
        grainState.RecordExists = true;
        grainState.ETag = newState.Etag; // Generate the ETag from the state.
    }

    public async Task WriteStateAsync<T>(string grainType, GrainId grainId, IGrainState<T> grainState)
    {
        using var activity = CreateLinkedDbActivity($"{_storageName}.WriteStateAsync", grainType);
        try
        {
            if (_logger.IsEnabled(LogLevel.Trace))
                _logger.LogTrace($"Writing state for grain {grainId} of type {grainType}.");

            var isOpaque = !typeof(T).IsVisible;
            if (isOpaque && grainState.State is null)
            {
                return;
            }

            var id = GenerateId(grainId);
            MartenGrainData<byte[]>? opaqueState = null;
            MartenGrainData<T>? state = null;
            string newETag;
            long lastModified;

            if (isOpaque)
            {
                var serializer = GetRequiredSerializer();
                opaqueState = MartenGrainData<byte[]>.Create(serializer.SerializeToArray(grainState.State), id);
                newETag = opaqueState.Etag;
                lastModified = opaqueState.LastModified.ToUnixTimeMilliseconds();
            }
            else
            {
                state = MartenGrainData<T>.Create(grainState.State, id);
                newETag = state.Etag;
                lastModified = state.LastModified.ToUnixTimeMilliseconds();
            }

            // Check write surge if write-behind is enabled
            if (_cache != null && _martenOptions.WriteBehind.EnableWriteBehind)
            {
                var writeCount = await _cache.IncrementWriteCounterAsync(_storageName);
                var overflow = writeCount > _martenOptions.WriteBehind.Threshold;

                if (overflow)
                {
                    if (_logger.IsEnabled(LogLevel.Debug))
                        _logger.LogDebug(
                            "Write overflow detected ({WriteCount} > {Threshold}), using write-behind for grain {GrainId}",
                            writeCount, _martenOptions.WriteBehind.Threshold, grainId);

                    // Write-behind path: cache only, mark dirty, skip DB
                    try
                    {
                        if (isOpaque)
                            await _cache.WriteAsync(_storageName, grainId, opaqueState!.Data, newETag, lastModified);
                        else
                            await _cache.WriteAsync(_storageName, grainId, grainState.State, newETag, lastModified);
                        await _cache.MarkDirtyAsync(_storageName, grainId);

                        grainState.ETag = newETag;
                        grainState.RecordExists = true;

                        _logger.LogTrace("Grain {GrainId} state written to cache and marked dirty", grainId);

                        return;
                    }
                    catch (Exception cacheEx)
                    {
                        _logger.LogError(cacheEx,
                            "Failed to write grain {GrainId} to cache during overflow, falling back to Marten",
                            grainId);
                        // Fall through to Marten write for durability
                    }
                }
            }

            if (isOpaque)
            {
                await WriteOpaqueStateAsync(grainId, grainState, opaqueState!);
                return;
            }

            // Write-through path: persist to Marten
            // If we have an existing record, validate ETag for optimistic concurrency
            if (grainState.RecordExists && grainState.ETag != null)
            {
                await using var readSession = _martenOptions.UseTenantPerStorage
                    ? _documentStore.QuerySession(_storageName)
                    : _documentStore.QuerySession();
                var existingDocument = await readSession.LoadAsync<MartenGrainData<T>>(id);

                if (existingDocument != null)
                {
                    var currentETag = existingDocument.Etag;
                    if (_martenOptions.CheckConcurrency && grainState.ETag != currentETag)
                    {
                        throw new InconsistentStateException(
                            $"ETag mismatch for grain {grainId}. Expected: {grainState.ETag}, Actual: {currentETag}");
                    }
                }
            }

            await using var session = _martenOptions.UseTenantPerStorage
                ? _documentStore.LightweightSession(_storageName)
                : _documentStore.LightweightSession();
            if (grainState.State is not null)
            {
                session.Store(state!);
                await session.SaveChangesAsync();
                grainState.ETag = newETag; // Update the ETag after successful write.
                grainState.RecordExists = true;

                // Update cache and ensure not marked dirty (write-through path)
                if (_cache != null && (_martenOptions.WriteBehind.EnableReadThrough ||
                                       _martenOptions.WriteBehind.EnableWriteBehind))
                {
                    await _cache.WriteAsync(_storageName, grainId, grainState.State, newETag, lastModified);
                    await _cache.ClearDirtyAsync(_storageName, grainId);
                }
            }
        }
        catch (Exception e)
        {
            _logger.LogCritical(e, "An error occurred executing {Method}- Error {Message}", nameof(WriteStateAsync),
                e.Message);
            // Rethrow the exception to propagate the error to the caller.
            activity?.AddException(e);
            throw;
        }
        finally
        {
            activity?.Stop();
        }
    }

    private async Task ReadOpaqueStateAsync<T>(
        IQuerySession session,
        GrainId grainId,
        IGrainState<T> grainState,
        string id)
    {
        var serializer = GetRequiredSerializer();
        if (_cache != null && _martenOptions.WriteBehind.EnableReadThrough)
        {
            var cached = await TryReadCacheAsync<byte[]>(grainId);
            if (cached is not null)
            {
                grainState.State = serializer.Deserialize<T>(cached.Data);
                grainState.RecordExists = true;
                grainState.ETag = cached.ETag;
                return;
            }
        }

        var document = await session.LoadAsync<MartenGrainData<byte[]>>(id);
        if (document is null)
        {
            var oldId = grainId.ToString();
            document = await session.LoadAsync<MartenGrainData<byte[]>>(oldId);
            if (document is null)
            {
                grainState.State = CreateDefaultState<T>();
                grainState.RecordExists = false;
                grainState.ETag = null;
                return;
            }

            var migrated = MartenGrainData<byte[]>.Create(document.Data, id);
            await using var migrationSession = _martenOptions.UseTenantPerStorage
                ? _documentStore.LightweightSession(_storageName)
                : _documentStore.LightweightSession();
            migrationSession.Store(migrated);
            migrationSession.Delete<MartenGrainData<byte[]>>(oldId);
            await migrationSession.SaveChangesAsync();
            document = migrated;
        }

        grainState.State = serializer.Deserialize<T>(document.Data);
        grainState.RecordExists = true;
        grainState.ETag = document.Etag;

        if (_cache != null && _martenOptions.WriteBehind.EnableReadThrough)
        {
            await TryWarmCacheAsync(
                grainId,
                document.Data,
                grainState.ETag,
                document.LastModified.ToUnixTimeMilliseconds());
        }
    }

    private async Task WriteOpaqueStateAsync<T>(
        GrainId grainId,
        IGrainState<T> grainState,
        MartenGrainData<byte[]> state)
    {
        var id = GenerateId(grainId);

        if (grainState.RecordExists && grainState.ETag is not null)
        {
            await using var readSession = _martenOptions.UseTenantPerStorage
                ? _documentStore.QuerySession(_storageName)
                : _documentStore.QuerySession();
            var existingDocument = await readSession.LoadAsync<MartenGrainData<byte[]>>(id);
            if (existingDocument is not null &&
                _martenOptions.CheckConcurrency &&
                grainState.ETag != existingDocument.Etag)
            {
                throw new InconsistentStateException(
                    $"ETag mismatch for grain {grainId}. Expected: {grainState.ETag}, Actual: {existingDocument.Etag}");
            }
        }

        await using var session = _martenOptions.UseTenantPerStorage
            ? _documentStore.LightweightSession(_storageName)
            : _documentStore.LightweightSession();
        session.Store(state);
        await session.SaveChangesAsync();

        grainState.ETag = state.Etag;
        grainState.RecordExists = true;

        if (_cache != null &&
            (_martenOptions.WriteBehind.EnableReadThrough || _martenOptions.WriteBehind.EnableWriteBehind))
        {
            await _cache.WriteAsync(
                _storageName,
                grainId,
                state.Data,
                state.Etag,
                state.LastModified.ToUnixTimeMilliseconds());
            await _cache.ClearDirtyAsync(_storageName, grainId);
        }
    }

    private Serializer GetRequiredSerializer() =>
        _serializer ?? throw new InvalidOperationException(
            "Orleans serializer is required to persist non-public grain state types.");

    /// <summary>
    /// Produces the empty state instance Orleans expects when no record exists. Delegates to the
    /// runtime's own activator so the grain observes exactly what <c>StateStorageBridge</c> would
    /// have created; the fallback mirrors it for state types with no public parameterless
    /// constructor. Never returns null for a reference type.
    /// </summary>
    private T CreateDefaultState<T>()
    {
        if (_activatorProvider is not null)
            return _activatorProvider.GetActivator<T>().Create();

        var type = typeof(T);
        return DefaultConstructible.GetOrAdd(
            type,
            static t => t.IsValueType || t.GetConstructor(Type.EmptyTypes) is not null)
            ? Activator.CreateInstance<T>()
            : (T)RuntimeHelpers.GetUninitializedObject(type);
    }

    /// <summary>
    /// Cache reads are an optimisation over the authoritative Marten document: an unavailable
    /// cache degrades to a slower read rather than failing the grain call.
    /// </summary>
    private async Task<CachedGrainState<T>?> TryReadCacheAsync<T>(GrainId grainId)
    {
        try
        {
            return await _cache!.ReadAsync<T>(_storageName, grainId);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Cache read failed for grain {GrainId} in storage {StorageName}; falling back to Marten",
                grainId, _storageName);
            return null;
        }
    }

    /// <summary>
    /// Best-effort cache warm-up after an authoritative read. A failure only costs the next read
    /// another Marten round trip, so it must never discard state we already loaded.
    /// </summary>
    private async Task TryWarmCacheAsync<T>(GrainId grainId, T data, string? etag, long lastModified)
    {
        try
        {
            await _cache!.WriteAsync(_storageName, grainId, data, etag!, lastModified);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Cache warm-up failed for grain {GrainId} in storage {StorageName}",
                grainId, _storageName);
        }
    }

    public void Participate(ISiloLifecycle lifecycle)
    {
        lifecycle.Subscribe(
            OptionFormattingUtilities.Name<MartenGrainStorage>(_storageName),
            ServiceLifecycleStage.RuntimeStorageServices,
            async ct =>
            {
                _logger.LogInformation("Adding Migrations");
                if (_environment.IsDevelopment())
                {
                    _documentStore.Options.DatabaseSchemaName = _storageName;
                    await _documentStore.Storage
                        .ApplyAllConfiguredChangesToDatabaseAsync(AutoCreate
                            .All); //RM for Production and use Marten migrations
                }
            });
    }


    private string GenerateId(GrainId grainId)
    {
        return $"{_clusterService}_{grainId.ToString().Replace('/', '_')}";
    }


    /// <summary>
    /// Creates a new Activity linked to the parent trace context stored in grain state
    /// </summary>
    private Activity? CreateLinkedDbActivity( string operationName, string stateName)
    {
        try
        {
            var parentContext = Activity.Current?.Context;

            var activity = parentContext is null
                ? _activitySource.StartActivity(operationName)
                : _activitySource.StartActivity(
                    operationName,
                    ActivityKind.Internal,
                    parentContext.Value);
            if (activity is null) return activity;
            activity.AddTag("db.name", stateName);
            activity.AddTag("db.system", "marten");
            activity.AddTag("db.operation", operationName);
            return activity;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to create linked activity for {Operation}", operationName);
            return null;
        }
    }
}
