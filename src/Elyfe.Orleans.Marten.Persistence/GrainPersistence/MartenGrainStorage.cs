using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Elyfe.Orleans.Marten.Persistence.Abstractions;
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

namespace Elyfe.Orleans.Marten.Persistence.GrainPersistence;

public class MartenGrainStorage : IGrainStorage, ILifecycleParticipant<ISiloLifecycle>
{
    private readonly string _clusterService;
    // private readonly IDocumentStore _documentStore = services.GetKeyedService<IDocumentStore>(storageName) ?? documentStore;
    private readonly IGrainStateCache? _cache;
    private readonly WriteBehindOptions _options;
    private readonly string _storageName;
    private readonly IDocumentStore _documentStore;
    private readonly ILogger<MartenGrainStorage> _logger;
    private readonly IHostEnvironment _environment;

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
        _options = services.GetService<IOptions<WriteBehindOptions>>()?.Value ?? new WriteBehindOptions();
        services.GetService<CacheToMartenWriter>()?.RegisterStorage(_storageName);
        
    }

    public async Task ClearStateAsync<T>(string grainType, GrainId grainId, IGrainState<T> grainState)
    {
        if (_logger.IsEnabled(LogLevel.Trace))
            _logger.LogTrace($"Clearing state for grain {grainId} of type {grainType}.");

        await using var session = _documentStore.LightweightSession();
        var id = GenerateId(grainId);
        session.Delete<MartenGrainData<T>>(id);
        await session.SaveChangesAsync();
    }

    public async Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        try
        {
            if (_logger.IsEnabled(LogLevel.Trace))
                _logger.LogTrace($"Reading state for grain {grainId} of type {typeof(T).Name}.");

            // Read-through cache: check cache first if enabled
            if (_cache != null && _options.EnableReadThrough)
            {
                var cached = await _cache.ReadAsync<T>(_storageName, grainId);
                if (cached != null)
                {
                    grainState.State = cached.Data;
                    grainState.ETag = cached.ETag;
                    grainState.RecordExists = true;
                    
                    if (_logger.IsEnabled(LogLevel.Debug))
                        _logger.LogDebug("Cache hit for grain {GrainId} in storage {StorageName}", grainId, _storageName);
                    
                    return;
                }
            }

            await using var session = _documentStore.QuerySession();
            var id = GenerateId(grainId);
            var document = await session.LoadAsync<MartenGrainData<T>>(id);

            if (document != null)
            {
                grainState.State = document.Data;
                grainState.RecordExists = true;
                grainState.ETag = document.Etag; // Generate the ETag from the state.
                
                // Warm cache after Marten read
                if (_cache != null && _options.EnableReadThrough)
                {
                    await _cache.WriteAsync(_storageName, grainId, document.Data, grainState.ETag, document.LastModified.ToUnixTimeMilliseconds());
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
#pragma warning disable CS8601 // Possible null reference assignment.
                    grainState.State = default;
#pragma warning restore CS8601 // Possible null reference assignment.
                    grainState.RecordExists = false;
                    grainState.ETag = null;
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "An error occurred executing {Method}- Error {Message}", nameof(ReadStateAsync),
                ex.Message);
        }
    }

    private async Task MigrateGrainStateAsync<T>(IGrainState<T> grainState, MartenGrainData<T> document, string id, string oldId)
    {
        var newState = MartenGrainData<T>.Create(document.Data, id);
        await using var migrationSession = _documentStore.LightweightSession();
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
        try
        {
            if (_logger.IsEnabled(LogLevel.Trace))
                _logger.LogTrace($"Writing state for grain {grainId} of type {grainType}.");

            var id = GenerateId(grainId);
            var state = MartenGrainData<T>.Create(grainState.State, id);
            var newETag = state.Etag;
            var lastModified = state.LastModified.ToUnixTimeMilliseconds();

            // Check write surge if write-behind is enabled
            bool overflow = false;
            if (_cache != null && _options.EnableWriteBehind)
            {
                var writeCount = await _cache.IncrementWriteCounterAsync(_storageName);
                overflow = writeCount > _options.Threshold;

                if (overflow)
                {
                    if (_logger.IsEnabled(LogLevel.Debug))
                        _logger.LogDebug("Write overflow detected ({WriteCount} > {Threshold}), using write-behind for grain {GrainId}", 
                            writeCount, _options.Threshold, grainId);

                    // Write-behind path: cache only, mark dirty, skip DB
                    try
                    {
                        await _cache.WriteAsync(_storageName, grainId, grainState.State, newETag, lastModified);
                        await _cache.MarkDirtyAsync(_storageName, grainId);
                        
                        grainState.ETag = newETag;
                        grainState.RecordExists = true;
                        
                        if (_logger.IsEnabled(LogLevel.Trace))
                            _logger.LogTrace("Grain {GrainId} state written to cache and marked dirty", grainId);
                        
                        return;
                    }
                    catch (Exception cacheEx)
                    {
                        _logger.LogError(cacheEx, "Failed to write grain {GrainId} to cache during overflow, falling back to Marten", grainId);
                        // Fall through to Marten write for durability
                    }
                }
            }

            // Write-through path: persist to Marten
            // If we have an existing record, validate ETag for optimistic concurrency
            if (grainState.RecordExists && grainState.ETag != null)
            {
                await using var readSession = _documentStore.QuerySession();
                var existingDocument = await readSession.LoadAsync<MartenGrainData<T>>(id);
            
                if (existingDocument != null)
                {
                    var currentETag = existingDocument.Etag;
                    /*if (grainState.ETag != currentETag)
                {
                    throw new InconsistentStateException($"ETag mismatch for grain {grainId}. Expected: {grainState.ETag}, Actual: {currentETag}");
                }*/
                }
            }

            await using var session = _documentStore.LightweightSession();
            if (grainState.State is not null)
            {
                session.Store(state);
                await session.SaveChangesAsync();
                grainState.ETag = newETag; // Update the ETag after successful write.
                grainState.RecordExists = true;

                // Update cache and ensure not marked dirty (write-through path)
                if (_cache != null && (_options.EnableReadThrough || _options.EnableWriteBehind))
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
            throw;
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
}