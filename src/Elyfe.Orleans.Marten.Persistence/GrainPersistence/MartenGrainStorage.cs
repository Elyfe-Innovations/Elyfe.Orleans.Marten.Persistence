using System.Security.Cryptography;
using System.Text;
using JasperFx;
using Marten;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Configuration;
using Orleans.Runtime;
using Orleans.Storage;
using Weasel.Core.Migrations;

namespace Elyfe.Orleans.Marten.Persistence.GrainPersistence;

public class MartenGrainStorage(
    string storageName,
    IDocumentStore documentStore,
    ILogger<MartenGrainStorage> logger,
    IOptions<ClusterOptions> clusterOptions,
    IHostEnvironment environment)
    : IGrainStorage, ILifecycleParticipant<ISiloLifecycle>
{
    private readonly string _clusterService = clusterOptions.Value.ServiceId;

    public async Task ClearStateAsync<T>(string grainType, GrainId grainId, IGrainState<T> grainState)
    {
        if (logger.IsEnabled(LogLevel.Trace))
            logger.LogTrace($"Clearing state for grain {grainId} of type {grainType}.");

        await using var session = documentStore.LightweightSession();

        var id = GenerateId(grainId) ?? grainId.ToString();

        session.Delete<MartenGrainData<T>>(id);
        await session.SaveChangesAsync();
    }

    public async Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        try
        {
            if (logger.IsEnabled(LogLevel.Trace))
                logger.LogTrace($"Reading state for grain {grainId} of type {typeof(T).Name}.");


            await using var session = documentStore.QuerySession();
            var id = grainId.ToString();
            var document = await session.LoadAsync<MartenGrainData<T>>(id);

            if (document != null)
            {
                
                grainState.State = document.Data;
                grainState.RecordExists = true;
                grainState.ETag = GenerateETag(document); // Generate the ETag from the state.
            }
            else
            {
                //Try with the old Id for Backward compatibility
                var oldId = $"{grainId}";
                document = await session.LoadAsync<MartenGrainData<T>>(oldId);
                if (document != null)
                {
                    grainState.State = document.Data;
                    grainState.RecordExists = true;
                    grainState.ETag = GenerateETag(document); // Generate the ETag from the state.
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
            logger.LogCritical(ex, "An error occurred executing {Method}- Error {Message}", nameof(ReadStateAsync),
                ex.Message);
        }
    }

    public async Task WriteStateAsync<T>(string grainType, GrainId grainId, IGrainState<T> grainState)
    {
        if (logger.IsEnabled(LogLevel.Trace))
            logger.LogTrace($"Writing state for grain {grainId} of type {grainType}.");

        var id = GenerateId(grainId);

        // If we have an existing record, validate ETag for optimistic concurrency
        if (grainState.RecordExists && grainState.ETag != null)
        {
            await using var readSession = documentStore.QuerySession();
            var existingDocument = await readSession.LoadAsync<MartenGrainData<T>>(id);
            
            if (existingDocument != null)
            {
                var currentETag = GenerateETag(existingDocument);
                if (grainState.ETag != currentETag)
                {
                    throw new InconsistentStateException($"ETag mismatch for grain {grainId}. Expected: {grainState.ETag}, Actual: {currentETag}");
                }
            }
        }

        var state = MartenGrainData<T>.Create(grainState.State, id);
        var newETag = GenerateETag(state);

        await using var session = documentStore.LightweightSession();
        if (grainState.State is not null)
        {
            session.Store(state);
            await session.SaveChangesAsync();
            grainState.ETag = newETag; // Update the ETag after successful write.
            grainState.RecordExists = true;
        }
    }

    public void Participate(ISiloLifecycle lifecycle)
    {
        lifecycle.Subscribe(
            OptionFormattingUtilities.Name<MartenGrainStorage>(storageName),
            ServiceLifecycleStage.RuntimeStorageServices,
            async ct =>
            {
                logger.LogInformation("Adding Migrations");
                if (environment.IsDevelopment())
                {
                    documentStore.Options.DatabaseSchemaName = storageName;
                    await documentStore.Storage
                        .ApplyAllConfiguredChangesToDatabaseAsync(AutoCreate
                            .All); //RM for Production and use Marten migrations
                }
            });
    }

    private static string GenerateETag<T>(MartenGrainData<T> state)
    {
        // Generate ETag with LastModified and data hash for better collision resistance
        var lastModified = state.LastModified.ToUnixTimeMilliseconds();
        var dataJson = System.Text.Json.JsonSerializer.Serialize(state.Data);
        var combined = $"{lastModified}_{dataJson}";
        
        using var hash = SHA256.Create();
        var hashBytes = hash.ComputeHash(Encoding.UTF8.GetBytes(combined));
        return Convert.ToBase64String(hashBytes);
    }
    private string GenerateId(GrainId grainId)
    {
        return $"{_clusterService}_{grainId}";
    }
}