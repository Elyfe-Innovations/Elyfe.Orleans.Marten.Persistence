using System.Security.Cryptography;
using System.Text;
using JasperFx;
using Marten;
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
    IOptions<ClusterOptions> clusterOptions)
    : IGrainStorage, ILifecycleParticipant<ISiloLifecycle>
{
    private readonly string _clusterService = clusterOptions.Value.ServiceId;
    
    public async Task ClearStateAsync<T>(string grainType, GrainId grainReference, IGrainState<T> grainState)
    {
        if (logger.IsEnabled(LogLevel.Trace))
            logger.LogTrace($"Clearing state for grain {grainReference} of type {grainType}.");

        await using var session = documentStore.LightweightSession();

        var id = grainReference.ToString();

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
                #pragma warning disable CS8601 // Possible null reference assignment.
                grainState.State = default;
#pragma warning restore CS8601 // Possible null reference assignment.
                grainState.RecordExists = false;
                grainState.ETag = null;
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

        var id = grainId.ToString();

        var state = MartenGrainData<T>.Create(grainState.State, id);

        var newETag = GenerateETag(state);

        // Optional: Implement optimistic concurrency check using ETag.
        // if (grainState.ETag != null && grainState.ETag != newETag)
        // {
        //     throw new InconsistentStateException($"ETag mismatch for grain {grainId.ToString()}.");
        // }

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
                //if (environment.IsDevelopment())
                //{
                documentStore.Options.DatabaseSchemaName = storageName;
                await documentStore.Storage.Database.WriteMigrationFileAsync("1.Hello.sql", ct);
                await documentStore.Storage
                    .ApplyAllConfiguredChangesToDatabaseAsync(AutoCreate
                        .All); //RM for Production and use Marten migrations
                //}
            });
    }

    private static string GenerateETag<T>(MartenGrainData<T> state)
    {
        //Generate Etag with LastModified
        var lastModified = state.LastModified.ToUnixTimeMilliseconds();
        var hash = SHA256.Create().ComputeHash(Encoding.UTF8.GetBytes(lastModified.ToString()));
        return Convert.ToBase64String(hash);
    }
}