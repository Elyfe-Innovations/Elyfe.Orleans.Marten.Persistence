using Elyfe.Orleans.Marten.Persistence.Options;
using Marten;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Storage;

namespace Elyfe.Orleans.Marten.Persistence.GrainPersistence;

internal static class MartenGrainStorageFactory
{
    internal static IGrainStorage Create(
        IServiceProvider services, string name)
    {
        // Ensure MartenStorageOptions is available
        var martenOptions = services.GetService<IOptions<MartenStorageOptions>>() 
            ?? Microsoft.Extensions.Options.Options.Create(new MartenStorageOptions());

        var martenGrainStorage = ActivatorUtilities.CreateInstance<MartenGrainStorage>(
            services,
            name,
            services.GetRequiredService<IDocumentStore>(),
            services,
            services.GetRequiredService<ILogger<MartenGrainStorage>>(),
            services.GetRequiredService<IOptions<ClusterOptions>>(),
            services.GetRequiredService<IHostEnvironment>());
        return martenGrainStorage;
    }
}