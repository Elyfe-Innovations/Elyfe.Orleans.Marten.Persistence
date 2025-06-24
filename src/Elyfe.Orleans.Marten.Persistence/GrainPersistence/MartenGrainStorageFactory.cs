using Marten;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Storage;

namespace Elyfe.Orleans.Marten.Persistence.GrainPersistence;

internal static class MartenGrainStorageFactory
{
    internal static IGrainStorage Create(
        IServiceProvider services, string name)
    {
        return ActivatorUtilities.CreateInstance<MartenGrainStorage>(
            services,
            name,
            services.GetRequiredService<IDocumentStore>(),
            services.GetRequiredService<ILogger<MartenGrainStorage>>());
    }
}