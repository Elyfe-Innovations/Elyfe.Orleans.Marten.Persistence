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
        IServiceProvider services,
        string name) =>
        Create<IDocumentStore>(services, name);

    internal static IGrainStorage Create<TStore>(
        IServiceProvider services,
        string name)
        where TStore : IDocumentStore
    {
        var store = services.GetRequiredService<TStore>();
        var storage = ActivatorUtilities.CreateInstance<MartenGrainStorage>(
            services,
            name,
            store,
            services,
            services.GetRequiredService<ILogger<MartenGrainStorage>>(),
            services.GetRequiredService<IOptions<ClusterOptions>>(),
            services.GetRequiredService<IHostEnvironment>());

        services.GetService<CacheToMartenWriter>()?.RegisterStorage(name, store);
        return storage;
    }
}