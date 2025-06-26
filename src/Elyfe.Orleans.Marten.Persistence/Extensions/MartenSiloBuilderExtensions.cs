using Elyfe.Orleans.Marten.Persistence.GrainPersistence;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Providers;
using Orleans.Runtime.Hosting;

namespace Elyfe.Orleans.Marten.Persistence.Extensions;

public static class MartenSiloBuilderExtensions
{
    public static ISiloBuilder AddMartenGrainStorageAsDefault(this ISiloBuilder builder) => builder.UseMartenGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME);

    public static ISiloBuilder UseMartenGrainStorage(this ISiloBuilder siloBuilder, string storageName = "Marten")
    {
        siloBuilder.ConfigureServices(services => services.AddMartenGrainStorage(storageName));
        return siloBuilder;
    }

    public static IServiceCollection AddMartenGrainStorage(this IServiceCollection services, string storageName)
    {
        return services.AddGrainStorage(storageName, MartenGrainStorageFactory.Create);
    }

    public static ISiloBuilder AddMartenGrainStorage(this ISiloBuilder siloBuilder, string storageName)
    {
        return siloBuilder.ConfigureServices(services => services.AddMartenGrainStorage(storageName));
    }
}