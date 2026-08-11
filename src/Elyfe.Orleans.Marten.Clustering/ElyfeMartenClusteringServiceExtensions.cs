using Marten;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Hosting;
using Orleans.Messaging;
using Orleans.Runtime;

namespace Elyfe.Orleans.Marten.Clustering;

/// <summary>
/// Registers Marten-backed Orleans clustering. The typed overloads pin membership to a specific
/// Marten store, which is how a platform keeps cluster infrastructure out of its domain databases.
/// </summary>
public static class ElyfeMartenClusteringServiceExtensions
{
    public static ISiloBuilder UseElyfeMartenClustering(
        this ISiloBuilder builder,
        Action<ElyfeMartenClusteringOptions>? configure = null)
    {
        builder.Services.AddElyfeMartenClusteringCore(configure);
        builder.Services.AddSingleton<IConfigureMarten, ElyfeMartenClusteringMartenConfiguration>();
        builder.Services.AddSingleton<IElyfeMartenClusteringStore, ElyfeMartenClusteringDefaultStore>();
        builder.Services.AddSingleton<IMembershipTable, ElyfeMartenMembershipTable>();
        return builder;
    }

    public static ISiloBuilder UseElyfeMartenClustering<TStore>(
        this ISiloBuilder builder,
        Action<ElyfeMartenClusteringOptions>? configure = null)
        where TStore : class, IDocumentStore
    {
        builder.Services.AddElyfeMartenClusteringCore(configure);
        builder.Services.AddElyfeMartenClusteringTypedStore<TStore>();
        builder.Services.AddSingleton<IMembershipTable, ElyfeMartenMembershipTable>();
        return builder;
    }

    public static IClientBuilder UseElyfeMartenClustering(
        this IClientBuilder builder,
        Action<ElyfeMartenClusteringOptions>? configure = null)
    {
        builder.Services.AddElyfeMartenClusteringCore(configure);
        builder.Services.AddSingleton<IConfigureMarten, ElyfeMartenClusteringMartenConfiguration>();
        builder.Services.AddSingleton<IElyfeMartenClusteringStore, ElyfeMartenClusteringDefaultStore>();
        builder.Services.AddSingleton<IGatewayListProvider, ElyfeMartenGatewayListProvider>();
        return builder;
    }

    public static IClientBuilder UseElyfeMartenClustering<TStore>(
        this IClientBuilder builder,
        Action<ElyfeMartenClusteringOptions>? configure = null)
        where TStore : class, IDocumentStore
    {
        builder.Services.AddElyfeMartenClusteringCore(configure);
        builder.Services.AddElyfeMartenClusteringTypedStore<TStore>();
        builder.Services.AddSingleton<IGatewayListProvider, ElyfeMartenGatewayListProvider>();
        return builder;
    }

    private static IServiceCollection AddElyfeMartenClusteringCore(
        this IServiceCollection services,
        Action<ElyfeMartenClusteringOptions>? configure)
    {
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.AddSingleton<
            IValidateOptions<ElyfeMartenClusteringOptions>,
            ElyfeMartenClusteringOptionsValidator>();
        return services;
    }

    private static IServiceCollection AddElyfeMartenClusteringTypedStore<TStore>(this IServiceCollection services)
        where TStore : class, IDocumentStore
    {
        services.AddSingleton<ElyfeMartenClusteringMartenConfiguration>();
        services.ConfigureMarten<TStore>(
            (serviceProvider, options) =>
                serviceProvider.GetRequiredService<ElyfeMartenClusteringMartenConfiguration>()
                    .Configure(serviceProvider, options));
        services.AddSingleton<IElyfeMartenClusteringStore>(
            serviceProvider => new ElyfeMartenClusteringTypedStore<TStore>(
                serviceProvider.GetRequiredService<TStore>()));
        return services;
    }
}
