using Marten;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Configuration;
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
        builder.Services.AddElyfeMartenMembershipTable();
        return builder;
    }

    public static ISiloBuilder UseElyfeMartenClustering<TStore>(
        this ISiloBuilder builder,
        Action<ElyfeMartenClusteringOptions>? configure = null)
        where TStore : class, IDocumentStore
    {
        builder.Services.AddElyfeMartenClusteringCore(configure);
        builder.Services.AddElyfeMartenClusteringTypedStore<TStore>();
        builder.Services.AddElyfeMartenMembershipTable();
        return builder;
    }

    public static IClientBuilder UseElyfeMartenClustering(
        this IClientBuilder builder,
        Action<ElyfeMartenClusteringOptions>? configure = null)
    {
        builder.Services.AddElyfeMartenClusteringCore(configure);
        builder.Services.AddSingleton<IConfigureMarten, ElyfeMartenClusteringMartenConfiguration>();
        builder.Services.AddSingleton<IElyfeMartenClusteringStore, ElyfeMartenClusteringDefaultStore>();
        builder.Services.AddElyfeMartenGatewayListProvider();
        return builder;
    }

    public static IClientBuilder UseElyfeMartenClustering<TStore>(
        this IClientBuilder builder,
        Action<ElyfeMartenClusteringOptions>? configure = null)
        where TStore : class, IDocumentStore
    {
        builder.Services.AddElyfeMartenClusteringCore(configure);
        builder.Services.AddElyfeMartenClusteringTypedStore<TStore>();
        builder.Services.AddElyfeMartenGatewayListProvider();
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

    /// <summary>
    /// Registers the membership table through a factory because it takes the internal
    /// <see cref="IElyfeMartenClusteringStore"/> seam, and Microsoft DI can only construct public
    /// constructors when activating by type.
    /// </summary>
    private static IServiceCollection AddElyfeMartenMembershipTable(this IServiceCollection services) =>
        services.AddSingleton<IMembershipTable>(serviceProvider => new ElyfeMartenMembershipTable(
            serviceProvider.GetRequiredService<IElyfeMartenClusteringStore>(),
            serviceProvider.GetRequiredService<IOptions<ClusterOptions>>(),
            serviceProvider.GetRequiredService<ILogger<ElyfeMartenMembershipTable>>()));

    /// <summary>
    /// Same factory rationale as <see cref="AddElyfeMartenMembershipTable"/> for the client gateway list.
    /// </summary>
    private static IServiceCollection AddElyfeMartenGatewayListProvider(this IServiceCollection services) =>
        services.AddSingleton<IGatewayListProvider>(serviceProvider => new ElyfeMartenGatewayListProvider(
            serviceProvider.GetRequiredService<IElyfeMartenClusteringStore>(),
            serviceProvider.GetRequiredService<IOptions<ElyfeMartenClusteringOptions>>(),
            serviceProvider.GetRequiredService<IOptions<ClusterOptions>>()));
}
