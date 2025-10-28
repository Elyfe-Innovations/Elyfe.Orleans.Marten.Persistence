using Elyfe.Orleans.Marten.Persistence.Abstractions;
using Elyfe.Orleans.Marten.Persistence.GrainPersistence;
using Elyfe.Orleans.Marten.Persistence.Options;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Providers;
using Orleans.Runtime.Hosting;
using StackExchange.Redis;

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
        return siloBuilder.ConfigureServices(services =>
        {
            // Ensure MartenStorageOptions is configured (with defaults if not already configured)
            services.AddOptions<MartenStorageOptions>().BindConfiguration("MartenStorage");
            services.AddMartenGrainStorage(storageName);
        });
    }

    /// <summary>
    /// Configures Redis cache and write-behind for Marten grain storage.
    /// </summary>
    public static ISiloBuilder AddMartenGrainStorageWithRedis(
        this ISiloBuilder siloBuilder, 
        string storageName,
        Action<WriteBehindOptions>? configureOptions = null)
    {
        return siloBuilder.ConfigureServices((services) =>
        {
            // Ensure MartenStorageOptions is configured
            services.AddOptions<MartenStorageOptions>().BindConfiguration("MartenStorage");

            // Configure write-behind options
            var optionsBuilder = services.AddOptions<WriteBehindOptions>();
            optionsBuilder.BindConfiguration("WriteBehind");
            if (configureOptions != null)
            {
                optionsBuilder.Configure(configureOptions);
            }

            // Register Redis if a connection string is provided
            var redisConnectionString = siloBuilder.Configuration.GetConnectionString("cache");
            if (!string.IsNullOrEmpty(redisConnectionString))
            {
                services.AddKeyedSingleton<IConnectionMultiplexer>("writeBack",(s, p) =>
                {
                    var config = ConfigurationOptions.Parse(redisConnectionString);
                    return ConnectionMultiplexer.Connect(config);
                });

                // Register cache with service ID from cluster options
                services.AddSingleton<IGrainStateCache>(sp =>
                {
                    var redis = sp.GetRequiredKeyedService<IConnectionMultiplexer>("writeBack");
                    var logger = sp.GetRequiredService<ILogger<RedisGrainStateCache>>();
                    var options = sp.GetRequiredService<IOptions<WriteBehindOptions>>();
                    var clusterOptions = sp.GetRequiredService<IOptions<ClusterOptions>>();
                    
                    return new RedisGrainStateCache(redis, logger, options, clusterOptions.Value.ServiceId);
                });

                // Register drainer hosted service
                services.AddHostedService<CacheToMartenWriter>();
            }
            else
            {
                // No Redis configured - disable cache features
                services.Configure<WriteBehindOptions>(opt =>
                {
                    opt.EnableReadThrough = false;
                    opt.EnableWriteBehind = false;
                });
            }
            
            // Add Marten storage
            services.AddMartenGrainStorage(storageName);
        });
    }
}