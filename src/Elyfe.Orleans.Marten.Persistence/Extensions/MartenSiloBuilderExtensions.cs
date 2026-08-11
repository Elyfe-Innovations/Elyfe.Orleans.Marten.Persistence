using Elyfe.Orleans.Marten.Persistence.Abstractions;
using Elyfe.Orleans.Marten.Persistence.GrainPersistence;
using Elyfe.Orleans.Marten.Persistence.Options;
using Marten;
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
    extension(ISiloBuilder builder)
    {
        public ISiloBuilder AddMartenGrainStorageAsDefault() =>
            builder.AddMartenGrainStorage<IDocumentStore>(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME);

        public ISiloBuilder AddMartenGrainStorageAsDefault<TStore>()
            where TStore : IDocumentStore =>
            builder.AddMartenGrainStorage<TStore>(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME);

        public ISiloBuilder UseMartenGrainStorage(string storageName = "Marten") =>
            builder.AddMartenGrainStorage<IDocumentStore>(storageName);

        public ISiloBuilder UseMartenGrainStorage<TStore>(string storageName = "Marten")
            where TStore : IDocumentStore =>
            builder.AddMartenGrainStorage<TStore>(storageName);

        public ISiloBuilder AddMartenGrainStorage(string storageName) =>
            builder.AddMartenGrainStorage<IDocumentStore>(storageName);

        public ISiloBuilder AddMartenGrainStorage<TStore>(string storageName)
            where TStore : IDocumentStore
        {
            return builder.ConfigureServices(services =>
            {
                services.Configure<MartenStorageOptions>(
                    builder.Configuration.GetSection("Orleans:Persistence:Marten"));
                services.AddMartenGrainStorage<TStore>(storageName);
            });
        }

        /// <summary>
        /// Configures Redis cache and write-behind for Marten grain storage.
        /// </summary>
        public ISiloBuilder AddMartenGrainStorageWithRedis(string storageName,
            Action<WriteBehindOptions>? configureOptions = null) =>
            builder.AddMartenGrainStorageWithRedis<IDocumentStore>(storageName, configureOptions);

        public ISiloBuilder AddMartenGrainStorageWithRedis<TStore>(string storageName,
            Action<WriteBehindOptions>? configureOptions = null)
            where TStore : IDocumentStore
        {
            return builder.ConfigureServices((services) =>
            {
                // Ensure MartenStorageOptions is configured
                services.AddOptions<MartenStorageOptions>().BindConfiguration("Orleans:Persistence:Marten");

                // Configure write-behind options
                var optionsBuilder = services.AddOptions<WriteBehindOptions>();
                optionsBuilder.BindConfiguration("Orleans:Persistence:Marten:WriteBehind");
                if (configureOptions != null)
                {
                    optionsBuilder.Configure(configureOptions);
                }

                // Register Redis if a connection string is provided
                var redisConnectionString = builder.Configuration.GetConnectionString("cache");
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
                        var options = sp.GetRequiredService<IOptions<MartenStorageOptions>>();
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
                services.AddMartenGrainStorage<TStore>(storageName);
            });
        }
    }


    extension(IServiceCollection services)
    {
        public IServiceCollection AddMartenGrainStorage(string storageName) =>
            services.AddMartenGrainStorage<IDocumentStore>(storageName);

        public IServiceCollection AddMartenGrainStorage<TStore>(string storageName)
            where TStore : IDocumentStore
        {
            return services.AddGrainStorage(storageName, MartenGrainStorageFactory.Create<TStore>);
        }
    }
}