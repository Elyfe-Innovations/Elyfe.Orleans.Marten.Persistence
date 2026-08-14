using Marten;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;

namespace Elyfe.Orleans.Marten.Reminders;

public static class ElyfeMartenReminderServiceExtensions
{
    public static ISiloBuilder UseElyfeMartenReminderService(
        this ISiloBuilder builder,
        Action<ElyfeMartenReminderOptions>? configure = null)
    {
        builder.Services.UseElyfeMartenReminderService(configure);
        return builder;
    }
    public static ISiloBuilder UseElyfeMartenReminderService<TStore>(
        this ISiloBuilder builder,
        Action<ElyfeMartenReminderOptions>? configure = null)
        where TStore : class, IDocumentStore
    {
        builder.Services.UseElyfeMartenReminderService<TStore>(configure);
        return builder;
    }


    public static IServiceCollection UseElyfeMartenReminderService(
        this IServiceCollection services,
        Action<ElyfeMartenReminderOptions>? configure = null)
    {
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.AddSingleton<IValidateOptions<ElyfeMartenReminderOptions>, ElyfeMartenReminderOptionsValidator>();
        services.AddSingleton<IConfigureMarten, ElyfeMartenReminderMartenConfiguration>();
        services.AddSingleton<IElyfeMartenReminderStore, ElyfeMartenReminderDefaultStore>();
        services.AddElyfeMartenReminderTable();
        services.AddReminders();
        return services;
    }

    public static IServiceCollection UseElyfeMartenReminderService<TStore>(
        this IServiceCollection services,
        Action<ElyfeMartenReminderOptions>? configure = null)
        where TStore : class, IDocumentStore
    {
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.AddSingleton<IValidateOptions<ElyfeMartenReminderOptions>, ElyfeMartenReminderOptionsValidator>();
        services.AddSingleton<ElyfeMartenReminderMartenConfiguration>();
        services.ConfigureMarten<TStore>(
            (serviceProvider, options) =>
                serviceProvider.GetRequiredService<ElyfeMartenReminderMartenConfiguration>()
                    .Configure(serviceProvider, options));
        services.AddSingleton<IElyfeMartenReminderStore>(
            serviceProvider => new ElyfeMartenReminderTypedStore<TStore>(
                serviceProvider.GetRequiredService<TStore>()));
        services.AddElyfeMartenReminderTable();
        services.AddReminders();
        return services;
    }

    /// <summary>
    /// Registers the reminder table through a factory because it takes the internal
    /// <see cref="IElyfeMartenReminderStore"/> seam, and Microsoft DI can only construct public
    /// constructors when activating by type.
    /// </summary>
    private static IServiceCollection AddElyfeMartenReminderTable(this IServiceCollection services) =>
        services.AddSingleton<IReminderTable>(serviceProvider => new ElyfeMartenReminderTable(
            serviceProvider.GetRequiredService<IElyfeMartenReminderStore>(),
            serviceProvider.GetRequiredService<IOptions<ElyfeMartenReminderOptions>>(),
            serviceProvider.GetRequiredService<IOptions<ClusterOptions>>(),
            serviceProvider.GetRequiredService<ILogger<ElyfeMartenReminderTable>>()));
}
