using Marten;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Hosting;

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
        services.AddSingleton<IReminderTable, ElyfeMartenReminderTable>();
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
        services.AddSingleton<IReminderTable, ElyfeMartenReminderTable>();
        services.AddReminders();
        return services;
    }
}
