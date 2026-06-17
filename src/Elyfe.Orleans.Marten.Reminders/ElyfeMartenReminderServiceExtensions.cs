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

    public static IServiceCollection UseElyfeMartenReminderService(
        this IServiceCollection services,
        Action<ElyfeMartenReminderOptions>? configure = null)
    {
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.AddSingleton<IReminderTable, ElyfeMartenReminderTable>();
        services.AddSingleton<IValidateOptions<ElyfeMartenReminderOptions>, ElyfeMartenReminderOptionsValidator>();
        services.AddReminders();
        return services;
    }
}
