using Marten;
using Microsoft.Extensions.Options;

namespace Elyfe.Orleans.Marten.Reminders;

internal sealed class ElyfeMartenReminderMartenConfiguration(IOptions<ElyfeMartenReminderOptions> optionsAccessor) : IConfigureMarten
{
    public void Configure(IServiceProvider services, StoreOptions options)
    {
        var reminderOptions = optionsAccessor.Value;

        options.Schema.For<ElyfeMartenReminderDocument>()
            .DatabaseSchemaName(reminderOptions.SchemaName)
            .DocumentAlias(reminderOptions.DocumentAlias)
            .Identity(document => document.Id)
            .Index(document => document.ServiceId)
            .Index(document => document.GrainId)
            .Index(document => document.GrainHash)
            .Index(document => document.StartAt);
    }
}
