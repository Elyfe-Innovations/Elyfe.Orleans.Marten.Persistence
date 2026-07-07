using Microsoft.Extensions.Options;

namespace Elyfe.Orleans.Marten.Reminders;

internal sealed class ElyfeMartenReminderOptionsValidator : IValidateOptions<ElyfeMartenReminderOptions>
{
    public ValidateOptionsResult Validate(string? name, ElyfeMartenReminderOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.SchemaName))
        {
            return ValidateOptionsResult.Fail("A schema name is required for Elyfe Marten reminders.");
        }

        if (string.IsNullOrWhiteSpace(options.DocumentAlias))
        {
            return ValidateOptionsResult.Fail("A Marten document alias is required for Elyfe Marten reminders.");
        }

        if (options.CommandTimeoutSeconds <= 0)
        {
            return ValidateOptionsResult.Fail("CommandTimeoutSeconds must be greater than zero.");
        }

        return ValidateOptionsResult.Success;
    }
}
