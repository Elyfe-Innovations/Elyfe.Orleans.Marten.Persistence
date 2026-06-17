using Microsoft.Extensions.Options;

namespace Elyfe.Orleans.Marten.Reminders;

internal sealed class ElyfeMartenReminderOptionsValidator : IValidateOptions<ElyfeMartenReminderOptions>
{
    public ValidateOptionsResult Validate(string? name, ElyfeMartenReminderOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            return ValidateOptionsResult.Fail("A PostgreSQL connection string is required for Elyfe Marten reminders.");
        }

        if (string.IsNullOrWhiteSpace(options.SchemaName))
        {
            return ValidateOptionsResult.Fail("A schema name is required for Elyfe Marten reminders.");
        }

        if (string.IsNullOrWhiteSpace(options.TableName))
        {
            return ValidateOptionsResult.Fail("A table name is required for Elyfe Marten reminders.");
        }

        if (options.CommandTimeoutSeconds <= 0)
        {
            return ValidateOptionsResult.Fail("CommandTimeoutSeconds must be greater than zero.");
        }

        if (options.BulkBatchSize <= 0)
        {
            return ValidateOptionsResult.Fail("BulkBatchSize must be greater than zero.");
        }

        return ValidateOptionsResult.Success;
    }
}
