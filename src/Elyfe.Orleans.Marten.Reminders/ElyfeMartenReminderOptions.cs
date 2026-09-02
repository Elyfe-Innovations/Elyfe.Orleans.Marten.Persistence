namespace Elyfe.Orleans.Marten.Reminders;

public sealed class ElyfeMartenReminderOptions
{
    public const string SectionName = "Orleans:Reminders:Marten";

    public string SchemaName { get; set; } = "reminders";

    public string DocumentAlias { get; set; } = "orleans_reminders";

    public bool AutoCreateSchema { get; set; }

    public int CommandTimeoutSeconds { get; set; } = 30;
}
