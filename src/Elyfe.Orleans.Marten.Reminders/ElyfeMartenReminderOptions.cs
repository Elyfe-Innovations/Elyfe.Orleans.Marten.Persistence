namespace Elyfe.Orleans.Marten.Reminders;

public sealed class ElyfeMartenReminderOptions
{
    public const string SectionName = "Orleans:Reminders:Marten";

    public string ConnectionString { get; set; } = string.Empty;

    public string SchemaName { get; set; } = "reminders";

    public string TableName { get; set; } = "orleans_reminders";

    public bool AutoCreateSchema { get; set; }

    public bool PreferTimescale { get; set; } = true;

    public TimeSpan TimescaleChunkInterval { get; set; } = TimeSpan.FromDays(7);

    public int CommandTimeoutSeconds { get; set; } = 30;

    public int BulkBatchSize { get; set; } = 500;
}
