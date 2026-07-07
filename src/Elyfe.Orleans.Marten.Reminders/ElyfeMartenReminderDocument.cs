using Orleans.Runtime;

namespace Elyfe.Orleans.Marten.Reminders;

public sealed class ElyfeMartenReminderDocument
{
    public required string Id { get; set; }

    public required string ServiceId { get; set; }

    public required string ClusterId { get; set; }

    public required string GrainId { get; set; }

    public long GrainHash { get; set; }

    public required string ReminderName { get; set; }

    public DateTime StartAt { get; set; }

    public long PeriodTicks { get; set; }

    public required string ETag { get; set; }

    public string ProviderVersion { get; set; } = "1";

    public static string BuildId(string serviceId, GrainId grainId, string reminderName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reminderName);
        return $"{serviceId}:{grainId}:{reminderName}";
    }

    public ReminderEntry ToReminderEntry()
    {
        return new ReminderEntry
        {
            GrainId = global::Orleans.Runtime.GrainId.Parse(GrainId),
            ReminderName = ReminderName,
            StartAt = StartAt,
            Period = TimeSpan.FromTicks(PeriodTicks),
            ETag = ETag
        };
    }
}
