using Marten;

namespace Elyfe.Orleans.Marten.Reminders;

/// <summary>
/// Resolves the Marten store that owns Orleans reminders. An explicit abstraction keeps the reminder
/// table from depending on whichever store happens to be registered as the unkeyed default, which is
/// what lets reminders be pinned to a dedicated typed store.
/// </summary>
internal interface IElyfeMartenReminderStore
{
    IDocumentStore Store { get; }
}

internal sealed class ElyfeMartenReminderDefaultStore(IDocumentStore store) : IElyfeMartenReminderStore
{
    public IDocumentStore Store { get; } = store;
}

internal sealed class ElyfeMartenReminderTypedStore<TStore>(TStore store) : IElyfeMartenReminderStore
    where TStore : class, IDocumentStore
{
    public IDocumentStore Store { get; } = store;
}