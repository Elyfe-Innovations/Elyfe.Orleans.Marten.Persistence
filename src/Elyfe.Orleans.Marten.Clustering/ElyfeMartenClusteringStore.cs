using Marten;

namespace Elyfe.Orleans.Marten.Clustering;

/// <summary>
/// Resolves the Marten store that owns Orleans clustering. An explicit abstraction keeps the
/// provider from depending on whichever store happens to be registered as the unkeyed default,
/// which is what lets membership be pinned to a platform-owned typed store.
/// </summary>
internal interface IElyfeMartenClusteringStore
{
    IDocumentStore Store { get; }
}

internal sealed class ElyfeMartenClusteringDefaultStore(IDocumentStore store) : IElyfeMartenClusteringStore
{
    public IDocumentStore Store { get; } = store;
}

internal sealed class ElyfeMartenClusteringTypedStore<TStore>(TStore store) : IElyfeMartenClusteringStore
    where TStore : class, IDocumentStore
{
    public IDocumentStore Store { get; } = store;
}
