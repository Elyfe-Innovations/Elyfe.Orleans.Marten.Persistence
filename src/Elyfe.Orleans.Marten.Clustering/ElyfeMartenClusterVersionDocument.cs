namespace Elyfe.Orleans.Marten.Clustering;

/// <summary>
/// Per-cluster membership table version. Orleans uses this as the compare-and-swap anchor for every
/// membership mutation, so it is stored as its own document keyed by cluster id.
/// </summary>
public sealed class ElyfeMartenClusterVersionDocument
{
    public required string Id { get; set; }

    public required string ClusterId { get; set; }

    public int Version { get; set; }

    /// <summary>Opaque version handed to Orleans as <c>TableVersion.VersionEtag</c>.</summary>
    public required string ETag { get; set; }
}
