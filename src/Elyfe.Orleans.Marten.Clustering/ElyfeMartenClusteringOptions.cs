namespace Elyfe.Orleans.Marten.Clustering;

/// <summary>
/// Options controlling how Orleans membership is stored in Marten.
/// </summary>
public sealed class ElyfeMartenClusteringOptions
{
    /// <summary>
    /// PostgreSQL schema that owns the clustering documents.
    /// </summary>
    public string DatabaseSchemaName { get; set; } = "clustering";

    /// <summary>
    /// Marten document alias for silo membership entries.
    /// </summary>
    public string MembershipDocumentAlias { get; set; } = "orleans_membership";

    /// <summary>
    /// Marten document alias for the per-cluster membership table version.
    /// </summary>
    public string ClusterVersionDocumentAlias { get; set; } = "orleans_cluster_version";

    /// <summary>
    /// How long a client may reuse a cached gateway list before refreshing it.
    /// </summary>
    public TimeSpan MaxStaleness { get; set; } = TimeSpan.FromSeconds(60);
}
