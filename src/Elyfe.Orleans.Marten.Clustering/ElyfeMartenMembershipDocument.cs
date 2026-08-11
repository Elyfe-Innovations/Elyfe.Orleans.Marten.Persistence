namespace Elyfe.Orleans.Marten.Clustering;

/// <summary>
/// A single silo's membership entry. The identity is cluster scoped so one database can host
/// membership for several clusters (the platform shares one infrastructure database).
/// </summary>
public sealed class ElyfeMartenMembershipDocument
{
    public required string Id { get; set; }

    public required string ClusterId { get; set; }

    /// <summary>Parsable silo address, including generation.</summary>
    public required string SiloAddress { get; set; }

    public required string Status { get; set; }

    public int ProxyPort { get; set; }

    public string HostName { get; set; } = string.Empty;

    public string SiloName { get; set; } = string.Empty;

    public string RoleName { get; set; } = string.Empty;

    public int UpdateZone { get; set; }

    public int FaultZone { get; set; }

    /// <summary>Stored as an instant so PostgreSQL uses <c>timestamptz</c>; Orleans exchanges UTC <see cref="DateTime"/>.</summary>
    public DateTimeOffset StartTime { get; set; }

    /// <summary>Silo liveness heartbeat, written by <c>UpdateIAmAlive</c>.</summary>
    public DateTimeOffset IAmAliveTime { get; set; }

    public List<ElyfeMartenSuspectTime> SuspectTimes { get; set; } = [];

    /// <summary>Opaque row version handed to Orleans as the entry etag.</summary>
    public required string ETag { get; set; }

    public static string BuildId(string clusterId, string siloAddress) => $"{clusterId}:{siloAddress}";
}

/// <summary>
/// A suspicion recorded against a silo by one of its peers.
/// </summary>
public sealed class ElyfeMartenSuspectTime
{
    public required string SiloAddress { get; set; }

    public DateTimeOffset Time { get; set; }
}
