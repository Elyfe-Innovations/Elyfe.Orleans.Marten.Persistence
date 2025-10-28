namespace Elyfe.Orleans.Marten.Persistence.Options;

/// <summary>
/// Configuration options for write-behind grain state caching.
/// </summary>
public class WriteBehindOptions
{
    /// <summary>
    /// Database number for write-behind cache.
    /// </summary>   
    public int CacheDatabase { get; set; } = 1;
    
    /// <summary>
    /// Global write threshold per second per storage before overflow to cache.
    /// </summary>
    public int Threshold { get; set; } = 100;

    /// <summary>
    /// Number of dirty grain states to drain in a single batch.
    /// </summary>
    public int BatchSize { get; set; } = 50;

    /// <summary>
    /// Interval between drain attempts in seconds.
    /// </summary>
    public int DrainIntervalSeconds { get; set; } = 5;

    /// <summary>
    /// TTL for cached state entries in seconds (0 = no expiration).
    /// </summary>
    public int StateTtlSeconds { get; set; } = 300;

    /// <summary>
    /// TTL for drain lock in seconds.
    /// </summary>
    public int DrainLockTtlSeconds { get; set; } = 30;

    /// <summary>
    /// Enable write-behind overflow to cache.
    /// </summary>
    public bool EnableWriteBehind { get; set; } = true;

    /// <summary>
    /// Enable read-through cache.
    /// </summary>
    public bool EnableReadThrough { get; set; } = true;
}

