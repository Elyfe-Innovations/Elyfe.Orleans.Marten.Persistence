namespace Elyfe.Orleans.Marten.Persistence.Options;

/// <summary>
/// Configuration options for Marten grain storage.
/// </summary>
public class MartenStorageOptions
{
    /// <summary>
    /// When true, uses the storage name as the Marten tenant identifier.
    /// This allows different storage providers to use separate database schemas/tenants,
    /// leveraging Marten's built-in multi-tenancy features.
    /// Default: false (all storage providers use the default tenant).
    /// </summary>
    public bool UseTenantPerStorage { get; set; } = false;
    public string TenantIdKey { get; set; } = "TenantId";
    public bool CheckConcurrency { get; set; } = false;
    public WriteBehindOptions WriteBehind { get; set; } = new();

    private readonly HashSet<Type> _canonicalDurableStateTypes = [];

    /// <summary>
    /// Opts a migrated, public state type into canonical-ID-only, synchronous durable storage.
    /// Its MartenGrainData mapping must use a regular keyed table and optimistic concurrency.
    /// Before enabling, reconcile legacy identities and drain all old write-behind work.
    /// This mode bypasses both cache modes and always enforces database concurrency checks.
    /// </summary>
    public void EnableCanonicalDurableState<T>()
    {
        if (!typeof(T).IsVisible)
            throw new ArgumentException("Canonical durable storage requires a public state type.");

        _canonicalDurableStateTypes.Add(typeof(T));
    }

    internal bool IsCanonicalDurableState<T>() => _canonicalDurableStateTypes.Contains(typeof(T));
}