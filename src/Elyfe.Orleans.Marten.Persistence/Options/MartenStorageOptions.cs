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
    public WriteBehindOptions WriteBehind { get; set; } = new();
}