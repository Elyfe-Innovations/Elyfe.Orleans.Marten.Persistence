using Orleans.Runtime;

namespace Elyfe.Orleans.Marten.Persistence.Abstractions;

/// <summary>
/// Abstraction for grain state caching with write-behind support.
/// </summary>
public interface IGrainStateCache
{
    /// <summary>
    /// Reads cached grain state.
    /// </summary>
    /// <returns>Cached state DTO or null if not found.</returns>
    Task<CachedGrainState<T>?> ReadAsync<T>(string storageName, GrainId grainId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Writes grain state to cache.
    /// </summary>
    Task WriteAsync<T>(string storageName, GrainId grainId, T state, string etag, long lastModified, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes grain state from cache.
    /// </summary>
    Task RemoveAsync(string storageName, GrainId grainId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Marks a grain as dirty (pending write-behind to persistent store).
    /// </summary>
    Task MarkDirtyAsync(string storageName, GrainId grainId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Clears dirty marker after successful persistence.
    /// </summary>
    Task ClearDirtyAsync(string storageName, GrainId grainId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns whether a write-behind write is currently pending for the grain.
    /// </summary>
    /// <remarks>
    /// Implementations MUST propagate failures rather than returning a value: callers use this to decide
    /// between falling back to Marten (which is only safe when nothing newer is pending in the cache) and
    /// refusing to serve possibly stale state, so an error is materially different from a false answer.
    /// </remarks>
    Task<bool> IsDirtyAsync(string storageName, GrainId grainId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets a batch of dirty grain keys for draining.
    /// </summary>
    Task<IReadOnlyList<string>> GetDirtyKeysAsync(string storageName, int batchSize, CancellationToken cancellationToken = default);

    /// <summary>
    /// Increments the write counter and returns the current count.
    /// </summary>
    Task<long> IncrementWriteCounterAsync(string storageName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Attempts to acquire a distributed lock for draining.
    /// </summary>
    Task<bool> TryAcquireDrainLockAsync(string storageName, TimeSpan lockTtl, CancellationToken cancellationToken = default);

    /// <summary>
    /// Releases the drain lock.
    /// </summary>
    Task ReleaseDrainLockAsync(string storageName, CancellationToken cancellationToken = default);
}

/// <summary>
/// Cached grain state DTO.
/// </summary>
public record CachedGrainState<T>(T Data, string ETag, long LastModified, Type stateType);

