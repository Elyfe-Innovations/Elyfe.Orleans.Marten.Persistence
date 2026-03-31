using System.Buffers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace Elyfe.Orleans.Marten.Persistence.GrainPersistence;

public class MartenGrainData<TData>
{
    public required TData Data { get; set; }
    public required string Id { get; set; }
    public DateTimeOffset LastModified { get; set; }

    private string? _cachedEtag;

    public string Etag => _cachedEtag ??= GenerateETag();
    public string GenerateETag()
    {
        // Stream data directly into the hash to avoid allocating the entire JSON as a string.
        // The previous approach serialized the full state to a string, then interpolated it,
        // then converted to bytes — 3x the state size in allocations, causing OOM on large grains.
        var lastModified = LastModified.ToUnixTimeMilliseconds();

        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);

        // Hash the timestamp prefix
        Span<byte> prefixBytes = stackalloc byte[32];
        var prefix = $"{lastModified}_";
        var prefixByteCount = Encoding.UTF8.GetBytes(prefix, prefixBytes);
        hash.AppendData(prefixBytes[..prefixByteCount]);

        // Stream-serialize data directly into the hash via a buffer
        var bufferWriter = new ArrayBufferWriter<byte>(4096);
        using (var writer = new Utf8JsonWriter(bufferWriter))
        {
            JsonSerializer.Serialize(writer, Data);
        }

        hash.AppendData(bufferWriter.WrittenSpan);

        Span<byte> hashBytes = stackalloc byte[SHA256.HashSizeInBytes];
        hash.GetHashAndReset(hashBytes);
        _cachedEtag = Convert.ToBase64String(hashBytes);
        return _cachedEtag;
    }
    
    public static MartenGrainData<TData> Create(TData data, string id)
    {
        return new MartenGrainData<TData> { Data = data, Id = id, LastModified = DateTimeOffset.Now };
    }
}