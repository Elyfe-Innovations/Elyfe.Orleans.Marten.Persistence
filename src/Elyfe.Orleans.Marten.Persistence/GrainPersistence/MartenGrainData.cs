using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace Elyfe.Orleans.Marten.Persistence.GrainPersistence;

public class MartenGrainData<TData>
{
    public required TData Data { get; set; }
    public required string Id { get; set; }
    public DateTimeOffset LastModified { get; set; }

    public string Etag => GenerateETag();
    public string GenerateETag()
    {
        // Generate ETag with LastModified and data hash for better collision resistance
        var lastModified = LastModified.ToUnixTimeMilliseconds();
        var dataJson = JsonSerializer.Serialize(Data);
        var combined = $"{lastModified}_{dataJson}";
        
        using var hash = SHA256.Create();
        var hashBytes = hash.ComputeHash(Encoding.UTF8.GetBytes(combined));
        return Convert.ToBase64String(hashBytes);
    }
    
    public static MartenGrainData<TData> Create(TData data, string id)
    {
        return new MartenGrainData<TData> { Data = data, Id = id, LastModified = DateTimeOffset.Now };
    }
}