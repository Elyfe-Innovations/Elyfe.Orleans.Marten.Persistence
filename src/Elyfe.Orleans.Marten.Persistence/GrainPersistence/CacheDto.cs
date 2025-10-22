using System.Text.Json;
using System.Text.Json.Serialization;

namespace Elyfe.Orleans.Marten.Persistence.GrainPersistence;

public class CacheDto<T>
{
    [JsonIgnore]
    public Type CacheType { get; set; } = null!;

    public string? TypeString
    {
        get => CacheType.AssemblyQualifiedName;
        set
        {
            if(value is null)
                return;
            CacheType = Type.GetType(value!)!;
        }
    }
    public string SerializedData { get; set; } = null!;

    public T? GetData()
    {
        var data = JsonSerializer.Deserialize(SerializedData, CacheType, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });
        return data is T typedData ? typedData : default;
    }

    public CacheDto()
    {
        
    }

    public CacheDto(T value, string etag, long lastModified)
    {
        CacheType = typeof(T);
        SerializedData = JsonSerializer.Serialize(value, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });
        ETag = etag;
        LastModified = lastModified;
    }


    public string ETag { get; init; } = null!;
    public long LastModified { get; init; }
}