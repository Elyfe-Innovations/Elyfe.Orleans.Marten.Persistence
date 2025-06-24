namespace Elyfe.Orleans.Marten.Persistence.GrainPersistence;

public class MartenGrainData<TData>
{
    public required TData Data { get; set; }
    public required string Id { get; set; }
    public DateTimeOffset LastModified { get; set; }

    public static MartenGrainData<TData> Create(TData data, string id)
    {
        return new MartenGrainData<TData> { Data = data, Id = id, LastModified = DateTimeOffset.Now };
    }
}