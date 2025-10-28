namespace Elyfe.Orleans.Marten.Persistence.Tests.GrainPersistence;

public class TestState
{
    public string Name { get; set; } = string.Empty;
    public int Value { get; set; }
    public string? TextValue { get; set; }
}
