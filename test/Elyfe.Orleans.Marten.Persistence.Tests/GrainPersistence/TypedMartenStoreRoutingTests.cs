using AwesomeAssertions;
using Elyfe.Orleans.Marten.Persistence.Extensions;
using Marten;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Hosting;
using Moq;
using Orleans.Configuration;
using Orleans.Storage;
using Xunit;

namespace Elyfe.Orleans.Marten.Persistence.Tests.GrainPersistence;

public sealed class TypedMartenStoreRoutingTests
{
    [Fact]
    public void NamedProvidersResolveTheirDeclaredDocumentStores()
    {
        var smsStore = new Mock<ISmsStore>().Object;
        var financeStore = new Mock<IFinanceStore>().Object;
        var services = CreateServices();
        services.AddSingleton(smsStore);
        services.AddSingleton(financeStore);
        services.AddMartenGrainStorage<ISmsStore>("sms");
        services.AddMartenGrainStorage<IFinanceStore>("finance");

        using var provider = services.BuildServiceProvider();

        var smsStorage = provider.GetRequiredKeyedService<IGrainStorage>("sms");
        var financeStorage = provider.GetRequiredKeyedService<IGrainStorage>("finance");

        GetDocumentStore(smsStorage).Should().BeSameAs(smsStore);
        GetDocumentStore(financeStorage).Should().BeSameAs(financeStore);
    }

    [Fact]
    public void MissingDeclaredDocumentStoreFailsProviderResolution()
    {
        var services = CreateServices();
        services.AddMartenGrainStorage<ISmsStore>("sms");

        using var provider = services.BuildServiceProvider();

        var resolve = () => provider.GetRequiredKeyedService<IGrainStorage>("sms");
        resolve.Should().Throw<InvalidOperationException>();
    }

    private static ServiceCollection CreateServices()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddOptions();
        services.Configure<ClusterOptions>(options => options.ServiceId = "typed-store-tests");
        services.AddSingleton<IHostEnvironment>(new TestHostEnvironment());
        return services;
    }

    private static IDocumentStore GetDocumentStore(IGrainStorage storage)
    {
        var field = storage.GetType().GetField(
            "_documentStore",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);

        field.Should().NotBeNull();
        return field!.GetValue(storage).Should().BeAssignableTo<IDocumentStore>().Subject;
    }

    public interface ISmsStore : IDocumentStore;

    public interface IFinanceStore : IDocumentStore;

    private sealed class TestHostEnvironment : IHostEnvironment
    {
        public string EnvironmentName { get; set; } = Environments.Development;
        public string ApplicationName { get; set; } = "TypedStoreTests";
        public string ContentRootPath { get; set; } = string.Empty;
        public IFileProvider ContentRootFileProvider { get; set; } = null!;
    }
}
