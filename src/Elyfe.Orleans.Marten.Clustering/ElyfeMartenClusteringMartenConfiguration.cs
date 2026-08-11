using Marten;
using Microsoft.Extensions.Options;

namespace Elyfe.Orleans.Marten.Clustering;

internal sealed class ElyfeMartenClusteringMartenConfiguration(
    IOptions<ElyfeMartenClusteringOptions> optionsAccessor) : IConfigureMarten
{
    public void Configure(IServiceProvider services, StoreOptions options)
    {
        var clusteringOptions = optionsAccessor.Value;

        // Optimistic concurrency is what makes the membership compare-and-swap safe: Marten guards
        // every update with the version it loaded, so a lost race fails instead of overwriting.
        options.Schema.For<ElyfeMartenClusterVersionDocument>()
            .DatabaseSchemaName(clusteringOptions.DatabaseSchemaName)
            .DocumentAlias(clusteringOptions.ClusterVersionDocumentAlias)
            .Identity(document => document.Id)
            .UseOptimisticConcurrency(true);

        options.Schema.For<ElyfeMartenMembershipDocument>()
            .DatabaseSchemaName(clusteringOptions.DatabaseSchemaName)
            .DocumentAlias(clusteringOptions.MembershipDocumentAlias)
            .Identity(document => document.Id)
            .UseOptimisticConcurrency(true)
            .Index(document => document.ClusterId)
            .Index(document => document.Status);
    }
}
