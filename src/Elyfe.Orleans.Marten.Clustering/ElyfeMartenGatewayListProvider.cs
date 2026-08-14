using System.Net;
using Marten;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Configuration;
using Orleans.Messaging;
using Orleans.Runtime;

namespace Elyfe.Orleans.Marten.Clustering;

/// <summary>
/// Supplies cluster gateways to Orleans clients from the Marten membership documents.
/// </summary>
internal sealed class ElyfeMartenGatewayListProvider(
    IElyfeMartenClusteringStore storeProvider,
    IOptions<ElyfeMartenClusteringOptions> clusteringOptions,
    IOptions<ClusterOptions> clusterOptions) : IGatewayListProvider
{
    private readonly IDocumentStore _store = storeProvider.Store;
    private readonly string _clusterId = clusterOptions.Value.ClusterId;

    public TimeSpan MaxStaleness { get; } = clusteringOptions.Value.MaxStaleness;

    public bool IsUpdatable => true;

    public Task InitializeGatewayListProvider() => Task.CompletedTask;

    public async Task<IList<Uri>> GetGateways()
    {
        var activeStatus = SiloStatus.Active.ToString();

        await using var session = _store.QuerySession();
        var documents = await session.Query<ElyfeMartenMembershipDocument>()
            .Where(document =>
                document.ClusterId == _clusterId
                && document.Status == activeStatus
                && document.ProxyPort != 0)
            .ToListAsync();

        return documents.Select(ToGatewayUri).ToList();
    }

    private static Uri ToGatewayUri(ElyfeMartenMembershipDocument document)
    {
        var siloAddress = SiloAddress.FromParsableString(document.SiloAddress);
        var gatewayAddress = SiloAddress.New(
            new IPEndPoint(siloAddress.Endpoint.Address, document.ProxyPort),
            siloAddress.Generation);

        return gatewayAddress.ToGatewayUri();
    }
}
