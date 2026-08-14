using JasperFx;
using Marten;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using Orleans;
using Orleans.Configuration;
using Orleans.Runtime;

namespace Elyfe.Orleans.Marten.Clustering;

/// <summary>
/// Orleans membership table backed by Marten documents.
/// </summary>
/// <remarks>
/// Every mutation is a compare-and-swap over two documents: the per-cluster version document and the
/// silo's own membership row. Orleans depends on those writes being atomic and on a lost race
/// returning <c>false</c> rather than throwing, so each mutation runs in one Marten session and
/// translates a concurrency failure into <c>false</c>.
/// </remarks>
internal sealed class ElyfeMartenMembershipTable(
    IElyfeMartenClusteringStore storeProvider,
    IOptions<ClusterOptions> clusterOptions,
    ILogger<ElyfeMartenMembershipTable> logger) : IMembershipTable
{
    private readonly IDocumentStore _store = storeProvider.Store;
    private readonly string _clusterId = clusterOptions.Value.ClusterId;

    public async Task InitializeMembershipTable(bool tryInitTableVersion)
    {
        if (!tryInitTableVersion)
        {
            return;
        }

        await using var session = _store.LightweightSession();
        if (await session.LoadAsync<ElyfeMartenClusterVersionDocument>(_clusterId) is not null)
        {
            return;
        }

        session.Insert(new ElyfeMartenClusterVersionDocument
        {
            Id = _clusterId,
            ClusterId = _clusterId,
            Version = 0,
            ETag = NewETag()
        });

        try
        {
            await session.SaveChangesAsync();
        }
        catch (Exception ex) when (IsConcurrencyFailure(ex))
        {
            // Another silo initialised the cluster version first, which is the expected race.
            logger.LogDebug("Cluster version for {ClusterId} was initialised concurrently.", _clusterId);
        }
    }

    public async Task<MembershipTableData> ReadRow(SiloAddress key)
    {
        await using var session = _store.QuerySession();
        var tableVersion = ToTableVersion(
            await session.LoadAsync<ElyfeMartenClusterVersionDocument>(_clusterId));

        var document = await session.LoadAsync<ElyfeMartenMembershipDocument>(
            ElyfeMartenMembershipDocument.BuildId(_clusterId, key.ToParsableString()));

        return document is null
            ? new MembershipTableData(tableVersion)
            : new MembershipTableData(ToEntryTuple(document), tableVersion);
    }

    public async Task<MembershipTableData> ReadAll()
    {
        await using var session = _store.QuerySession();
        var tableVersion = ToTableVersion(
            await session.LoadAsync<ElyfeMartenClusterVersionDocument>(_clusterId));

        var documents = await session.Query<ElyfeMartenMembershipDocument>()
            .Where(document => document.ClusterId == _clusterId)
            .ToListAsync();

        return new MembershipTableData(documents.Select(ToEntryTuple).ToList(), tableVersion);
    }

    public async Task<bool> InsertRow(MembershipEntry entry, TableVersion tableVersion)
    {
        ArgumentNullException.ThrowIfNull(entry);

        await using var session = _store.LightweightSession();
        var versionDocument = await session.LoadAsync<ElyfeMartenClusterVersionDocument>(_clusterId);
        if (!MatchesVersion(versionDocument, tableVersion))
        {
            return false;
        }

        var id = ElyfeMartenMembershipDocument.BuildId(_clusterId, entry.SiloAddress.ToParsableString());
        if (await session.LoadAsync<ElyfeMartenMembershipDocument>(id) is not null)
        {
            return false;
        }

        session.Insert(ToDocument(id, _clusterId, entry, NewETag()));
        ApplyVersion(session, versionDocument!, tableVersion);

        return await TrySaveAsync(session, nameof(InsertRow));
    }

    public async Task<bool> UpdateRow(MembershipEntry entry, string etag, TableVersion tableVersion)
    {
        ArgumentNullException.ThrowIfNull(entry);

        await using var session = _store.LightweightSession();
        var versionDocument = await session.LoadAsync<ElyfeMartenClusterVersionDocument>(_clusterId);
        if (!MatchesVersion(versionDocument, tableVersion))
        {
            return false;
        }

        var id = ElyfeMartenMembershipDocument.BuildId(_clusterId, entry.SiloAddress.ToParsableString());
        var existing = await session.LoadAsync<ElyfeMartenMembershipDocument>(id);
        if (existing is null || !string.Equals(existing.ETag, etag, StringComparison.Ordinal))
        {
            return false;
        }

        CopyInto(existing, entry);
        existing.ETag = NewETag();
        session.Update(existing);
        ApplyVersion(session, versionDocument!, tableVersion);

        return await TrySaveAsync(session, nameof(UpdateRow));
    }

    public async Task UpdateIAmAlive(MembershipEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);

        // Liveness only. Deliberately does not touch the table version: bumping it here would make
        // every heartbeat look like a membership change and churn the whole cluster.
        await using var session = _store.LightweightSession();
        var id = ElyfeMartenMembershipDocument.BuildId(_clusterId, entry.SiloAddress.ToParsableString());
        var existing = await session.LoadAsync<ElyfeMartenMembershipDocument>(id);
        if (existing is null)
        {
            return;
        }

        existing.IAmAliveTime = ToInstant(entry.IAmAliveTime);
        session.Update(existing);

        try
        {
            await session.SaveChangesAsync();
        }
        catch (Exception ex) when (IsConcurrencyFailure(ex))
        {
            // A concurrent writer already advanced this row; the next heartbeat re-reports liveness.
            logger.LogDebug("IAmAlive update for {SiloAddress} lost a race.", entry.SiloAddress);
        }
    }

    public async Task CleanupDefunctSiloEntries(DateTimeOffset beforeDate)
    {
        var deadStatus = SiloStatus.Dead.ToString();

        await using var session = _store.LightweightSession();
        session.DeleteWhere<ElyfeMartenMembershipDocument>(document =>
            document.ClusterId == _clusterId
            && document.Status == deadStatus
            && document.IAmAliveTime < beforeDate);
        await session.SaveChangesAsync();
    }

    public async Task DeleteMembershipTableEntries(string clusterId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);

        await using var session = _store.LightweightSession();
        session.DeleteWhere<ElyfeMartenMembershipDocument>(document => document.ClusterId == clusterId);
        session.Delete<ElyfeMartenClusterVersionDocument>(clusterId);
        await session.SaveChangesAsync();
    }

    private async Task<bool> TrySaveAsync(IDocumentSession session, string operation)
    {
        try
        {
            await session.SaveChangesAsync();
            return true;
        }
        catch (Exception ex) when (IsConcurrencyFailure(ex))
        {
            logger.LogDebug("{Operation} lost a membership race for cluster {ClusterId}.", operation, _clusterId);
            return false;
        }
    }

    private static void ApplyVersion(
        IDocumentSession session,
        ElyfeMartenClusterVersionDocument versionDocument,
        TableVersion tableVersion)
    {
        versionDocument.Version = tableVersion.Version;
        versionDocument.ETag = NewETag();
        session.Update(versionDocument);
    }

    private static bool MatchesVersion(ElyfeMartenClusterVersionDocument? document, TableVersion tableVersion) =>
        document is not null && string.Equals(document.ETag, tableVersion.VersionEtag, StringComparison.Ordinal);

    private static bool IsConcurrencyFailure(Exception exception) => exception switch
    {
        ConcurrencyException => true,
        PostgresException postgres when postgres.SqlState == PostgresErrorCodes.UniqueViolation => true,
        AggregateException aggregate => aggregate.InnerExceptions.Any(IsConcurrencyFailure),
        _ => false
    };

    private static TableVersion ToTableVersion(ElyfeMartenClusterVersionDocument? document) =>
        document is null
            ? new TableVersion(0, "0")
            : new TableVersion(document.Version, document.ETag);

    private static Tuple<MembershipEntry, string> ToEntryTuple(ElyfeMartenMembershipDocument document) =>
        Tuple.Create(ToEntry(document), document.ETag);

    private static MembershipEntry ToEntry(ElyfeMartenMembershipDocument document)
    {
        var entry = new MembershipEntry
        {
            SiloAddress = SiloAddress.FromParsableString(document.SiloAddress),
            Status = Enum.Parse<SiloStatus>(document.Status),
            ProxyPort = document.ProxyPort,
            HostName = document.HostName,
            SiloName = document.SiloName,
            RoleName = document.RoleName,
            UpdateZone = document.UpdateZone,
            FaultZone = document.FaultZone,
            StartTime = document.StartTime.UtcDateTime,
            IAmAliveTime = document.IAmAliveTime.UtcDateTime
        };

        if (document.SuspectTimes.Count > 0)
        {
            entry.SuspectTimes = document.SuspectTimes
                .Select(suspect => Tuple.Create(
                    SiloAddress.FromParsableString(suspect.SiloAddress),
                    suspect.Time.UtcDateTime))
                .ToList();
        }

        return entry;
    }

    private static ElyfeMartenMembershipDocument ToDocument(
        string id,
        string clusterId,
        MembershipEntry entry,
        string etag)
    {
        var document = new ElyfeMartenMembershipDocument
        {
            Id = id,
            ClusterId = clusterId,
            SiloAddress = entry.SiloAddress.ToParsableString(),
            Status = entry.Status.ToString(),
            ETag = etag
        };

        CopyInto(document, entry);
        return document;
    }

    private static void CopyInto(ElyfeMartenMembershipDocument document, MembershipEntry entry)
    {
        document.SiloAddress = entry.SiloAddress.ToParsableString();
        document.Status = entry.Status.ToString();
        document.ProxyPort = entry.ProxyPort;
        document.HostName = entry.HostName ?? string.Empty;
        document.SiloName = entry.SiloName ?? string.Empty;
        document.RoleName = entry.RoleName ?? string.Empty;
        document.UpdateZone = entry.UpdateZone;
        document.FaultZone = entry.FaultZone;
        document.StartTime = ToInstant(entry.StartTime);
        document.IAmAliveTime = ToInstant(entry.IAmAliveTime);
        document.SuspectTimes = entry.SuspectTimes is null
            ? []
            : entry.SuspectTimes
                .Select(suspect => new ElyfeMartenSuspectTime
                {
                    SiloAddress = suspect.Item1.ToParsableString(),
                    Time = ToInstant(suspect.Item2)
                })
                .ToList();
    }

    /// <summary>
    /// Orleans exchanges <see cref="DateTime"/>; PostgreSQL rejects <see cref="DateTimeKind.Utc"/> values
    /// for <c>timestamp without time zone</c>, so instants are normalised to UTC offsets on the way in.
    /// </summary>
    private static DateTimeOffset ToInstant(DateTime value) => value.Kind switch
    {
        DateTimeKind.Utc => new DateTimeOffset(value, TimeSpan.Zero),
        DateTimeKind.Local => new DateTimeOffset(value).ToUniversalTime(),
        _ => new DateTimeOffset(DateTime.SpecifyKind(value, DateTimeKind.Utc), TimeSpan.Zero)
    };

    private static string NewETag() => Guid.NewGuid().ToString("N");
}
