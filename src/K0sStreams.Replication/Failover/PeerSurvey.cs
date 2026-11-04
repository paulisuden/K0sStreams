using K0sStreams.Contracts;
using K0sStreams.Contracts.Grpc;
using K0sStreams.Replication.Common;
using K0sStreams.Replication.Follower;
using K0sStreams.Replication.Network;

namespace K0sStreams.Replication.Failover;

/// <summary>Where one broker's log of a topic partition ends, and how to reach that broker.</summary>
internal sealed record PartitionTip(int Partition, LogTip Tip);

/// <summary>
/// Survey answer from one broker: where each partition of a topic ends.
/// </summary>
internal sealed record SurveyAnswer(string NodeId, IReplicationPeer Peer, IReadOnlyList<PartitionTip> PartitionTips);

/// <summary>
/// Step 1 of a promotion: tells this broker and a majority of the others that a new epoch started, so from then on they
/// refuse the old leader, and finds out what each of them has. This broker answers through its own
/// <see cref="ReplicationNode"/>, the same way the others do.
/// </summary>
internal sealed class PeerSurvey
{
    private readonly ReplicationNode _self;
    private readonly string _selfId;
    private readonly KeyValuePair<string, IReplicationPeer>[] _peers;
    private readonly int _needed;
    private readonly MajorityCall _majority;
    private readonly EpochTracker _epochs;
    private readonly ITopicCatalog _catalog;

    public PeerSurvey(
        ReplicationNode self, IClusterState cluster, ITopicCatalog catalog, IReadOnlyDictionary<string, IReplicationPeer> peers, MajorityCall majority, EpochTracker epochs)
    {
        ArgumentNullException.ThrowIfNull(peers);
        ArgumentNullException.ThrowIfNull(catalog);
        _self = self;
        _selfId = cluster.NodeId;
        _peers = [.. peers.OrderBy(static peer => peer.Key, StringComparer.Ordinal)];
        _needed = (_peers.Length + 1) / 2;
        _majority = majority;
        _epochs = epochs;
        _catalog = catalog;
    }

    /// <summary>The most complete log for a specific partition; on a tie the earlier answer wins.</summary>
    public static PartitionTip MostCompleteForPartition(IReadOnlyList<SurveyAnswer> answers, int partition)
    {
        ArgumentNullException.ThrowIfNull(answers);
        var best = answers[0].PartitionTips.First(p => p.Partition == partition);
        foreach (var answer in answers)
        {
            var tip = answer.PartitionTips.First(p => p.Partition == partition);
            if (tip.Tip.IsMoreCompleteThan(best.Tip))
            {
                best = tip;
            }
        }

        return best;
    }

    /// <summary>Every topic that this broker or the answering majority holds a log for.</summary>
    public async Task<IReadOnlyCollection<string>> ListTopicsAsync(long epoch, CancellationToken ct)
    {
        var request = new ListTopicsRequest { Epoch = epoch };
        var topics = new SortedSet<string>(StringComparer.Ordinal);
        topics.UnionWith((await _self.ListTopicsAsync(request, ct).ConfigureAwait(false)).Topics);
        var answers = await _majority.AskAsync(_peers, _needed, async (peer, token) =>
        {
            var response = await peer.ListTopicsAsync(request, token).ConfigureAwait(false);
            EnsureNotNewer(response.Epoch, epoch);
            return response;
        }, ct).ConfigureAwait(false);

        foreach (var answer in answers)
        {
            topics.UnionWith(answer.Answer.Topics);
        }

        return topics;
    }

    /// <summary>Announces <paramref name="epoch"/> for <paramref name="topic"/> and returns where each log ends, this broker's first.</summary>
    public async Task<IReadOnlyList<SurveyAnswer>> SurveyAsync(string topic, long epoch, CancellationToken ct)
    {
        TopicConfig? config = _catalog.Find(topic);
        if (config is null)
        {
            // Topic not in catalog yet (catalog is eventually consistent), default to 1 partition for the survey.
            // The actual partition count is determined by what peers report during the survey.
            config = new TopicConfig(topic, 1, 3, TimeSpan.FromSeconds(5));
        }

        var answers = new List<SurveyAnswer>();
        var selfTips = new List<PartitionTip>(config.Partitions);
        for (int partition = 0; partition < config.Partitions; partition++)
        {
            var tip = await AskTipAsync(_self, topic, partition, epoch, ct).ConfigureAwait(false);
            selfTips.Add(new PartitionTip(partition, tip.Tip));
        }
        answers.Add(new SurveyAnswer(_selfId, _self, selfTips));

        var peerTips = await _majority.AskAsync(_peers, _needed, (peer, token) => AskAllPartitionsAsync(peer, topic, config.Partitions, epoch, token), ct)
            .ConfigureAwait(false);

        foreach (var peerTip in peerTips)
        {
            answers.Add(new SurveyAnswer(peerTip.NodeId, peerTip.Peer, peerTip.Answer));
        }
        return answers;
    }

    private async Task<IReadOnlyList<PartitionTip>> AskAllPartitionsAsync(IReplicationPeer peer, string topic, int partitions, long epoch, CancellationToken ct)
    {
        var tips = new List<PartitionTip>(partitions);
        for (int partition = 0; partition < partitions; partition++)
        {
            var tip = await AskTipAsync(peer, topic, partition, epoch, ct).ConfigureAwait(false);
            tips.Add(new PartitionTip(partition, tip.Tip));
        }
        return tips;
    }

    /// <summary>
    /// An empty Append at the new epoch, like a new leader's first heartbeat. The broker adopts the epoch under the
    /// topic's lock, so an append from the old leader still running there is either counted in the answer or refused
    /// afterwards. Then reads the epoch of its last record.
    /// </summary>
    private async Task<PartitionTip> AskTipAsync(IReplicationPeer peer, string topic, int partition, long epoch, CancellationToken ct)
    {
        var announce = new AppendRequest { Epoch = epoch, Topic = topic, Partition = partition, PrevOffset = -1, LeaderHw = -1, LeaderId = _selfId };
        var response = await peer.AppendAsync(announce, ct).ConfigureAwait(false);
        EnsureNotNewer(response.Epoch, epoch);
        if (!response.Ok)
        {
            throw new InvalidOperationException($"Announcing epoch {epoch} for {topic} partition {partition} failed with {response.Code}: {response.Error}");
        }

        var tip = response.EndOffset < 0 ? LogTip.Empty : await LastRecordAsync(peer, topic, partition, epoch, response.EndOffset, ct).ConfigureAwait(false);
        return new PartitionTip(partition, tip);
    }

    private static async Task<LogTip> LastRecordAsync(IReplicationPeer peer, string topic, int partition, long epoch, long endOffset, CancellationToken ct)
    {
        var request = new FetchRequest { Epoch = epoch, Topic = topic, Partition = partition, FromOffset = endOffset, MaxRecords = 1 };
        await foreach (var batch in peer.FetchAsync(request, ct).ConfigureAwait(false))
        {
            foreach (var bytes in batch.Records)
            {
                var record = RecordBytes.Decode(bytes);
                return new LogTip(record.Offset, record.Epoch);
            }
        }

        throw new InvalidOperationException($"The log of {topic} partition {partition} shrank while it was being surveyed.");
    }

    /// <summary>A broker at a newer epoch means another leader took over: remember that epoch and give up.</summary>
    private void EnsureNotNewer(long answerEpoch, long epoch)
    {
        if (answerEpoch > epoch)
        {
            _epochs.Observe(answerEpoch);
            throw new NotLeaderException($"A broker is at epoch {answerEpoch}, newer than {epoch}.");
        }
    }
}