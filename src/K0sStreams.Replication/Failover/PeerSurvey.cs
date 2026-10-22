using K0sStreams.Contracts;
using K0sStreams.Contracts.Grpc;
using K0sStreams.Replication.Common;
using K0sStreams.Replication.Follower;
using K0sStreams.Replication.Network;

namespace K0sStreams.Replication.Failover;

/// <summary>Where one broker's log of a topic ends, and how to reach that broker.</summary>
internal sealed record SurveyAnswer(string NodeId, IReplicationPeer Peer, LogTip Tip);

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

    public PeerSurvey(
        ReplicationNode self, IClusterState cluster, IReadOnlyDictionary<string, IReplicationPeer> peers, MajorityCall majority, EpochTracker epochs)
    {
        ArgumentNullException.ThrowIfNull(peers);
        _self = self;
        _selfId = cluster.NodeId;
        _peers = [.. peers.OrderBy(static peer => peer.Key, StringComparer.Ordinal)];
        _needed = (_peers.Length + 1) / 2;
        _majority = majority;
        _epochs = epochs;
    }

    /// <summary>The most complete log; on a tie the earlier answer, so this broker (always first) keeps its own.</summary>
    public static SurveyAnswer MostComplete(IReadOnlyList<SurveyAnswer> answers)
    {
        ArgumentNullException.ThrowIfNull(answers);
        var best = answers[0];
        foreach (var answer in answers)
        {
            if (answer.Tip.IsMoreCompleteThan(best.Tip))
            {
                best = answer;
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
        var answers = new List<SurveyAnswer> { new(_selfId, _self, await AskTipAsync(_self, topic, epoch, ct).ConfigureAwait(false)) };
        var peerAnswers = await _majority.AskAsync(_peers, _needed, (peer, token) => AskTipAsync(peer, topic, epoch, token), ct)
            .ConfigureAwait(false);
        answers.AddRange(peerAnswers.Select(static answer => new SurveyAnswer(answer.NodeId, answer.Peer, answer.Answer)));
        return answers;
    }

    /// <summary>
    /// An empty Append at the new epoch, like a new leader's first heartbeat. The broker adopts the epoch under the
    /// topic's lock, so an append from the old leader still running there is either counted in the answer or refused
    /// afterwards. Then reads the epoch of its last record.
    /// </summary>
    private async Task<LogTip> AskTipAsync(IReplicationPeer peer, string topic, long epoch, CancellationToken ct)
    {
        var announce = new AppendRequest { Epoch = epoch, Topic = topic, PrevOffset = -1, LeaderHw = -1, LeaderId = _selfId };
        var response = await peer.AppendAsync(announce, ct).ConfigureAwait(false);
        EnsureNotNewer(response.Epoch, epoch);
        if (!response.Ok)
        {
            throw new InvalidOperationException($"Announcing epoch {epoch} for {topic} failed with {response.Code}: {response.Error}");
        }

        return response.EndOffset < 0 ? LogTip.Empty : await LastRecordAsync(peer, topic, epoch, response.EndOffset, ct).ConfigureAwait(false);
    }

    private static async Task<LogTip> LastRecordAsync(IReplicationPeer peer, string topic, long epoch, long endOffset, CancellationToken ct)
    {
        var request = new FetchRequest { Epoch = epoch, Topic = topic, FromOffset = endOffset, MaxRecords = 1 };
        await foreach (var batch in peer.FetchAsync(request, ct).ConfigureAwait(false))
        {
            foreach (var bytes in batch.Records)
            {
                var record = RecordBytes.Decode(bytes);
                return new LogTip(record.Offset, record.Epoch);
            }
        }

        throw new InvalidOperationException($"The log of {topic} shrank while it was being surveyed.");
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
