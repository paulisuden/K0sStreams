using System.Collections.Concurrent;

namespace K0sStreams.Replication;

/// <summary>
/// One period of leadership, at one epoch. It's stopped as a whole: its loops are cancelled, its topics fail their
/// pending waits, and a later leadership period starts a new term.
/// </summary>
internal sealed class LeaderTerm(long epoch) : IDisposable
{
    private readonly CancellationTokenSource _cts = new();
    private volatile bool _stopped;

    public long Epoch => epoch;

    /// <summary>Cancelled when the term stops. Read it before the term is disposed.</summary>
    public CancellationToken Token => _cts.Token;

    public bool IsStopped => _stopped;

    /// <summary>Replication state per topic. Added to only under <see cref="QuorumReplicator"/>'s lock.</summary>
    public ConcurrentDictionary<string, TopicReplication> Topics { get; } = new(StringComparer.Ordinal);

    /// <summary>One loop per follower and topic. Guarded by <see cref="QuorumReplicator"/>'s lock.</summary>
    public List<Task> Loops { get; } = [];

    public void MarkStopped() => _stopped = true;

    public Task CancelAsync() => _cts.CancelAsync();

    public void Dispose() => _cts.Dispose();
}
