using K0sStreams.Contracts;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace K0sStreams.Replication;

/// <summary>
/// Leader side of replication (docs/replication.md, sections 5.1 to 5.5). <see cref="WaitForQuorumAsync"/> completes once
/// a majority of brokers have the record on disk and the high watermark covers it. Followers are fed by one
/// <see cref="PeerReplicator"/> per follower and topic, started on the topic's first write of each leadership term.
/// With no peers configured, a write is confirmed by this broker alone.
/// </summary>
internal sealed partial class QuorumReplicator : IReplicator, IDisposable, IAsyncDisposable
{
    private readonly Lock _gate = new();
    private readonly IClusterState _cluster;
    private readonly KeyValuePair<string, IReplicationPeer>[] _peers;
    private LeaderTerm? _term;
    private bool _disposed;

    public QuorumReplicator(
        ILog log,
        IClusterState cluster,
        EpochTracker epochs,
        IReadOnlyDictionary<string, IReplicationPeer> peers,
        IOptions<ReplicationOptions> options,
        TimeProvider time,
        ILogger<QuorumReplicator> logger)
    {
        ArgumentNullException.ThrowIfNull(peers);
        Log = log;
        _cluster = cluster;
        Epochs = epochs;
        _peers = [.. peers.OrderBy(static peer => peer.Key, StringComparer.Ordinal)];
        Options = options.Value;
        Time = time;
        Logger = logger;
        NodeId = cluster.NodeId;
        cluster.EpochChanged += OnEpochChanged;
        if (_peers.Length == 0)
        {
            LogNoPeers(logger);
        }
    }

    internal ILog Log { get; }

    internal EpochTracker Epochs { get; }

    internal ReplicationOptions Options { get; }

    internal TimeProvider Time { get; }

    internal ILogger Logger { get; }

    internal string NodeId { get; }

    public ValueTask WaitForQuorumAsync(string topic, int partition, long offset, CancellationToken ct = default)
    {
        if (!SingleLog.IsValid(topic, partition))
        {
            throw new ArgumentException($"Invalid topic '{topic}', or partition {partition}: every topic is a single log, partition {SingleLog.Partition}.");
        }

        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(offset, Log.EndOffset(topic));
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed), this);
        if (ct.IsCancellationRequested)
        {
            return ValueTask.FromCanceled(ct);
        }

        var term = CurrentTerm();
        if (term is null)
        {
            return ValueTask.FromException(new NotLeaderException("This broker is not the leader."));
        }

        if (_peers.Length == 0)
        {
            Log.AdvanceHighWatermark(topic, offset);
            return ValueTask.CompletedTask;
        }

        if (offset <= Log.HighWatermark(topic))
        {
            return ValueTask.CompletedTask;
        }

        var replication = GetOrStartTopic(term, topic, offset);
        return replication is null
            ? ValueTask.FromException(new NotLeaderException("This broker stopped leading before the write was confirmed."))
            : new ValueTask(replication.WaitAsync(offset, ct));
    }

    /// <summary>True while this broker leads at <paramref name="term"/>'s epoch, with no newer epoch seen from anyone.</summary>
    internal bool IsLeading(LeaderTerm term) =>
        !term.IsStopped && _cluster.IsLeader && _cluster.CurrentEpoch == term.Epoch && Epochs.Current == term.Epoch;

    /// <summary>Stops <paramref name="term"/>: pending waits fail with <see cref="NotLeaderException"/> and its loops end.</summary>
    internal Task[] StepDown(LeaderTerm term, string reason)
    {
        TopicReplication[] topics;
        Task[] loops;
        lock (_gate)
        {
            if (term.IsStopped)
            {
                return [];
            }

            term.MarkStopped();
            if (ReferenceEquals(_term, term))
            {
                _term = null;
            }

            topics = [.. term.Topics.Values];
            loops = [.. term.Loops];
        }

        LogSteppingDown(Logger, term.Epoch, reason);
        foreach (var topic in topics)
        {
            topic.Stop(reason);
        }

        _ = CancelAndDisposeAsync(term, loops);
        return loops;
    }

    public void Dispose() => BeginDispose();

    public async ValueTask DisposeAsync()
    {
        var loops = BeginDispose();
        if (loops.Length > 0)
        {
            try
            {
                await Task.WhenAll(loops).WaitAsync(Options.RpcTimeout, Time).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                // A loop stuck in a call that ignores cancellation; it ends with its call.
            }
        }
    }

    private static async Task CancelAndDisposeAsync(LeaderTerm term, Task[] loops)
    {
        await term.CancelAsync().ConfigureAwait(false);
        await Task.WhenAll(loops).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        term.Dispose();
    }

    private Task[] BeginDispose()
    {
        LeaderTerm? term;
        lock (_gate)
        {
            if (_disposed)
            {
                return [];
            }

            _disposed = true;
            term = _term;
        }

        _cluster.EpochChanged -= OnEpochChanged;
        return term is null ? [] : StepDown(term, "this broker is shutting down");
    }

    /// <summary>The current term, starting one if this broker leads and has none. Null if it doesn't lead.</summary>
    private LeaderTerm? CurrentTerm()
    {
        var term = Volatile.Read(ref _term);
        if (term is not null)
        {
            if (IsLeading(term))
            {
                return term;
            }

            StepDown(term, "leadership changed");
        }

        lock (_gate)
        {
            if (_term is not null)
            {
                return IsLeading(_term) ? _term : null;
            }

            long epoch = _cluster.CurrentEpoch;
            if (_disposed || !_cluster.IsLeader || Epochs.Current != epoch)
            {
                return null;
            }

            _term = new LeaderTerm(epoch);
            LogLeading(Logger, epoch, _peers.Length);
            return _term;
        }
    }

    /// <summary>The topic's replication state in <paramref name="term"/>, created with its loops on first use. Null if the term stopped.</summary>
    private TopicReplication? GetOrStartTopic(LeaderTerm term, string topic, long offset)
    {
        if (term.Topics.TryGetValue(topic, out var existing))
        {
            return existing;
        }

        lock (_gate)
        {
            if (term.IsStopped)
            {
                return null;
            }

            if (term.Topics.TryGetValue(topic, out existing))
            {
                return existing;
            }

            var replication = new TopicReplication(Log, topic, term.Epoch, _peers.Length);
            term.Topics[topic] = replication;
            long initialNext = Math.Min(offset, Log.EndOffset(topic) + 1);
            for (int i = 0; i < _peers.Length; i++)
            {
                var loop = new PeerReplicator(this, term, replication, i, _peers[i].Key, _peers[i].Value, initialNext);
                term.Loops.Add(Task.Run(loop.RunAsync));
            }

            return replication;
        }
    }

    private void OnEpochChanged(long epoch)
    {
        // Raised on coordination's thread: never let an exception escape into it.
        try
        {
            LeaderTerm? term;
            lock (_gate)
            {
                term = _term;
            }

            if (term is not null && (epoch != term.Epoch || !_cluster.IsLeader))
            {
                StepDown(term, $"the epoch changed to {epoch}");
            }
        }
        catch (Exception ex)
        {
            LogEpochChangeFailed(Logger, ex, epoch);
        }
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "No other brokers configured (Replication:Peers): writes are confirmed by this broker alone.")]
    private static partial void LogNoPeers(ILogger logger);

    [LoggerMessage(Level = LogLevel.Information, Message = "Leading at epoch {Epoch}, replicating to {Peers} broker(s).")]
    private static partial void LogLeading(ILogger logger, long epoch, int peers);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Stopped leading at epoch {Epoch}: {Reason}.")]
    private static partial void LogSteppingDown(ILogger logger, long epoch, string reason);

    [LoggerMessage(Level = LogLevel.Error, Message = "Failed to handle the change to epoch {Epoch}.")]
    private static partial void LogEpochChangeFailed(ILogger logger, Exception exception, long epoch);
}
