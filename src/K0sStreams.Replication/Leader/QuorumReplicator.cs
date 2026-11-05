using System.Diagnostics;
using K0sStreams.Contracts;
using K0sStreams.Replication.Common;
using K0sStreams.Replication.Network;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace K0sStreams.Replication.Leader;

/// <summary>
/// Leader side of replication (docs/replication.md, sections 5.1 to 5.5). <see cref="WaitForQuorumAsync"/> completes once
/// a majority of brokers have the record on disk and the high watermark covers it. Followers are fed by one
/// <see cref="PeerReplicator"/> per follower and topic+partition, started on the topic's first write of each leadership term.
/// With no peers configured, a write is confirmed by this broker alone.
/// </summary>
internal sealed partial class QuorumReplicator : IReplicator, IDisposable, IAsyncDisposable
{
    private readonly Lock _gate = new();
    private readonly IClusterState _cluster;
    private readonly IServiceProvider _services;
    private readonly KnownTopics _knownTopics;
    private readonly KeyValuePair<string, IReplicationPeer>[] _peers;
    private LeaderTerm? _term;
    private bool _disposed;

    public QuorumReplicator(
        ILog log,
        IClusterState cluster,
        EpochTracker epochs,
        IServiceProvider services,
        KnownTopics knownTopics,
        IReadOnlyDictionary<string, IReplicationPeer> peers,
        IOptions<ReplicationOptions> options,
        TimeProvider time,
        ILogger<QuorumReplicator> logger)
    {
        ArgumentNullException.ThrowIfNull(peers);
        Log = log;
        _cluster = cluster;
        _services = services;
        Epochs = epochs;
        _knownTopics = knownTopics;
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

    public async ValueTask WaitForQuorumAsync(string topic, int partition, long offset, TimeSpan? timeout = null, CancellationToken ct = default)
    {
        if (!TopicConfig.IsValidName(topic) || partition < 0)
        {
            throw new ArgumentException($"Invalid topic '{topic}' or partition {partition}.");
        }

        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(offset, Log.EndOffset(topic, partition));
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed), this);
        if (ct.IsCancellationRequested)
        {
            return;
        }

        // Before the leadership check, never after: a new leader's ListTopics then either lists this topic, or had
        // already raised this broker's epoch so the check refuses (see AppendHandler).
        _knownTopics.Add(topic);
        var term = CurrentTerm();
        if (term is null)
        {
            throw new NotLeaderException("This broker is not the leader.");
        }

        if (_peers.Length == 0)
        {
            Log.AdvanceHighWatermark(topic, partition, offset);
            return;
        }

        if (offset <= Log.HighWatermark(topic, partition))
        {
            return;
        }

        var replication = GetOrStartTopic(term, topic, partition, offset);
        if (replication is null)
        {
            throw new NotLeaderException("This broker stopped leading before the write was confirmed.");
        }

        long started = Stopwatch.GetTimestamp();
        var waitTask = replication.WaitAsync(offset, ct);
        if (timeout is null)
        {
            await waitTask.ConfigureAwait(false);
            K0sMetrics.RecordQuorumWait(topic, partition, Stopwatch.GetElapsedTime(started).TotalSeconds, timedOut: false);
            return;
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(timeout.Value);
        try
        {
            await waitTask.WaitAsync(cts.Token).ConfigureAwait(false);
            K0sMetrics.RecordQuorumWait(topic, partition, Stopwatch.GetElapsedTime(started).TotalSeconds, timedOut: false);
        }
        catch (OperationCanceledException) when (cts.Token.IsCancellationRequested && !ct.IsCancellationRequested)
        {
            K0sMetrics.RecordQuorumWait(topic, partition, Stopwatch.GetElapsedTime(started).TotalSeconds, timedOut: true);
            throw new QuorumLostException($"Quorum not reached for {topic} partition {partition} at offset {offset} within {timeout.Value}.");
        }
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

    /// <summary>The topic+partition's replication state in <paramref name="term"/>, created with its loops on first use. Null if the term stopped.</summary>
    private TopicReplication? GetOrStartTopic(LeaderTerm term, string topic, int partition, long offset)
    {
        var key = (topic, partition);
        if (term.Topics.TryGetValue(key, out var existing))
        {
            return existing;
        }

        lock (_gate)
        {
            if (term.IsStopped)
            {
                return null;
            }

            if (term.Topics.TryGetValue(key, out existing))
            {
                return existing;
            }

            var replication = new TopicReplication(Log, topic, partition, term.Epoch, _peers.Length);
            term.Topics[key] = replication;
            long initialNext = Math.Min(offset, Log.EndOffset(topic, partition) + 1);
            for (int i = 0; i < _peers.Length; i++)
            {
                var loop = new PeerReplicator(this, term, replication, i, _peers[i].Key, _peers[i].Value, initialNext);
                term.Loops.Add(Task.Run(loop.RunAsync));
            }

            return replication;
        }
    }

    /// <summary>
    /// Right after taking over, starts replicating every topic this broker holds, in every partition. The promotion left
    /// an EpochChange record at the end of each partition; once a majority has it, it's confirmed, and so is every older
    /// record before it (Raft's no-op entry), without waiting for a client write.
    /// </summary>
    private void StartKnownTopics()
    {
        var term = _peers.Length == 0 ? null : CurrentTerm();
        if (term is null)
        {
            return;
        }

        foreach ((string topic, int partitions) in TopicsToReplicate())
        {
            for (int partition = 0; partition < partitions; partition++)
            {
                if (Log.EndOffset(topic, partition) >= 0)
                {
                    GetOrStartTopic(term, topic, partition, Log.HighWatermark(topic, partition) + 1);
                }
            }
        }
    }

    /// <summary>
    /// The topics this broker holds a log for, with their partition count. The catalog is the source: it's log-backed and
    /// replicated, so a broker that just restarted still knows every topic and how many partitions each one has. What the
    /// catalog doesn't list are the internal topics replication has seen (the catalog's own log, for one), which count as
    /// a single partition.
    /// </summary>
    private Dictionary<string, int> TopicsToReplicate()
    {
        var catalog = _services.GetRequiredService<ITopicCatalog>();
        var topics = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (TopicConfig config in catalog.List())
        {
            topics[config.Name] = config.Partitions;
        }

        foreach (string topic in _knownTopics.Snapshot())
        {
            topics.TryAdd(topic, 1);
        }

        return topics;
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

            if (_cluster.IsLeader && _cluster.CurrentEpoch == epoch)
            {
                StartKnownTopics();
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