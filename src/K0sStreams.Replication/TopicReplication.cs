using K0sStreams.Contracts;

namespace K0sStreams.Replication;

/// <summary>
/// Leader-side state of one topic during one <see cref="LeaderTerm"/>: how far each follower is known to match the
/// leader, the high watermark, and the writes waiting to be confirmed. Everything is guarded by one lock, and that lock
/// is never held while awaiting.
/// </summary>
internal sealed class TopicReplication
{
    /// <summary>Match epoch of a follower whose match was lowered without verifying it: it can't drive the HW.</summary>
    private const long UnknownEpoch = -1;

    private readonly Lock _gate = new();
    private readonly ILog _log;
    private readonly long[] _match;
    private readonly long[] _matchEpoch;
    private readonly int _needed;
    private readonly AsyncSignal[] _signals;
    private readonly PriorityQueue<TaskCompletionSource, long> _waiters = new();
    private long _highWatermark;
    private bool _stopped;

    public TopicReplication(ILog log, string topic, long epoch, int peers)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(peers, 1);
        _log = log;
        Topic = topic;
        Epoch = epoch;
        _needed = (peers + 1) / 2;
        _match = new long[peers];
        _matchEpoch = new long[peers];
        Array.Fill(_match, -1);
        _signals = new AsyncSignal[peers];
        for (int i = 0; i < peers; i++)
        {
            _signals[i] = new AsyncSignal();
        }

        _highWatermark = log.HighWatermark(topic);
    }

    public string Topic { get; }

    public long Epoch { get; }

    public long HighWatermark => Volatile.Read(ref _highWatermark);

    public AsyncSignal SignalOf(int peer) => _signals[peer];

    /// <summary>
    /// The highest offset that at least <paramref name="needed"/> followers have. The leader's own copy counts too,
    /// implicitly: the leader has every offset a follower matched.
    /// </summary>
    public static long QuorumOffset(ReadOnlySpan<long> matches, int needed)
    {
        Span<long> sorted = matches.Length <= 16 ? stackalloc long[matches.Length] : new long[matches.Length];
        matches.CopyTo(sorted);
        sorted.Sort();
        return sorted[^needed];
    }

    /// <summary>Completes when the HW reaches <paramref name="offset"/>; fails with <see cref="NotLeaderException"/> if the term stops first.</summary>
    public Task WaitAsync(long offset, CancellationToken ct)
    {
        TaskCompletionSource waiter;
        lock (_gate)
        {
            if (_stopped)
            {
                return Task.FromException(new NotLeaderException("This broker stopped leading before the write was confirmed."));
            }

            if (offset <= _highWatermark)
            {
                return Task.CompletedTask;
            }

            waiter = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _waiters.Enqueue(waiter, offset);
        }

        SignalAll();
        return waiter.Task.WaitAsync(ct);
    }

    /// <summary>
    /// A follower confirmed it matches the leader up to <paramref name="match"/>, a record written in
    /// <paramref name="matchEpoch"/>. Advances the HW when a quorum has a record of this term's epoch.
    /// </summary>
    public void ReportProgress(int peer, long match, long matchEpoch)
    {
        bool advanced = false;
        lock (_gate)
        {
            if (_stopped)
            {
                return;
            }

            _match[peer] = match;
            _matchEpoch[peer] = matchEpoch;
            long candidate = QuorumOffset(_match, _needed);

            // Raft's rule: only a record of the current epoch is confirmed by counting copies. Older records become
            // confirmed with it. Epochs never go down along the log, so checking the candidate is enough.
            if (candidate > _highWatermark && IsCurrentEpochAt(candidate))
            {
                _log.AdvanceHighWatermark(Topic, candidate);
                Volatile.Write(ref _highWatermark, candidate);
                advanced = true;
                while (_waiters.TryPeek(out var waiter, out long offset) && offset <= candidate)
                {
                    _waiters.Dequeue();
                    waiter.TrySetResult();
                }
            }
        }

        if (advanced)
        {
            // Followers learn the new HW on the next Append; send it now instead of waiting for a heartbeat.
            SignalAll();
        }
    }

    /// <summary>
    /// A follower answered with an end offset below what it had confirmed: it lost records (a log that isn't durable).
    /// Its match is lowered so it stops counting toward the quorum. Returns true in that case, so the caller can log it.
    /// </summary>
    public bool ReportMismatch(int peer, long followerEnd)
    {
        lock (_gate)
        {
            if (_stopped || followerEnd >= _match[peer])
            {
                return false;
            }

            _match[peer] = followerEnd;
            _matchEpoch[peer] = UnknownEpoch;
            return true;
        }
    }

    /// <summary>Fails every pending wait with <see cref="NotLeaderException"/> and refuses new ones.</summary>
    public void Stop(string reason)
    {
        lock (_gate)
        {
            if (_stopped)
            {
                return;
            }

            _stopped = true;
            while (_waiters.TryDequeue(out var waiter, out _))
            {
                waiter.TrySetException(new NotLeaderException(reason));
            }
        }

        SignalAll();
    }

    /// <summary>True if a follower verified <paramref name="offset"/> as a record of this term's epoch.</summary>
    private bool IsCurrentEpochAt(long offset)
    {
        for (int i = 0; i < _match.Length; i++)
        {
            if (_match[i] == offset && _matchEpoch[i] == Epoch)
            {
                return true;
            }
        }

        return false;
    }

    private void SignalAll()
    {
        foreach (var signal in _signals)
        {
            signal.Set();
        }
    }
}
