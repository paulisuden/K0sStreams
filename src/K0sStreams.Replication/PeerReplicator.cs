using Google.Protobuf;
using K0sStreams.Contracts;
using K0sStreams.Contracts.Grpc;
using Microsoft.Extensions.Logging;

namespace K0sStreams.Replication;

/// <summary>
/// Leader side, one per follower and topic: sends the follower what it's missing with <c>Append</c>, steps back on a
/// mismatch, sends heartbeats when idle, and retries with backoff when the follower fails. One request in flight at a
/// time, so a slow or dead follower never holds up the others. See docs/replication.md, sections 5.1 to 5.6.
/// </summary>
internal sealed partial class PeerReplicator
{
    private readonly QuorumReplicator _owner;
    private readonly LeaderTerm _term;
    private readonly TopicReplication _topic;
    private readonly int _index;
    private readonly string _peerId;
    private readonly IReplicationPeer _peer;
    private readonly CancellationToken _ct;
    private readonly AsyncSignal _signal;
    private long _next;
    private int _heartbeatDue;
    private TimeSpan _backoff;
    private bool _failing;

    /// <param name="initialNext">First offset to send. Any value up to the leader's end offset + 1 is safe: the follower's
    /// consistency check makes the loop step back if it guessed too far.</param>
    public PeerReplicator(
        QuorumReplicator owner, LeaderTerm term, TopicReplication topic, int index, string peerId, IReplicationPeer peer, long initialNext)
    {
        _owner = owner;
        _term = term;
        _topic = topic;
        _index = index;
        _peerId = peerId;
        _peer = peer;
        _next = initialNext;
        _ct = term.Token;
        _signal = topic.SignalOf(index);
    }

    public async Task RunAsync()
    {
        var options = _owner.Options;
        using var heartbeat = _owner.Time.CreateTimer(
            static state => ((PeerReplicator)state!).OnHeartbeatDue(), this, options.HeartbeatInterval, Timeout.InfiniteTimeSpan);
        using var wakeOnCancel = _ct.UnsafeRegister(static state => ((AsyncSignal)state!).Set(), _signal);

        // Probe once at start: it tells the follower the current HW and finds out where the follower is.
        bool probe = true;
        long lastSentHw = long.MinValue;
        try
        {
            while (!_ct.IsCancellationRequested)
            {
                try
                {
                    if (!_owner.IsLeading(_term))
                    {
                        _owner.StepDown(_term, "this broker is no longer the leader at this epoch");
                        return;
                    }

                    long end = _owner.Log.EndOffset(_topic.Topic);
                    long hw = _topic.HighWatermark;
                    _next = Math.Min(_next, end + 1);
                    bool heartbeatDue = Interlocked.Exchange(ref _heartbeatDue, 0) == 1;
                    if (!probe && _next > end && hw <= lastSentHw && !heartbeatDue)
                    {
                        await _signal.WaitAsync().ConfigureAwait(false);
                        continue;
                    }

                    var (request, lastEpoch) = await BuildAsync(hw).ConfigureAwait(false);
                    var response = await _peer.AppendAsync(request, _ct).ConfigureAwait(false);
                    heartbeat.Change(options.HeartbeatInterval, Timeout.InfiniteTimeSpan);
                    if (_failing)
                    {
                        _failing = false;
                        LogRecovered(_owner.Logger, _peerId, _topic.Topic);
                    }

                    if (response.Code == AppendError.StaleEpoch || response.Epoch > _term.Epoch)
                    {
                        // Remember the newer epoch first, so this broker doesn't start leading again at the old one.
                        _owner.Epochs.Observe(response.Epoch);
                        LogFenced(_owner.Logger, _peerId, response.Epoch, _term.Epoch);
                        _owner.StepDown(_term, $"{_peerId} is at epoch {response.Epoch}, newer than {_term.Epoch}");
                        return;
                    }

                    if (response.Ok)
                    {
                        // The match is what this request verified, never response.EndOffset: past it, the follower may
                        // still hold records it hasn't checked against the leader.
                        long match = request.PrevOffset + request.Records.Count;
                        _next = match + 1;
                        lastSentHw = request.LeaderHw;
                        probe = false;
                        _backoff = TimeSpan.Zero;
                        _topic.ReportProgress(_index, match, lastEpoch);
                    }
                    else if (response.Code == AppendError.LogMismatch)
                    {
                        if (_topic.ReportMismatch(_index, response.EndOffset))
                        {
                            LogFollowerLostRecords(_owner.Logger, _peerId, _topic.Topic, response.EndOffset);
                        }

                        // Step back: to just after the follower's end if it's behind, or one record if the record before
                        // the batch conflicts. Retry at once, it's progress.
                        _next = Math.Max(0, Math.Min(_next - 1, response.EndOffset + 1));
                        probe = true;
                        _backoff = TimeSpan.Zero;
                    }
                    else
                    {
                        LogRejected(_owner.Logger, _peerId, _topic.Topic, response.Code, response.Error);
                        probe = true;
                        await BackoffAsync().ConfigureAwait(false);
                    }
                }
                catch (Exception ex) when (!_ct.IsCancellationRequested)
                {
                    if (_failing)
                    {
                        LogStillFailing(_owner.Logger, _peerId, _topic.Topic, ex.Message);
                    }
                    else
                    {
                        _failing = true;
                        LogFailing(_owner.Logger, ex, _peerId, _topic.Topic);
                    }

                    probe = true;
                    await BackoffAsync().ConfigureAwait(false);
                }
            }
        }
        catch (Exception) when (_ct.IsCancellationRequested)
        {
            // The term stopped: cancelled calls and delays end here.
        }
    }

    private void OnHeartbeatDue()
    {
        Volatile.Write(ref _heartbeatDue, 1);
        _signal.Set();
    }

    /// <summary>Waits <see cref="ReplicationOptions.RetryBackoff"/>, doubling up to the maximum. Writes don't cut it short.</summary>
    private Task BackoffAsync()
    {
        var options = _owner.Options;
        _backoff = _backoff == TimeSpan.Zero
            ? options.RetryBackoff
            : TimeSpan.FromTicks(Math.Min(_backoff.Ticks * 2, options.MaxRetryBackoff.Ticks));
        return Task.Delay(_backoff, _owner.Time, _ct);
    }

    /// <summary>
    /// The Append for the records from <see cref="_next"/>: also reads the record before them for <c>prev_epoch</c>.
    /// Returns the epoch of the last record the follower will have verified, for the HW epoch rule.
    /// </summary>
    private async Task<(AppendRequest Request, long LastEpoch)> BuildAsync(long highWatermark)
    {
        var options = _owner.Options;
        long prev = _next - 1;
        var request = new AppendRequest
        {
            Epoch = _term.Epoch,
            Topic = _topic.Topic,
            PrevOffset = prev,
            LeaderHw = highWatermark,
            LeaderId = _owner.NodeId,
        };

        long lastEpoch = 0;
        bool needPrevious = prev >= 0;
        int bytes = 0;
        int count = options.MaxBatchRecords + (needPrevious ? 1 : 0);
        await foreach (var record in _owner.Log.ReadAsync(_topic.Topic, Math.Max(prev, 0), count, _ct).ConfigureAwait(false))
        {
            if (needPrevious)
            {
                if (record.Offset != prev)
                {
                    throw new InvalidOperationException($"Read offset {record.Offset} from {_topic.Topic}; expected {prev}.");
                }

                request.PrevEpoch = record.Epoch;
                lastEpoch = record.Epoch;
                needPrevious = false;
                continue;
            }

            int size = RecordCodec.GetEncodedSize(record);
            if (request.Records.Count > 0 && bytes + size > options.MaxBatchBytes)
            {
                break;
            }

            // The array is new and never modified afterwards, so it can back the ByteString without a copy.
            request.Records.Add(UnsafeByteOperations.UnsafeWrap(RecordCodec.Encode(record)));
            bytes += size;
            lastEpoch = record.Epoch;
        }

        if (needPrevious)
        {
            throw new InvalidOperationException($"Offset {prev} of {_topic.Topic} is missing from the leader's log.");
        }

        return (request, lastEpoch);
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Replicating {Topic} to {Peer} failed; retrying with backoff.")]
    private static partial void LogFailing(ILogger logger, Exception exception, string peer, string topic);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Replicating {Topic} to {Peer} still fails: {Reason}")]
    private static partial void LogStillFailing(ILogger logger, string peer, string topic, string reason);

    [LoggerMessage(Level = LogLevel.Information, Message = "{Peer} answers again for {Topic}.")]
    private static partial void LogRecovered(ILogger logger, string peer, string topic);

    [LoggerMessage(Level = LogLevel.Error, Message = "{Peer} rejected records of {Topic} with {Code}: {Error}")]
    private static partial void LogRejected(ILogger logger, string peer, string topic, AppendError code, string error);

    [LoggerMessage(Level = LogLevel.Warning, Message = "{Peer} reports end offset {EndOffset} for {Topic}, below what it had confirmed: its log lost records.")]
    private static partial void LogFollowerLostRecords(ILogger logger, string peer, string topic, long endOffset);

    [LoggerMessage(Level = LogLevel.Warning, Message = "{Peer} is at epoch {PeerEpoch}, newer than this leader's {Epoch}: this broker was replaced as leader.")]
    private static partial void LogFenced(ILogger logger, string peer, long peerEpoch, long epoch);
}
