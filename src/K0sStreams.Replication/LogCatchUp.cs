using K0sStreams.Contracts;
using K0sStreams.Contracts.Grpc;
using Microsoft.Extensions.Options;

namespace K0sStreams.Replication;

/// <summary>
/// Step 2 of a promotion: copies into this broker's log what a more complete log has after this broker's confirmed
/// prefix, up to where that log ended in the survey. The records go through this broker's own <c>Append</c> handling, as
/// if it were its own follower, so every follower check applies: the epoch fence, continuity, CRC, epochs, and never
/// truncating a confirmed record.
/// </summary>
internal sealed class LogCatchUp(ILog log, ReplicationNode self, IClusterState cluster, IOptions<ReplicationOptions> options, TimeProvider time)
{
    private readonly ReplicationOptions _options = options.Value;

    /// <returns>False if the source's log changed since the survey, so the topic has to be surveyed again.</returns>
    public async Task<bool> TryCopyAsync(string topic, long epoch, SurveyAnswer source, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(source);
        long highWatermark = log.HighWatermark(topic);
        if (source.Tip.EndOffset < highWatermark)
        {
            throw new InvalidOperationException($"{source.NodeId} ends {topic} at {source.Tip.EndOffset}, before this broker's confirmed {highWatermark}.");
        }

        // From the confirmed record itself, not after it: its epoch becomes the first batch's prev_epoch, which checks
        // that both logs agree on everything confirmed.
        long from = Math.Max(highWatermark, 0);
        var request = new FetchRequest { Epoch = epoch, Topic = topic, FromOffset = from, MaxRecords = checked((int)(source.Tip.EndOffset - from + 1)) };

        // A deadline per batch rather than for the whole copy: a long copy is fine, a stalled one isn't.
        using var idle = new CancellationTokenSource(_options.RpcTimeout, time);
        using var call = CancellationTokenSource.CreateLinkedTokenSource(ct, idle.Token);
        var cursor = new LogTip(highWatermark, 0);
        bool atConfirmedRecord = highWatermark >= 0;
        await foreach (var batch in source.Peer.FetchAsync(request, call.Token).ConfigureAwait(false))
        {
            idle.CancelAfter(_options.RpcTimeout);
            var append = new AppendRequest { Epoch = epoch, Topic = topic, LeaderHw = batch.LeaderHw, LeaderId = cluster.NodeId };
            var batchEnd = cursor;
            foreach (var bytes in batch.Records)
            {
                var record = RecordBytes.Decode(bytes);
                if (atConfirmedRecord)
                {
                    if (record.Offset != highWatermark)
                    {
                        return false;
                    }

                    cursor = batchEnd = new LogTip(record.Offset, record.Epoch);
                    atConfirmedRecord = false;
                    continue;
                }

                append.Records.Add(bytes);
                batchEnd = new LogTip(record.Offset, record.Epoch);
            }

            if (append.Records.Count > 0)
            {
                append.PrevOffset = cursor.EndOffset;
                append.PrevEpoch = cursor.LastEpoch;
                await AppendToSelfAsync(append, source.NodeId, ct).ConfigureAwait(false);
                cursor = batchEnd;
            }
        }

        return cursor == source.Tip;
    }

    private async Task AppendToSelfAsync(AppendRequest append, string sourceId, CancellationToken ct)
    {
        var response = await self.AppendAsync(append, ct).ConfigureAwait(false);
        if (response.Code == AppendError.StaleEpoch)
        {
            throw new NotLeaderException($"Epoch {append.Epoch} was replaced while copying {append.Topic} from {sourceId}.");
        }

        if (!response.Ok)
        {
            // A mismatch here means the two logs disagree on a confirmed record. That is never resolved by deleting it.
            throw new InvalidOperationException($"Copying {append.Topic} from {sourceId} failed with {response.Code}: {response.Error}");
        }
    }
}
