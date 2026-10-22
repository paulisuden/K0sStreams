using System.Text;
using K0sStreams.Contracts;
using K0sStreams.Contracts.Grpc;
using K0sStreams.Replication.Common;
using K0sStreams.Replication.Follower;
using K0sStreams.Replication.Network;

namespace K0sStreams.Replication.Failover;

/// <summary>
/// Step 3 of a promotion: writes an <see cref="RecordType.EpochChange"/> record at the end of the topic, in the new
/// epoch. Once the new leader has it on a majority it's confirmed, and so is every older record before it (Raft's no-op
/// entry: a leader only confirms by counting copies of its own epoch's records).
/// </summary>
internal sealed class EpochChangeWriter(ILog log, ReplicationNode self, IClusterState cluster, TimeProvider time)
{
    public async Task WriteAsync(string topic, long epoch, CancellationToken ct)
    {
        var tip = await TipAsync(topic, ct).ConfigureAwait(false);
        if (tip.LastEpoch == epoch)
        {
            // Already written by an earlier attempt at this epoch.
            return;
        }

        var record = new Record(
            tip.EndOffset + 1, epoch, RecordType.EpochChange, time.GetUtcNow().ToUnixTimeMilliseconds(), 0,
            ReadOnlyMemory<byte>.Empty, Encoding.UTF8.GetBytes(cluster.NodeId));
        var append = new AppendRequest
        {
            Epoch = epoch,
            Topic = topic,
            PrevOffset = tip.EndOffset,
            PrevEpoch = tip.LastEpoch,
            LeaderHw = -1,
            LeaderId = cluster.NodeId,
        };
        append.Records.Add(RecordBytes.Encode(record));

        // Through this broker's own Append handling, like the catch-up: fenced and checked against the tip.
        var response = await self.AppendAsync(append, ct).ConfigureAwait(false);
        if (response.Code == AppendError.StaleEpoch)
        {
            throw new NotLeaderException($"Epoch {epoch} was replaced before {topic} could be marked.");
        }

        if (!response.Ok)
        {
            throw new InvalidOperationException($"Writing the epoch change of {topic} failed with {response.Code}: {response.Error}");
        }
    }

    private async Task<LogTip> TipAsync(string topic, CancellationToken ct)
    {
        long endOffset = log.EndOffset(topic);
        if (endOffset >= 0)
        {
            await foreach (var record in log.ReadAsync(topic, endOffset, 1, ct).ConfigureAwait(false))
            {
                return new LogTip(record.Offset, record.Epoch);
            }
        }

        return LogTip.Empty;
    }
}
