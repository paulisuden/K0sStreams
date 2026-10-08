using System.Runtime.CompilerServices;
using Google.Protobuf;
using Grpc.Core;
using K0sStreams.Contracts;
using K0sStreams.Contracts.Grpc;
using Microsoft.Extensions.Options;

namespace K0sStreams.Replication;

/// <summary>
/// This broker's side of the replication protocol. Other brokers reach it through <see cref="ReplicationGrpcService"/>;
/// tests call it directly.
/// </summary>
internal sealed class ReplicationNode(
    ILog log,
    IClusterState cluster,
    EpochTracker epochs,
    AppendHandler appendHandler,
    IOptions<ReplicationOptions> options) : IReplicationPeer
{
    private readonly ReplicationOptions _options = options.Value;

    public Task<AppendResponse> AppendAsync(AppendRequest request, CancellationToken ct = default) =>
        appendHandler.HandleAsync(request, ct);

    /// <summary>
    /// Streams records as they are in the log, without stopping at the high watermark. Each batch carries this
    /// broker's high watermark at the moment it is sent. The receiver has to verify continuity: the log can be
    /// truncated while the stream is open.
    /// </summary>
    public async IAsyncEnumerable<RecordBatch> FetchAsync(FetchRequest request, [EnumeratorCancellation] CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        EnsureValid(request.Topic, request.Partition);
        if (request.FromOffset < 0)
        {
            throw InvalidArgument($"from_offset must not be negative; it was {request.FromOffset}.");
        }

        epochs.Observe(request.Epoch);
        string topic = request.Topic;
        long endOffset = log.EndOffset(topic);
        long last = request.MaxRecords > 0 ? Math.Min(endOffset, request.FromOffset + request.MaxRecords - 1) : endOffset;

        long next = request.FromOffset;
        var batch = new RecordBatch();
        int batchBytes = 0;
        while (next <= last)
        {
            long readFrom = next;
            int count = (int)Math.Min(_options.MaxBatchRecords, last - next + 1);
            await foreach (var record in log.ReadAsync(topic, next, count, ct).ConfigureAwait(false))
            {
                int size = RecordCodec.GetEncodedSize(record);
                if (batch.Records.Count == _options.MaxBatchRecords || (batch.Records.Count > 0 && batchBytes + size > _options.MaxBatchBytes))
                {
                    yield return Seal(batch, topic);
                    batch = new RecordBatch();
                    batchBytes = 0;
                }

                // The array is new and never modified afterwards, so it can back the ByteString without a copy.
                batch.Records.Add(UnsafeByteOperations.UnsafeWrap(RecordCodec.Encode(record)));
                batchBytes += size;
                next = record.Offset + 1;
            }

            if (next == readFrom)
            {
                // The log got shorter while reading.
                break;
            }
        }

        if (batch.Records.Count > 0)
        {
            yield return Seal(batch, topic);
        }
    }

    public Task<StateResponse> GetStateAsync(StateRequest request, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        EnsureValid(request.Topic, request.Partition);
        return Task.FromResult(new StateResponse
        {
            Epoch = epochs.Current,
            EndOffset = log.EndOffset(request.Topic),
            HighWatermark = log.HighWatermark(request.Topic),
            NodeId = cluster.NodeId,
            IsLeader = cluster.IsLeader,
        });
    }

    private RecordBatch Seal(RecordBatch batch, string topic)
    {
        batch.LeaderHw = log.HighWatermark(topic);
        return batch;
    }

    private static void EnsureValid(string topic, int partition)
    {
        if (!SingleLog.IsValid(topic, partition))
        {
            throw InvalidArgument($"Invalid topic '{topic}', or partition {partition}: every topic is a single log, partition {SingleLog.Partition}.");
        }
    }

    private static RpcException InvalidArgument(string detail) => new(new Status(StatusCode.InvalidArgument, detail));
}
