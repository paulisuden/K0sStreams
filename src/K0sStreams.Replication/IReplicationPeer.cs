using K0sStreams.Contracts.Grpc;

namespace K0sStreams.Replication;

/// <summary>
/// The operations one broker calls on another (see <c>replication.proto</c>). <see cref="ReplicationNode"/> is the
/// local implementation; <see cref="GrpcReplicationPeer"/> reaches a remote broker. Invalid requests fail with an
/// <see cref="Grpc.Core.RpcException"/> in both, so callers handle a local and a remote peer the same way.
/// </summary>
internal interface IReplicationPeer
{
    /// <summary>Leader → follower: append records after <c>prev_offset</c>, or a heartbeat when there are none.</summary>
    Task<AppendResponse> AppendAsync(AppendRequest request, CancellationToken ct = default);

    /// <summary>Records from <c>from_offset</c> up to <c>max_records</c> (or to the end of the log when it is 0), in batches.</summary>
    IAsyncEnumerable<RecordBatch> FetchAsync(FetchRequest request, CancellationToken ct = default);

    /// <summary>Epoch, end offset, high watermark and role of the broker for one partition.</summary>
    Task<StateResponse> GetStateAsync(StateRequest request, CancellationToken ct = default);
}
