using Grpc.Core;
using K0sStreams.Contracts.Grpc;
using GrpcReplication = K0sStreams.Contracts.Grpc.Replication;

namespace K0sStreams.Replication;

/// <summary>gRPC endpoint of the replication protocol (port 9091). Only adapts calls to <see cref="ReplicationNode"/>.</summary>
internal sealed class ReplicationGrpcService(ReplicationNode node) : GrpcReplication.ReplicationBase
{
    public override Task<AppendResponse> Append(AppendRequest request, ServerCallContext context) =>
        node.AppendAsync(request, context.CancellationToken);

    public override async Task Fetch(FetchRequest request, IServerStreamWriter<RecordBatch> responseStream, ServerCallContext context)
    {
        await foreach (var batch in node.FetchAsync(request, context.CancellationToken).ConfigureAwait(false))
        {
            await responseStream.WriteAsync(batch, context.CancellationToken).ConfigureAwait(false);
        }
    }

    public override Task<StateResponse> GetState(StateRequest request, ServerCallContext context) =>
        node.GetStateAsync(request, context.CancellationToken);
}
