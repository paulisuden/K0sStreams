using System.Runtime.CompilerServices;
using Grpc.Core;
using Grpc.Net.Client;
using K0sStreams.Contracts.Grpc;
using GrpcReplication = K0sStreams.Contracts.Grpc.Replication;

namespace K0sStreams.Replication;

/// <summary>
/// A remote broker reached over gRPC. Unary calls get a deadline of <paramref name="rpcTimeout"/>; a Fetch stream
/// only ends with the caller's cancellation token, since a long catch-up can legitimately take a while.
/// Failures surface as <see cref="RpcException"/>.
/// </summary>
internal sealed class GrpcReplicationPeer(GrpcReplication.ReplicationClient client, TimeSpan rpcTimeout, TimeProvider time)
    : IReplicationPeer
{
    /// <summary>Channel to another broker's gRPC port, with message limits large enough for the biggest record.</summary>
    public static GrpcChannel CreateChannel(Uri address) =>
        GrpcChannel.ForAddress(address, new GrpcChannelOptions
        {
            MaxReceiveMessageSize = ReplicationOptions.MaxMessageSize,
            MaxSendMessageSize = ReplicationOptions.MaxMessageSize,
        });

    public async Task<AppendResponse> AppendAsync(AppendRequest request, CancellationToken ct = default)
    {
        using var call = client.AppendAsync(request, deadline: Deadline(), cancellationToken: ct);
        return await call.ResponseAsync.ConfigureAwait(false);
    }

    public async IAsyncEnumerable<RecordBatch> FetchAsync(FetchRequest request, [EnumeratorCancellation] CancellationToken ct = default)
    {
        using var call = client.Fetch(request, cancellationToken: ct);
        await foreach (var batch in call.ResponseStream.ReadAllAsync(ct).ConfigureAwait(false))
        {
            yield return batch;
        }
    }

    public async Task<StateResponse> GetStateAsync(StateRequest request, CancellationToken ct = default)
    {
        using var call = client.GetStateAsync(request, deadline: Deadline(), cancellationToken: ct);
        return await call.ResponseAsync.ConfigureAwait(false);
    }

    private DateTime Deadline() => time.GetUtcNow().UtcDateTime + rpcTimeout;
}
