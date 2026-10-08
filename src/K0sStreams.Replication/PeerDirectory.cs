using Grpc.Net.Client;
using K0sStreams.Contracts;
using Microsoft.Extensions.Options;
using GrpcReplication = K0sStreams.Contracts.Grpc.Replication;

namespace K0sStreams.Replication;

/// <summary>
/// gRPC clients for the other brokers in <see cref="ReplicationOptions.Peers"/>, by NodeId. Channels are long-lived:
/// they are created once (without connecting) and reused for every call.
/// </summary>
internal sealed class PeerDirectory : IDisposable
{
    private readonly List<GrpcChannel> _channels = [];

    public PeerDirectory(IOptions<ReplicationOptions> options, IClusterState cluster, TimeProvider time)
    {
        var settings = options.Value;
        var peers = new Dictionary<string, IReplicationPeer>(StringComparer.Ordinal);
        foreach (var (nodeId, address) in settings.Peers)
        {
            if (nodeId == cluster.NodeId)
            {
                continue;
            }

            var channel = GrpcReplicationPeer.CreateChannel(address);
            _channels.Add(channel);
            peers.Add(nodeId, new GrpcReplicationPeer(new GrpcReplication.ReplicationClient(channel), settings.RpcTimeout, time));
        }

        Peers = peers;
    }

    /// <summary>Every broker except this one.</summary>
    public IReadOnlyDictionary<string, IReplicationPeer> Peers { get; }

    public void Dispose()
    {
        foreach (var channel in _channels)
        {
            channel.Dispose();
        }
    }
}
