using K0sStreams.Contracts.Fakes;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace K0sStreams.Replication.Tests.Support;

/// <summary>One broker's replication stack over an in-memory log, called directly (no network).</summary>
internal sealed class TestNode : IDisposable
{
    public TestNode(string nodeId = "broker-1", long epoch = 1, ReplicationOptions? options = null, bool isLeader = false)
    {
        Cluster = new ControllableClusterState(nodeId, isLeader, epoch);
        Epochs = new EpochTracker(Cluster);
        AppendHandler = new AppendHandler(Log, Epochs, NullLogger<AppendHandler>.Instance);
        Node = new ReplicationNode(Log, Cluster, Epochs, AppendHandler, Options.Create(options ?? new ReplicationOptions()));
    }

    public InMemoryLog Log { get; } = new();

    public ControllableClusterState Cluster { get; }

    public EpochTracker Epochs { get; }

    public AppendHandler AppendHandler { get; }

    public ReplicationNode Node { get; }

    public void Dispose() => AppendHandler.Dispose();
}
