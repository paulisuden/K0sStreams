using K0sStreams.Contracts;
using K0sStreams.Contracts.Fakes;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace K0sStreams.Replication.Tests.Support;

/// <summary>One broker's replication stack over an in-memory log, called directly (no network).</summary>
internal sealed class TestNode : IDisposable
{
    public TestNode(string nodeId = "broker-1", long epoch = 1, ReplicationOptions? options = null, bool isLeader = false, ILog? log = null)
    {
        Log = log ?? new InMemoryLog();
        Cluster = new ControllableClusterState(nodeId, isLeader, epoch);
        Epochs = new EpochTracker(Cluster);
        AppendHandler = new AppendHandler(Log, Epochs, Cluster, KnownTopics, NullLogger<AppendHandler>.Instance);
        Node = new ReplicationNode(Log, Cluster, Epochs, AppendHandler, KnownTopics, Options.Create(options ?? new ReplicationOptions()));
    }

    public ILog Log { get; }

    public ControllableClusterState Cluster { get; }

    public EpochTracker Epochs { get; }

    public KnownTopics KnownTopics { get; } = new();

    public AppendHandler AppendHandler { get; }

    public ReplicationNode Node { get; }

    public void Dispose() => AppendHandler.Dispose();
}
