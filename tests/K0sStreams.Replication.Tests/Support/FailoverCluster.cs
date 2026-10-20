using System.Text;
using K0sStreams.Contracts;
using K0sStreams.Replication.Common;
using K0sStreams.Replication.Failover;
using K0sStreams.Replication.Leader;
using K0sStreams.Replication.Network;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using static K0sStreams.Replication.Tests.Support.TestRecords;
using Record = K0sStreams.Contracts.Record;

namespace K0sStreams.Replication.Tests.Support;

/// <summary>One complete broker: the follower side, the leader side and the promotion, over an in-memory log.</summary>
internal sealed class FailoverBroker : IAsyncDisposable
{
    public FailoverBroker(TestNode node, IReadOnlyDictionary<string, IReplicationPeer> peers, ReplicationOptions options, TimeProvider time)
    {
        Node = node;
        var wrapped = Microsoft.Extensions.Options.Options.Create(options);
        Replicator = new QuorumReplicator(
            node.Log, node.Cluster, node.Epochs, node.KnownTopics, peers, wrapped, time, NullLogger<QuorumReplicator>.Instance);
        Survey = new PeerSurvey(node.Node, node.Cluster, peers, new MajorityCall(wrapped, time), node.Epochs);
        Promotion = new LeaderPromotion(
            node.Cluster,
            node.Epochs,
            Survey,
            new LogCatchUp(node.Log, node.Node, node.Cluster, wrapped, time),
            new EpochChangeWriter(node.Log, node.Node, node.Cluster, time),
            wrapped,
            time,
            NullLogger<LeaderPromotion>.Instance);
    }

    public TestNode Node { get; }

    public ILog Log => Node.Log;

    public ControllableClusterState Cluster => Node.Cluster;

    public QuorumReplicator Replicator { get; }

    public PeerSurvey Survey { get; }

    public LeaderPromotion Promotion { get; }

    public async ValueTask DisposeAsync()
    {
        await Replicator.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        Promotion.Dispose();
        Node.Dispose();
    }
}

/// <summary>
/// Brokers in one process, each one complete, linked to every other through a <see cref="FaultyPeer"/>. broker-0 starts
/// as the leader at epoch 1. <see cref="ElectAsync"/> plays coordination's part: promote a broker, then announce it.
/// </summary>
internal sealed class FailoverCluster : IAsyncDisposable
{
    private readonly FailoverBroker[] _brokers;
    private readonly FaultyPeer?[,] _links;
    private readonly HashSet<int> _isolated = [];

    public FailoverCluster(int size = 3)
    {
        var nodes = Enumerable.Range(0, size).Select(i => new TestNode($"broker-{i}", 1, Options, isLeader: i == 0)).ToArray();
        _links = new FaultyPeer?[size, size];
        _brokers = new FailoverBroker[size];
        for (int from = 0; from < size; from++)
        {
            var peers = new Dictionary<string, IReplicationPeer>(StringComparer.Ordinal);
            for (int to = 0; to < size; to++)
            {
                if (from != to)
                {
                    var link = new FaultyPeer(nodes[to].Node);
                    _links[from, to] = link;
                    peers[nodes[to].Cluster.NodeId] = link;
                }
            }

            _brokers[from] = new FailoverBroker(nodes[from], peers, Options, Time);
        }
    }

    public FakeTimeProvider Time { get; } = new();

    public ReplicationOptions Options { get; } = new();

    public IReadOnlyList<FailoverBroker> Brokers => _brokers;

    /// <summary>Cuts every link to and from a broker, as if it crashed or was partitioned away.</summary>
    public void Isolate(int index)
    {
        _isolated.Add(index);
        foreach (var link in LinksBetween(index, Enumerable.Range(0, _brokers.Length)))
        {
            link.Disconnect();
        }
    }

    /// <summary>Reconnects a broker to every broker that isn't isolated itself.</summary>
    public void Heal(int index)
    {
        _isolated.Remove(index);
        foreach (var link in LinksBetween(index, Enumerable.Range(0, _brokers.Length).Where(other => !_isolated.Contains(other))))
        {
            link.Reconnect();
        }
    }

    /// <summary>
    /// What coordination does when <paramref name="leader"/> wins the Lease at <paramref name="epoch"/>: promote it,
    /// then tell every broker that isn't isolated. An isolated broker still believes the old state.
    /// </summary>
    public async Task ElectAsync(int leader, long epoch)
    {
        await PromoteAsync(leader, epoch);
        for (int i = 0; i < _brokers.Length; i++)
        {
            if (!_isolated.Contains(i))
            {
                _brokers[i].Cluster.ChangeEpoch(epoch, leader: i == leader);
            }
        }
    }

    public async Task PromoteAsync(int broker, long epoch)
    {
        var promotion = _brokers[broker].Promotion.PromoteAsync(epoch).AsTask();
        await Time.EventuallyAsync(() => promotion.IsCompleted, $"broker-{broker} to get ready to lead epoch {epoch}");
        await promotion;
    }

    /// <summary>Puts records with the given epochs in a broker's log, with its HW at <paramref name="highWatermark"/>.</summary>
    public async Task SeedAsync(int broker, long highWatermark, params long[] epochs)
    {
        var node = _brokers[broker].Node;
        await TestRecords.SeedAsync(node.Log, epochs);
        node.KnownTopics.Add(Topic);
        if (highWatermark >= 0)
        {
            node.Log.AdvanceHighWatermark(Topic, highWatermark);
        }
    }

    /// <summary>Appends a message to a broker's log at its current epoch, as B's API does on the leader.</summary>
    public async Task<long> WriteAsync(int broker, string value = "v")
    {
        var node = _brokers[broker];
        return await node.Log.AppendAsync(
            Topic, Record.NewMessage(node.Cluster.CurrentEpoch, 0, ReadOnlyMemory<byte>.Empty, Encoding.UTF8.GetBytes(value)));
    }

    public async Task ConfirmAsync(int broker, long offset)
    {
        var wait = _brokers[broker].Replicator.WaitForQuorumAsync(Topic, 0, offset).AsTask();
        await Time.EventuallyAsync(() => wait.IsCompleted, $"offset {offset} to be confirmed by broker-{broker}");
        await wait;
    }

    public Task EventuallyAsync(Func<bool> condition, string what) => Time.EventuallyAsync(condition, what);

    public async ValueTask DisposeAsync()
    {
        foreach (var broker in _brokers)
        {
            await broker.DisposeAsync();
        }
    }

    private IEnumerable<FaultyPeer> LinksBetween(int index, IEnumerable<int> others)
    {
        foreach (int other in others)
        {
            if (_links[index, other] is { } outgoing)
            {
                yield return outgoing;
            }

            if (_links[other, index] is { } incoming)
            {
                yield return incoming;
            }
        }
    }
}
