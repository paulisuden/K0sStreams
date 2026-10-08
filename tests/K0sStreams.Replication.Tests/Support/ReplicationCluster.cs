using System.Text;
using K0sStreams.Contracts;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using static K0sStreams.Replication.Tests.Support.TestRecords;
using Record = K0sStreams.Contracts.Record;

namespace K0sStreams.Replication.Tests.Support;

/// <summary>
/// A leader and its followers in one process, with no network: the leader's <see cref="QuorumReplicator"/> reaches each
/// follower's <see cref="ReplicationNode"/> through a <see cref="FaultyPeer"/>. Time is a <see cref="FakeTimeProvider"/>.
/// </summary>
internal sealed class ReplicationCluster : IAsyncDisposable
{
    private readonly TestNode[] _followers;
    private readonly FaultyPeer[] _links;
    private readonly List<TestNode> _restarted = [];

    public ReplicationCluster(int followers = 2, long leaderEpoch = 1)
    {
        Leader = new TestNode("broker-0", leaderEpoch, Options, isLeader: true);
        _followers = new TestNode[followers];
        _links = new FaultyPeer[followers];
        var peers = new Dictionary<string, IReplicationPeer>(StringComparer.Ordinal);
        for (int i = 0; i < followers; i++)
        {
            _followers[i] = new TestNode($"broker-{i + 1}", options: Options);
            _links[i] = new FaultyPeer(_followers[i].Node);
            peers[_followers[i].Cluster.NodeId] = _links[i];
        }

        Replicator = new QuorumReplicator(
            Leader.Log,
            Leader.Cluster,
            Leader.Epochs,
            peers,
            Microsoft.Extensions.Options.Options.Create(Options),
            Time,
            NullLogger<QuorumReplicator>.Instance);
    }

    public FakeTimeProvider Time { get; } = new();

    public ReplicationOptions Options { get; } = new();

    public TestNode Leader { get; }

    public IReadOnlyList<TestNode> Followers => _followers;

    public IReadOnlyList<FaultyPeer> Links => _links;

    public QuorumReplicator Replicator { get; }

    /// <summary>Appends a message to the leader's log, stamped with the leader's epoch, as B's API will.</summary>
    public async Task<long> WriteAsync(string value = "v") =>
        await Leader.Log.AppendAsync(
            Topic, Record.NewMessage(Leader.Cluster.CurrentEpoch, 0, ReadOnlyMemory<byte>.Empty, Encoding.UTF8.GetBytes(value)));

    public Task WaitAsync(long offset, CancellationToken ct = default) =>
        Replicator.WaitForQuorumAsync(Topic, 0, offset, ct).AsTask();

    /// <summary>Waits until the write at <paramref name="offset"/> is confirmed, letting fake time move meanwhile.</summary>
    public async Task ConfirmAsync(long offset)
    {
        var wait = WaitAsync(offset);
        await Time.EventuallyAsync(() => wait.IsCompleted, $"offset {offset} to be confirmed");
        await wait;
    }

    public Task EventuallyAsync(Func<bool> condition, string what) => Time.EventuallyAsync(condition, what);

    /// <summary>Replaces follower <paramref name="index"/> with a fresh broker with an empty log, like a restart with InMemoryLog.</summary>
    public void RestartFollower(int index)
    {
        _restarted.Add(_followers[index]);
        _followers[index] = new TestNode(_followers[index].Cluster.NodeId, options: Options);
        _links[index].Inner = _followers[index].Node;
    }

    public async ValueTask DisposeAsync()
    {
        await Replicator.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        Leader.Dispose();
        foreach (var node in _followers.Concat(_restarted))
        {
            node.Dispose();
        }
    }
}
