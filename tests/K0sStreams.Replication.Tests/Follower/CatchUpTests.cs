using K0sStreams.Contracts;
using K0sStreams.Replication.Follower;
using K0sStreams.Replication.Network;
using K0sStreams.Replication.Tests.Support;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using static K0sStreams.Replication.Tests.Support.TestRecords;
using Record = K0sStreams.Contracts.Record;

namespace K0sStreams.Replication.Tests.Follower;

/// <summary>
/// El seguidor que vuelve pide lo que le falta (docs/replication.md, §5.4). Sin esto, un broker reiniciado queda
/// sirviendo un log atrasado hasta que el líder tenga algo nuevo que empujar.
/// </summary>
public class CatchUpTests
{
    [Fact]
    public async Task A_stale_follower_fetches_what_it_missed_without_the_leader_pushing()
    {
        var leader = await LeaderAsync();
        await SeedPartitionAsync(leader.Log, 5);
        leader.Log.AdvanceHighWatermark(Topic, 0, 4);

        var follower = await FollowerAsync();
        await SeedPartitionAsync(follower.Log, 3);
        follower.Log.AdvanceHighWatermark(Topic, 0, 2);

        // Un tercer broker que NO es líder y que tiene más registros que nadie: no es una fuente válida.
        var bystander = await FollowerAsync("broker-2");
        await SeedPartitionAsync(bystander.Log, 7);

        FollowerCatchUp catchUp = Build(follower, bystander, leader);
        await catchUp.ReconcileAsync();

        follower.Log.EndOffset(Topic, 0).Should().Be(4);
        follower.Log.HighWatermark(Topic, 0).Should().Be(4);
        (await EncodedAsync(follower.Log)).Should().Equal(await EncodedAsync(leader.Log));
    }

    [Fact]
    public async Task A_follower_with_every_record_but_a_stale_high_watermark_learns_the_leaders()
    {
        var leader = await LeaderAsync();
        await SeedPartitionAsync(leader.Log, 5);
        leader.Log.AdvanceHighWatermark(Topic, 0, 4);

        var follower = await FollowerAsync();
        await SeedPartitionAsync(follower.Log, 5);
        follower.Log.AdvanceHighWatermark(Topic, 0, 2);

        await Build(follower, leader).ReconcileAsync();

        follower.Log.HighWatermark(Topic, 0).Should().Be(4);
        (await EncodedAsync(follower.Log)).Should().Equal(await EncodedAsync(leader.Log));
    }

    [Fact]
    public async Task A_follower_with_an_unconfirmed_tail_from_an_old_leader_overwrites_it()
    {
        var leader = await LeaderAsync();
        await SeedPartitionAsync(leader.Log, 5);
        leader.Log.AdvanceHighWatermark(Topic, 0, 4);

        // Los offsets 3 y 4 no se confirmaron nunca: son de un líder viejo, con otra época.
        var follower = await FollowerAsync();
        await SeedPartitionAsync(follower.Log, 3, epoch: 1);
        await SeedPartitionAsync(follower.Log, 2, epoch: 2, from: 3);
        follower.Log.AdvanceHighWatermark(Topic, 0, 2);

        await Build(follower, leader).ReconcileAsync();

        follower.Log.HighWatermark(Topic, 0).Should().Be(4);
        (await EncodedAsync(follower.Log)).Should().Equal(await EncodedAsync(leader.Log));
    }

    [Fact]
    public async Task Every_partition_of_the_topic_catches_up()
    {
        const int Partitions = 3;
        var leader = await LeaderAsync(Partitions);
        for (int partition = 0; partition < Partitions; partition++)
        {
            await SeedPartitionAsync(leader.Log, 4, partition: partition);
            leader.Log.AdvanceHighWatermark(Topic, partition, 3);
        }

        var follower = await FollowerAsync(partitions: Partitions);
        await Build(follower, leader).ReconcileAsync();

        foreach (int partition in Enumerable.Range(0, Partitions))
        {
            follower.Log.EndOffset(Topic, partition).Should().Be(3);
            follower.Log.HighWatermark(Topic, partition).Should().Be(3);
        }
    }

    [Fact]
    public async Task The_leader_does_not_fetch_from_anyone()
    {
        var leader = await LeaderAsync();
        await SeedPartitionAsync(leader.Log, 3);

        var other = await FollowerAsync("broker-1");
        await SeedPartitionAsync(other.Log, 7);

        await Build(leader, other).ReconcileAsync();

        leader.Log.EndOffset(Topic, 0).Should().Be(2);
    }

    [Fact]
    public async Task Without_a_leader_nothing_is_copied()
    {
        var follower = await FollowerAsync();
        await SeedPartitionAsync(follower.Log, 2);

        var other = await FollowerAsync("broker-2");
        await SeedPartitionAsync(other.Log, 7);

        await Build(follower, other).ReconcileAsync();

        follower.Log.EndOffset(Topic, 0).Should().Be(1);
        follower.Log.HighWatermark(Topic, 0).Should().Be(-1);
    }

    [Fact]
    public async Task An_empty_log_with_no_topics_in_the_catalog_is_left_alone()
    {
        var leader = await LeaderAsync();
        var follower = await FollowerAsync(topics: false);

        await Build(follower, leader).ReconcileAsync();

        follower.Log.EndOffset(Topic, 0).Should().Be(-1);
    }

    private static async Task<TestNode> LeaderAsync(int partitions = 1, string nodeId = "broker-0")
    {
        var leader = new TestNode(nodeId, epoch: 1, isLeader: true);
        await leader.Catalog.CreateAsync(Config(partitions));
        return leader;
    }

    private static async Task<TestNode> FollowerAsync(string nodeId = "broker-1", int partitions = 1, bool topics = true)
    {
        var follower = new TestNode(nodeId, epoch: 1);
        if (topics)
        {
            await follower.Catalog.CreateAsync(Config(partitions));
        }

        return follower;
    }

    private static TopicConfig Config(int partitions) =>
        new(Topic, partitions, MaxRetries: 3, RetryBackoff: TimeSpan.FromSeconds(5));

    /// <summary>Un seguidor y los otros brokers a los que les puede preguntar.</summary>
    private static FollowerCatchUp Build(TestNode self, params TestNode[] others)
    {
        var peers = others.ToDictionary(
            static peer => peer.Cluster.NodeId,
            static peer => (IReplicationPeer)peer.Node,
            StringComparer.Ordinal);
        return new FollowerCatchUp(
            self.Log,
            self.Catalog,
            self.Cluster,
            self.Node,
            peers,
            Options.Create(new ReplicationOptions()),
            new FakeTimeProvider(),
            NullLogger<FollowerCatchUp>.Instance);
    }

    private static async Task SeedPartitionAsync(ILog log, int records, long epoch = 1, int partition = 0, int from = 0)
    {
        for (int i = from; i < from + records; i++)
        {
            await log.AppendAsync(Topic, partition, Message(Record.Unassigned, epoch, $"p{partition}-{i}"));
        }
    }
}
