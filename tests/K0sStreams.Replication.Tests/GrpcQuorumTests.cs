using K0sStreams.Contracts;
using K0sStreams.Replication.Tests.Support;
using static K0sStreams.Replication.Tests.Support.TestRecords;
using Record = K0sStreams.Contracts.Record;

namespace K0sStreams.Replication.Tests;

/// <summary>The phase 3 done criterion over real gRPC: three brokers on loopback, registered like the real host.</summary>
public class GrpcQuorumTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(15);

    [Fact]
    public async Task Over_grpc_a_follower_down_does_not_block_writes_and_catches_up_after_restart()
    {
        await using var follower1 = await ReplicationTestServer.StartAsync("broker-1");
        var follower2 = await ReplicationTestServer.StartAsync("broker-2");
        int follower2Port = follower2.Port;
        await using var leader = await ReplicationTestServer.StartAsync("broker-0", isLeader: true, settings: new()
        {
            ["Replication:Peers:broker-1"] = follower1.Url,
            ["Replication:Peers:broker-2"] = follower2.Url,
            ["Replication:HeartbeatInterval"] = "00:00:00.100",
            ["Replication:RetryBackoff"] = "00:00:00.050",
            ["Replication:MaxRetryBackoff"] = "00:00:00.200",
        });

        long first = await leader.Log.AppendAsync(Topic, Message(Record.Unassigned, 1, "first"));
        await leader.Replicator.WaitForQuorumAsync(Topic, 0, first).AsTask().WaitAsync(Patience);

        await follower2.DisposeAsync();
        long second = await leader.Log.AppendAsync(Topic, Message(Record.Unassigned, 1, "second"));
        await leader.Replicator.WaitForQuorumAsync(Topic, 0, second).AsTask().WaitAsync(Patience);

        await using var restarted = await ReplicationTestServer.StartAsync("broker-2", port: follower2Port);
        await Eventually.UntilAsync(() => restarted.Log.EndOffset(Topic) == second, "the restarted follower to catch up");
        (await EncodedAsync(restarted.Log)).Should().Equal(await EncodedAsync(leader.Log));
    }
}
