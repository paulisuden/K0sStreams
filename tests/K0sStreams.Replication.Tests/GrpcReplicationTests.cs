using Grpc.Core;
using K0sStreams.Contracts;
using K0sStreams.Contracts.Grpc;
using K0sStreams.Replication.Tests.Support;
using static K0sStreams.Replication.Tests.Support.TestRecords;
using GrpcReplication = K0sStreams.Contracts.Grpc.Replication;

namespace K0sStreams.Replication.Tests;

/// <summary>Two brokers talking real gRPC over HTTP/2 on loopback: the phase 2 "replicate a record between two processes" check.</summary>
public sealed class GrpcReplicationTests : IAsyncLifetime
{
    private ReplicationTestServer _leader = null!;
    private ReplicationTestServer _follower = null!;

    public async Task InitializeAsync()
    {
        _leader = await ReplicationTestServer.StartAsync("broker-0");
        _follower = await ReplicationTestServer.StartAsync("broker-1");
    }

    public async Task DisposeAsync()
    {
        await _leader.DisposeAsync();
        await _follower.DisposeAsync();
    }

    [Fact]
    public async Task Records_fetched_from_one_broker_are_replicated_to_another()
    {
        await SeedAsync(_leader.Log, 1, 1, 1);
        _leader.Log.AdvanceHighWatermark(Topic, 0, 2);

        var batches = await _leader.Client.FetchAsync(new FetchRequest { Epoch = 1, Topic = Topic, Partition = 0, FromOffset = 0 }).ToListAsync();
        var append = Append(1, -1, 0, batches[^1].LeaderHw);
        append.Records.AddRange(batches.SelectMany(batch => batch.Records));
        var response = await _follower.Client.AppendAsync(append);

        response.Ok.Should().BeTrue();
        (await EncodedAsync(_follower.Log)).Should().Equal(await EncodedAsync(_leader.Log));
        var state = await _follower.Client.GetStateAsync(new StateRequest { Topic = Topic, Partition = 0 });
        state.Should().Be(new StateResponse { Epoch = 1, EndOffset = 2, HighWatermark = 2, NodeId = "broker-1", IsLeader = false });
    }

    [Fact]
    public async Task A_stale_leader_is_fenced()
    {
        _follower.Cluster.ChangeEpoch(3, leader: false);

        var response = await _follower.Client.AppendAsync(Append(2, -1, 0, -1, Message(0, 2)));

        response.Code.Should().Be(AppendError.StaleEpoch);
        response.Epoch.Should().Be(3);
    }

    [Fact]
    public async Task A_record_larger_than_the_default_grpc_limit_replicates()
    {
        var big = Message(0, 1, new string('x', 6 * 1024 * 1024));

        (await _follower.Client.AppendAsync(Append(1, -1, 0, -1, big))).Ok.Should().BeTrue();

        var batches = await _follower.Client.FetchAsync(new FetchRequest { Epoch = 1, Topic = Topic, Partition = 0, FromOffset = 0 }).ToListAsync();
        batches.Should().ContainSingle().Which.Records.Should().ContainSingle().Which.Length.Should().Be(RecordCodec.GetEncodedSize(big));
    }

    [Fact]
    public async Task Invalid_requests_fail_with_invalid_argument()
    {
        var fetch = () => _follower.Client.FetchAsync(new FetchRequest { Topic = "../etc" }).ToListAsync().AsTask();
        var getState = () => _follower.Client.GetStateAsync(new StateRequest { Topic = "../etc" });

        (await fetch.Should().ThrowAsync<RpcException>()).Which.StatusCode.Should().Be(StatusCode.InvalidArgument);
        (await getState.Should().ThrowAsync<RpcException>()).Which.StatusCode.Should().Be(StatusCode.InvalidArgument);
    }

    [Fact]
    public async Task An_unreachable_broker_fails_with_unavailable()
    {
        using var channel = GrpcReplicationPeer.CreateChannel(new Uri("http://127.0.0.1:1"));
        var peer = new GrpcReplicationPeer(new GrpcReplication.ReplicationClient(channel), TimeSpan.FromSeconds(5), TimeProvider.System);

        var act = () => peer.GetStateAsync(new StateRequest { Topic = Topic });

        (await act.Should().ThrowAsync<RpcException>()).Which.StatusCode.Should().Be(StatusCode.Unavailable);
    }
}
