using Google.Protobuf;
using Grpc.Core;
using K0sStreams.Contracts;
using K0sStreams.Contracts.Grpc;
using K0sStreams.Replication.Tests.Support;
using static K0sStreams.Replication.Tests.Support.TestRecords;
using Record = K0sStreams.Contracts.Record;

namespace K0sStreams.Replication.Tests;

/// <summary>Fetch and GetState, called directly on a node.</summary>
public sealed class ReplicationNodeTests : IDisposable
{
    private readonly TestNode _broker = new(nodeId: "broker-0", options: new ReplicationOptions { MaxBatchRecords = 4 });

    public void Dispose() => _broker.Dispose();

    [Fact]
    public async Task Fetch_streams_the_log_in_batches_of_at_most_MaxBatchRecords()
    {
        await SeedAsync(_broker.Log, [.. Enumerable.Repeat(1L, 10)]);

        var batches = await FetchAsync(_broker, from: 0);

        batches.Select(batch => batch.Records.Count).Should().Equal(4, 4, 2);
        Offsets(batches).Should().Equal(Enumerable.Range(0, 10).Select(offset => (long)offset));
    }

    [Fact]
    public async Task Fetch_splits_batches_by_size()
    {
        // Every seeded record is HeaderSize + 2 bytes ("m0", "m1"…): two fit in a batch, three don't.
        using var broker = new TestNode(options: new ReplicationOptions { MaxBatchBytes = (2 * RecordCodec.HeaderSize) + 10 });
        await SeedAsync(broker.Log, 1, 1, 1, 1, 1);

        var batches = await FetchAsync(broker, from: 0);

        batches.Select(batch => batch.Records.Count).Should().Equal(2, 2, 1);
    }

    [Fact]
    public async Task Fetch_stops_at_max_records()
    {
        await SeedAsync(_broker.Log, 1, 1, 1, 1, 1, 1);

        Offsets(await FetchAsync(_broker, from: 3, max: 2)).Should().Equal(3, 4);
    }

    [Fact]
    public async Task Fetch_past_the_end_returns_nothing()
    {
        await SeedAsync(_broker.Log, 1, 1);

        (await FetchAsync(_broker, from: 5)).Should().BeEmpty();
    }

    [Fact]
    public async Task Fetch_does_not_stop_at_the_high_watermark_and_reports_it()
    {
        await SeedAsync(_broker.Log, 1, 1, 1);
        _broker.Log.AdvanceHighWatermark(Topic, 0);

        var batches = await FetchAsync(_broker, from: 0);

        Offsets(batches).Should().Equal(0, 1, 2);
        batches.Should().OnlyContain(batch => batch.LeaderHw == 0);
    }

    [Fact]
    public async Task Fetch_with_a_newer_epoch_is_adopted()
    {
        await FetchAsync(_broker, from: 0, epoch: 7);

        _broker.Epochs.Current.Should().Be(7);
    }

    [Theory]
    [InlineData("../etc", 0, 0)]
    [InlineData("orders", -1, 0)]
    [InlineData("orders", 1, 0)]
    [InlineData("orders", 0, -1)]
    public async Task Invalid_fetch_fails_with_invalid_argument(string topic, int partition, long from)
    {
        var request = new FetchRequest { Epoch = 1, Topic = topic, Partition = partition, FromOffset = from };

        var act = () => _broker.Node.FetchAsync(request).ToListAsync().AsTask();

        (await act.Should().ThrowAsync<RpcException>()).Which.StatusCode.Should().Be(StatusCode.InvalidArgument);
    }

    [Fact]
    public async Task GetState_reports_epoch_offsets_and_role()
    {
        await SeedAsync(_broker.Log, 1, 1, 1);
        _broker.Log.AdvanceHighWatermark(Topic, 1);
        _broker.Cluster.ChangeEpoch(2, leader: true);

        var state = await _broker.Node.GetStateAsync(new StateRequest { Topic = Topic });

        state.Should().Be(new StateResponse { Epoch = 2, EndOffset = 2, HighWatermark = 1, NodeId = "broker-0", IsLeader = true });
    }

    [Fact]
    public async Task GetState_of_an_untouched_topic_reports_an_empty_log()
    {
        var state = await _broker.Node.GetStateAsync(new StateRequest { Topic = "payments" });

        state.EndOffset.Should().Be(-1);
        state.HighWatermark.Should().Be(-1);
    }

    [Fact]
    public async Task Invalid_GetState_fails_with_invalid_argument()
    {
        var act = () => _broker.Node.GetStateAsync(new StateRequest { Topic = "../etc" });

        (await act.Should().ThrowAsync<RpcException>()).Which.StatusCode.Should().Be(StatusCode.InvalidArgument);
    }

    private static async Task<List<RecordBatch>> FetchAsync(TestNode broker, long from, int max = 0, long epoch = 1) =>
        await broker.Node.FetchAsync(new FetchRequest { Epoch = epoch, Topic = Topic, FromOffset = from, MaxRecords = max }).ToListAsync();

    private static List<long> Offsets(IEnumerable<RecordBatch> batches) =>
        [.. batches.SelectMany(batch => batch.Records).Select(Decode).Select(record => record.Offset)];

    private static Record Decode(ByteString bytes)
    {
        RecordCodec.TryDecode(bytes.Span, out var record, out _).Should().Be(DecodeStatus.Ok);
        return record!;
    }
}
