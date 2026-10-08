using Google.Protobuf;
using K0sStreams.Contracts;
using K0sStreams.Contracts.Grpc;
using K0sStreams.Replication.Tests.Support;
using static K0sStreams.Replication.Tests.Support.TestRecords;

namespace K0sStreams.Replication.Tests;

/// <summary>Follower side of Append: docs/replication.md, section 5.3.</summary>
public sealed class AppendHandlerTests : IDisposable
{
    private readonly TestNode _follower = new(epoch: 1);

    public void Dispose() => _follower.Dispose();

    private Task<AppendResponse> AppendAsync(AppendRequest request) => _follower.Node.AppendAsync(request);

    [Fact]
    public async Task Append_to_an_empty_log_stores_the_records_as_sent()
    {
        var records = Sequence(1, 1, 1);

        var response = await AppendAsync(Append(1, -1, 0, -1, records));

        response.Ok.Should().BeTrue();
        response.Code.Should().Be(AppendError.None);
        response.EndOffset.Should().Be(2);
        (await EncodedAsync(_follower.Log)).Should().Equal(records.Select(r => Convert.ToBase64String(RecordCodec.Encode(r))));
    }

    [Fact]
    public async Task Append_after_the_last_record_extends_the_log_and_follows_the_leaders_high_watermark()
    {
        await SeedAsync(_follower.Log, 1, 1);
        _follower.Log.AdvanceHighWatermark(Topic, 0, 0);

        var response = await AppendAsync(Append(1, 1, 1, leaderHw: 1, Message(2, 1), Message(3, 1)));

        response.Ok.Should().BeTrue();
        response.Epoch.Should().Be(1);
        response.EndOffset.Should().Be(3);
        (await EpochsAsync(_follower.Log)).Should().Equal(1, 1, 1, 1);
        _follower.Log.HighWatermark(Topic, 0).Should().Be(1);
    }

    [Fact]
    public async Task Append_with_an_older_epoch_is_rejected_and_reports_the_local_epoch()
    {
        _follower.Cluster.ChangeEpoch(3, leader: false);

        var response = await AppendAsync(Append(2, -1, 0, -1, Message(0, 2)));

        response.Ok.Should().BeFalse();
        response.Code.Should().Be(AppendError.StaleEpoch);
        response.Epoch.Should().Be(3);
        _follower.Log.EndOffset(Topic, 0).Should().Be(-1);
    }

    [Fact]
    public async Task A_newer_epoch_is_adopted_and_fences_older_leaders()
    {
        (await AppendAsync(Append(4, -1, 0, -1, Message(0, 4)))).Epoch.Should().Be(4);

        var fromOldLeader = await AppendAsync(Append(3, 0, 4, -1));

        fromOldLeader.Code.Should().Be(AppendError.StaleEpoch);
        fromOldLeader.Epoch.Should().Be(4);
    }

    [Fact]
    public async Task A_newer_epoch_is_adopted_even_when_the_log_does_not_match()
    {
        var response = await AppendAsync(Append(5, 10, 5, -1));

        response.Code.Should().Be(AppendError.LogMismatch);
        _follower.Epochs.Current.Should().Be(5);
    }

    [Fact]
    public async Task Missing_previous_record_is_a_mismatch_that_reports_the_end_offset()
    {
        await SeedAsync(_follower.Log, 1, 1);

        var response = await AppendAsync(Append(1, 5, 1, -1, Message(6, 1)));

        response.Code.Should().Be(AppendError.LogMismatch);
        response.EndOffset.Should().Be(1);
        _follower.Log.EndOffset(Topic, 0).Should().Be(1);
    }

    [Fact]
    public async Task Previous_record_with_another_epoch_is_a_mismatch()
    {
        await SeedAsync(_follower.Log, 1, 1);

        var response = await AppendAsync(Append(2, 1, 2, -1, Message(2, 2)));

        response.Code.Should().Be(AppendError.LogMismatch);
        _follower.Log.EndOffset(Topic, 0).Should().Be(1);
    }

    [Fact]
    public async Task Record_with_a_bad_crc_is_rejected_and_nothing_is_appended()
    {
        var request = Append(1, -1, 0, -1, Sequence(1, 1));
        var damaged = request.Records[1].ToByteArray();
        damaged[^1] ^= 0xFF;
        request.Records[1] = ByteString.CopyFrom(damaged);

        var response = await AppendAsync(request);

        response.Code.Should().Be(AppendError.CorruptRecord);
        _follower.Log.EndOffset(Topic, 0).Should().Be(-1);
    }

    [Fact]
    public async Task Incomplete_or_padded_record_bytes_are_rejected()
    {
        var cut = Append(1, -1, 0, -1, Message(0, 1, "hello"));
        cut.Records[0] = ByteString.CopyFrom(cut.Records[0].Span[..^2]);
        var padded = Append(1, -1, 0, -1, Message(0, 1, "hello"));
        padded.Records[0] = ByteString.CopyFrom([.. padded.Records[0].ToByteArray(), 0]);

        (await AppendAsync(cut)).Code.Should().Be(AppendError.CorruptRecord);
        (await AppendAsync(padded)).Code.Should().Be(AppendError.CorruptRecord);
    }

    [Fact]
    public async Task Records_with_a_gap_in_their_offsets_are_rejected()
    {
        var response = await AppendAsync(Append(1, -1, 0, -1, Message(0, 1), Message(2, 1)));

        response.Code.Should().Be(AppendError.CorruptRecord);
        _follower.Log.EndOffset(Topic, 0).Should().Be(-1);
    }

    [Theory]
    [InlineData(3)] // newer than the leader sending it
    [InlineData(1)] // older than the record before it
    public async Task Records_with_an_impossible_epoch_are_rejected(long recordEpoch)
    {
        await SeedAsync(_follower.Log, 2);

        var response = await AppendAsync(Append(2, 0, 2, -1, Message(1, recordEpoch)));

        response.Code.Should().Be(AppendError.CorruptRecord);
    }

    [Theory]
    [InlineData("../etc", 0)]
    [InlineData("", 0)]
    [InlineData("Orders", 0)]
    [InlineData("orders", -1)]
    [InlineData("orders", 64)]
    public async Task Invalid_topic_or_partition_is_rejected(string topic, int partition)
    {
        var request = Append(1, -1, 0, -1, Message(0, 1));
        request.Topic = topic;
        request.Partition = partition;

        var response = await AppendAsync(request);

        response.Code.Should().Be(AppendError.UnknownTopic);
    }

    [Fact]
    public async Task Repeating_an_append_changes_nothing()
    {
        var request = Append(1, -1, 0, -1, Sequence(1, 1));
        await AppendAsync(request);

        var again = await AppendAsync(request);

        again.Ok.Should().BeTrue();
        again.EndOffset.Should().Be(1);
        (await EpochsAsync(_follower.Log)).Should().Equal(1, 1);
    }

    [Fact]
    public async Task Divergent_tail_is_replaced_by_the_leaders_records()
    {
        await SeedAsync(_follower.Log, 1, 1, 1); // offsets 1 and 2 came from an old leader and were never confirmed

        var response = await AppendAsync(Append(2, 0, 1, -1, Message(1, 2), Message(2, 2), Message(3, 2)));

        response.Ok.Should().BeTrue();
        response.EndOffset.Should().Be(3);
        (await EpochsAsync(_follower.Log)).Should().Equal(1, 2, 2, 2);
    }

    [Fact]
    public async Task Records_before_the_first_conflict_are_kept()
    {
        await SeedAsync(_follower.Log, 1, 1, 1);

        await AppendAsync(Append(2, -1, 0, -1, Message(0, 1), Message(1, 1), Message(2, 2)));

        (await EpochsAsync(_follower.Log)).Should().Equal(1, 1, 2);
    }

    [Fact]
    public async Task A_shorter_matching_append_does_not_truncate_the_tail()
    {
        await SeedAsync(_follower.Log, 1, 1, 1, 1);

        var response = await AppendAsync(Append(1, -1, 0, -1, Message(0, 1), Message(1, 1)));

        response.Ok.Should().BeTrue();
        response.EndOffset.Should().Be(3);
    }

    [Fact]
    public async Task Heartbeat_moves_the_high_watermark_up_to_the_leaders()
    {
        await SeedAsync(_follower.Log, 1, 1, 1);

        (await AppendAsync(Append(1, 2, 1, leaderHw: 2))).Ok.Should().BeTrue();

        _follower.Log.HighWatermark(Topic, 0).Should().Be(2);
    }

    [Fact]
    public async Task High_watermark_stops_at_the_prefix_verified_against_the_leader()
    {
        await SeedAsync(_follower.Log, 1, 1, 1, 1); // this request does not vouch for offsets 2 and 3

        await AppendAsync(Append(1, 1, 1, leaderHw: 3));

        _follower.Log.HighWatermark(Topic, 0).Should().Be(1);
    }

    [Fact]
    public async Task High_watermark_never_moves_back()
    {
        await SeedAsync(_follower.Log, 1, 1, 1);
        await AppendAsync(Append(1, 2, 1, leaderHw: 2));

        await AppendAsync(Append(1, 2, 1, leaderHw: 0));

        _follower.Log.HighWatermark(Topic, 0).Should().Be(2);
    }

    [Fact]
    public async Task Conflict_with_confirmed_records_is_refused()
    {
        await SeedAsync(_follower.Log, 1, 1);
        _follower.Log.AdvanceHighWatermark(Topic, 0, 1);

        var act = () => AppendAsync(Append(2, 0, 1, -1, Message(1, 2)));

        await act.Should().ThrowAsync<InvalidOperationException>();
        (await EpochsAsync(_follower.Log)).Should().Equal(1, 1);
    }

    [Fact]
    public async Task Queue_events_replicate_like_messages()
    {
        var ack = new QueueEvent(RecordType.Ack, "billing", 0, 0, 0).ToRecord(epoch: 1, timestamp: 1234) with { Offset = 1 };

        (await AppendAsync(Append(1, -1, 0, -1, Message(0, 1, "order"), ack))).Ok.Should().BeTrue();

        var stored = await _follower.Log.ReadAsync(Topic, 0, 1, 1).SingleAsync();
        QueueEvent.FromRecord(stored).Should().Be(new QueueEvent(RecordType.Ack, "billing", 0, 0, 0));
    }

    [Fact]
    public async Task Concurrent_appends_to_the_same_partition_do_not_interfere()
    {
        var request = Append(1, -1, 0, -1, Sequence(1, 1, 1, 1, 1));

        var responses = await Task.WhenAll(Enumerable.Range(0, 20).Select(_ => AppendAsync(request)));

        responses.Should().OnlyContain(response => response.Ok);
        (await EpochsAsync(_follower.Log)).Should().Equal(1, 1, 1, 1, 1);
    }
}
