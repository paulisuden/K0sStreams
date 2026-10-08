using K0sStreams.Contracts;
using K0sStreams.Replication.Tests.Support;
using static K0sStreams.Replication.Tests.Support.TestRecords;

namespace K0sStreams.Replication.Tests;

/// <summary>Leader side: quorum, high watermark, catch-up and stepping down. docs/replication.md, sections 5.1 to 5.6.</summary>
public class QuorumReplicatorTests
{
    [Fact]
    public void Quorum_offset_is_the_needed_highest_follower_match()
    {
        TopicReplication.QuorumOffset([7, 3], needed: 1).Should().Be(7);
        TopicReplication.QuorumOffset([10, 7, 3, -1], needed: 2).Should().Be(7);
    }

    [Fact]
    public async Task A_write_is_confirmed_and_reaches_both_followers()
    {
        await using var cluster = new ReplicationCluster();
        long offset = await cluster.WriteAsync();

        await cluster.ConfirmAsync(offset);

        cluster.Leader.Log.HighWatermark(Topic).Should().Be(offset);
        await cluster.EventuallyAsync(() => cluster.Followers.All(f => f.Log.EndOffset(Topic) == offset), "both followers to have the record");
        foreach (var follower in cluster.Followers)
        {
            (await EncodedAsync(follower.Log)).Should().Equal(await EncodedAsync(cluster.Leader.Log));
        }
    }

    [Fact]
    public async Task A_write_is_confirmed_with_one_follower_down()
    {
        await using var cluster = new ReplicationCluster();
        cluster.Links[0].Disconnect();
        long offset = await cluster.WriteAsync();

        await cluster.ConfirmAsync(offset);

        cluster.Followers[0].Log.EndOffset(Topic).Should().Be(-1);
        cluster.Followers[1].Log.EndOffset(Topic).Should().Be(offset);
    }

    [Fact]
    public async Task A_slow_follower_does_not_delay_confirmation()
    {
        await using var cluster = new ReplicationCluster();
        cluster.Links[0].Pause();
        long offset = await cluster.WriteAsync();

        await cluster.ConfirmAsync(offset);

        cluster.Followers[0].Log.EndOffset(Topic).Should().Be(-1);
    }

    [Fact]
    public async Task With_both_followers_down_the_wait_lasts_until_cancelled()
    {
        await using var cluster = new ReplicationCluster();
        cluster.Links[0].Disconnect();
        cluster.Links[1].Disconnect();
        long offset = await cluster.WriteAsync();
        using var cts = new CancellationTokenSource();
        var wait = cluster.WaitAsync(offset, cts.Token);

        await cluster.Time.AdvanceSlowlyAsync(TimeSpan.FromSeconds(10));

        wait.IsCompleted.Should().BeFalse();
        cluster.Leader.Log.HighWatermark(Topic).Should().Be(-1);
        await cts.CancelAsync();
        await FluentActions.Awaiting(() => wait).Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task Pending_writes_confirm_when_a_follower_returns()
    {
        await using var cluster = new ReplicationCluster();
        cluster.Links[0].Disconnect();
        cluster.Links[1].Disconnect();
        long offset = await cluster.WriteAsync();
        var wait = cluster.WaitAsync(offset);
        await cluster.Time.AdvanceSlowlyAsync(TimeSpan.FromSeconds(2));

        cluster.Links[1].Reconnect();

        await cluster.EventuallyAsync(() => wait.IsCompleted, "the write to be confirmed");
        await wait;
    }

    [Fact]
    public async Task A_follower_restarted_empty_catches_up_and_counts_again()
    {
        await using var cluster = new ReplicationCluster();
        for (int i = 0; i < 3; i++)
        {
            await cluster.ConfirmAsync(await cluster.WriteAsync());
        }

        cluster.Links[0].Disconnect();
        cluster.RestartFollower(1);
        long offset = await cluster.WriteAsync();

        await cluster.ConfirmAsync(offset);

        (await EncodedAsync(cluster.Followers[1].Log)).Should().Equal(await EncodedAsync(cluster.Leader.Log));
    }

    [Fact]
    public async Task A_divergent_follower_tail_is_replaced()
    {
        await using var cluster = new ReplicationCluster(leaderEpoch: 2);
        await SeedAsync(cluster.Leader.Log, 1, 2, 2);
        await SeedAsync(cluster.Followers[0].Log, 1, 1, 1); // 1 and 2 came from the old leader
        cluster.Links[1].Disconnect();
        long offset = await cluster.WriteAsync();

        await cluster.ConfirmAsync(offset);

        (await EpochsAsync(cluster.Followers[0].Log)).Should().Equal(1, 2, 2, 2);
    }

    [Fact]
    public async Task A_lost_response_is_retried_without_duplicating_records()
    {
        await using var cluster = new ReplicationCluster();
        cluster.Links[1].Disconnect();
        cluster.Links[0].DropNextResponse();
        long offset = await cluster.WriteAsync();

        await cluster.ConfirmAsync(offset);

        (await EncodedAsync(cluster.Followers[0].Log)).Should().Equal(await EncodedAsync(cluster.Leader.Log));
    }

    [Fact]
    public async Task Many_concurrent_writes_are_all_confirmed()
    {
        await using var cluster = new ReplicationCluster();

        var all = Task.WhenAll(Enumerable.Range(0, 200).Select(_ => Task.Run(async () => await cluster.WaitAsync(await cluster.WriteAsync()))));

        await cluster.EventuallyAsync(() => all.IsCompleted, "every write to be confirmed");
        await all;
        cluster.Leader.Log.HighWatermark(Topic).Should().Be(199);
    }

    [Fact]
    public async Task Waiting_for_an_already_confirmed_offset_completes_synchronously()
    {
        await using var cluster = new ReplicationCluster();
        long offset = await cluster.WriteAsync();
        await cluster.ConfirmAsync(offset);

        cluster.Replicator.WaitForQuorumAsync(Topic, 0, offset).AsTask().IsCompletedSuccessfully.Should().BeTrue();
    }

    [Fact]
    public async Task Followers_learn_the_high_watermark_after_confirmation()
    {
        await using var cluster = new ReplicationCluster();
        long offset = await cluster.WriteAsync();

        await cluster.ConfirmAsync(offset);

        await cluster.EventuallyAsync(() => cluster.Followers.All(f => f.Log.HighWatermark(Topic) == offset), "followers to learn the HW");
    }

    [Fact]
    public async Task A_follower_restarted_while_idle_catches_up_through_heartbeats()
    {
        await using var cluster = new ReplicationCluster();
        long offset = await cluster.WriteAsync();
        await cluster.ConfirmAsync(offset);

        cluster.RestartFollower(0);

        await cluster.EventuallyAsync(() => cluster.Followers[0].Log.EndOffset(Topic) == offset, "the restarted follower to catch up");
    }

    [Fact]
    public async Task The_high_watermark_waits_for_a_record_of_the_current_epoch()
    {
        await using var cluster = new ReplicationCluster(leaderEpoch: 2);
        await SeedAsync(cluster.Leader.Log, 1, 1); // left by the previous leader, never confirmed
        var oldRecord = cluster.WaitAsync(1);
        await cluster.EventuallyAsync(() => cluster.Followers.All(f => f.Log.EndOffset(Topic) == 1), "followers to copy the old records");
        await cluster.Time.AdvanceSlowlyAsync(TimeSpan.FromSeconds(1));
        oldRecord.IsCompleted.Should().BeFalse();
        cluster.Leader.Log.HighWatermark(Topic).Should().Be(-1);

        long offset = await cluster.WriteAsync();
        await cluster.ConfirmAsync(offset);

        await oldRecord;
        cluster.Leader.Log.HighWatermark(Topic).Should().Be(offset);
    }

    [Fact]
    public async Task A_newer_epoch_from_a_follower_fails_pending_waits_with_NotLeaderException()
    {
        await using var cluster = new ReplicationCluster();
        foreach (var follower in cluster.Followers)
        {
            follower.Cluster.ChangeEpoch(2, leader: false);
        }

        long offset = await cluster.WriteAsync();
        var wait = cluster.WaitAsync(offset);

        await cluster.EventuallyAsync(() => wait.IsCompleted, "the wait to end");
        await FluentActions.Awaiting(() => wait).Should().ThrowAsync<NotLeaderException>();
        cluster.Leader.Epochs.Current.Should().Be(2);
        await FluentActions.Awaiting(() => cluster.WaitAsync(offset)).Should().ThrowAsync<NotLeaderException>();
    }

    [Fact]
    public async Task Demotion_while_waiting_fails_the_wait_with_NotLeaderException()
    {
        await using var cluster = new ReplicationCluster();
        cluster.Links[0].Disconnect();
        cluster.Links[1].Disconnect();
        long offset = await cluster.WriteAsync();
        var wait = cluster.WaitAsync(offset);

        cluster.Leader.Cluster.ChangeEpoch(2, leader: false);

        await FluentActions.Awaiting(() => wait).Should().ThrowAsync<NotLeaderException>();
    }

    [Fact]
    public async Task A_broker_that_is_not_leader_refuses_to_wait()
    {
        await using var cluster = new ReplicationCluster();
        cluster.Leader.Cluster.ChangeEpoch(1, leader: false);
        long offset = await cluster.WriteAsync();

        await FluentActions.Awaiting(() => cluster.WaitAsync(offset)).Should().ThrowAsync<NotLeaderException>();
    }

    [Fact]
    public async Task A_cancelled_wait_does_not_affect_other_waiters()
    {
        await using var cluster = new ReplicationCluster();
        cluster.Links[0].Disconnect();
        cluster.Links[1].Disconnect();
        long offset = await cluster.WriteAsync();
        using var cts = new CancellationTokenSource();
        var cancelled = cluster.WaitAsync(offset, cts.Token);
        var other = cluster.WaitAsync(offset);

        await cts.CancelAsync();
        cluster.Links[0].Reconnect();

        await FluentActions.Awaiting(() => cancelled).Should().ThrowAsync<OperationCanceledException>();
        await cluster.EventuallyAsync(() => other.IsCompleted, "the other wait to complete");
        await other;
    }

    [Fact]
    public async Task Without_peers_writes_are_confirmed_at_once()
    {
        await using var cluster = new ReplicationCluster(followers: 0);
        long offset = await cluster.WriteAsync();

        cluster.Replicator.WaitForQuorumAsync(Topic, 0, offset).AsTask().IsCompletedSuccessfully.Should().BeTrue();
        cluster.Leader.Log.HighWatermark(Topic).Should().Be(offset);
    }

    [Fact]
    public async Task Dispose_cancels_in_flight_appends_and_fails_pending_waits()
    {
        await using var cluster = new ReplicationCluster();
        cluster.Links[0].Pause();
        cluster.Links[1].Pause();
        long offset = await cluster.WriteAsync();
        var wait = cluster.WaitAsync(offset);

        await cluster.Replicator.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));

        await FluentActions.Awaiting(() => wait).Should().ThrowAsync<NotLeaderException>();
    }

    [Fact]
    public async Task Invalid_arguments_are_rejected()
    {
        await using var cluster = new ReplicationCluster();
        long offset = await cluster.WriteAsync();

        await FluentActions.Awaiting(() => cluster.Replicator.WaitForQuorumAsync("../etc", 0, 0).AsTask()).Should().ThrowAsync<ArgumentException>();
        await FluentActions.Awaiting(() => cluster.Replicator.WaitForQuorumAsync(Topic, 1, offset).AsTask()).Should().ThrowAsync<ArgumentException>();
        await FluentActions.Awaiting(() => cluster.Replicator.WaitForQuorumAsync(Topic, 0, -1).AsTask()).Should().ThrowAsync<ArgumentOutOfRangeException>();
        await FluentActions.Awaiting(() => cluster.Replicator.WaitForQuorumAsync(Topic, 0, offset + 1).AsTask()).Should().ThrowAsync<ArgumentOutOfRangeException>();
    }
}
