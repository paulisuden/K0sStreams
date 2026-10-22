using K0sStreams.Contracts;
using K0sStreams.Replication.Common;
using K0sStreams.Replication.Tests.Support;
using static K0sStreams.Replication.Tests.Support.TestRecords;

namespace K0sStreams.Replication.Tests.Failover;

/// <summary>
/// Failover end to end, on three complete brokers: a new leader gets ready without losing any confirmed record.
/// docs/replication.md §10.1 and docs/replication-scenarios.md §18.
/// </summary>
public class LeaderPromotionTests
{
    [Fact]
    public async Task A_confirmed_record_survives_a_lagging_broker_becoming_leader()
    {
        // 2 was confirmed by broker-0 and broker-1; broker-2 never got it, then wins the Lease.
        await using var cluster = new FailoverCluster();
        await cluster.SeedAsync(0, 2, 1, 1, 1);
        await cluster.SeedAsync(1, 1, 1, 1, 1);
        await cluster.SeedAsync(2, 1, 1, 1);
        cluster.Isolate(0);

        await cluster.ElectAsync(2, 2);

        (await EpochsAsync(cluster.Brokers[2].Log)).Should().Equal(1, 1, 1, 2);
        await cluster.EventuallyAsync(() => cluster.Brokers[1].Log.HighWatermark(Topic) == 3, "broker-1 to confirm the epoch change");
        (await EpochsAsync(cluster.Brokers[1].Log)).Should().Equal(1, 1, 1, 2);
        await cluster.ConfirmAsync(2, await cluster.WriteAsync(2));
    }

    [Fact]
    public async Task A_record_the_new_leader_had_not_seen_confirmed_is_kept_and_confirmed()
    {
        // broker-1 has 2, but its HW still says 1: truncating to the HW would lose a confirmed record.
        await using var cluster = new FailoverCluster();
        await cluster.SeedAsync(0, 2, 1, 1, 1);
        await cluster.SeedAsync(1, 1, 1, 1, 1);
        await cluster.SeedAsync(2, 1, 1, 1);
        cluster.Isolate(0);

        await cluster.ElectAsync(1, 2);

        await cluster.EventuallyAsync(() => cluster.Brokers[1].Log.HighWatermark(Topic) == 3, "the epoch change, and 2 with it, to be confirmed");
        (await EpochsAsync(cluster.Brokers[1].Log)).Should().Equal(1, 1, 1, 2);
        (await EpochsAsync(cluster.Brokers[2].Log)).Should().Equal(1, 1, 1, 2);
    }

    [Fact]
    public async Task The_old_leaders_unconfirmed_tail_is_replaced_when_it_returns()
    {
        await using var cluster = new FailoverCluster();
        await cluster.SeedAsync(0, 2, 1, 1, 1, 1); // 3 reached only broker-0: never confirmed
        await cluster.SeedAsync(1, 2, 1, 1, 1);
        await cluster.SeedAsync(2, 2, 1, 1, 1);
        cluster.Isolate(0);
        await cluster.ElectAsync(1, 2);

        cluster.Heal(0);

        await cluster.EventuallyAsync(() => cluster.Brokers[0].Log.HighWatermark(Topic) == 3, "broker-0 to follow the new leader");
        (await EpochsAsync(cluster.Brokers[0].Log)).Should().Equal(1, 1, 1, 2);
    }

    [Fact]
    public async Task The_old_leader_can_no_longer_confirm_anything()
    {
        await using var cluster = new FailoverCluster();
        await cluster.SeedAsync(0, 0, 1);
        await cluster.SeedAsync(1, 0, 1);
        await cluster.SeedAsync(2, 0, 1);
        cluster.Isolate(0);
        await cluster.ElectAsync(2, 2);
        cluster.Heal(0); // it comes back still believing it leads epoch 1

        long offset = await cluster.WriteAsync(0, "from the old leader");
        var wait = cluster.Brokers[0].Replicator.WaitForQuorumAsync(Topic, 0, offset).AsTask();

        await cluster.EventuallyAsync(() => wait.IsCompleted, "the old leader's write to end");
        await FluentActions.Awaiting(() => wait).Should().ThrowAsync<NotLeaderException>();
        (await cluster.Brokers[1].Log.ReadAsync(Topic, 1, 10).ToListAsync()).Should().OnlyContain(record => record.Epoch == 2);
    }

    [Fact]
    public async Task A_replaced_leader_not_yet_told_by_coordination_still_follows_the_new_leader()
    {
        await using var cluster = new FailoverCluster();
        await cluster.SeedAsync(0, 0, 1);
        await cluster.SeedAsync(1, 0, 1);
        await cluster.SeedAsync(2, 0, 1);
        await cluster.PromoteAsync(2, 2);
        cluster.Brokers[0].Node.Epochs.Observe(2); // the survey reached it...
        cluster.Brokers[1].Cluster.ChangeEpoch(2, leader: false);
        cluster.Brokers[2].Cluster.ChangeEpoch(2, leader: true); // ...but its coordination still says it leads epoch 1

        long offset = await cluster.WriteAsync(2);
        await cluster.ConfirmAsync(2, offset);

        await cluster.EventuallyAsync(() => cluster.Brokers[0].Log.HighWatermark(Topic) == offset, "broker-0 to follow the new leader");
    }

    [Fact]
    public async Task A_topic_only_another_broker_has_is_promoted_too()
    {
        await using var cluster = new FailoverCluster();
        await cluster.SeedAsync(1, 1, 1, 1); // broker-2 never received any record of it
        cluster.Isolate(0);

        await cluster.ElectAsync(2, 2);

        (await EpochsAsync(cluster.Brokers[2].Log)).Should().Equal(1, 1, 2);
    }

    [Fact]
    public async Task A_topic_without_records_gets_no_epoch_change()
    {
        await using var cluster = new FailoverCluster();
        cluster.Brokers[1].Node.KnownTopics.Add("ghost"); // e.g. a refused append left only its name
        cluster.Isolate(0);

        await cluster.ElectAsync(2, 2);

        cluster.Brokers[2].Log.EndOffset("ghost").Should().Be(-1);
    }

    [Fact]
    public async Task Promotion_waits_until_a_majority_answers()
    {
        await using var cluster = new FailoverCluster();
        await cluster.SeedAsync(2, 0, 1);
        cluster.Isolate(0);
        cluster.Isolate(1);
        var promotion = cluster.Brokers[2].Promotion.PromoteAsync(2).AsTask();
        await cluster.Time.AdvanceSlowlyAsync(TimeSpan.FromSeconds(10));
        promotion.IsCompleted.Should().BeFalse();

        cluster.Heal(1);

        await cluster.EventuallyAsync(() => promotion.IsCompleted, "the promotion to finish");
        await promotion;
    }

    [Fact]
    public async Task A_cancelled_promotion_can_be_retried()
    {
        await using var cluster = new FailoverCluster();
        await cluster.SeedAsync(1, 0, 1, 1);
        cluster.Isolate(0);
        cluster.Isolate(1);
        using var cts = new CancellationTokenSource();
        var first = cluster.Brokers[2].Promotion.PromoteAsync(2, cts.Token).AsTask();
        await cluster.Time.AdvanceSlowlyAsync(TimeSpan.FromSeconds(1));
        await cts.CancelAsync();
        await FluentActions.Awaiting(() => first).Should().ThrowAsync<OperationCanceledException>();

        cluster.Heal(1);
        await cluster.ElectAsync(2, 2);

        (await EpochsAsync(cluster.Brokers[2].Log)).Should().Equal(1, 1, 2);
    }

    [Fact]
    public async Task Promoting_twice_at_the_same_epoch_marks_it_once()
    {
        await using var cluster = new FailoverCluster();
        await cluster.SeedAsync(1, 0, 1);
        await cluster.SeedAsync(2, 0, 1);
        cluster.Isolate(0);

        await cluster.PromoteAsync(2, 2);
        await cluster.PromoteAsync(2, 2);

        (await EpochsAsync(cluster.Brokers[2].Log)).Should().Equal(1, 2);
    }

    [Fact]
    public async Task An_old_leader_elected_again_keeps_its_unconfirmed_tail_and_confirms_it()
    {
        await using var cluster = new FailoverCluster();
        await cluster.SeedAsync(0, 1, 1, 1, 1); // 2 reached nobody else
        await cluster.SeedAsync(1, 1, 1, 1);
        await cluster.SeedAsync(2, 1, 1, 1);
        cluster.Brokers[0].Cluster.ChangeEpoch(1, leader: false); // it lost the Lease...

        await cluster.ElectAsync(0, 3); // ...and won it again, at a later epoch

        await cluster.EventuallyAsync(() => cluster.Brokers.All(broker => broker.Log.HighWatermark(Topic) == 3), "every broker to confirm the epoch change");
        foreach (var broker in cluster.Brokers)
        {
            (await EpochsAsync(broker.Log)).Should().Equal(1, 1, 1, 3);
        }
    }

    [Fact]
    public async Task A_lone_broker_only_marks_the_new_epoch()
    {
        await using var cluster = new FailoverCluster(size: 1);
        cluster.Brokers[0].Cluster.ChangeEpoch(1, leader: false);
        await cluster.SeedAsync(0, 0, 1, 1);

        await cluster.ElectAsync(0, 2);

        (await EpochsAsync(cluster.Brokers[0].Log)).Should().Equal(1, 1, 2);
    }

    [Fact]
    public async Task Promoting_to_an_epoch_older_than_one_already_seen_is_refused()
    {
        await using var cluster = new FailoverCluster();
        cluster.Brokers[2].Node.Epochs.Observe(3);

        await FluentActions.Awaiting(() => cluster.Brokers[2].Promotion.PromoteAsync(2).AsTask()).Should().ThrowAsync<NotLeaderException>();
    }

    [Fact]
    public async Task Promoting_after_announcing_leadership_is_refused()
    {
        await using var cluster = new FailoverCluster();

        // broker-0 already leads: records of its epoch could land after the new epoch's.
        await FluentActions.Awaiting(() => cluster.Brokers[0].Promotion.PromoteAsync(2).AsTask()).Should().ThrowAsync<InvalidOperationException>();
    }
}
