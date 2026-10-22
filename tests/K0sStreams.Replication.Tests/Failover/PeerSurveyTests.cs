using K0sStreams.Contracts;
using K0sStreams.Contracts.Grpc;
using K0sStreams.Replication.Common;
using K0sStreams.Replication.Failover;
using K0sStreams.Replication.Network;
using K0sStreams.Replication.Tests.Support;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using static K0sStreams.Replication.Tests.Support.TestRecords;

namespace K0sStreams.Replication.Tests.Failover;

/// <summary>Step 1 of a promotion: announcing the new epoch and finding where every log ends.</summary>
public class PeerSurveyTests
{
    [Fact]
    public async Task The_brokers_that_answer_refuse_the_old_leader_from_then_on()
    {
        await using var cluster = new FailoverCluster();
        await cluster.SeedAsync(1, 0, 1);
        cluster.Isolate(0);

        await SurveyAsync(cluster, surveyor: 2, epoch: 2);

        cluster.Brokers[1].Node.Epochs.Current.Should().Be(2);
        var fromOldLeader = await cluster.Brokers[1].Node.Node.AppendAsync(Append(1, 0, 1, -1));
        fromOldLeader.Code.Should().Be(AppendError.StaleEpoch);
    }

    [Fact]
    public async Task It_reports_where_each_log_ends_this_brokers_first()
    {
        await using var cluster = new FailoverCluster();
        await cluster.SeedAsync(1, 0, 1, 1, 2);
        await cluster.SeedAsync(2, 0, 1);
        cluster.Isolate(0);

        var answers = await SurveyAsync(cluster, surveyor: 2, epoch: 3);

        answers.Select(answer => (answer.NodeId, answer.Tip)).Should().Equal(("broker-2", new LogTip(0, 1)), ("broker-1", new LogTip(2, 2)));
    }

    [Fact]
    public async Task A_broker_at_a_newer_epoch_makes_the_survey_fail()
    {
        await using var cluster = new FailoverCluster();
        cluster.Isolate(0);
        cluster.Brokers[1].Cluster.ChangeEpoch(5, leader: false);

        await FluentActions.Awaiting(() => SurveyAsync(cluster, surveyor: 2, epoch: 2)).Should().ThrowAsync<NotLeaderException>();

        cluster.Brokers[2].Node.Epochs.Current.Should().Be(5);
    }

    [Fact]
    public async Task Topics_come_from_this_broker_and_the_answering_majority()
    {
        await using var cluster = new FailoverCluster();
        cluster.Brokers[1].Node.KnownTopics.Add("payments");
        cluster.Brokers[2].Node.KnownTopics.Add("orders");
        cluster.Isolate(0);

        var topics = cluster.Brokers[2].Survey.ListTopicsAsync(2, CancellationToken.None);
        await cluster.EventuallyAsync(() => topics.IsCompleted, "the topics");

        (await topics).Should().Equal("orders", "payments");
    }

    [Fact]
    public async Task The_announcement_waits_for_an_append_in_progress_and_counts_it()
    {
        var blocking = new BlockingLog();
        using var follower = new TestNode("broker-1", log: blocking);
        using var candidate = new TestNode("broker-2");
        var survey = new PeerSurvey(
            candidate.Node,
            candidate.Cluster,
            new Dictionary<string, IReplicationPeer> { ["broker-1"] = follower.Node },
            new MajorityCall(Options.Create(new ReplicationOptions()), new FakeTimeProvider()),
            candidate.Epochs);
        blocking.Block();
        var fromOldLeader = follower.Node.AppendAsync(Append(1, -1, 0, -1, Message(0, 1))); // held inside the log, with the topic locked

        var answers = survey.SurveyAsync(Topic, 2, CancellationToken.None);
        answers.IsCompleted.Should().BeFalse();
        blocking.Release();

        (await fromOldLeader).Ok.Should().BeTrue();
        (await answers).Single(answer => answer.NodeId == "broker-1").Tip.Should().Be(new LogTip(0, 1));
        (await follower.Node.AppendAsync(Append(1, 0, 1, -1))).Code.Should().Be(AppendError.StaleEpoch);
    }

    private static async Task<IReadOnlyList<SurveyAnswer>> SurveyAsync(FailoverCluster cluster, int surveyor, long epoch)
    {
        var survey = cluster.Brokers[surveyor].Survey.SurveyAsync(Topic, epoch, CancellationToken.None);
        await cluster.EventuallyAsync(() => survey.IsCompleted, "the survey");
        return await survey;
    }
}
