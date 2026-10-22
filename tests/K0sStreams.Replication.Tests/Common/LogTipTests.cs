using K0sStreams.Replication.Common;
using K0sStreams.Replication.Failover;
using K0sStreams.Replication.Tests.Support;

namespace K0sStreams.Replication.Tests.Common;

/// <summary>How complete a log is (Raft's rule), and which survey answer a new leader copies from.</summary>
public class LogTipTests
{
    [Fact]
    public void A_newer_last_epoch_beats_a_longer_log()
    {
        new LogTip(5, 2).IsMoreCompleteThan(new LogTip(9, 1)).Should().BeTrue();
        new LogTip(9, 1).IsMoreCompleteThan(new LogTip(5, 2)).Should().BeFalse();
    }

    [Fact]
    public void With_the_same_last_epoch_the_longer_log_wins()
    {
        new LogTip(9, 1).IsMoreCompleteThan(new LogTip(5, 1)).Should().BeTrue();
        new LogTip(5, 1).IsMoreCompleteThan(new LogTip(9, 1)).Should().BeFalse();
    }

    [Fact]
    public void Equal_tips_are_not_more_complete_than_each_other() =>
        new LogTip(5, 1).IsMoreCompleteThan(new LogTip(5, 1)).Should().BeFalse();

    [Fact]
    public void An_empty_log_is_the_least_complete()
    {
        LogTip.Empty.IsEmpty.Should().BeTrue();
        new LogTip(0, 1).IsMoreCompleteThan(LogTip.Empty).Should().BeTrue();
        LogTip.Empty.IsMoreCompleteThan(new LogTip(0, 1)).Should().BeFalse();
    }

    [Fact]
    public void The_most_complete_answer_wins_the_survey()
    {
        using var broker = new TestNode();
        var answers = new[]
        {
            new SurveyAnswer("broker-2", broker.Node, new LogTip(5, 1)),
            new SurveyAnswer("broker-1", broker.Node, new LogTip(3, 2)),
            new SurveyAnswer("broker-0", broker.Node, new LogTip(9, 1)),
        };

        PeerSurvey.MostComplete(answers).NodeId.Should().Be("broker-1");
    }

    [Fact]
    public void On_a_tie_the_survey_keeps_this_brokers_own_log()
    {
        using var broker = new TestNode();
        var answers = new[]
        {
            new SurveyAnswer("broker-2", broker.Node, new LogTip(5, 1)),
            new SurveyAnswer("broker-1", broker.Node, new LogTip(5, 1)),
        };

        PeerSurvey.MostComplete(answers).NodeId.Should().Be("broker-2");
    }
}
