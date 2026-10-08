using K0sStreams.Replication.Tests.Support;

namespace K0sStreams.Replication.Tests;

public class EpochTrackerTests
{
    private readonly ControllableClusterState _cluster = new("broker-1", epoch: 2);
    private readonly EpochTracker _epochs;

    public EpochTrackerTests() => _epochs = new EpochTracker(_cluster);

    [Fact]
    public void Starts_at_the_cluster_epoch() => _epochs.Current.Should().Be(2);

    [Fact]
    public void A_newer_observed_epoch_wins()
    {
        _epochs.Observe(5).Should().Be(5);
        _epochs.Current.Should().Be(5);
    }

    [Fact]
    public void An_older_observed_epoch_changes_nothing() => _epochs.Observe(1).Should().Be(2);

    [Fact]
    public void The_cluster_epoch_wins_once_it_moves_past_the_observed_one()
    {
        _epochs.Observe(3);

        _cluster.ChangeEpoch(4, leader: true);

        _epochs.Current.Should().Be(4);
    }

    [Fact]
    public void Concurrent_observations_keep_the_highest()
    {
        Parallel.For(1, 1001, epoch => _epochs.Observe(epoch));

        _epochs.Current.Should().Be(1000);
    }
}
