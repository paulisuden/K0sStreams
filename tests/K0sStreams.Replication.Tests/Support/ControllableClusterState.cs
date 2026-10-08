using K0sStreams.Contracts;

namespace K0sStreams.Replication.Tests.Support;

/// <summary>Cluster state the tests can change, unlike <c>StaticClusterState</c>: leader, epoch and <see cref="EpochChanged"/>.</summary>
internal sealed class ControllableClusterState(string nodeId, bool isLeader = false, long epoch = 1) : IClusterState
{
    public event Action<long>? EpochChanged;

    public string NodeId => nodeId;

    public bool IsLeader { get; private set; } = isLeader;

    public long CurrentEpoch { get; private set; } = epoch;

    public string? LeaderAddress => null;

    public void ChangeEpoch(long newEpoch, bool leader)
    {
        CurrentEpoch = newEpoch;
        IsLeader = leader;
        EpochChanged?.Invoke(newEpoch);
    }
}
