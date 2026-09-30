namespace K0sStreams.Contracts.Fakes;

/// <summary>Rol fijo, sin Lease. Por defecto: "soy líder, época 1".</summary>
public sealed class StaticClusterState(string nodeId, bool isLeader = true, long epoch = 1, string? leaderAddress = null)
    : IClusterState
{
    public string NodeId => nodeId;

    public bool IsLeader => isLeader;

    public long CurrentEpoch => epoch;

    public string? LeaderAddress => leaderAddress;

    // La época nunca cambia.
    public event Action<long>? EpochChanged
    {
        add { }
        remove { }
    }
}
