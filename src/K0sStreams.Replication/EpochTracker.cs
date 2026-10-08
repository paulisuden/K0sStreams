using K0sStreams.Contracts;

namespace K0sStreams.Replication;

/// <summary>
/// The epoch this broker uses for fencing: the newest of the epoch reported by coordination and the newest epoch
/// accepted from a leader. Both count because a new leader can start replicating before this broker's coordination
/// has seen the Lease change.
/// </summary>
internal sealed class EpochTracker(IClusterState cluster)
{
    private long _observed;

    public long Current => Math.Max(cluster.CurrentEpoch, Volatile.Read(ref _observed));

    /// <summary>Remembers <paramref name="epoch"/> if it is newer than anything seen so far, and returns the current epoch.</summary>
    public long Observe(long epoch)
    {
        long seen = Volatile.Read(ref _observed);
        while (epoch > seen)
        {
            long previous = Interlocked.CompareExchange(ref _observed, epoch, seen);
            if (previous == seen)
            {
                break;
            }

            seen = previous;
        }

        return Current;
    }
}
