namespace K0sStreams.Contracts;

/// <summary>
/// Makes this broker ready to lead a new epoch without losing any confirmed record (docs/replication.md §10.1).
/// Implemented by Replication; called by Coordination after it wins the Lease.
/// </summary>
/// <remarks>
/// Coordination must:
/// <list type="bullet">
/// <item>call it while <see cref="IClusterState.IsLeader"/> is still false, so nothing is written at the old epoch meanwhile;</item>
/// <item>only after it completes, set <c>IsLeader = true</c> and <c>CurrentEpoch</c> to the new epoch, then raise <see cref="IClusterState.EpochChanged"/>;</item>
/// <item>cancel the token if it loses the Lease meanwhile;</item>
/// <item>give each epoch to one broker only, and report the Lease's epoch on every broker, including after a restart;</item>
/// <item>not take this interface in the constructor of its <see cref="IClusterState"/> (that would be a dependency cycle).</item>
/// </list>
/// </remarks>
public interface ILeaderPromotion
{
    /// <summary>
    /// Announces <paramref name="newEpoch"/> to the other brokers, which then refuse the old leader; copies any record this
    /// broker is missing from the most complete log among a majority; and writes an <see cref="RecordType.EpochChange"/>
    /// record in every topic. Waits while no majority answers.
    /// Throws <see cref="NotLeaderException"/> if some broker is already at a newer epoch.
    /// </summary>
    ValueTask PromoteAsync(long newEpoch, CancellationToken ct = default);
}
