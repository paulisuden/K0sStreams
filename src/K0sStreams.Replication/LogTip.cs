namespace K0sStreams.Replication;

/// <summary>
/// Where a log ends: its last record. Raft's rule for how complete a log is: a last record from a newer epoch wins; with
/// the same last epoch, the longer log wins. Two logs with the same tip are identical.
/// </summary>
/// <param name="EndOffset">Offset of the last record; -1 if the log is empty.</param>
/// <param name="LastEpoch">Epoch of the last record; 0 if the log is empty.</param>
internal readonly record struct LogTip(long EndOffset, long LastEpoch)
{
    public static LogTip Empty { get; } = new(-1, 0);

    public bool IsEmpty => EndOffset < 0;

    public bool IsMoreCompleteThan(LogTip other) =>
        LastEpoch != other.LastEpoch ? LastEpoch > other.LastEpoch : EndOffset > other.EndOffset;
}
