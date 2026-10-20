namespace K0sStreams.Replication;

/// <summary>
/// Topics this broker holds a log for, as far as replication has seen since it started: topics it received as a
/// follower and topics it wrote as the leader. A new leader gathers them from a majority (<c>ListTopics</c>), so it
/// reconciles every topic when it takes over. Kept in memory only: after a restart, a broker relearns its topics as
/// they are written again.
/// </summary>
internal sealed class KnownTopics
{
    private readonly Lock _gate = new();
    private readonly HashSet<string> _topics = new(StringComparer.Ordinal);

    /// <summary>
    /// Records <paramref name="topic"/>. It's a full memory barrier, so a caller that adds a topic and then checks the
    /// epoch can rely on the order: a concurrent <c>ListTopics</c> either lists the topic, or had already raised the epoch
    /// that check will see.
    /// </summary>
    public void Add(string topic)
    {
        lock (_gate)
        {
            _topics.Add(topic);
        }

        Interlocked.MemoryBarrier();
    }

    public string[] Snapshot()
    {
        lock (_gate)
        {
            return [.. _topics.Order(StringComparer.Ordinal)];
        }
    }
}
