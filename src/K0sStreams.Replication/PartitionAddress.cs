using K0sStreams.Contracts;

namespace K0sStreams.Replication;

internal static class PartitionAddress
{
    /// <summary>
    /// Same rules as topic creation. Requests from other brokers are validated before touching the log because the
    /// topic name becomes a folder on disk: this is also what blocks names like <c>../</c>.
    /// </summary>
    public static bool IsValid(string topic, int partition) =>
        TopicConfig.IsValidName(topic) && partition is >= 0 and < TopicConfig.MaxPartitions;
}
