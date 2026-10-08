using K0sStreams.Contracts;

namespace K0sStreams.Replication;

/// <summary>
/// Every topic is a single log (DEC-001, docs/decisions.md). <see cref="ILog"/> and <c>replication.proto</c> still carry
/// a partition until the contracts drop it, so replication pins it here, in one place, and the rest of the block already
/// reads as it will afterwards. When the contracts change, delete this file and validate with
/// <see cref="TopicConfig.IsValidName"/>.
/// </summary>
internal static class SingleLog
{
    /// <summary>The partition every topic is stored in. It is also the protobuf default, so senders can leave it out.</summary>
    public const int Partition = 0;

    /// <summary>
    /// A valid topic name and no partition other than <see cref="Partition"/>. The name becomes a folder on disk, so this
    /// also blocks names like <c>../</c>.
    /// </summary>
    public static bool IsValid(string topic, int partition) => TopicConfig.IsValidName(topic) && partition == Partition;

    public static ValueTask<long> AppendAsync(this ILog log, string topic, Record record, CancellationToken ct = default) =>
        log.AppendAsync(topic, Partition, record, ct);

    public static IAsyncEnumerable<Record> ReadAsync(this ILog log, string topic, long fromOffset, int max, CancellationToken ct = default) =>
        log.ReadAsync(topic, Partition, fromOffset, max, ct);

    public static long HighWatermark(this ILog log, string topic) => log.HighWatermark(topic, Partition);

    public static long EndOffset(this ILog log, string topic) => log.EndOffset(topic, Partition);

    public static void AdvanceHighWatermark(this ILog log, string topic, long offset) =>
        log.AdvanceHighWatermark(topic, Partition, offset);

    public static ValueTask TruncateAsync(this ILog log, string topic, long toOffset, CancellationToken ct = default) =>
        log.TruncateAsync(topic, Partition, toOffset, ct);
}
