using K0sStreams.Contracts;

namespace K0sStreams.Replication;

/// <summary>Settings of the replication block, read from the <c>Replication</c> configuration section.</summary>
public sealed class ReplicationOptions
{
    public const string SectionName = "Replication";

    /// <summary>Largest gRPC message between brokers: one record of the maximum size plus room for the batch framing.</summary>
    public const int MaxMessageSize = 2 * RecordCodec.MaxRecordSize;

    /// <summary>gRPC address of every broker by NodeId, e.g. <c>broker-1 → http://broker-1.broker:9091</c>. This broker's own entry is ignored.</summary>
    public IDictionary<string, Uri> Peers { get; } = new Dictionary<string, Uri>(StringComparer.Ordinal);

    /// <summary>Deadline of unary calls (Append, GetState) to another broker.</summary>
    public TimeSpan RpcTimeout { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>An idle leader sends each follower an empty Append this often, so followers learn the high watermark.</summary>
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>First wait before retrying a follower that failed or could not be reached. It doubles on each failure.</summary>
    public TimeSpan RetryBackoff { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>Longest wait between retries to a follower that keeps failing.</summary>
    public TimeSpan MaxRetryBackoff { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>Most records in one batch sent to another broker.</summary>
    public int MaxBatchRecords { get; set; } = 500;

    /// <summary>Target size of a batch in bytes. A single record larger than this still travels, alone in its batch.</summary>
    public int MaxBatchBytes { get; set; } = 1024 * 1024;

    internal bool IsValid() =>
        RpcTimeout > TimeSpan.Zero
        && HeartbeatInterval > TimeSpan.Zero
        && RetryBackoff > TimeSpan.Zero
        && MaxRetryBackoff >= RetryBackoff
        && MaxBatchRecords > 0
        && MaxBatchBytes is > 0 and <= RecordCodec.MaxRecordSize
        && Peers.Values.All(static address => address.IsAbsoluteUri);
}
