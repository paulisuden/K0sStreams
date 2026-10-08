using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using K0sStreams.Contracts;
using K0sStreams.Contracts.Grpc;
using Microsoft.Extensions.Logging;

namespace K0sStreams.Replication;

/// <summary>
/// Follower side of <c>Append</c> (docs/replication.md, section 5.3): fencing by epoch, consistency check against the
/// record before the batch, truncation of a divergent tail, append, and the follower's high watermark.
/// Appends to the same topic run one at a time. Every topic is a single log (see <see cref="SingleLog"/>).
/// </summary>
internal sealed partial class AppendHandler(ILog log, EpochTracker epochs, ILogger<AppendHandler> logger) : IDisposable
{
    /// <summary>Value of <c>prev_offset</c> when the batch starts at offset 0.</summary>
    private const long NoPrevious = -1;

    private readonly ConcurrentDictionary<string, SemaphoreSlim> _gates = new(StringComparer.Ordinal);

    public async Task<AppendResponse> HandleAsync(AppendRequest request, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (!SingleLog.IsValid(request.Topic, request.Partition))
        {
            return Reject(AppendError.UnknownTopic, epochs.Current, NoPrevious, $"Invalid topic '{request.Topic}', or partition {request.Partition}: every topic is a single log, partition {SingleLog.Partition}.");
        }

        var gate = _gates.GetOrAdd(request.Topic, static _ => new SemaphoreSlim(1, 1));

        // Make sure that  only one append to the same topic runs at a time, 
        // so that the log is not corrupted and the high watermark is advanced correctly.
        await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            // Perform actual append
            return await AppendAsync(request, ct).ConfigureAwait(false);
        }
        finally
        {
            gate.Release();
        }
    }

    public void Dispose()
    {
        foreach (var gate in _gates.Values)
        {
            gate.Dispose();
        }
    }

    private async Task<AppendResponse> AppendAsync(AppendRequest request, CancellationToken ct)
    {
        string topic = request.Topic;

        long localEpoch = epochs.Current;
        if (request.Epoch < localEpoch)
        {
            return Reject(AppendError.StaleEpoch, localEpoch, log.EndOffset(topic), $"Epoch {request.Epoch} is older than the local epoch {localEpoch}.");
        }

        // A newer epoch is adopted even if the checks below fail: from here on the previous leader is fenced.
        localEpoch = epochs.Observe(request.Epoch);

        if (!await HasRecordAsync(topic, request.PrevOffset, request.PrevEpoch, ct).ConfigureAwait(false))
        {
            return Reject(AppendError.LogMismatch, localEpoch, log.EndOffset(topic), $"No record with offset {request.PrevOffset} and epoch {request.PrevEpoch}.");
        }

        if (!TryDecode(request, out var records, out string? error))
        {
            return Reject(AppendError.CorruptRecord, localEpoch, log.EndOffset(topic), error);
        }

        // Skip records that are already in the log (same offset and epoch)
        int firstNew = await SkipExistingAsync(topic, records, ct).ConfigureAwait(false);
        for (int i = firstNew; i < records.Length; i++)
        {
            await log.AppendAsync(topic, records[i], ct).ConfigureAwait(false);
        }

        // Only the prefix verified against the leader may be marked as confirmed: past it, this log can still hold
        // records from an old leader.
        long highWatermark = Math.Min(request.LeaderHw, request.PrevOffset + records.Length);
        if (highWatermark > log.HighWatermark(topic))
        {
            log.AdvanceHighWatermark(topic, highWatermark);
        }

        return new AppendResponse { Ok = true, Epoch = localEpoch, EndOffset = log.EndOffset(topic) };
    }

    private async ValueTask<bool> HasRecordAsync(string topic, long offset, long epoch, CancellationToken ct)
    {
        if (offset == NoPrevious)
        {
            return true;
        }

        if (offset < NoPrevious || offset > log.EndOffset(topic))
        {
            return false;
        }

        await foreach (var record in log.ReadAsync(topic, offset, 1, ct).ConfigureAwait(false))
        {
            return record.Epoch == epoch;
        }

        return false;
    }

    /// <summary>
    /// Decodes the batch and checks that it can follow <c>prev_offset</c>: contiguous offsets, and epochs that never go
    /// down and are not newer than the leader's.
    /// </summary>
    private static bool TryDecode(AppendRequest request, out Record[] records, [NotNullWhen(false)] out string? error)
    {
        records = request.Records.Count == 0 ? [] : new Record[request.Records.Count];
        long minEpoch = request.PrevOffset == NoPrevious ? 0 : request.PrevEpoch;
        for (int i = 0; i < records.Length; i++)
        {
            var bytes = request.Records[i].Span;
            if (RecordCodec.TryDecode(bytes, out var record, out int consumed) is not DecodeStatus.Ok || record is null || consumed != bytes.Length)
            {
                error = $"Record {i} of the batch does not decode.";
                return false;
            }

            long expectedOffset = request.PrevOffset + 1 + i;
            if (record.Offset != expectedOffset)
            {
                error = $"Record {i} of the batch has offset {record.Offset}; expected {expectedOffset}.";
                return false;
            }

            if (record.Epoch < minEpoch || record.Epoch > request.Epoch)
            {
                error = $"Record {i} of the batch has epoch {record.Epoch}; expected between {minEpoch} and {request.Epoch}.";
                return false;
            }

            minEpoch = record.Epoch;
            records[i] = record;
        }

        error = null;
        return true;
    }

    /// <summary>
    /// Returns how many records at the start of the batch are already in the log (same offset and epoch means same
    /// record). If the next one conflicts with a local record, truncates the log before it. A batch that only repeats
    /// what the log has never truncates anything.
    /// </summary>
    private async ValueTask<int> SkipExistingAsync(string topic, Record[] records, CancellationToken ct)
    {
        long endOffset = log.EndOffset(topic);
        if (records.Length == 0 || records[0].Offset > endOffset)
        {
            return 0;
        }

        int overlap = (int)(Math.Min(endOffset, records[^1].Offset) - records[0].Offset + 1);
        int matching = 0;
        await foreach (var existing in log.ReadAsync(topic, records[0].Offset, overlap, ct).ConfigureAwait(false))
        {
            if (existing.Epoch != records[matching].Epoch)
            {
                break;
            }

            matching++;
        }

        if (matching < records.Length && records[matching].Offset <= endOffset)
        {
            await TruncateDivergentTailAsync(topic, records[matching].Offset - 1, endOffset, ct).ConfigureAwait(false);
        }

        return matching;
    }

    private async ValueTask TruncateDivergentTailAsync(string topic, long toOffset, long endOffset, CancellationToken ct)
    {
        LogTruncating(logger, topic, toOffset, endOffset);
        try
        {
            await log.TruncateAsync(topic, toOffset, ct).ConfigureAwait(false);
        }
        catch (InvalidOperationException ex)
        {
            // The leader disagrees with a record this broker already counted as confirmed. That breaks the
            // replication invariants: never hide it.
            LogConflictWithConfirmedData(logger, ex, topic, toOffset + 1);
            throw;
        }
    }

    private static AppendResponse Reject(AppendError code, long epoch, long endOffset, string error) =>
        new() { Ok = false, Code = code, Epoch = epoch, EndOffset = endOffset, Error = error };

    [LoggerMessage(Level = LogLevel.Warning, Message = "Truncating the divergent tail of {Topic}: keeping up to offset {ToOffset}, the local end offset was {EndOffset}.")]
    private static partial void LogTruncating(ILogger logger, string topic, long toOffset, long endOffset);

    [LoggerMessage(Level = LogLevel.Critical, Message = "The leader's log conflicts with confirmed data in {Topic} at offset {Offset}. Refusing to truncate.")]
    private static partial void LogConflictWithConfirmedData(ILogger logger, Exception exception, string topic, long offset);
}
