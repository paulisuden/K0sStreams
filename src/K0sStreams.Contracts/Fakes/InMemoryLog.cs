using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace K0sStreams.Contracts.Fakes;

/// <summary><see cref="ILog"/> en memoria, sin persistencia. Reemplazo temporal del log de Storage.</summary>
public sealed class InMemoryLog : ILog
{
    private readonly ConcurrentDictionary<(string Topic, int Partition), PartitionLog> _partitions = new();

    public ValueTask<long> AppendAsync(string topic, int partition, Record record, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(record);
        var log = Get(topic, partition);
        lock (log.Gate)
        {
            long next = log.Records.Count;
            if (record.Offset != Record.Unassigned && record.Offset != next)
            {
                throw new ArgumentException($"Se esperaba el offset {next} y llegó {record.Offset}.", nameof(record));
            }

            log.Records.Add(record with { Offset = next });
            return ValueTask.FromResult(next);
        }
    }

    public async IAsyncEnumerable<Record> ReadAsync(
        string topic, int partition, long fromOffset, int max, [EnumeratorCancellation] CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(fromOffset);
        ArgumentOutOfRangeException.ThrowIfNegative(max);
        Record[] slice;
        var log = Get(topic, partition);
        lock (log.Gate)
        {
            int start = (int)Math.Min(fromOffset, log.Records.Count);
            slice = log.Records.GetRange(start, Math.Min(max, log.Records.Count - start)).ToArray();
        }

        foreach (var record in slice)
        {
            ct.ThrowIfCancellationRequested();
            yield return record;
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }

    public long HighWatermark(string topic, int partition)
    {
        var log = Get(topic, partition);
        lock (log.Gate)
        {
            return log.HighWatermark;
        }
    }

    public long EndOffset(string topic, int partition)
    {
        var log = Get(topic, partition);
        lock (log.Gate)
        {
            return log.Records.Count - 1;
        }
    }

    public void AdvanceHighWatermark(string topic, int partition, long offset)
    {
        var log = Get(topic, partition);
        lock (log.Gate)
        {
            ArgumentOutOfRangeException.ThrowIfGreaterThan(offset, log.Records.Count - 1);
            log.HighWatermark = Math.Max(log.HighWatermark, offset);
        }
    }

    public ValueTask TruncateAsync(string topic, int partition, long toOffset, CancellationToken ct = default)
    {
        var log = Get(topic, partition);
        lock (log.Gate)
        {
            if (toOffset < log.HighWatermark)
            {
                throw new InvalidOperationException(
                    $"No se puede truncar a {toOffset}: el high watermark es {log.HighWatermark}.");
            }

            int keep = (int)Math.Min(toOffset + 1, log.Records.Count);
            log.Records.RemoveRange(keep, log.Records.Count - keep);
        }

        return ValueTask.CompletedTask;
    }

    private PartitionLog Get(string topic, int partition) => _partitions.GetOrAdd((topic, partition), _ => new PartitionLog());

    private sealed class PartitionLog
    {
        public Lock Gate { get; } = new();

        public List<Record> Records { get; } = [];

        public long HighWatermark { get; set; } = -1;
    }
}
