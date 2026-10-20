using K0sStreams.Contracts;
using K0sStreams.Contracts.Fakes;
using Record = K0sStreams.Contracts.Record;

namespace K0sStreams.Replication.Tests.Support;

/// <summary>An in-memory log whose appends can be held, to freeze an Append halfway, while it holds its topic's lock.</summary>
internal sealed class BlockingLog : ILog
{
    private readonly InMemoryLog _inner = new();
    private TaskCompletionSource _gate = Open();

    public void Block() => _gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

    public void Release() => _gate.TrySetResult();

    public async ValueTask<long> AppendAsync(string topic, int partition, Record record, CancellationToken ct = default)
    {
        await _gate.Task.WaitAsync(ct);
        return await _inner.AppendAsync(topic, partition, record, ct);
    }

    public IAsyncEnumerable<Record> ReadAsync(string topic, int partition, long fromOffset, int max, CancellationToken ct = default) =>
        _inner.ReadAsync(topic, partition, fromOffset, max, ct);

    public long HighWatermark(string topic, int partition) => _inner.HighWatermark(topic, partition);

    public long EndOffset(string topic, int partition) => _inner.EndOffset(topic, partition);

    public void AdvanceHighWatermark(string topic, int partition, long offset) => _inner.AdvanceHighWatermark(topic, partition, offset);

    public ValueTask TruncateAsync(string topic, int partition, long toOffset, CancellationToken ct = default) =>
        _inner.TruncateAsync(topic, partition, toOffset, ct);

    private static TaskCompletionSource Open()
    {
        var open = new TaskCompletionSource();
        open.SetResult();
        return open;
    }
}
