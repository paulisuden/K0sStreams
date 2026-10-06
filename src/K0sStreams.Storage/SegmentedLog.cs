using K0sStreams.Contracts;

namespace K0sStreams.Storage;

/// <summary>
/// Log append-only en disco. Reemplaza a <c>InMemoryLog</c>.
/// Cada tópico y partición vive en <c>{DataDir}/{topic}/{partition}/</c> como segmentos <c>.log</c> + <c>.index</c>.
/// </summary>
/// <remarks>Todavía sin implementar: ver el plan de la iteración 1.</remarks>
public sealed class SegmentedLog : ILog
{
    /// <param name="dataDir">Carpeta raíz de los datos. En Docker es <c>/data</c> (configuración <c>Broker:DataDir</c>).</param>
    public SegmentedLog(string dataDir)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dataDir);
        DataDir = dataDir;
    }

    /// <summary>Carpeta raíz donde se guardan los segmentos.</summary>
    public string DataDir { get; }

    public ValueTask<long> AppendAsync(string topic, int partition, Record record, CancellationToken ct = default) =>
        throw new NotImplementedException();

    public IAsyncEnumerable<Record> ReadAsync(string topic, int partition, long fromOffset, int max, CancellationToken ct = default) =>
        throw new NotImplementedException();

    public long HighWatermark(string topic, int partition) =>
        throw new NotImplementedException();

    public long EndOffset(string topic, int partition) =>
        throw new NotImplementedException();

    public void AdvanceHighWatermark(string topic, int partition, long offset) =>
        throw new NotImplementedException();

    public ValueTask TruncateAsync(string topic, int partition, long toOffset, CancellationToken ct = default) =>
        throw new NotImplementedException();
}
