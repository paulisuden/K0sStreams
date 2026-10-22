using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using K0sStreams.Contracts;

namespace K0sStreams.Storage;

/// <summary>
/// Catálogo de tópicos guardado en el mismo log que los mensajes, en el tópico interno <see cref="TopicsLog"/>: cada
/// tópico creado es un registro, replicado y confirmado por quórum como un mensaje. Así sobrevive a reinicios y
/// failovers, y el log sigue siendo la única fuente de verdad: lo que hay en memoria es una copia que cualquier broker
/// rearma leyendo el log.
/// </summary>
public sealed class LogTopicCatalog(ILog log, IReplicator replicator, IClusterState cluster, TimeProvider time)
    : ITopicCatalog, IDisposable
{
    /// <summary>
    /// Tópico interno con un registro por tópico creado. Necesita un nombre válido, porque la replicación rechaza los
    /// demás; por eso queda reservado y no se puede crear un tópico que se llame así.
    /// </summary>
    public const string TopicsLog = "k0s.topics";

    // Cada tópico es un solo log (DEC-001).
    private const int Partition = 0;
    private const int BatchSize = 512;

    private readonly ConcurrentDictionary<string, TopicConfig> _topics = new(StringComparer.Ordinal);
    private readonly object _gate = new();
    private readonly SemaphoreSlim _creating = new(1, 1);

    // Primer offset de TopicsLog que todavía no se cargó en memoria.
    private long _nextOffset;

    public async ValueTask<bool> CreateAsync(TopicConfig config, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(config);
        if (config.Name == TopicsLog)
        {
            return false;
        }

        // Uno a la vez: si dos pedidos con el mismo nombre llegaran juntos, los dos lo escribirían.
        await _creating.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (Find(config.Name) is not null)
            {
                return false;
            }

            var record = Record.NewMessage(
                cluster.CurrentEpoch,
                time.GetUtcNow().ToUnixTimeMilliseconds(),
                Encoding.UTF8.GetBytes(config.Name),
                JsonSerializer.SerializeToUtf8Bytes(config));
            long offset = await log.AppendAsync(TopicsLog, Partition, record, ct).ConfigureAwait(false);
            await replicator.WaitForQuorumAsync(TopicsLog, Partition, offset, ct).ConfigureAwait(false);
            CatchUp();
            return true;
        }
        finally
        {
            _creating.Release();
        }
    }

    /// <summary>
    /// Si no lo conoce, antes de responder carga lo que se confirmó en el log desde la última vez: en un seguidor o en
    /// un líder nuevo, el tópico pudo haberlo creado otro broker.
    /// </summary>
    public TopicConfig? Find(string name)
    {
        if (_topics.TryGetValue(name, out TopicConfig? config))
        {
            return config;
        }

        CatchUp();
        return _topics.GetValueOrDefault(name);
    }

    public IReadOnlyCollection<TopicConfig> List()
    {
        CatchUp();
        return [.. _topics.Values.OrderBy(topic => topic.Name, StringComparer.Ordinal)];
    }

    public void Dispose() => _creating.Dispose();

    /// <summary>Carga los tópicos confirmados (hasta el high watermark) que todavía no están en memoria.</summary>
    private void CatchUp()
    {
        lock (_gate)
        {
            long highWatermark = log.HighWatermark(TopicsLog, Partition);
            while (_nextOffset <= highWatermark)
            {
                // ITopicCatalog es sincrónico y el log se lee en forma asíncrona: se espera acá. El tópico interno
                // tiene un registro por tópico, así que es chico.
                List<Record> batch = ReadBatchAsync(_nextOffset).AsTask().GetAwaiter().GetResult();
                if (batch.Count == 0)
                {
                    return;
                }

                foreach (Record record in batch)
                {
                    if (record.Offset > highWatermark)
                    {
                        return;
                    }

                    // También hay registros EpochChange: la promoción de un líder escribe uno en cada tópico.
                    if (record.Type == RecordType.Message)
                    {
                        TopicConfig config = JsonSerializer.Deserialize<TopicConfig>(record.Value.Span)
                            ?? throw new InvalidDataException($"Registro {record.Offset} de {TopicsLog} vacío.");
                        _topics.TryAdd(config.Name, config);
                    }

                    _nextOffset = record.Offset + 1;
                }
            }
        }
    }

    private async ValueTask<List<Record>> ReadBatchAsync(long fromOffset)
    {
        var batch = new List<Record>();
        await foreach (Record record in log.ReadAsync(TopicsLog, Partition, fromOffset, BatchSize).ConfigureAwait(false))
        {
            batch.Add(record);
        }

        return batch;
    }
}
