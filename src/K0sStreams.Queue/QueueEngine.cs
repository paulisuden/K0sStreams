using System.Collections.Concurrent;
using K0sStreams.Contracts;

namespace K0sStreams.Queue;

/// <summary>
/// Motor de cola (tipo SQS) sobre el log. El estado de cada cola vive en el log como
/// <see cref="QueueEvent"/>; este tipo solo lo materializa para responder rápido, y
/// <see cref="Apply"/> lo reconstruye cuando un broker nuevo se vuelve líder.
/// </summary>
/// <param name="log">Log append-only. Única fuente de verdad.</param>
/// <param name="catalog">Tópicos existentes y su configuración.</param>
/// <param name="cluster">Rol y época de este broker.</param>
/// <param name="time">Reloj, inyectado para poder probar el visibility timeout.</param>
public sealed class QueueEngine(ILog log, ITopicCatalog catalog, IClusterState cluster, TimeProvider time) : IQueueEngine
{
    private readonly ConcurrentDictionary<string, QueueState> _queues = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public async ValueTask<Delivery?> ReceiveAsync(string topic, string queue, TimeSpan visibility, CancellationToken ct = default)
    {
        if (visibility <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(visibility), visibility, "La visibilidad tiene que ser mayor que cero.");
        }

        _ = catalog.Find(topic) ?? throw new InvalidOperationException($"El tópico '{topic}' no existe.");
        QueueState state = _queues.GetOrAdd(Key(topic, queue), _ => new QueueState());

        // Se lee hasta el high watermark, no hasta el end offset: un mensaje que todavía no
        // llegó a quórum no se puede entregar. Los eventos de cola del log se saltean.
        long highWatermark = log.HighWatermark(topic);
        long from = 0;
        while (from <= highWatermark)
        {
            Record? found = null;
            long lastScanned = from - 1;
            int foundAttempt = 0;
            DateTimeOffset foundUntil = default;
            await foreach (var record in log.ReadAsync(topic, from, BatchSize, ct).ConfigureAwait(false))
            {
                lastScanned = record.Offset;
                if (record.Type != RecordType.Message)
                {
                    continue;
                }

                DateTimeOffset now = time.GetUtcNow();
                DateTimeOffset visibleUntil = now.Add(visibility);
                int? attempt = state.TryBeginAttempt(record.Offset, now, visibleUntil);
                if (attempt is null)
                {
                    continue;
                }

                found = record;
                foundAttempt = attempt.Value;
                foundUntil = visibleUntil;
                break;
            }

            if (found is not null)
            {
                await AppendEventAsync(
                    topic,
                    new QueueEvent(RecordType.Receive, queue, found.Offset, foundUntil.ToUnixTimeMilliseconds()),
                    ct).ConfigureAwait(false);

                return new Delivery(found.Offset, found, foundAttempt, foundUntil);
            }

            if (lastScanned < from)
            {
                break;
            }

            from = lastScanned + 1;
        }

        return null;
    }

    /// <inheritdoc />
    public async ValueTask<bool> AckAsync(string topic, string queue, long offset, CancellationToken ct = default)
    {
        QueueState state = _queues.GetOrAdd(Key(topic, queue), _ => new QueueState());
        if (!state.TryAcknowledge(offset))
        {
            return false;
        }

        await AppendEventAsync(
            topic,
            new QueueEvent(RecordType.Ack, queue, offset, UntilMs: 0),
            ct).ConfigureAwait(false);
        return true;
    }

    /// <inheritdoc />
    public ValueTask<bool> NackAsync(string topic, string queue, long offset, string? reason, CancellationToken ct = default)
    {
        // Fase 2: reintentos con backoff y DLQ. Sin Nack no hay estado que cambiar.
        return ValueTask.FromResult(false);
    }

    /// <inheritdoc />
    public IReadOnlyList<DeadLetter> GetDeadLetters(string topic, string queue) => [];

    /// <inheritdoc />
    public void Apply(string topic, Record queueEvent)
    {
        ArgumentNullException.ThrowIfNull(queueEvent);
        if (!QueueEvent.IsQueueEvent(queueEvent.Type))
        {
            return;
        }

        var replayed = QueueEvent.FromRecord(queueEvent);
        QueueState state = _queues.GetOrAdd(Key(topic, replayed.Queue), _ => new QueueState());
        state.Apply(replayed);
    }

    private static string Key(string topic, string queue) => $"{topic}{queue}";

    private async ValueTask AppendEventAsync(string topic, QueueEvent queueEvent, CancellationToken ct) =>
        await log.AppendAsync(
            topic,
            queueEvent.ToRecord(cluster.CurrentEpoch, time.GetUtcNow().ToUnixTimeMilliseconds()),
            ct).ConfigureAwait(false);

    private const int BatchSize = 512;

    /// <summary>Estado materializado de una cola, reconstruible desde el log.</summary>
    private sealed class QueueState
    {
        private readonly object _gate = new();
        private readonly Dictionary<long, int> _attempts = [];
        private readonly Dictionary<long, DateTimeOffset> _visibleUntil = [];
        private readonly HashSet<long> _inFlight = [];
        private readonly HashSet<long> _acknowledged = [];

        /// <summary>
        /// Decide y confirma el intento bajo el mismo lock: si fueran dos pasos, dos receives
        /// concurrentes podrían pasar la comprobación y entregar el mismo offset.
        /// Devuelve el número de intento, o null si el mensaje no está disponible.
        /// </summary>
        public int? TryBeginAttempt(long offset, DateTimeOffset now, DateTimeOffset visibleUntil)
        {
            lock (_gate)
            {
                if (_acknowledged.Contains(offset))
                {
                    return null;
                }

                // En proceso y sin vencer: todavía no se puede volver a entregar.
                if (_inFlight.Contains(offset) && now < _visibleUntil[offset])
                {
                    return null;
                }

                _inFlight.Add(offset);
                _visibleUntil[offset] = visibleUntil;
                _attempts.TryGetValue(offset, out int attempt);
                attempt++;
                _attempts[offset] = attempt;
                return attempt;
            }
        }

        public bool TryAcknowledge(long offset)
        {
            lock (_gate)
            {
                if (!_inFlight.Remove(offset))
                {
                    return false;
                }

                _acknowledged.Add(offset);
                return true;
            }
        }

        public void Apply(QueueEvent replayed)
        {
            lock (_gate)
            {
                switch (replayed.Type)
                {
                    case RecordType.Receive:
                        // UntilMs es el fin de la visibilidad: hay que reconstruirlo también,
                        // o un mensaje en proceso volvería a entregarse de inmediato.
                        _inFlight.Add(replayed.TargetOffset);
                        _visibleUntil[replayed.TargetOffset] = DateTimeOffset.FromUnixTimeMilliseconds(replayed.UntilMs);
                        _attempts.TryGetValue(replayed.TargetOffset, out int attempt);
                        _attempts[replayed.TargetOffset] = attempt + 1;
                        break;
                    case RecordType.Ack:
                        _inFlight.Remove(replayed.TargetOffset);
                        _visibleUntil.Remove(replayed.TargetOffset);
                        _acknowledged.Add(replayed.TargetOffset);
                        break;
                    case RecordType.Timeout:
                        // Venció la visibilidad: el mensaje vuelve a estar disponible. Sin esto,
                        // un Timeout del log se aceptaría y se ignoraría, dejando el mensaje
                        // invisible para siempre. El número de intento se conserva.
                        _inFlight.Remove(replayed.TargetOffset);
                        _visibleUntil.Remove(replayed.TargetOffset);
                        break;
                    default:
                        // Nack es de la fase 2 (backoff y DLQ).
                        break;
                }
            }
        }
    }
}
