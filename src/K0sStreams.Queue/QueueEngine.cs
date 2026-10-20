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

        TopicConfig? topicConfig = catalog.Find(topic) ?? throw new InvalidOperationException($"El tópico '{topic}' no existe.");
        QueueState state = _queues.GetOrAdd(Key(topic, queue), _ => new QueueState());

        // Se lee hasta el high watermark, no hasta el end offset: un mensaje que todavía no
        // llegó a quórum no se puede entregar. Los eventos de cola del log se saltean.
        int partitions = topicConfig.Partitions;
        long from = 0;
        foreach (int partition in PartitionsRange(partitions))
        {
            long highWatermark = log.HighWatermark(topic, partition);
            if (highWatermark < 0)
            {
                continue;
            }

            long scanFrom = from == 0 ? 0 : from;
            while (scanFrom <= highWatermark)
            {
                Record? found = null;
                long lastScanned = scanFrom - 1;
                int foundAttempt = 0;
                DateTimeOffset foundUntil = default;
                await foreach (var record in log.ReadAsync(topic, partition, scanFrom, BatchSize, ct).ConfigureAwait(false))
                {
                    lastScanned = record.Offset;
                    if (record.Type != RecordType.Message)
                    {
                        continue;
                    }

                    DateTimeOffset now = time.GetUtcNow();
                    if (record.DeliverAt > 0 && record.DeliverAt > now.ToUnixTimeMilliseconds())
                    {
                        continue;
                    }

                    DateTimeOffset visibleUntil = now.Add(visibility);
                    int? attempt = state.TryBeginAttempt(partition, record.Offset, now, visibleUntil);
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
                        new QueueEvent(RecordType.Receive, queue, partition, found.Offset, foundUntil.ToUnixTimeMilliseconds()),
                        ct).ConfigureAwait(false);

                    return new Delivery(partition, found.Offset, found, foundAttempt, foundUntil);
                }

                if (lastScanned < scanFrom)
                {
                    break;
                }

                scanFrom = lastScanned + 1;
            }
        }

        return null;
    }

    /// <inheritdoc />
    public async ValueTask<bool> AckAsync(string topic, string queue, int partition, long offset, CancellationToken ct = default)
    {
        QueueState state = _queues.GetOrAdd(Key(topic, queue), _ => new QueueState());
        if (!state.TryAcknowledge(partition, offset))
        {
            return false;
        }

        await AppendEventAsync(
            topic,
            new QueueEvent(RecordType.Ack, queue, partition, offset, UntilMs: 0),
            ct).ConfigureAwait(false);
        return true;
    }

    /// <inheritdoc />
    public async ValueTask<bool> NackAsync(string topic, string queue, int partition, long offset, string? reason, CancellationToken ct = default)
    {
        TopicConfig? topicConfig = catalog.Find(topic);
        if (topicConfig is null)
        {
            return false;
        }

        QueueState state = _queues.GetOrAdd(Key(topic, queue), _ => new QueueState());
        int? attempt = state.GetAttempt(partition, offset);
        if (attempt is null || attempt.Value == 0)
        {
            return false;
        }

        if (!state.TryRelease(partition, offset))
        {
            return false;
        }

        DateTimeOffset now = time.GetUtcNow();
        if (attempt.Value > topicConfig.MaxRetries)
        {
            state.MarkDeadLetter(partition, offset);
            await AppendEventAsync(
                topic,
                new QueueEvent(RecordType.Nack, queue, partition, offset, now.ToUnixTimeMilliseconds(), reason),
                ct).ConfigureAwait(false);
            return true;
        }

        double multiplier = Math.Pow(2, attempt.Value - 1);
        TimeSpan delay = TimeSpan.FromTicks((long)(topicConfig.RetryBackoff.Ticks * multiplier));
        if (delay < TimeSpan.Zero)
        {
            delay = topicConfig.RetryBackoff;
        }
        DateTimeOffset retryAt = now.Add(delay);
        long untilMs = retryAt.ToUnixTimeMilliseconds();
        state.ScheduleRetry(partition, offset, retryAt);

        await AppendEventAsync(
            topic,
            new QueueEvent(RecordType.Nack, queue, partition, offset, untilMs, reason),
            ct).ConfigureAwait(false);
        return true;
    }

    /// <inheritdoc />
    public IReadOnlyList<DeadLetter> GetDeadLetters(string topic, string queue)
    {
        if (catalog.Find(topic) is null)
        {
            return [];
        }

        QueueState state = _queues.GetOrAdd(Key(topic, queue), _ => new QueueState());
        return state.GetDeadLetters(topic, queue, log);
    }

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

    private static string Key(string topic, string queue) => $"{topic}/{queue}";

    private static IEnumerable<int> PartitionsRange(int partitions)
    {
        for (int i = 0; i < partitions; i++)
        {
            yield return i;
        }
    }

    private async ValueTask AppendEventAsync(string topic, QueueEvent queueEvent, CancellationToken ct) =>
        await log.AppendAsync(
            topic,
            queueEvent.Partition,
            queueEvent.ToRecord(cluster.CurrentEpoch, time.GetUtcNow().ToUnixTimeMilliseconds()),
            ct).ConfigureAwait(false);

    private const int BatchSize = 512;

    /// <summary>Estado materializado de una cola, reconstruible desde el log.</summary>
    private sealed class QueueState
    {
        private readonly object _gate = new();
        private readonly Dictionary<(int Partition, long Offset), int> _attempts = [];
        private readonly Dictionary<(int Partition, long Offset), DateTimeOffset> _visibleUntil = [];
        private readonly Dictionary<(int Partition, long Offset), DateTimeOffset> _scheduledUntil = [];
        private readonly HashSet<(int Partition, long Offset)> _inFlight = [];
        private readonly HashSet<(int Partition, long Offset)> _acknowledged = [];
        private readonly HashSet<(int Partition, long Offset)> _deadLettered = [];
        private readonly Dictionary<(int Partition, long Offset), string?> _lastReason = [];

        /// <summary>
        /// Decide y confirma el intento bajo el mismo lock: si fueran dos pasos, dos receives
        /// concurrentes podrían pasar la comprobación y entregar el mismo offset.
        /// Devuelve el número de intento, o null si el mensaje no está disponible.
        /// </summary>
        public int? TryBeginAttempt(int partition, long offset, DateTimeOffset now, DateTimeOffset visibleUntil)
        {
            lock (_gate)
            {
                var key = (partition, offset);
                if (_acknowledged.Contains(key) || _deadLettered.Contains(key))
                {
                    return null;
                }

                if (_scheduledUntil.TryGetValue(key, out DateTimeOffset scheduled) && now < scheduled)
                {
                    return null;
                }
                if (_scheduledUntil.ContainsKey(key) && now >= scheduled)
                {
                    _scheduledUntil.Remove(key);
                }

                // En proceso y sin vencer: todavía no se puede volver a entregar.
                if (_inFlight.Contains(key))
                {
                    if (_visibleUntil.TryGetValue(key, out DateTimeOffset until2) && now < until2)
                    {
                        return null;
                    }
                    _inFlight.Remove(key);
                    _visibleUntil.Remove(key);
                }

                _inFlight.Add(key);
                _visibleUntil[key] = visibleUntil;
                _attempts.TryGetValue(key, out int attempt);
                attempt++;
                _attempts[key] = attempt;
                return attempt;
            }
        }

        public bool TryAcknowledge(int partition, long offset)
        {
            lock (_gate)
            {
                var key = (partition, offset);
                if (!_inFlight.Remove(key))
                {
                    return false;
                }

                _visibleUntil.Remove(key);
                _scheduledUntil.Remove(key);
                _acknowledged.Add(key);
                return true;
            }
        }

        public int? GetAttempt(int partition, long offset)
        {
            lock (_gate)
            {
                var key = (partition, offset);
                if (_acknowledged.Contains(key) || _deadLettered.Contains(key))
                {
                    return null;
                }

                if (!_inFlight.Contains(key) && !_scheduledUntil.ContainsKey(key))
                {
                    // could be in-flight? check
                }

                _attempts.TryGetValue(key, out int attempt);
                if (attempt == 0)
                {
                    return null;
                }
                return attempt;
            }
        }

        public bool TryRelease(int partition, long offset)
        {
            lock (_gate)
            {
                var key = (partition, offset);
                if (!_inFlight.Contains(key))
                {
                    return false;
                }

                _inFlight.Remove(key);
                _visibleUntil.Remove(key);
                return true;
            }
        }

        public void ScheduleRetry(int partition, long offset, DateTimeOffset until)
        {
            lock (_gate)
            {
                var key = (partition, offset);
                _inFlight.Remove(key);
                _visibleUntil.Remove(key);
                _scheduledUntil[key] = until;
            }
        }

        public void MarkDeadLetter(int partition, long offset)
        {
            lock (_gate)
            {
                var key = (partition, offset);
                _deadLettered.Add(key);
            }
        }

        public List<DeadLetter> GetDeadLetters(string topic, string queue, ILog log)
        {
            lock (_gate)
            {
                var result = new List<DeadLetter>();
                foreach (var key in _deadLettered)
                {
                    try
                    {
                        Record? record = null;
                        var enumerator = log.ReadAsync(topic, key.Partition, key.Offset, 1).GetAsyncEnumerator();
                        if (enumerator.MoveNextAsync().AsTask().GetAwaiter().GetResult())
                        {
                            record = enumerator.Current;
                        }
                        enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult();
                        if (record is not null && record.Offset == key.Offset)
                        {
                            _attempts.TryGetValue(key, out int attempts);
                            _lastReason.TryGetValue(key, out string? reason);
                            result.Add(new DeadLetter(key.Partition, key.Offset, record, attempts, reason));
                        }
                    }
                    catch
                    {
                        // ignore
                    }
                }
                return result;
            }
        }

        public void Apply(QueueEvent replayed)
        {
            lock (_gate)
            {
                var key = (replayed.Partition, replayed.TargetOffset);
                switch (replayed.Type)
                {
                    case RecordType.Receive:
                        // UntilMs es el fin de la visibilidad: hay que reconstruirlo también,
                        // o un mensaje en proceso volvería a entregarse de inmediato.
                        _inFlight.Add(key);
                        _visibleUntil[key] = DateTimeOffset.FromUnixTimeMilliseconds(replayed.UntilMs);
                        _attempts.TryGetValue(key, out int attemptReceive);
                        _attempts[key] = attemptReceive + 1;
                        break;
                    case RecordType.Ack:
                        _inFlight.Remove(key);
                        _visibleUntil.Remove(key);
                        _acknowledged.Add(key);
                        break;
                    case RecordType.Timeout:
                        // Venció la visibilidad: el mensaje vuelve a estar disponible. Sin esto,
                        // un Timeout del log se aceptaría y se ignoraría, dejando el mensaje
                        // invisible para siempre. El número de intento se conserva.
                        _inFlight.Remove(key);
                        _visibleUntil.Remove(key);
                        break;
                    case RecordType.Nack:
                        _inFlight.Remove(key);
                        _visibleUntil.Remove(key);
                        if (replayed.Reason is not null)
                        {
                            _lastReason[key] = replayed.Reason;
                        }
                        if (replayed.UntilMs > 0)
                        {
                            _scheduledUntil[key] = DateTimeOffset.FromUnixTimeMilliseconds(replayed.UntilMs);
                        }
                        else
                        {
                            _deadLettered.Add(key);
                        }
                        break;
                    default:
                        break;
                }
            }
        }
    }
}
