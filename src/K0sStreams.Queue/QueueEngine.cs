using System.Collections.Concurrent;
using K0sStreams.Contracts;

namespace K0sStreams.Queue;

/// <summary>
/// Motor de cola (tipo SQS) sobre el log. Lo durable es el log de mensajes, el cursor
/// confirmado de cada cola y las marcas de DLQ. La entrega en proceso, la visibilidad y
/// los reintentos viven en memoria: si el líder cambia, lo no confirmado se vuelve a
/// entregar (at-least-once).
/// </summary>
/// <param name="log">Log append-only. Única fuente de verdad de los mensajes.</param>
/// <param name="catalog">Tópicos existentes y su configuración.</param>
/// <param name="cluster">Rol y época de este broker.</param>
/// <param name="replicator">Confirma los offsets y las marcas de DLQ por quórum.</param>
/// <param name="time">Reloj, inyectado para poder probar visibilidad y auto-commit.</param>
public sealed class QueueEngine(ILog log, ITopicCatalog catalog, IClusterState cluster, IReplicator replicator, TimeProvider time) : IQueueEngine, IDisposable
{
    /// <summary>Ventana máxima de acks que se pueden perder en un failover.</summary>
    private static readonly TimeSpan AutoCommitInterval = TimeSpan.FromSeconds(1);

    private const int BatchSize = 512;

    /// <summary>
    /// En este alcance hay un solo log por tópico (la partición es 0). El contrato conserva
    /// la partición para poder particionar en el futuro.
    /// </summary>
    private const int LogPartition = 0;

    private readonly ConcurrentDictionary<string, QueueState> _queues = new(StringComparer.Ordinal);

    // Offsets confirmados por (tópico/cola/partición). Solo se alimenta de eventos OffsetCommit.
    private readonly ConcurrentDictionary<string, long> _committedOffsets = new(StringComparer.Ordinal);

    // Época en la que se releyó cada tópico desde el log, y un semáforo para que se relea una sola vez.
    private readonly ConcurrentDictionary<string, long> _rebuiltInEpoch = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _rebuilding = new(1, 1);

    // Serializa los volcados de offsets para no escribir dos veces el mismo cursor.
    private readonly SemaphoreSlim _flushing = new(1, 1);
    private DateTimeOffset _lastFlush = DateTimeOffset.MinValue;

    /// <inheritdoc />
    public async ValueTask<Delivery?> ReceiveAsync(string topic, string queue, TimeSpan visibility, CancellationToken ct = default)
    {
        if (visibility <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(visibility), visibility, "La visibilidad tiene que ser mayor que cero.");
        }

        if (catalog.Find(topic) is null)
        {
            throw new InvalidOperationException($"El tópico '{topic}' no existe.");
        }

        await EnsureRebuiltAsync(topic, ct).ConfigureAwait(false);
        await MaybeFlushAsync(ct).ConfigureAwait(false);

        QueueState state = StateOf(topic, queue);

        // Se lee hasta el high watermark: un mensaje que todavía no llegó a quórum no se entrega.
        DateTimeOffset now = time.GetUtcNow();
        DateTimeOffset visibleUntil = now.Add(visibility);

        long highWatermark = log.HighWatermark(topic, LogPartition);
        if (highWatermark < 0)
        {
            return null;
        }

        long scanFrom = state.ScanFrom(LogPartition);
        while (scanFrom <= highWatermark)
        {
            Record? found = null;
            long lastScanned = scanFrom - 1;
            int foundAttempt = 0;

            await foreach (var record in log.ReadAsync(topic, LogPartition, scanFrom, BatchSize, ct).ConfigureAwait(false))
            {
                lastScanned = record.Offset;
                if (record.Type != RecordType.Message)
                {
                    // Los marcadores de offset/DLQ ocupan un offset del log pero no se entregan.
                    state.MarkResolved(LogPartition, record.Offset);
                    continue;
                }

                if (record.DeliverAt > 0 && record.DeliverAt > now.ToUnixTimeMilliseconds())
                {
                    continue;
                }

                int? attempt = state.TryBeginAttempt(LogPartition, record.Offset, now, visibleUntil);
                if (attempt is null)
                {
                    continue;
                }

                found = record;
                foundAttempt = attempt.Value;
                break;
            }

            if (found is not null)
            {
                return new Delivery(LogPartition, found.Offset, found, foundAttempt, visibleUntil);
            }

            if (lastScanned < scanFrom)
            {
                break;
            }

            scanFrom = lastScanned + 1;
        }

        return null;
    }

    /// <inheritdoc />
    public async ValueTask<bool> AckAsync(string topic, string queue, int partition, long offset, CancellationToken ct = default)
    {
        await EnsureRebuiltAsync(topic, ct).ConfigureAwait(false);

        QueueState state = StateOf(topic, queue);
        if (!state.TryAcknowledge(partition, offset, out long cursor))
        {
            return false;
        }

        // El ack es memoria; el cursor se vuelve durable en el próximo volcado.
        state.MarkDirty(partition, cursor);
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

        await EnsureRebuiltAsync(topic, ct).ConfigureAwait(false);

        QueueState state = StateOf(topic, queue);
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
            // La DLQ es el único evento de entrega que sí es un hecho durable.
            state.MarkDeadLetter(partition, offset, reason);
            await AppendDlqMarkerAsync(topic, queue, partition, offset, reason, ct).ConfigureAwait(false);
            return true;
        }

        double multiplier = Math.Pow(2, attempt.Value - 1);
        TimeSpan delay = TimeSpan.FromTicks((long)(topicConfig.RetryBackoff.Ticks * multiplier));
        if (delay < TimeSpan.Zero)
        {
            delay = topicConfig.RetryBackoff;
        }

        state.ScheduleRetry(partition, offset, now.Add(delay));
        return true;
    }

    /// <inheritdoc />
    public IReadOnlyList<DeadLetter> GetDeadLetters(string topic, string queue)
    {
        if (catalog.Find(topic) is null)
        {
            return [];
        }

        // El contrato es sincrónico: se espera la relectura igual que antes.
        EnsureRebuiltAsync(topic, CancellationToken.None).AsTask().GetAwaiter().GetResult();
        QueueState state = StateOf(topic, queue);
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

        QueueEvent replayed = QueueEvent.FromRecord(queueEvent);
        QueueState state = StateOf(topic, replayed.Queue);

        switch (replayed.Type)
        {
            // Los eventos de entrega de versiones anteriores se ignoran: esa información
            // ya no es durable y su único efecto correcto es volver a entregar.
            case RecordType.OffsetCommit:
                _committedOffsets.AddOrUpdate(
                    Key(topic, replayed.Queue, replayed.Partition),
                    replayed.TargetOffset,
                    (_, existing) => Math.Max(existing, replayed.TargetOffset));
                state.SetCommitted(replayed.Partition, replayed.TargetOffset);
                break;

            case RecordType.Nack when replayed.UntilMs == 0:
                state.ApplyDeadLetter(replayed.Partition, replayed.TargetOffset, replayed.Reason);
                break;
        }
    }

    public void Dispose()
    {
        _rebuilding.Dispose();
        _flushing.Dispose();
    }

    private QueueState StateOf(string topic, string queue) =>
        _queues.GetOrAdd(Key(topic, queue), _ => new QueueState());

    private static string Key(string topic, string queue) => $"{topic}/{queue}";

    private static string Key(string topic, string queue, int partition) => $"{topic}/{queue}/{partition}";

    /// <summary>
    /// Relee del log los eventos durables del tópico la primera vez que se usa en cada época.
    /// Solo interesan los offsets confirmados y las marcas de DLQ; el resto es información
    /// de entrega que, por diseño, no se conserva.
    /// </summary>
    private async ValueTask EnsureRebuiltAsync(string topic, CancellationToken ct)
    {
        long epoch = cluster.CurrentEpoch;
        if (IsRebuilt(topic, epoch))
        {
            return;
        }

        await _rebuilding.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (IsRebuilt(topic, epoch))
            {
                return;
            }

            // El estado en memoria es del líder anterior; se descarta y se reconstruye.
            foreach (string key in _queues.Keys.Where(key => key.StartsWith(topic + "/", StringComparison.Ordinal)))
            {
                _queues.TryRemove(key, out _);
            }

            await ReplayAsync(topic, ct).ConfigureAwait(false);
            _rebuiltInEpoch[topic] = epoch;
        }
        finally
        {
            _rebuilding.Release();
        }
    }

    private bool IsRebuilt(string topic, long epoch) =>
        _rebuiltInEpoch.TryGetValue(topic, out long rebuilt) && rebuilt == epoch;

    /// <summary>
    /// Recorre el log del tópico y aplica solo los eventos durables. Un líder nuevo termina
    /// confirmando todo su log, por eso no se corta en el high watermark.
    /// </summary>
    private async ValueTask ReplayAsync(string topic, CancellationToken ct)
    {
        long next = 0;
        while (true)
        {
            int read = 0;
            await foreach (var record in log.ReadAsync(topic, LogPartition, next, BatchSize, ct).ConfigureAwait(false))
            {
                Apply(topic, record);
                next = record.Offset + 1;
                read++;
            }

            if (read == 0)
            {
                break;
            }
        }
    }

    /// <summary>
    /// Vuelca los cursores sucios si pasó la ventana de auto-commit. Se llama desde
    /// <see cref="ReceiveAsync"/>: una cola se consume en bucle, así que el volcado
    /// ocurre solo, sin un temporizador de fondo.
    /// </summary>
    private async ValueTask MaybeFlushAsync(CancellationToken ct)
    {
        if (time.GetUtcNow() - _lastFlush < AutoCommitInterval)
        {
            return;
        }

        await FlushAsync(ct).ConfigureAwait(false);
    }

    private async ValueTask FlushAsync(CancellationToken ct)
    {
        if (!await _flushing.WaitAsync(0, ct).ConfigureAwait(false))
        {
            return;
        }

        try
        {
            _lastFlush = time.GetUtcNow();
            if (!cluster.IsLeader)
            {
                return;
            }

            foreach ((string topic, string queue, QueueState state) in SnapshotQueues())
            {
                foreach ((int partition, long cursor) in state.DirtySnapshot())
                {
                    try
                    {
                        await AppendCommitAsync(topic, queue, partition, cursor, ct).ConfigureAwait(false);
                        state.ClearDirty(partition, cursor);
                    }
                    catch (OperationCanceledException) when (ct.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch
                    {
                        // Queda sucio: se reintenta en la próxima ventana.
                        state.MarkDirty(partition, cursor);
                    }
                }
            }
        }
        finally
        {
            _flushing.Release();
        }
    }

    private IEnumerable<(string Topic, string Queue, QueueState State)> SnapshotQueues()
    {
        foreach ((string key, QueueState state) in _queues)
        {
            int separator = key.IndexOf('/');
            if (separator > 0)
            {
                yield return (key[..separator], key[(separator + 1)..], state);
            }
        }
    }

    private async ValueTask AppendCommitAsync(string topic, string queue, int partition, long offset, CancellationToken ct)
    {
        QueueEvent commitEvent = QueueEvent.ConfirmarOffset(queue, partition, offset);

        long commitOffset = await log.AppendAsync(
            topic,
            partition,
            commitEvent.ToRecord(cluster.CurrentEpoch, time.GetUtcNow().ToUnixTimeMilliseconds()),
            ct).ConfigureAwait(false);

        await replicator.WaitForQuorumAsync(topic, partition, commitOffset, ct).ConfigureAwait(false);

        _committedOffsets.AddOrUpdate(
            Key(topic, queue, partition),
            offset,
            (_, existing) => Math.Max(existing, offset));
    }

    private async ValueTask AppendDlqMarkerAsync(string topic, string queue, int partition, long offset, string? reason, CancellationToken ct)
    {
        QueueEvent marker = new(RecordType.Nack, queue, partition, offset, UntilMs: 0, reason);

        long markerOffset = await log.AppendAsync(
            topic,
            partition,
            marker.ToRecord(cluster.CurrentEpoch, time.GetUtcNow().ToUnixTimeMilliseconds()),
            ct).ConfigureAwait(false);

        await replicator.WaitForQuorumAsync(topic, partition, markerOffset, ct).ConfigureAwait(false);
    }

    /// <summary>Estado materializado de una cola. Lo durable es solo el cursor; el resto es efímero.</summary>
    private sealed class QueueState
    {
        private readonly object _gate = new();

        // Último offset confirmado (durable, viene del log).
        private readonly Dictionary<int, long> _committed = [];

        // Último offset resuelto de forma contigua (acks y eventos internos).
        private readonly Dictionary<int, long> _prefix = [];

        private readonly Dictionary<(int Partition, long Offset), DateTimeOffset> _inFlight = [];
        private readonly Dictionary<(int Partition, long Offset), DateTimeOffset> _scheduledUntil = [];
        private readonly Dictionary<(int Partition, long Offset), int> _attempts = [];
        private readonly HashSet<(int Partition, long Offset)> _done = [];
        private readonly HashSet<(int Partition, long Offset)> _deadLettered = [];
        private readonly Dictionary<(int Partition, long Offset), string?> _lastReason = [];
        private readonly HashSet<int> _dirty = [];

        /// <summary>Primer offset que hay que mirar en una partición: nada por debajo se entrega.</summary>
        public long ScanFrom(int partition)
        {
            lock (_gate)
            {
                long committed = _committed.GetValueOrDefault(partition, -1);
                long prefix = _prefix.GetValueOrDefault(partition, -1);
                return Math.Max(committed, prefix) + 1;
            }
        }

        public void SetCommitted(int partition, long offset)
        {
            lock (_gate)
            {
                _committed[partition] = Math.Max(_committed.GetValueOrDefault(partition, -1), offset);
                _prefix[partition] = Math.Max(_prefix.GetValueOrDefault(partition, -1), offset);
                _dirty.Remove(partition);
            }
        }

        /// <summary>
        /// Decide y registra el intento bajo el mismo lock: si fueran dos pasos, dos receives
        /// concurrentes podrían entregar el mismo offset.
        /// </summary>
        public int? TryBeginAttempt(int partition, long offset, DateTimeOffset now, DateTimeOffset visibleUntil)
        {
            lock (_gate)
            {
                var key = (partition, offset);
                if (_done.Contains(key) || _deadLettered.Contains(key))
                {
                    return null;
                }

                if (_scheduledUntil.TryGetValue(key, out DateTimeOffset scheduled) && now < scheduled)
                {
                    return null;
                }

                if (_inFlight.TryGetValue(key, out DateTimeOffset until))
                {
                    if (now < until)
                    {
                        return null;
                    }

                    _inFlight.Remove(key);
                }

                _attempts.TryGetValue(key, out int attempt);
                attempt++;
                _attempts[key] = attempt;
                _inFlight[key] = visibleUntil;
                return attempt;
            }
        }

        public bool TryAcknowledge(int partition, long offset, out long cursor)
        {
            lock (_gate)
            {
                var key = (partition, offset);
                if (!_inFlight.Remove(key))
                {
                    cursor = 0;
                    return false;
                }

                _scheduledUntil.Remove(key);
                _done.Add(key);
                cursor = AdvancePrefix(partition);
                return true;
            }
        }

        public int? GetAttempt(int partition, long offset)
        {
            lock (_gate)
            {
                var key = (partition, offset);
                if (_done.Contains(key) || _deadLettered.Contains(key))
                {
                    return null;
                }

                _attempts.TryGetValue(key, out int attempt);
                return attempt == 0 ? null : attempt;
            }
        }

        public bool TryRelease(int partition, long offset)
        {
            lock (_gate)
            {
                var key = (partition, offset);
                return _inFlight.Remove(key);
            }
        }

        public void ScheduleRetry(int partition, long offset, DateTimeOffset until)
        {
            lock (_gate)
            {
                var key = (partition, offset);
                _inFlight.Remove(key);
                _scheduledUntil[key] = until;
            }
        }

        public void MarkDeadLetter(int partition, long offset, string? reason)
        {
            lock (_gate)
            {
                var key = (partition, offset);
                _inFlight.Remove(key);
                _scheduledUntil.Remove(key);
                _deadLettered.Add(key);
                _done.Add(key);
                _lastReason[key] = reason;
                long cursor = AdvancePrefix(partition);
                if (cursor > _committed.GetValueOrDefault(partition, -1))
                {
                    _dirty.Add(partition);
                }
            }
        }

        /// <summary>Marca un offset que no es un mensaje (commit, marcador de DLQ) como resuelto.</summary>
        public void MarkResolved(int partition, long offset)
        {
            lock (_gate)
            {
                var key = (partition, offset);
                if (_done.Add(key))
                {
                    AdvancePrefix(partition);
                }
            }
        }

        public void MarkDirty(int partition, long cursor)
        {
            lock (_gate)
            {
                if (cursor > _committed.GetValueOrDefault(partition, -1))
                {
                    _dirty.Add(partition);
                }
            }
        }

        public IReadOnlyList<(int Partition, long Cursor)> DirtySnapshot()
        {
            lock (_gate)
            {
                return [.. _dirty.Select(partition => (partition, _prefix.GetValueOrDefault(partition, -1)))];
            }
        }

        public void ClearDirty(int partition, long cursor)
        {
            lock (_gate)
            {
                if (_prefix.GetValueOrDefault(partition, -1) == cursor)
                {
                    _dirty.Remove(partition);
                }
            }
        }

        public void ApplyDeadLetter(int partition, long offset, string? reason)
        {
            lock (_gate)
            {
                var key = (partition, offset);
                _deadLettered.Add(key);
                _done.Add(key);
                _lastReason[key] = reason;
                AdvancePrefix(partition);
            }
        }

        public List<DeadLetter> GetDeadLetters(string topic, string queue, ILog log)
        {
            lock (_gate)
            {
                var result = new List<DeadLetter>();
                foreach ((int partition, long offset) in _deadLettered)
                {
                    try
                    {
                        Record? record = null;
                        var enumerator = log.ReadAsync(topic, partition, offset, 1).GetAsyncEnumerator();
                        if (enumerator.MoveNextAsync().AsTask().GetAwaiter().GetResult())
                        {
                            record = enumerator.Current;
                        }

                        enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult();

                        if (record is not null && record.Offset == offset)
                        {
                            _attempts.TryGetValue((partition, offset), out int attempts);
                            _lastReason.TryGetValue((partition, offset), out string? reason);
                            result.Add(new DeadLetter(partition, offset, record, attempts, reason));
                        }
                    }
                    catch
                    {
                        // Un registro ya retenido fuera del log no se puede listar.
                    }
                }

                return result;
            }
        }

        /// <summary>
        /// Avanza el cursor contiguo mientras los offsets siguientes estén resueltos. Devuelve el cursor nuevo.
        /// Debe llamarse con el lock tomado.
        /// </summary>
        private long AdvancePrefix(int partition)
        {
            long prefix = _prefix.GetValueOrDefault(partition, -1);
            while (_done.Remove((partition, prefix + 1)))
            {
                prefix++;
            }

            _prefix[partition] = prefix;
            return prefix;
        }
    }
}
