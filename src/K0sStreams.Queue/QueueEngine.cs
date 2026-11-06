using System.Collections.Concurrent;
using System.Text;
using K0sStreams.Contracts;

namespace K0sStreams.Queue;

/// <summary>
/// Motor de cola (tipo SQS) sobre el log. El log guarda los mensajes y, como estado
/// durable de la cola, solo los offsets confirmados y las marcas de DLQ. Todo lo demás
/// (entregas en proceso, visibilidad, reintentos, miembros del grupo) vive en memoria
/// y se descarta al cambiar de líder: volver a entregar es correcto en at-least-once.
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

    /// <summary>Un consumidor deja de pertenecer al grupo si no late en este plazo.</summary>
    private static readonly TimeSpan SessionTimeout = TimeSpan.FromSeconds(45);

    private const int BatchSize = 512;

    private readonly ConcurrentDictionary<(string Topic, string Queue), QueueState> _queues = new();
    private readonly ConcurrentDictionary<(string Topic, string Queue), ConsumerGroupState> _consumerGroups = new();

    // Offsets confirmados por (tópico/cola/partición). Solo se alimenta de eventos OffsetCommit.
    private readonly ConcurrentDictionary<string, long> _committedOffsets = new(StringComparer.Ordinal);

    // Época en la que se releyó cada tópico desde el log, y un semáforo para que se relea una sola vez.
    private readonly ConcurrentDictionary<string, long> _rebuiltInEpoch = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _rebuilding = new(1, 1);

    // Serializa los volcados de offsets para no escribir dos veces el mismo cursor.
    private readonly SemaphoreSlim _flushing = new(1, 1);
    private DateTimeOffset _lastFlush = DateTimeOffset.MinValue;

    /// <inheritdoc />
    public async ValueTask<Delivery?> ReceiveAsync(string topic, string queue, string consumerId, TimeSpan visibility, CancellationToken ct = default)
    {
        if (visibility <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(visibility), visibility, "La visibilidad tiene que ser mayor que cero.");
        }

        TopicConfig topicConfig = catalog.Find(topic)
            ?? throw new InvalidOperationException($"El tópico '{topic}' no existe.");

        await EnsureRebuiltAsync(topic, ct).ConfigureAwait(false);
        await MaybeFlushAsync(ct).ConfigureAwait(false);

        ConsumerGroupState group = _consumerGroups.GetOrAdd(
            (topic, queue),
            _ => new ConsumerGroupState(topicConfig.Partitions, time));

        // Si el consumidor no está en el grupo (p. ej. lo echó un rebalance) no recibe nada.
        ConsumerAssignment? assignment = group.GetAssignment(consumerId);
        if (assignment is null || assignment.Partitions.Count == 0)
        {
            K0sMetrics.RecordQueueReceive(topic, queue, "not_assigned");
            return null;
        }

        QueueState state = StateOf(topic, queue);

        // Se lee hasta el high watermark: un mensaje que todavía no llegó a quórum no se entrega.
        DateTimeOffset now = time.GetUtcNow();
        DateTimeOffset visibleUntil = now.Add(visibility);

        foreach (int partition in assignment.Partitions)
        {
            long highWatermark = log.HighWatermark(topic, partition);
            if (highWatermark < 0)
            {
                continue;
            }

            // Desde el mayor entre lo confirmado y lo ya resuelto: nada por debajo vuelve a entregarse.
            long scanFrom = state.ScanFrom(partition);
            while (scanFrom <= highWatermark)
            {
                Record? found = null;
                long lastScanned = scanFrom - 1;
                int foundAttempt = 0;

                await foreach (var record in log.ReadAsync(topic, partition, scanFrom, BatchSize, ct).ConfigureAwait(false))
                {
                    lastScanned = record.Offset;
                    if (record.Type != RecordType.Message)
                    {
                        // Los marcadores de offset/DLQ ocupan un offset del log pero no se entregan.
                        state.MarkResolved(partition, record.Offset);
                        continue;
                    }

                    if (record.DeliverAt > 0 && record.DeliverAt > now.ToUnixTimeMilliseconds())
                    {
                        continue;
                    }

                    int? attempt = state.TryBeginAttempt(partition, record.Offset, consumerId, now, visibleUntil);
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
                    K0sMetrics.RecordQueueReceive(topic, queue, "hit");
                    return new Delivery(partition, found.Offset, found, foundAttempt, visibleUntil);
                }

                if (lastScanned < scanFrom)
                {
                    break;
                }

                scanFrom = lastScanned + 1;
            }
        }

        K0sMetrics.RecordQueueReceive(topic, queue, "empty");
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

        // El ack es memoria; el cursor se vuelve durable en el próximo volcado o commit.
        state.MarkDirty(partition, cursor);
        K0sMetrics.RecordQueueAck(topic, queue);
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
            K0sMetrics.RecordQueueNack(topic, queue, deadLettered: true);
            return true;
        }

        double multiplier = Math.Pow(2, attempt.Value - 1);
        TimeSpan delay = TimeSpan.FromTicks((long)(topicConfig.RetryBackoff.Ticks * multiplier));
        if (delay < TimeSpan.Zero)
        {
            delay = topicConfig.RetryBackoff;
        }

        state.ScheduleRetry(partition, offset, now.Add(delay));
        K0sMetrics.RecordQueueNack(topic, queue, deadLettered: false);
        return true;
    }

    /// <inheritdoc />
    public async ValueTask<ConsumerAssignment> JoinGroupAsync(string topic, string queue, string consumerId, CancellationToken ct = default)
    {
        TopicConfig topicConfig = catalog.Find(topic)
            ?? throw new InvalidOperationException($"El tópico '{topic}' no existe.");

        await EnsureRebuiltAsync(topic, ct).ConfigureAwait(false);

        ConsumerGroupState group = _consumerGroups.GetOrAdd(
            (topic, queue),
            _ => new ConsumerGroupState(topicConfig.Partitions, time));

        ConsumerAssignment assignment = group.Join(consumerId);
        return new ConsumerAssignment(consumerId, assignment.Partitions, assignment.Generation);
    }

    /// <inheritdoc />
    public async ValueTask LeaveGroupAsync(string topic, string queue, string consumerId, CancellationToken ct = default)
    {
        if (catalog.Find(topic) is null)
        {
            return;
        }

        await EnsureRebuiltAsync(topic, ct).ConfigureAwait(false);

        if (_consumerGroups.TryGetValue((topic, queue), out ConsumerGroupState? group))
        {
            group.Leave(consumerId);
        }

        // Lo que tenía en proceso vuelve a estar disponible y sus acks se vuelcan ya.
        if (_queues.TryGetValue((topic, queue), out QueueState? state))
        {
            state.ReleaseConsumer(consumerId);
        }

        await FlushAsync(ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<ConsumerAssignment> HeartbeatAsync(string topic, string queue, string consumerId, CancellationToken ct = default)
    {
        TopicConfig topicConfig = catalog.Find(topic)
            ?? throw new InvalidOperationException($"El tópico '{topic}' no existe.");

        await EnsureRebuiltAsync(topic, ct).ConfigureAwait(false);

        // La membresía es efímera: el latido solo toca memoria, nunca el log.
        if (!_consumerGroups.TryGetValue((topic, queue), out ConsumerGroupState? group))
        {
            throw new InvalidOperationException($"El consumidor '{consumerId}' no pertenece al grupo.");
        }

        ConsumerAssignment assignment = group.Heartbeat(consumerId);
        return new ConsumerAssignment(consumerId, assignment.Partitions, assignment.Generation);
    }

    /// <inheritdoc />
    public async ValueTask CommitOffsetsAsync(string topic, string queue, string consumerId, IReadOnlyDictionary<int, long> offsets, CancellationToken ct = default)
    {
        if (catalog.Find(topic) is null)
        {
            return;
        }

        await EnsureRebuiltAsync(topic, ct).ConfigureAwait(false);

        if (!_consumerGroups.TryGetValue((topic, queue), out ConsumerGroupState? group))
        {
            throw new InvalidOperationException($"El consumidor '{consumerId}' no pertenece al grupo.");
        }

        ConsumerAssignment? assignment = group.GetAssignment(consumerId);
        if (assignment is null)
        {
            throw new InvalidOperationException($"El consumidor '{consumerId}' no pertenece al grupo.");
        }

        QueueState state = StateOf(topic, queue);

        foreach ((int partition, long offset) in offsets)
        {
            if (!assignment.Partitions.Contains(partition))
            {
                throw new InvalidOperationException($"El consumidor '{consumerId}' no tiene asignada la partición {partition}.");
            }

            await AppendCommitAsync(topic, queue, partition, offset, ct).ConfigureAwait(false);
            state.SetCommitted(partition, offset);
        }
    }

    /// <inheritdoc />
    public IReadOnlyDictionary<int, long> GetCommittedOffsets(string topic, string queue)
    {
        if (catalog.Find(topic) is null)
        {
            return new Dictionary<int, long>();
        }

        EnsureRebuiltAsync(topic, CancellationToken.None).AsTask().GetAwaiter().GetResult();
        return StateOf(topic, queue).CommittedSnapshot();
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
    public IReadOnlyList<ConsumerState> GetConsumerStates(string topic, string queue)
    {
        if (!_consumerGroups.TryGetValue((topic, queue), out ConsumerGroupState? group))
        {
            return [];
        }

        return group.GetAllStates();
    }

    /// <inheritdoc />
    public void Apply(string topic, Record queueEvent)
    {
        ArgumentNullException.ThrowIfNull(queueEvent);
        if (!QueueEvent.EsEventoDeCola(queueEvent.Type))
        {
            return;
        }

        QueueEvent replayed = QueueEvent.DesdeRegistro(queueEvent);
        QueueState state = StateOf(topic, replayed.Cola);

        switch (replayed.Tipo)
        {
            // Los eventos de entrega de versiones anteriores se ignoran: esa información
            // ya no es durable y su único efecto correcto es volver a entregar.
            case RecordType.OffsetCommit:
                _committedOffsets.AddOrUpdate(
                    Key(topic, replayed.Cola, replayed.Particion),
                    replayed.OffsetObjetivo,
                    (_, existing) => Math.Max(existing, replayed.OffsetObjetivo));
                state.SetCommitted(replayed.Particion, replayed.OffsetObjetivo);
                break;

            case RecordType.Nack when replayed.HastaMs == 0:
                state.ApplyDeadLetter(replayed.Particion, replayed.OffsetObjetivo, replayed.Razon);
                break;
        }
    }

    public void Dispose()
    {
        _rebuilding.Dispose();
        _flushing.Dispose();
    }

    private QueueState StateOf(string topic, string queue) =>
        _queues.GetOrAdd((topic, queue), _ => new QueueState());

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
            foreach ((string Topic, string Queue) key in _queues.Keys.Where(key => key.Topic == topic))
            {
                _queues.TryRemove(key, out _);
            }

            // La membresía también era del líder anterior: los consumidores se vuelven a unir.
            foreach ((string Topic, string Queue) key in _consumerGroups.Keys.Where(key => key.Topic == topic))
            {
                _consumerGroups.TryRemove(key, out _);
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
    /// Recorre todas las particiones del tópico y aplica solo los eventos durables.
    /// Un líder nuevo termina confirmando todo su log, por eso no se corta en el high watermark.
    /// </summary>
    private async ValueTask ReplayAsync(string topic, CancellationToken ct)
    {
        int partitions = catalog.Find(topic) is { } config ? config.Partitions : 1;
        for (int partition = 0; partition < partitions; partition++)
        {
            long next = 0;
            while (true)
            {
                int read = 0;
                await foreach (var record in log.ReadAsync(topic, partition, next, BatchSize, ct).ConfigureAwait(false))
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

            foreach (((string Topic, string Queue) key, QueueState state) in _queues)
            {
                foreach ((int partition, long cursor) in state.DirtySnapshot())
                {
                    try
                    {
                        await AppendCommitAsync(key.Topic, key.Queue, partition, cursor, ct).ConfigureAwait(false);
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

    private async ValueTask AppendCommitAsync(string topic, string queue, int partition, long offset, CancellationToken ct)
    {
        QueueEvent commitEvent = QueueEvent.ConfirmarOffset(queue, partition, offset);

        long commitOffset = await log.AppendAsync(
            topic,
            partition,
            commitEvent.ARegistro(cluster.CurrentEpoch, time.GetUtcNow().ToUnixTimeMilliseconds()),
            ct).ConfigureAwait(false);

        await replicator.WaitForQuorumAsync(topic, partition, commitOffset, null, ct).ConfigureAwait(false);

        _committedOffsets.AddOrUpdate(
            Key(topic, queue, partition),
            offset,
            (_, existing) => Math.Max(existing, offset));
    }

    private async ValueTask AppendDlqMarkerAsync(string topic, string queue, int partition, long offset, string? reason, CancellationToken ct)
    {
        QueueEvent marker = new(RecordType.Nack, queue, partition, offset, HastaMs: 0, reason);

        long markerOffset = await log.AppendAsync(
            topic,
            partition,
            marker.ARegistro(cluster.CurrentEpoch, time.GetUtcNow().ToUnixTimeMilliseconds()),
            ct).ConfigureAwait(false);

        await replicator.WaitForQuorumAsync(topic, partition, markerOffset, null, ct).ConfigureAwait(false);
    }

    /// <summary>Estado materializado de una cola. Lo durable es solo el cursor; el resto es efímero.</summary>
    private sealed class QueueState
    {
        private readonly object _gate = new();

        // Último offset confirmado (durable, viene del log).
        private readonly Dictionary<int, long> _committed = [];

        // Último offset resuelto de forma contigua (acks y eventos internos).
        private readonly Dictionary<int, long> _prefix = [];

        private readonly Dictionary<(int Partition, long Offset), Lease> _inFlight = [];
        private readonly Dictionary<(int Partition, long Offset), DateTimeOffset> _scheduledUntil = [];
        private readonly Dictionary<(int Partition, long Offset), int> _attempts = [];
        private readonly HashSet<(int Partition, long Offset)> _done = [];
        private readonly HashSet<(int Partition, long Offset)> _deadLettered = [];
        private readonly Dictionary<(int Partition, long Offset), string?> _lastReason = [];
        private readonly HashSet<int> _dirty = [];

        public Dictionary<int, long> CommittedSnapshot()
        {
            lock (_gate)
            {
                return new Dictionary<int, long>(_committed);
            }
        }

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
        public int? TryBeginAttempt(int partition, long offset, string consumerId, DateTimeOffset now, DateTimeOffset visibleUntil)
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

                if (_inFlight.TryGetValue(key, out Lease lease))
                {
                    if (now < lease.VisibleUntil)
                    {
                        return null;
                    }

                    _inFlight.Remove(key);
                }

                _attempts.TryGetValue(key, out int attempt);
                attempt++;
                _attempts[key] = attempt;
                _inFlight[key] = new Lease(consumerId, visibleUntil);
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
                if (!_inFlight.Remove(key))
                {
                    return false;
                }

                return true;
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

        public void ReleaseConsumer(string consumerId)
        {
            lock (_gate)
            {
                foreach (var key in _inFlight.Where(pair => pair.Value.ConsumerId == consumerId).Select(pair => pair.Key).ToList())
                {
                    _inFlight.Remove(key);
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

        private readonly record struct Lease(string ConsumerId, DateTimeOffset VisibleUntil);
    }

    /// <summary>
    /// Coordinador de grupo en memoria. La membresía es efímera: si el líder cambia,
    /// los consumidores se vuelven a unir. El reparto se recalcula entero cada vez que
    /// cambia la membresía; nunca se acumula sobre el anterior.
    /// </summary>
    private sealed class ConsumerGroupState(int totalPartitions, TimeProvider time)
    {
        private readonly object _gate = new();
        private readonly Dictionary<string, DateTimeOffset> _members = new(StringComparer.Ordinal);
        private readonly Dictionary<string, int[]> _assignment = new(StringComparer.Ordinal);
        private long _generation;

        public ConsumerAssignment Join(string consumerId)
        {
            lock (_gate)
            {
                _members[consumerId] = time.GetUtcNow();
                RebalanceIfNeeded();
                return AssignmentOf(consumerId)!;
            }
        }

        public void Leave(string consumerId)
        {
            lock (_gate)
            {
                _members.Remove(consumerId);
                RebalanceIfNeeded();
            }
        }

        public ConsumerAssignment Heartbeat(string consumerId)
        {
            lock (_gate)
            {
                if (!_members.ContainsKey(consumerId))
                {
                    throw new InvalidOperationException($"Consumer {consumerId} not in group.");
                }

                _members[consumerId] = time.GetUtcNow();
                RebalanceIfNeeded();
                return AssignmentOf(consumerId)!;
            }
        }

        public ConsumerAssignment? GetAssignment(string consumerId)
        {
            lock (_gate)
            {
                RebalanceIfNeeded();
                return AssignmentOf(consumerId);
            }
        }

        public IReadOnlyList<ConsumerState> GetAllStates()
        {
            lock (_gate)
            {
                RebalanceIfNeeded();
                return [.. _members.Select(pair =>
                    new ConsumerState(pair.Key, pair.Value, _assignment.TryGetValue(pair.Key, out int[]? parts) ? parts : []))];
            }
        }

        private ConsumerAssignment? AssignmentOf(string consumerId) =>
            _assignment.TryGetValue(consumerId, out int[]? partitions)
                ? new ConsumerAssignment(consumerId, partitions, _generation)
                : null;

        /// <summary>Descarta miembros vencidos y reparte las particiones si cambió la membresía.</summary>
        private void RebalanceIfNeeded()
        {
            DateTimeOffset now = time.GetUtcNow();
            var expired = _members
                .Where(pair => now - pair.Value > SessionTimeout)
                .Select(pair => pair.Key)
                .ToList();

            foreach (string member in expired)
            {
                _members.Remove(member);
            }

            string[] active = [.. _members.Keys.Order(StringComparer.Ordinal)];

            // Sin miembros no hay reparto, pero la generación igual avanza si había uno.
            var desired = new Dictionary<string, int[]>(StringComparer.Ordinal);
            foreach (string member in active)
            {
                desired[member] = [];
            }

            // Reparto determinista: partición i al miembro i % n, con la lista ordenada.
            for (int partition = 0; partition < totalPartitions && active.Length > 0; partition++)
            {
                string owner = active[partition % active.Length];
                desired[owner] = [.. desired[owner], partition];
            }

            if (SameAssignment(desired))
            {
                return;
            }

            // Solo una membresía o un reparto distinto justifica una generación nueva.
            _assignment.Clear();
            foreach ((string member, int[] partitions) in desired)
            {
                _assignment[member] = partitions;
            }

            if (_assignment.Count > 0 || _generation > 0)
            {
                _generation++;
            }
        }

        private bool SameAssignment(Dictionary<string, int[]> desired)
        {
            if (desired.Count != _assignment.Count)
            {
                return false;
            }

            foreach ((string member, int[] partitions) in desired)
            {
                if (!_assignment.TryGetValue(member, out int[]? current)
                    || !current.SequenceEqual(partitions))
                {
                    return false;
                }
            }

            return true;
        }
    }
}
