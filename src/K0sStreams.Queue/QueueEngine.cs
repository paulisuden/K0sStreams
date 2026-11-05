using System.Collections.Concurrent;
using System.Text;
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
/// <param name="replicator">Confirma cada evento de cola por quórum, igual que un mensaje.</param>
/// <param name="time">Reloj, inyectado para poder probar el visibility timeout.</param>
public sealed class QueueEngine(ILog log, ITopicCatalog catalog, IClusterState cluster, IReplicator replicator, TimeProvider time) : IQueueEngine, IDisposable
{
    private readonly ConcurrentDictionary<string, QueueState> _queues = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, ConsumerGroupState> _consumerGroups = new(StringComparer.Ordinal);

    // Offsets confirmados por consumidor por (tópico/cola/partición). Un receive nunca
    // entrega un offset menor o igual al confirmado para esa cola+partición.
    private readonly ConcurrentDictionary<string, long> _committedOffsets = new(StringComparer.Ordinal);

    // Época en la que se releyó cada tópico desde el log, y un semáforo para que se relea una sola vez (ver EnsureRebuiltAsync).
    private readonly ConcurrentDictionary<string, long> _rebuiltInEpoch = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _rebuilding = new(1, 1);

    /// <inheritdoc />
    public async ValueTask<Delivery?> ReceiveAsync(string topic, string queue, string consumerId, TimeSpan visibility, CancellationToken ct = default)
    {
        if (visibility <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(visibility), visibility, "La visibilidad tiene que ser mayor que cero.");
        }

        TopicConfig? topicConfig = catalog.Find(topic) ?? throw new InvalidOperationException($"El tópico '{topic}' no existe.");
        await EnsureRebuiltAsync(topic, ct).ConfigureAwait(false);

        var groupKey = Key(topic, queue);
        var group = _consumerGroups.GetOrAdd(groupKey, _ => new ConsumerGroupState(topicConfig.Partitions));

        // Verify consumer is in group and get assigned partitions
        var assignment = group.GetAssignment(consumerId);
        if (assignment is null || assignment.Partitions.Count == 0)
        {
            K0sMetrics.RecordQueueReceive(topic, queue, "not_assigned");
            return null; // Consumer not in group or no partitions assigned
        }

        QueueState state = _queues.GetOrAdd(Key(topic, queue), _ => new QueueState());

        // Se lee hasta el high watermark, no hasta el end offset: un mensaje que todavía no
        // llegó a quórum no se puede entregar. Los eventos de cola del log se saltean.
        var now = time.GetUtcNow();
        var visibleUntil = now.Add(visibility);

        foreach (int partition in assignment.Partitions)
        {
            long highWatermark = log.HighWatermark(topic, partition);
            if (highWatermark < 0)
            {
                continue;
            }

            // No volver a entregar offsets ya confirmados por este consumidor en esta cola.
            long scanFrom = _committedOffsets.GetOrAdd(Key(topic, queue, partition), -1) + 1;
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

                    if (record.DeliverAt > 0 && record.DeliverAt > now.ToUnixTimeMilliseconds())
                    {
                        continue;
                    }

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

                    K0sMetrics.RecordQueueReceive(topic, queue, "hit");
                    return new Delivery(partition, found.Offset, found, foundAttempt, foundUntil);
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
        QueueState state = _queues.GetOrAdd(Key(topic, queue), _ => new QueueState());
        if (!state.TryAcknowledge(partition, offset))
        {
            return false;
        }

        await AppendEventAsync(
            topic,
            new QueueEvent(RecordType.Ack, queue, partition, offset, HastaMs: 0),
            ct).ConfigureAwait(false);
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
                // UntilMs 0: sin próximo intento. Así Apply lo reconstruye como DLQ y no como un reintento programado.
                new QueueEvent(RecordType.Nack, queue, partition, offset, HastaMs: 0, reason),
                ct).ConfigureAwait(false);
            K0sMetrics.RecordQueueNack(topic, queue, deadLettered: true);
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
        K0sMetrics.RecordQueueNack(topic, queue, deadLettered: false);
        return true;
    }

    /// <inheritdoc />
    public async ValueTask<ConsumerAssignment> JoinGroupAsync(string topic, string queue, string consumerId, CancellationToken ct = default)
    {
        TopicConfig? topicConfig = catalog.Find(topic) ?? throw new InvalidOperationException($"El tópico '{topic}' no existe.");
        await EnsureRebuiltAsync(topic, ct).ConfigureAwait(false);

        var groupKey = Key(topic, queue);
        var group = _consumerGroups.GetOrAdd(groupKey, _ => new ConsumerGroupState(topicConfig.Partitions));

        var assignment = group.Join(consumerId);
        // El estado del grupo se persiste al salir o hacer heartbeat; el alta inicial vive en memoria
        // hasta el siguiente cambio. Si el líder cae, el consumidor se vuelve a unir.

        return new ConsumerAssignment(consumerId, assignment.Partitions, assignment.Generation);
    }

    /// <inheritdoc />
    public async ValueTask LeaveGroupAsync(string topic, string queue, string consumerId, CancellationToken ct = default)
    {
        TopicConfig? topicConfig = catalog.Find(topic) ?? throw new InvalidOperationException($"El tópico '{topic}' no existe.");
        await EnsureRebuiltAsync(topic, ct).ConfigureAwait(false);

        var groupKey = Key(topic, queue);
        if (_consumerGroups.TryGetValue(groupKey, out var group))
        {
            group.Leave(consumerId);
            await PersistGroupStateAsync(topic, queue, group, ct).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async ValueTask<ConsumerAssignment> HeartbeatAsync(string topic, string queue, string consumerId, CancellationToken ct = default)
    {
        TopicConfig? topicConfig = catalog.Find(topic) ?? throw new InvalidOperationException($"El tópico '{topic}' no existe.");
        await EnsureRebuiltAsync(topic, ct).ConfigureAwait(false);

        var groupKey = Key(topic, queue);
        if (!_consumerGroups.TryGetValue(groupKey, out var group))
        {
            throw new InvalidOperationException($"El consumidor '{consumerId}' no pertenece al grupo.");
        }

        var assignment = group.Heartbeat(consumerId);
        await PersistGroupStateAsync(topic, queue, group, ct).ConfigureAwait(false);

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

        var groupKey = Key(topic, queue);
        if (!_consumerGroups.TryGetValue(groupKey, out var group))
        {
            throw new InvalidOperationException($"El consumidor '{consumerId}' no pertenece al grupo.");
        }

        // Verify consumer owns the partitions
        var assignment = group.GetAssignment(consumerId);
        if (assignment is null)
        {
            throw new InvalidOperationException($"El consumidor '{consumerId}' no pertenece al grupo.");
        }

        foreach (var (partition, offset) in offsets)
        {
            if (!assignment.Partitions.Contains(partition))
            {
                throw new InvalidOperationException($"El consumidor '{consumerId}' no tiene asignada la partición {partition}.");
            }

            // Escribir el evento de confirmación al log y esperar quórum, igual que un mensaje.
            var commitEvent = QueueEvent.ConfirmarOffset(queue, partition, offset);
            long commitOffset = await log.AppendAsync(
                topic,
                partition,
                commitEvent.ARegistro(cluster.CurrentEpoch, time.GetUtcNow().ToUnixTimeMilliseconds()),
                ct).ConfigureAwait(false);
            await replicator.WaitForQuorumAsync(topic, partition, commitOffset, null, ct).ConfigureAwait(false);

            _committedOffsets.AddOrUpdate(Key(topic, queue, partition), offset, (_, existing) => Math.Max(existing, offset));
        }
    }

    /// <inheritdoc />
    public IReadOnlyList<DeadLetter> GetDeadLetters(string topic, string queue)
    {
        if (catalog.Find(topic) is null)
        {
            return [];
        }

        // El contrato es sincrónico: se espera la relectura igual que GetDeadLetters ya espera la lectura del log.
        EnsureRebuiltAsync(topic, CancellationToken.None).AsTask().GetAwaiter().GetResult();
        QueueState state = _queues.GetOrAdd(Key(topic, queue), _ => new QueueState());
        return state.GetDeadLetters(topic, queue, log);
    }

    /// <inheritdoc />
    public IReadOnlyList<ConsumerState> GetConsumerStates(string topic, string queue)
    {
        var groupKey = Key(topic, queue);
        if (!_consumerGroups.TryGetValue(groupKey, out var group))
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

        var replayed = QueueEvent.DesdeRegistro(queueEvent);
        switch (replayed.Tipo)
        {
            case RecordType.Receive or RecordType.Ack or RecordType.Nack or RecordType.Timeout:
            {
                QueueState state = _queues.GetOrAdd(Key(topic, replayed.Cola), _ => new QueueState());
                state.Apply(replayed);
                break;
            }

            case RecordType.OffsetCommit:
                _committedOffsets.AddOrUpdate(
                    Key(topic, replayed.Cola, replayed.Particion),
                    replayed.OffsetObjetivo,
                    (_, existing) => Math.Max(existing, replayed.OffsetObjetivo));
                break;

            case RecordType.GroupState:
                var miembros = QueueEvent.DeserializarEstadoDelGrupo(queueEvent.Value.Span, out long generacion);
                if (miembros is not null)
                {
                    int partitions = catalog.Find(topic) is { } config ? config.Partitions : 1;
                    var group = new ConsumerGroupState(partitions);
                    group.RebuildFromPersisted(generacion, miembros);
                    _consumerGroups[Key(topic, replayed.Cola)] = group;
                }
                break;
        }
    }

    public void Dispose() => _rebuilding.Dispose();

    private static string Key(string topic, string queue) => $"{topic}/{queue}";

    private static string Key(string topic, string queue, int partition) => $"{topic}/{queue}/{partition}";

    /// <summary>
    /// Relee del log los eventos de cola del tópico la primera vez que se usa en cada época. La memoria puede no coincidir
    /// con el log, que es la fuente de verdad, al arrancar (está vacía) y al cambiar de líder (otro líder pudo escribir
    /// eventos mientras tanto).
    /// </summary>
    private async ValueTask EnsureRebuiltAsync(string topic, CancellationToken ct)
    {
        long epoch = cluster.CurrentEpoch;
        if (IsRebuilt(topic, epoch))
        {
            return;
        }

        // Uno a la vez: si dos pedidos releyeran a la vez, cada evento se aplicaría dos veces.
        await _rebuilding.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (IsRebuilt(topic, epoch))
            {
                return;
            }

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

    private bool IsRebuilt(string topic, long epoch) => _rebuiltInEpoch.TryGetValue(topic, out long rebuilt) && rebuilt == epoch;

    /// <summary>
    /// Aplica todos los eventos del log, no solo hasta el high watermark: un líder nuevo termina confirmando todo su log.
    /// Recorre todas las particiones del tópico (según el catálogo), no solo la 0.
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

    private static IEnumerable<int> PartitionsRange(int partitions)
    {
        for (int i = 0; i < partitions; i++)
        {
            yield return i;
        }
    }

    /// <summary>
    /// Escribe el evento y espera el quórum antes de responder, igual que un mensaje: si el líder cayera antes, el
    /// nuevo líder podría no tener, por ejemplo, un ack ya confirmado al cliente, y volvería a entregar el mensaje.
    /// </summary>
    private async ValueTask AppendEventAsync(string topic, QueueEvent queueEvent, CancellationToken ct)
    {
        long offset = await log.AppendAsync(
            topic,
            queueEvent.Particion,
            queueEvent.ARegistro(cluster.CurrentEpoch, time.GetUtcNow().ToUnixTimeMilliseconds()),
            ct).ConfigureAwait(false);
        await replicator.WaitForQuorumAsync(topic, queueEvent.Particion, offset, null, ct).ConfigureAwait(false);
    }

    private async ValueTask PersistGroupStateAsync(string topic, string queue, ConsumerGroupState group, CancellationToken ct)
    {
        var members = new Dictionary<string, IReadOnlyList<int>>(StringComparer.Ordinal);
        foreach (var state in group.GetAllStates())
        {
            members[state.ConsumerId] = state.AssignedPartitions;
        }

        var payload = QueueEvent.SerializarEstadoDelGrupo(group.Generation, members);
        var record = new Record(
            Record.Unassigned,
            cluster.CurrentEpoch,
            RecordType.GroupState,
            time.GetUtcNow().ToUnixTimeMilliseconds(),
            0,
            Encoding.UTF8.GetBytes(queue),
            payload);

        long offset = await log.AppendAsync(topic, 0, record, ct).ConfigureAwait(false);
        await replicator.WaitForQuorumAsync(topic, 0, offset, null, ct).ConfigureAwait(false);
    }

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
                var key = (replayed.Particion, replayed.OffsetObjetivo);
                switch (replayed.Tipo)
                {
                    case RecordType.Receive:
                        // UntilMs es el fin de la visibilidad: hay que reconstruirlo también,
                        // o un mensaje en proceso volvería a entregarse de inmediato.
                        _inFlight.Add(key);
                        _visibleUntil[key] = DateTimeOffset.FromUnixTimeMilliseconds(replayed.HastaMs);
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
                        if (replayed.Razon is not null)
                        {
                            _lastReason[key] = replayed.Razon;
                        }
                        if (replayed.HastaMs > 0)
                        {
                            _scheduledUntil[key] = DateTimeOffset.FromUnixTimeMilliseconds(replayed.HastaMs);
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

    /// <summary>Estado del grupo de consumidores para una cola.</summary>
    private sealed class ConsumerGroupState
    {
        private readonly object _gate = new();
        private readonly int _totalPartitions;
        private readonly Dictionary<string, ConsumerMember> _members = new(StringComparer.Ordinal);
        private long _generation;

        public ConsumerGroupState(int totalPartitions)
        {
            _totalPartitions = totalPartitions;
        }

        public ConsumerAssignment Join(string consumerId)
        {
            lock (_gate)
            {
                if (!_members.TryGetValue(consumerId, out var member))
                {
                    member = new ConsumerMember(consumerId);
                    _members[consumerId] = member;
                }

                member.LastHeartbeat = DateTimeOffset.UtcNow;
                Rebalance();
                return new ConsumerAssignment(consumerId, _members[consumerId].AssignedPartitions, _generation);
            }
        }

        public void Leave(string consumerId)
        {
            lock (_gate)
            {
                _members.Remove(consumerId);
                Rebalance();
            }
        }

        public ConsumerAssignment Heartbeat(string consumerId)
        {
            lock (_gate)
            {
                if (!_members.TryGetValue(consumerId, out var member))
                {
                    throw new InvalidOperationException($"Consumer {consumerId} not in group.");
                }

                member.LastHeartbeat = DateTimeOffset.UtcNow;
                return new ConsumerAssignment(consumerId, member.AssignedPartitions, _generation);
            }
        }

        public ConsumerAssignment? GetAssignment(string consumerId)
        {
            lock (_gate)
            {
                if (_members.TryGetValue(consumerId, out var member))
                {
                    return new ConsumerAssignment(consumerId, member.AssignedPartitions, _generation);
                }
                return null;
            }
        }

        public IReadOnlyList<ConsumerState> GetAllStates()
        {
            lock (_gate)
            {
                return [.. _members.Values.Select(m => new ConsumerState(m.ConsumerId, m.LastHeartbeat, m.AssignedPartitions))];
            }
        }

        /// <summary>Generación actual del grupo (incrementa en cada rebalance).</summary>
        public long Generation => _generation;

        /// <summary>
        /// Reconstruye el estado del grupo desde un evento GroupState persistido en el log.
        /// Se usa cuando un broker se reinicia o se convierte en líder.
        /// </summary>
        internal void RebuildFromPersisted(long generacion, IReadOnlyDictionary<string, IReadOnlyList<int>> miembros)
        {
            lock (_gate)
            {
                _generation = generacion;
                _members.Clear();
                foreach (var (consumerId, particiones) in miembros)
                {
                    var member = new ConsumerMember(consumerId);
                    foreach (var p in particiones)
                    {
                        member.AssignedPartitions.Add(p);
                    }
                    member.LastHeartbeat = DateTimeOffset.UtcNow;
                    _members[consumerId] = member;
                }
            }
        }

        private void Rebalance()
        {
            // Cooperative sticky assignment: minimize partition movement
            var activeMembers = _members.Values.Where(m => m.IsActive).OrderBy(m => m.ConsumerId).ToList();
            if (activeMembers.Count == 0)
            {
                return;
            }

            _generation++;

            // Distribute partitions evenly
            var partitions = Enumerable.Range(0, _totalPartitions).ToList();
            for (int i = 0; i < partitions.Count; i++)
            {
                var consumer = activeMembers[i % activeMembers.Count];
                consumer.AssignedPartitions.Add(partitions[i]);
            }
        }

        private sealed class ConsumerMember
        {
            public string ConsumerId { get; }
            public DateTimeOffset LastHeartbeat { get; set; }
            public List<int> AssignedPartitions { get; } = new();
            public bool IsActive => (DateTimeOffset.UtcNow - LastHeartbeat) < TimeSpan.FromSeconds(30);

            public ConsumerMember(string consumerId)
            {
                ConsumerId = consumerId;
                LastHeartbeat = DateTimeOffset.UtcNow;
            }
        }
    }
}