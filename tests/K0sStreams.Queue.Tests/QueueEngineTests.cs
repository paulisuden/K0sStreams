using System.Text;
using K0sStreams.Contracts;
using K0sStreams.Contracts.Fakes;
using K0sStreams.Queue;
using Record = K0sStreams.Contracts.Record;

namespace K0sStreams.Queue.Tests;

/// <summary>
/// Tests del motor de cola sobre el log en memoria: orden, visibilidad, ack y replay.
/// Fases 1: receive, ack, visibility timeout y <c>Apply</c>. Nack/DLQ/programados llegan en la fase 2.
/// </summary>
public sealed class QueueEngineTests : IDisposable
{
    private static readonly DateTimeOffset Epoch = new(2026, 10, 1, 0, 0, 0, TimeSpan.Zero);

    private readonly InMemoryLog _log = new();
    private readonly InMemoryTopicCatalog _catalog = new();
    private readonly StaticClusterState _cluster = new("broker-0");
    private readonly InstantReplicator _replicator;
    private readonly FakeTimeProvider _time = new(Epoch);
    private readonly QueueEngine _engine;

    public QueueEngineTests()
    {
        _replicator = new InstantReplicator(_log);
        _engine = new QueueEngine(_log, _catalog, _cluster, _replicator, _time);
    }

    public void Dispose() => _engine.Dispose();

    /// <summary>
    /// Publica un mensaje y lo confirma por quórum, igual que haría la API: sin avanzar el
    /// high watermark el mensaje no es visible ni para la cola (como en producción).
    /// </summary>
    private async ValueTask AppendMessagesAsync(string topic, int partition, params string[] values)
    {
        foreach (string value in values)
        {
            long offset = await _log.AppendAsync(topic, partition, Message(value));
            await _replicator.WaitForQuorumAsync(topic, partition, offset);
        }
    }

    private async ValueTask AppendScheduledAsync(string topic, int partition, long deliverAtMs, params string[] values)
    {
        foreach (string value in values)
        {
            var record = new Record(Record.Unassigned, 1, RecordType.Message, _time.GetUtcNow().ToUnixTimeMilliseconds(), deliverAtMs, ReadOnlyMemory<byte>.Empty, Encoding.UTF8.GetBytes(value));
            long offset = await _log.AppendAsync(topic, partition, record);
            await _replicator.WaitForQuorumAsync(topic, partition, offset);
        }
    }

    private Record Message(string value) =>
        Record.NewMessage(1, _time.GetUtcNow().ToUnixTimeMilliseconds(), ReadOnlyMemory<byte>.Empty, Encoding.UTF8.GetBytes(value));

    private async Task<List<Record>> ReadAllAsync(string topic, int partition)
    {
        var records = new List<Record>();
        await foreach (var record in _log.ReadAsync(topic, partition, 0, int.MaxValue))
        {
            records.Add(record);
        }

        return records;
    }

    private async ValueTask CreateTopicAsync(string topic = "pedidos", int partitions = 1, int maxRetries = 3, TimeSpan? retryBackoff = null)
    {
        (await _catalog.CreateAsync(new TopicConfig(topic, partitions, maxRetries, retryBackoff ?? TimeSpan.FromSeconds(5)))).Should().BeTrue();
    }

    private async ValueTask<ConsumerAssignment> JoinGroupAsync(string topic = "pedidos", string queue = "facturacion", string consumerId = "consumer-1")
    {
        return await _engine.JoinGroupAsync(topic, queue, consumerId);
    }

    [Fact]
    public async Task Receive_sin_mensajes_devuelve_null()
    {
        await CreateTopicAsync();
        await JoinGroupAsync();

        (await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30))).Should().BeNull();
    }

    [Fact]
    public async Task Receive_devuelve_el_mensaje_de_menor_offset()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a", "b", "c");
        await JoinGroupAsync();

        var delivery = await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30));

        delivery.Should().NotBeNull();
        delivery!.Partition.Should().Be(0);
        delivery.Offset.Should().Be(0);
        Encoding.UTF8.GetString(delivery.Message.Value.Span).Should().Be("a");
    }

    [Fact]
    public async Task Receive_no_escribe_en_el_log()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a");
        await JoinGroupAsync();

        await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30));

        // La entrega es estado efímero: no toca el log ni el high watermark.
        var records = await ReadAllAsync("pedidos", 0);
        records.Should().ContainSingle().Which.Type.Should().Be(RecordType.Message);
        _log.HighWatermark("pedidos", 0).Should().Be(0);
    }

    [Fact]
    public async Task Ack_devuelve_true_y_el_mensaje_no_se_vuelve_a_entregar()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a", "b");
        await JoinGroupAsync();
        var delivery = await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30));

        (await _engine.AckAsync("pedidos", "facturacion", delivery!.Partition, delivery.Offset)).Should().BeTrue();

        (await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30)))!.Offset.Should().Be(1);
    }

    [Fact]
    public async Task Receives_concurrentes_no_entregan_el_mismo_mensaje()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a");
        await JoinGroupAsync();

        var deliveries = await Task.WhenAll(
            _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30)).AsTask(),
            _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30)).AsTask());

        deliveries.Count(delivery => delivery is not null).Should().Be(1);
    }

    [Fact]
    public async Task Ack_devuelve_false_si_el_mensaje_no_esta_en_proceso()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a");
        await JoinGroupAsync();

        (await _engine.AckAsync("pedidos", "facturacion", 0, 0)).Should().BeFalse();
    }

    [Fact]
    public async Task Ack_de_otra_cola_no_confirma_el_mensaje()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a");
        await JoinGroupAsync();
        var delivery = await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30));

        (await _engine.AckAsync("pedidos", "envio", delivery!.Partition, delivery.Offset)).Should().BeFalse();
        (await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30))).Should().BeNull();
    }

    [Fact]
    public async Task Dos_colas_del_mismo_topico_reciben_cada_mensaje()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a");
        await JoinGroupAsync(queue: "facturacion");
        await JoinGroupAsync(queue: "envio", consumerId: "consumer-2");

        var facturacion = await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30));
        var envio = await _engine.ReceiveAsync("pedidos", "envio", "consumer-2", TimeSpan.FromSeconds(30));

        facturacion.Should().NotBeNull();
        envio.Should().NotBeNull();
        envio!.Partition.Should().Be(facturacion!.Partition);
        envio.Offset.Should().Be(facturacion.Offset);
    }

    [Fact]
    public async Task Dos_receive_entregan_mensajes_distintos()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a", "b");
        await JoinGroupAsync();

        var first = await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30));
        var second = await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30));

        first!.Offset.Should().Be(0);
        second!.Offset.Should().Be(1);
    }

    [Fact]
    public async Task Timeout_devuelve_el_mensaje_a_la_cola_al_cumplirse_la_visibilidad()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a");
        await JoinGroupAsync();
        await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30));

        (await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30))).Should().BeNull();

        _time.Advance(TimeSpan.FromSeconds(31));

        (await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30)))!.Offset.Should().Be(0);
    }

    [Fact]
    public async Task El_segundo_receive_aumenta_el_intento()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a");
        await JoinGroupAsync();
        var first = await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30));
        first!.Attempt.Should().Be(1);

        _time.Advance(TimeSpan.FromSeconds(31));
        var second = await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30));

        second!.Attempt.Should().Be(2);
    }

    [Fact]
    public async Task Nack_devuelve_el_mensaje_con_backoff_y_aumenta_intento()
    {
        await CreateTopicAsync("pedidos", 1, 3, TimeSpan.FromSeconds(1));
        await AppendMessagesAsync("pedidos", 0, "a");
        await JoinGroupAsync();
        var first = await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30));
        first!.Attempt.Should().Be(1);

        (await _engine.NackAsync("pedidos", "facturacion", 0, first.Offset, "fallo")).Should().BeTrue();
        (await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30))).Should().BeNull();

        _time.Advance(TimeSpan.FromSeconds(2));
        var second = await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30));
        second!.Attempt.Should().Be(2);
    }

    [Fact]
    public async Task Nack_lleva_a_DLQ_cuando_se_superan_reintentos()
    {
        await CreateTopicAsync("pedidos", 1, 1, TimeSpan.FromSeconds(1));
        await AppendMessagesAsync("pedidos", 0, "a");
        await JoinGroupAsync();
        var first = await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30));
        (await _engine.NackAsync("pedidos", "facturacion", 0, first!.Offset, "fallo")).Should().BeTrue();

        _time.Advance(TimeSpan.FromSeconds(2));
        var second = await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30));
        (await _engine.NackAsync("pedidos", "facturacion", 0, second!.Offset, "fallo2")).Should().BeTrue();

        var dlq = _engine.GetDeadLetters("pedidos", "facturacion");
        dlq.Should().ContainSingle();
        dlq[0].Offset.Should().Be(0);
        dlq[0].Attempts.Should().Be(2);
    }

    [Fact]
    public async Task Mensaje_programado_no_se_entrega_hasta_deliverAt()
    {
        await CreateTopicAsync("pedidos", 1, 3, TimeSpan.FromSeconds(1));
        long future = Epoch.AddSeconds(60).ToUnixTimeMilliseconds();
        await AppendScheduledAsync("pedidos", 0, future, "a");
        await JoinGroupAsync();

        (await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30))).Should().BeNull();

        _time.Advance(TimeSpan.FromSeconds(61));
        // El miembro sigue vivo: renueva antes de que venza la sesión.
        await _engine.HeartbeatAsync("pedidos", "facturacion", "consumer-1");
        var delivery = await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30));
        delivery.Should().NotBeNull();
        delivery!.Offset.Should().Be(0);
    }

    [Fact]
    public async Task El_cursor_se_vuelca_al_log_al_pasar_la_ventana_de_auto_commit()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a");
        await JoinGroupAsync();

        var delivery = await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30));
        await _engine.AckAsync("pedidos", "facturacion", delivery!.Partition, delivery.Offset);

        // El ack todavía no escribió nada: solo quedó sucio en memoria.
        var records = await ReadAllAsync("pedidos", 0);
        records.Should().ContainSingle().Which.Type.Should().Be(RecordType.Message);

        // Pasada la ventana, el siguiente receive vuelca el cursor al log con quórum.
        _time.Advance(TimeSpan.FromSeconds(2));
        await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30));

        records = await ReadAllAsync("pedidos", 0);
        var commit = records.Should().ContainSingle(record => record.Type == RecordType.OffsetCommit).Which;
        QueueEvent.DesdeRegistro(commit).OffsetObjetivo.Should().Be(0);
    }

    [Fact]
    public async Task Receive_y_ack_no_esperan_quorum()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a");

        // Replicador que siempre falla: recibir y confirmar no tocan el log.
        using var engine = new QueueEngine(_log, _catalog, _cluster, new NotLeaderReplicator(), _time);
        await engine.JoinGroupAsync("pedidos", "facturacion", "consumer-1");

        var delivery = await engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30));
        delivery.Should().NotBeNull();
        (await engine.AckAsync("pedidos", "facturacion", delivery!.Partition, delivery.Offset)).Should().BeTrue();

        // El commit sí es durable y por eso exige líder y quórum.
        await FluentActions
            .Awaiting(() => engine.CommitOffsetsAsync(
                "pedidos",
                "facturacion",
                "consumer-1",
                new Dictionary<int, long> { [0] = 0 }).AsTask())
            .Should().ThrowAsync<NotLeaderException>();
    }

    [Fact]
    public async Task Un_motor_nuevo_renuda_desde_el_offset_confirmado()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a", "b", "c");
        await JoinGroupAsync();

        var delivery = await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30));
        await _engine.AckAsync("pedidos", "facturacion", delivery!.Partition, delivery.Offset);
        await _engine.CommitOffsetsAsync(
            "pedidos",
            "facturacion",
            "consumer-1",
            new Dictionary<int, long> { [0] = delivery.Offset });

        using var restarted = new QueueEngine(_log, _catalog, _cluster, _replicator, _time);
        await restarted.JoinGroupAsync("pedidos", "facturacion", "consumer-1");

        (await restarted.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30)))!.Offset.Should().Be(1);
    }

    [Fact]
    public async Task Un_motor_nuevo_reentrega_lo_que_estaba_en_proceso()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a");
        await JoinGroupAsync();

        await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30));

        // La entrega en proceso es efímera: el líder nuevo no la conoce y la reentrega.
        using var restarted = new QueueEngine(_log, _catalog, _cluster, _replicator, _time);
        await restarted.JoinGroupAsync("pedidos", "facturacion", "consumer-1");

        var again = await restarted.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30));
        again!.Offset.Should().Be(0);
        again.Attempt.Should().Be(1);
    }

    [Fact]
    public async Task Al_cambiar_de_epoca_relee_los_offsets_confirmados()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a", "b");
        var cluster = new ChangingClusterState();
        using var engine = new QueueEngine(_log, _catalog, cluster, _replicator, _time);
        await engine.JoinGroupAsync("pedidos", "facturacion", "consumer-1");
        var delivery = await engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30));

        // Otro broker lidera la época 2 y confirma el offset. El log compartido hace de replicación.
        using var otherLeader = new QueueEngine(_log, _catalog, new ChangingClusterState { CurrentEpoch = 2 }, _replicator, _time);
        await otherLeader.JoinGroupAsync("pedidos", "facturacion", "consumer-2");
        await otherLeader.CommitOffsetsAsync(
            "pedidos",
            "facturacion",
            "consumer-2",
            new Dictionary<int, long> { [0] = delivery!.Offset });

        // Este broker vuelve a liderar en la época 3: al releer el log ve el commit del otro líder.
        cluster.CurrentEpoch = 3;
        await engine.JoinGroupAsync("pedidos", "facturacion", "consumer-1");

        (await engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30)))!.Offset.Should().Be(1);
    }

    [Fact]
    public async Task La_DLQ_sobrevive_a_un_reinicio()
    {
        await CreateTopicAsync(maxRetries: 0);
        await AppendMessagesAsync("pedidos", 0, "a");
        await JoinGroupAsync();
        var delivery = await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30));
        await _engine.NackAsync("pedidos", "facturacion", delivery!.Partition, delivery.Offset, "rompe");

        using var restarted = new QueueEngine(_log, _catalog, _cluster, _replicator, _time);
        await restarted.JoinGroupAsync("pedidos", "facturacion", "consumer-1");

        restarted.GetDeadLetters("pedidos", "facturacion").Should().ContainSingle().Which.Offset.Should().Be(0);
    }

    [Fact]
    public async Task Receives_concurrentes_en_un_motor_nuevo_entregan_una_sola_vez()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a");
        await JoinGroupAsync();
        await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30));

        using var restarted = new QueueEngine(_log, _catalog, _cluster, _replicator, _time);
        await restarted.JoinGroupAsync("pedidos", "facturacion", "consumer-1");
        var deliveries = await Task.WhenAll(Enumerable.Range(0, 8).Select(
            _ => restarted.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30)).AsTask()));

        // El lock del estado evita entregas duplicadas; el intento arranca de cero porque la entrega era efímera.
        deliveries.Should().ContainSingle(delivery => delivery != null).Which!.Attempt.Should().Be(1);
    }

    [Fact]
    public async Task Apply_reconstruye_un_mensaje_confirmado()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a", "b");
        await JoinGroupAsync();
        var delivery = await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30));
        await _engine.AckAsync("pedidos", "facturacion", delivery!.Partition, delivery.Offset);
        await _engine.CommitOffsetsAsync(
            "pedidos",
            "facturacion",
            "consumer-1",
            new Dictionary<int, long> { [0] = delivery.Offset });

        var recovered = new QueueEngine(_log, _catalog, _cluster, _replicator, _time);
        foreach (var record in await ReadAllAsync("pedidos", 0))
        {
            if (QueueEvent.EsEventoDeCola(record.Type))
            {
                recovered.Apply("pedidos", record);
            }
        }

        await recovered.JoinGroupAsync("pedidos", "facturacion", "consumer-1");
        (await recovered.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30)))!.Offset.Should().Be(1);
        (await recovered.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30))).Should().BeNull();
    }

    [Fact]
    public async Task Apply_ignora_los_registros_que_no_son_eventos_de_cola()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a");
        await JoinGroupAsync();

        var recovered = new QueueEngine(_log, _catalog, _cluster, _replicator, _time);
        foreach (var record in await ReadAllAsync("pedidos", 0))
        {
            recovered.Apply("pedidos", record);
        }

        await recovered.JoinGroupAsync("pedidos", "facturacion", "consumer-1");
        (await recovered.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30)))!.Offset.Should().Be(0);
    }

    [Fact]
    public async Task CommitOffsets_escribe_evento_en_el_log_y_no_entrega_de_nuevo_despues_de_reinicio()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a", "b", "c");
        await JoinGroupAsync();

        // Recibir y confirmar el offset 0
        var d1 = await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30));
        d1.Should().NotBeNull();
        await _engine.CommitOffsetsAsync("pedidos", "facturacion", "consumer-1", new Dictionary<int, long> { [0] = 0 });

        // Verificar que el evento de confirmación está en el log
        var records = await ReadAllAsync("pedidos", 0);
        records.Should().Contain(r => r.Type == RecordType.OffsetCommit);

        // Reiniciar el motor: el offset 0 ya no debería entregarse
        using var restarted = new QueueEngine(_log, _catalog, _cluster, _replicator, _time);
        await restarted.JoinGroupAsync("pedidos", "facturacion", "consumer-1");
        (await restarted.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30)))!.Offset.Should().Be(1);
    }

    [Fact]
    public async Task Heartbeat_no_escribe_en_el_log()
    {
        await CreateTopicAsync();
        await JoinGroupAsync();

        await _engine.HeartbeatAsync("pedidos", "facturacion", "consumer-1");

        // La membresía es efímera: el latido no deja rastro durable.
        (await ReadAllAsync("pedidos", 0)).Should().BeEmpty();
    }

    [Fact]
    public async Task Los_consumidores_se_reunen_y_reciben_reparto_tras_un_reinicio()
    {
        await CreateTopicAsync(partitions: 2);

        await _engine.JoinGroupAsync("pedidos", "facturacion", "consumer-1");
        await _engine.JoinGroupAsync("pedidos", "facturacion", "consumer-2");

        // La membresía no es durable: el motor nuevo arranca vacío y los consumidores se vuelven a unir.
        using var restarted = new QueueEngine(_log, _catalog, _cluster, _replicator, _time);
        await restarted.JoinGroupAsync("pedidos", "facturacion", "consumer-1");
        await restarted.JoinGroupAsync("pedidos", "facturacion", "consumer-2");

        var states = restarted.GetConsumerStates("pedidos", "facturacion");

        states.Should().HaveCount(2);
        states.SelectMany(state => state.AssignedPartitions).Should().BeEquivalentTo([0, 1]);
    }

    [Fact]
    public async Task El_reparto_es_disjunto_y_cubre_todas_las_particiones()
    {
        await CreateTopicAsync(partitions: 3);

        await _engine.JoinGroupAsync("pedidos", "facturacion", "consumer-1");
        await _engine.JoinGroupAsync("pedidos", "facturacion", "consumer-2");

        int[] assigned = [.. _engine.GetConsumerStates("pedidos", "facturacion")
            .SelectMany(state => state.AssignedPartitions)];

        assigned.Should().BeEquivalentTo([0, 1, 2]);
        assigned.Should().OnlyHaveUniqueItems();
    }

    [Fact]
    public async Task Unirse_varias_veces_no_duplica_la_asignacion()
    {
        await CreateTopicAsync(partitions: 3);

        await _engine.JoinGroupAsync("pedidos", "facturacion", "consumer-1");
        await _engine.JoinGroupAsync("pedidos", "facturacion", "consumer-1");
        await _engine.HeartbeatAsync("pedidos", "facturacion", "consumer-1");

        var state = _engine.GetConsumerStates("pedidos", "facturacion").Should().ContainSingle().Subject;

        state.AssignedPartitions.Should().BeEquivalentTo([0, 1, 2]);
        state.AssignedPartitions.Should().OnlyHaveUniqueItems();
    }

    [Fact]
    public async Task Un_miembro_vencido_libera_sus_particiones()
    {
        await CreateTopicAsync(partitions: 3);
        await _engine.JoinGroupAsync("pedidos", "facturacion", "consumer-1");
        await _engine.JoinGroupAsync("pedidos", "facturacion", "consumer-2");

        _time.Advance(TimeSpan.FromSeconds(46));
        await _engine.HeartbeatAsync("pedidos", "facturacion", "consumer-2");

        var survivors = _engine.GetConsumerStates("pedidos", "facturacion");

        survivors.Should().ContainSingle().Which.ConsumerId.Should().Be("consumer-2");
        survivors.Single().AssignedPartitions.Should().BeEquivalentTo([0, 1, 2]);
    }

    [Fact]
    public async Task La_generacion_solo_avanza_cuando_cambia_la_membresia()
    {
        await CreateTopicAsync(partitions: 2);

        var first = await _engine.JoinGroupAsync("pedidos", "facturacion", "consumer-1");
        var heartbeat = await _engine.HeartbeatAsync("pedidos", "facturacion", "consumer-1");
        var second = await _engine.JoinGroupAsync("pedidos", "facturacion", "consumer-2");

        heartbeat.Generation.Should().Be(first.Generation);
        second.Generation.Should().BeGreaterThan(first.Generation);
    }

    [Fact]
    public async Task Un_ack_fuera_de_orden_no_avanza_el_cursor_sobre_el_hueco()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a", "b", "c");
        await JoinGroupAsync();

        var a = await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30));
        await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30));
        var c = await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30));

        // Confirmar c deja un hueco en a/b: el cursor no puede saltarlo.
        await _engine.AckAsync("pedidos", "facturacion", c!.Partition, c.Offset);
        await FlushAsync();
        (await ReadAllAsync("pedidos", 0)).Should().NotContain(record => record.Type == RecordType.OffsetCommit);

        // Al cerrar a, el cursor avanza solo hasta a.
        await _engine.AckAsync("pedidos", "facturacion", a!.Partition, a.Offset);
        await FlushAsync();
        var firstCommit = (await ReadAllAsync("pedidos", 0))
            .Should().ContainSingle(record => record.Type == RecordType.OffsetCommit).Which;
        QueueEvent.DesdeRegistro(firstCommit).OffsetObjetivo.Should().Be(0);
    }

    [Fact]
    public async Task GetCommittedOffsets_devuelve_el_cursor_confirmado()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a", "b");
        await JoinGroupAsync();

        var delivery = await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30));
        await _engine.AckAsync("pedidos", "facturacion", delivery!.Partition, delivery.Offset);
        await _engine.CommitOffsetsAsync(
            "pedidos",
            "facturacion",
            "consumer-1",
            new Dictionary<int, long> { [0] = delivery.Offset });

        _engine.GetCommittedOffsets("pedidos", "facturacion")
            .Should().ContainKey(0).WhoseValue.Should().Be(0);
    }

    private async ValueTask FlushAsync()
    {
        _time.Advance(TimeSpan.FromSeconds(2));
        await _engine.ReceiveAsync("pedidos", "facturacion", "consumer-1", TimeSpan.FromSeconds(30));
    }
}

/// <summary>Un líder que perdió el liderazgo mientras esperaba el quórum.</summary>
internal sealed class NotLeaderReplicator : IReplicator
{
    public ValueTask WaitForQuorumAsync(string topic, int partition, long offset, TimeSpan? timeout = null, CancellationToken ct = default) =>
        ValueTask.FromException(new NotLeaderException("Otro broker es el líder."));
}

/// <summary>Un broker que siempre es líder, con una época que el test puede cambiar.</summary>
internal sealed class ChangingClusterState : IClusterState
{
    public string NodeId => "broker-0";

    public bool IsLeader => true;

    public long CurrentEpoch { get; set; } = 1;

    public string? LeaderAddress => null;

    public event Action<long>? EpochChanged
    {
        add { }
        remove { }
    }
}