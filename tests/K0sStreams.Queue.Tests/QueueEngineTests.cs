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

    [Fact]
    public async Task Receive_sin_mensajes_devuelve_null()
    {
        await CreateTopicAsync();

        (await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30))).Should().BeNull();
    }

    [Fact]
    public async Task Receive_devuelve_el_mensaje_de_menor_offset()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a", "b", "c");

        var delivery = await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));

        delivery.Should().NotBeNull();
        delivery!.Partition.Should().Be(0);
        delivery.Offset.Should().Be(0);
        Encoding.UTF8.GetString(delivery.Message.Value.Span).Should().Be("a");
    }

    [Fact]
    public async Task Receive_escribe_un_evento_de_cola_en_el_log()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a");
        var visibleUntil = Epoch.AddSeconds(30);

        var delivery = await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));

        var records = await ReadAllAsync("pedidos", 0);
        records.Should().HaveCount(2);
        records[1].Type.Should().Be(RecordType.Receive);
        var queueEvent = QueueEvent.FromRecord(records[1]);
        queueEvent.Queue.Should().Be("facturacion");
        queueEvent.Partition.Should().Be(delivery!.Partition);
        queueEvent.TargetOffset.Should().Be(delivery.Offset);
        queueEvent.UntilMs.Should().Be(visibleUntil.ToUnixTimeMilliseconds());
    }

    [Fact]
    public async Task Ack_devuelve_true_y_el_mensaje_no_se_vuelve_a_entregar()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a", "b");
        var delivery = await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));

        (await _engine.AckAsync("pedidos", "facturacion", delivery!.Partition, delivery.Offset)).Should().BeTrue();

        (await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30)))!.Offset.Should().Be(1);
    }

    [Fact]
    public async Task Receives_concurrentes_no_entregan_el_mismo_mensaje()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a");

        var deliveries = await Task.WhenAll(
            _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30)).AsTask(),
            _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30)).AsTask());

        deliveries.Count(delivery => delivery is not null).Should().Be(1);
    }

    [Fact]
    public async Task Ack_devuelve_false_si_el_mensaje_no_esta_en_proceso()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a");

        (await _engine.AckAsync("pedidos", "facturacion", 0, 0)).Should().BeFalse();
    }

    [Fact]
    public async Task Ack_de_otra_cola_no_confirma_el_mensaje()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a");
        var delivery = await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));

        (await _engine.AckAsync("pedidos", "envio", delivery!.Partition, delivery.Offset)).Should().BeFalse();
        (await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30))).Should().BeNull();
    }

    [Fact]
    public async Task Dos_colas_del_mismo_topico_reciben_cada_mensaje()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a");

        var facturacion = await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));
        var envio = await _engine.ReceiveAsync("pedidos", "envio", TimeSpan.FromSeconds(30));

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

        var first = await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));
        var second = await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));

        first!.Offset.Should().Be(0);
        second!.Offset.Should().Be(1);
    }

    [Fact]
    public async Task Timeout_devuelve_el_mensaje_a_la_cola_al_cumplirse_la_visibilidad()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a");
        await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));

        (await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30))).Should().BeNull();

        _time.Advance(TimeSpan.FromSeconds(31));

        (await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30)))!.Offset.Should().Be(0);
    }

    [Fact]
    public async Task El_segundo_receive_aumenta_el_intento()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a");
        var first = await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));
        first!.Attempt.Should().Be(1);

        _time.Advance(TimeSpan.FromSeconds(31));
        var second = await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));

        second!.Attempt.Should().Be(2);
    }

    [Fact]
    public async Task Nack_devuelve_el_mensaje_con_backoff_y_aumenta_intento()
    {
        await CreateTopicAsync("pedidos", 1, 3, TimeSpan.FromSeconds(1));
        await AppendMessagesAsync("pedidos", 0, "a");
        var first = await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));
        first!.Attempt.Should().Be(1);

        (await _engine.NackAsync("pedidos", "facturacion", 0, first.Offset, "fallo")).Should().BeTrue();
        (await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30))).Should().BeNull();

        _time.Advance(TimeSpan.FromSeconds(2));
        var second = await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));
        second!.Attempt.Should().Be(2);
    }

    [Fact]
    public async Task Nack_lleva_a_DLQ_cuando_se_superan_reintentos()
    {
        await CreateTopicAsync("pedidos", 1, 1, TimeSpan.FromSeconds(1));
        await AppendMessagesAsync("pedidos", 0, "a");
        var first = await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));
        (await _engine.NackAsync("pedidos", "facturacion", 0, first!.Offset, "fallo")).Should().BeTrue();

        _time.Advance(TimeSpan.FromSeconds(2));
        var second = await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));
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

        (await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30))).Should().BeNull();

        _time.Advance(TimeSpan.FromSeconds(61));
        var delivery = await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));
        delivery.Should().NotBeNull();
        delivery!.Offset.Should().Be(0);
    }

    [Fact]
    public async Task Cada_evento_de_cola_queda_confirmado_por_quorum_antes_de_responder()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a");

        var delivery = await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));
        await _engine.AckAsync("pedidos", "facturacion", delivery!.Partition, delivery.Offset);

        // Offsets: 0 el mensaje, 1 el receive, 2 el ack. El high watermark solo llega a 2 si el ack tuvo quórum.
        _log.HighWatermark("pedidos", 0).Should().Be(2);
    }

    [Fact]
    public async Task Si_deja_de_ser_lider_esperando_el_quorum_el_receive_falla_con_NotLeaderException()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a");
        var engine = new QueueEngine(_log, _catalog, _cluster, new NotLeaderReplicator(), _time);

        var receive = async () => await engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));

        await receive.Should().ThrowAsync<NotLeaderException>();
    }

    [Fact]
    public async Task Un_motor_nuevo_reconstruye_la_cola_desde_el_log_sin_que_nadie_llame_a_Apply()
    {
        // Como un broker que se reinicia: la memoria arranca vacía y el log tiene todo.
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a", "b", "c");
        await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));
        var b = await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));
        await _engine.AckAsync("pedidos", "facturacion", b!.Partition, b.Offset);

        using var restarted = new QueueEngine(_log, _catalog, _cluster, _replicator, _time);

        // a sigue en proceso y b está confirmado: lo próximo es c.
        (await restarted.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30)))!.Offset.Should().Be(2);

        _time.Advance(TimeSpan.FromSeconds(31));
        var again = await restarted.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));
        again!.Offset.Should().Be(0);
        again.Attempt.Should().Be(2);
    }

    [Fact]
    public async Task Al_cambiar_de_epoca_relee_lo_que_escribio_otro_lider()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a");
        var cluster = new ChangingClusterState();
        using var engine = new QueueEngine(_log, _catalog, cluster, _replicator, _time);
        var delivery = await engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));

        // Otro broker lidera la época 2 y confirma el mensaje. El log compartido hace de replicación.
        using var otherLeader = new QueueEngine(_log, _catalog, new ChangingClusterState { CurrentEpoch = 2 }, _replicator, _time);
        (await otherLeader.AckAsync("pedidos", "facturacion", delivery!.Partition, delivery.Offset)).Should().BeTrue();

        // Este broker vuelve a liderar en la época 3. Sin releer, creería que el mensaje sigue en proceso y lo
        // entregaría de nuevo al vencer la visibilidad.
        cluster.CurrentEpoch = 3;
        _time.Advance(TimeSpan.FromSeconds(31));

        (await engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30))).Should().BeNull();
    }

    [Fact]
    public async Task La_DLQ_sobrevive_a_un_reinicio()
    {
        await CreateTopicAsync(maxRetries: 0);
        await AppendMessagesAsync("pedidos", 0, "a");
        var delivery = await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));
        await _engine.NackAsync("pedidos", "facturacion", delivery!.Partition, delivery.Offset, "rompe");

        using var restarted = new QueueEngine(_log, _catalog, _cluster, _replicator, _time);

        restarted.GetDeadLetters("pedidos", "facturacion").Should().ContainSingle().Which.Offset.Should().Be(0);
    }

    [Fact]
    public async Task Receives_concurrentes_en_un_motor_nuevo_releen_el_log_una_sola_vez()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a");
        await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));
        _time.Advance(TimeSpan.FromSeconds(31));

        using var restarted = new QueueEngine(_log, _catalog, _cluster, _replicator, _time);
        var deliveries = await Task.WhenAll(Enumerable.Range(0, 8).Select(
            _ => restarted.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30)).AsTask()));

        // Si se releyera dos veces, el receive anterior contaría doble y este sería el intento 3.
        deliveries.Should().ContainSingle(delivery => delivery != null).Which!.Attempt.Should().Be(2);
    }

    [Fact]
    public async Task Apply_reconstruye_el_estado_de_los_mensajes_en_proceso()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a", "b", "c");
        await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));
        (await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30)))!.Offset.Should().Be(1);

        var recovered = new QueueEngine(_log, _catalog, _cluster, _replicator, _time);
        foreach (var record in await ReadAllAsync("pedidos", 0))
        {
            if (QueueEvent.IsQueueEvent(record.Type))
            {
                recovered.Apply("pedidos", record);
            }
        }

        // a y b quedaron en proceso, así que el líder nuevo no puede volver a entregarlos:
        // c es el primer mensaje que sigue disponible.
        (await recovered.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30)))!.Offset.Should().Be(2);
    }

    [Fact]
    public async Task Apply_reconstruye_el_visibility_timeout_del_lider_anterior()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a");

        await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));

        var recovered = new QueueEngine(_log, _catalog, _cluster, _replicator, _time);
        foreach (var record in await ReadAllAsync("pedidos", 0))
        {
            if (QueueEvent.IsQueueEvent(record.Type))
            {
                recovered.Apply("pedidos", record);
            }
        }

        // Si el plazo no se reconstruyera desde el log, el mensaje volvería a entregarse
        // de inmediato en vez de esperar a que venza el visibility timeout del líder anterior.
        (await recovered.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30))).Should().BeNull();

        _time.Advance(TimeSpan.FromSeconds(31));

        (await recovered.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30)))!.Offset.Should().Be(0);
    }

    [Fact]
    public async Task Apply_reconstruye_un_mensaje_confirmado()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a", "b");
        var delivery = await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));
        await _engine.AckAsync("pedidos", "facturacion", delivery!.Partition, delivery.Offset);

        var recovered = new QueueEngine(_log, _catalog, _cluster, _replicator, _time);
        foreach (var record in await ReadAllAsync("pedidos", 0))
        {
            if (QueueEvent.IsQueueEvent(record.Type))
            {
                recovered.Apply("pedidos", record);
            }
        }

        (await recovered.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30)))!.Offset.Should().Be(1);
        (await recovered.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30))).Should().BeNull();
    }

    [Fact]
    public async Task Apply_ignora_los_registros_que_no_son_eventos_de_cola()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", 0, "a");

        var recovered = new QueueEngine(_log, _catalog, _cluster, _replicator, _time);
        foreach (var record in await ReadAllAsync("pedidos", 0))
        {
            recovered.Apply("pedidos", record);
        }

        (await recovered.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30)))!.Offset.Should().Be(0);
    }
}

/// <summary>Un líder que perdió el liderazgo mientras esperaba el quórum.</summary>
internal sealed class NotLeaderReplicator : IReplicator
{
    public ValueTask WaitForQuorumAsync(string topic, int partition, long offset, CancellationToken ct = default) =>
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
