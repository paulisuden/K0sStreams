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
public sealed class QueueEngineTests
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
        _engine = new QueueEngine(_log, _catalog, _cluster, _time);
    }

    /// <summary>
    /// Publica un mensaje y lo confirma por quórum, igual que haría la API: sin avanzar el
    /// high watermark el mensaje no es visible ni para la cola (como en producción).
    /// </summary>
    private async ValueTask AppendMessagesAsync(string topic, params string[] values)
    {
        foreach (string value in values)
        {
            long offset = await _log.AppendAsync(topic, Message(value));
            await _replicator.WaitForQuorumAsync(topic, offset);
        }
    }

    private Record Message(string value) =>
        Record.NewMessage(1, _time.GetUtcNow().ToUnixTimeMilliseconds(), ReadOnlyMemory<byte>.Empty, Encoding.UTF8.GetBytes(value));

    private async Task<List<Record>> ReadAllAsync(string topic)
    {
        var records = new List<Record>();
        await foreach (var record in _log.ReadAsync(topic, 0, int.MaxValue))
        {
            records.Add(record);
        }

        return records;
    }

    private async ValueTask CreateTopicAsync(string topic = "pedidos")
    {
        (await _catalog.CreateAsync(new TopicConfig(topic, 3, TimeSpan.FromSeconds(5)))).Should().BeTrue();
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
        await AppendMessagesAsync("pedidos", "a", "b", "c");

        var delivery = await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));

        delivery.Should().NotBeNull();
        delivery!.Offset.Should().Be(0);
        Encoding.UTF8.GetString(delivery.Message.Value.Span).Should().Be("a");
    }

    [Fact]
    public async Task Receive_escribe_un_evento_de_cola_en_el_log()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", "a");
        var visibleUntil = Epoch.AddSeconds(30);

        var delivery = await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));

        var records = await ReadAllAsync("pedidos");
        records.Should().HaveCount(2);
        records[1].Type.Should().Be(RecordType.Receive);
        var queueEvent = QueueEvent.FromRecord(records[1]);
        queueEvent.Queue.Should().Be("facturacion");
        queueEvent.TargetOffset.Should().Be(delivery!.Offset);
        queueEvent.UntilMs.Should().Be(visibleUntil.ToUnixTimeMilliseconds());
    }

    [Fact]
    public async Task Ack_devuelve_true_y_el_mensaje_no_se_vuelve_a_entregar()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", "a", "b");
        var delivery = await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));

        (await _engine.AckAsync("pedidos", "facturacion", delivery!.Offset)).Should().BeTrue();

        (await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30)))!.Offset.Should().Be(1);
    }

    [Fact]
    public async Task Receives_concurrentes_no_entregan_el_mismo_mensaje()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", "a");

        // Un solo mensaje y dos receives a la vez: si la reserva no fuera atómica,
        // ambos pasarían la comprobación y el mismo offset se entregaría dos veces.
        var deliveries = await Task.WhenAll(
            _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30)).AsTask(),
            _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30)).AsTask());

        deliveries.Count(delivery => delivery is not null).Should().Be(1);
    }

    [Fact]
    public async Task Ack_devuelve_false_si_el_mensaje_no_esta_en_proceso()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", "a");

        (await _engine.AckAsync("pedidos", "facturacion", 0)).Should().BeFalse();
    }

    [Fact]
    public async Task Ack_de_otra_cola_no_confirma_el_mensaje()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", "a");
        var delivery = await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));

        (await _engine.AckAsync("pedidos", "envio", delivery!.Offset)).Should().BeFalse();
        (await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30))).Should().BeNull();
    }

    [Fact]
    public async Task Dos_colas_del_mismo_topico_reciben_cada_mensaje()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", "a");

        var facturacion = await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));
        var envio = await _engine.ReceiveAsync("pedidos", "envio", TimeSpan.FromSeconds(30));

        facturacion.Should().NotBeNull();
        envio.Should().NotBeNull();
        envio!.Offset.Should().Be(facturacion!.Offset);
    }

    [Fact]
    public async Task Dos_receive_entregan_mensajes_distintos()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", "a", "b");

        var first = await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));
        var second = await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));

        first!.Offset.Should().Be(0);
        second!.Offset.Should().Be(1);
    }

    [Fact]
    public async Task Timeout_devuelve_el_mensaje_a_la_cola_al_cumplirse_la_visibilidad()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", "a");
        await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));

        (await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30))).Should().BeNull();

        _time.Advance(TimeSpan.FromSeconds(31));

        (await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30)))!.Offset.Should().Be(0);
    }

    [Fact]
    public async Task El_segundo_receive_aumenta_el_intento()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", "a");
        var first = await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));
        first!.Attempt.Should().Be(1);

        _time.Advance(TimeSpan.FromSeconds(31));
        var second = await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));

        second!.Attempt.Should().Be(2);
    }

    [Fact]
    public async Task Apply_reconstruye_el_estado_a_partir_del_log()
    {
        await CreateTopicAsync();
        await AppendMessagesAsync("pedidos", "a", "b", "c");
        await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));
        (await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30)))!.Offset.Should().Be(1);

        var recovered = new QueueEngine(_log, _catalog, _cluster, _time);
        foreach (var record in await ReadAllAsync("pedidos"))
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
        await AppendMessagesAsync("pedidos", "a");

        await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));

        var recovered = new QueueEngine(_log, _catalog, _cluster, _time);
        foreach (var record in await ReadAllAsync("pedidos"))
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
        await AppendMessagesAsync("pedidos", "a", "b");
        var delivery = await _engine.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30));
        await _engine.AckAsync("pedidos", "facturacion", delivery!.Offset);

        var recovered = new QueueEngine(_log, _catalog, _cluster, _time);
        foreach (var record in await ReadAllAsync("pedidos"))
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
        await AppendMessagesAsync("pedidos", "a");

        var recovered = new QueueEngine(_log, _catalog, _cluster, _time);
        foreach (var record in await ReadAllAsync("pedidos"))
        {
            recovered.Apply("pedidos", record);
        }

        (await recovered.ReceiveAsync("pedidos", "facturacion", TimeSpan.FromSeconds(30)))!.Offset.Should().Be(0);
    }
}
