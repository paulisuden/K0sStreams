using System.Net;
using System.Net.Http.Json;
using K0sStreams.Contracts.Api;
using Microsoft.AspNetCore.Mvc.Testing;

namespace K0sStreams.Broker.Tests;

/// <summary>
/// Prueba las rutas REST de B sobre el host real. El log y el catálogo son singletons y el
/// host se comparte entre los tests de la clase, así que cada test usa un tópico propio para
/// no depender del orden en que corren.
/// </summary>
public sealed class ApiEndpointsTests : IClassFixture<WebApplicationFactory<Program>>
{
    private readonly HttpClient _client;

    public ApiEndpointsTests(WebApplicationFactory<Program> factory) => _client = factory.CreateClient();

    /// <summary>Nombre de tópico único por test, para que los singletons del host no se pisen.</summary>
    private static string NewTopic() => $"t{Guid.NewGuid():N}";

    private Task<HttpResponseMessage> CreateTopicAsync(string name) =>
        _client.PostAsJsonAsync("/v1/topics", new CreateTopicRequest(name, 3, TimeSpan.FromSeconds(5)));

    private Task<HttpResponseMessage> PublishAsync(string topic, string value, string? key = null) =>
        _client.PostAsJsonAsync($"/v1/topics/{topic}/messages", new PublishRequest(key, value));

    private Task<HttpResponseMessage> ReceiveAsync(string topic, string queue = "facturacion") =>
        _client.PostAsJsonAsync($"/v1/topics/{topic}/queues/{queue}/receive", new ReceiveRequest());

    private Task<HttpResponseMessage> AckAsync(string topic, long offset, string queue = "facturacion") =>
        _client.PostAsync($"/v1/topics/{topic}/queues/{queue}/ack/{offset}", null);

    [Fact]
    public async Task Crear_topico_devuelve_created_y_la_configuracion()
    {
        string topic = NewTopic();

        var response = await CreateTopicAsync(topic);

        response.StatusCode.Should().Be(HttpStatusCode.Created);
        TopicResponse? body = await response.Content.ReadFromJsonAsync<TopicResponse>();
        body!.Name.Should().Be(topic);
    }

    [Fact]
    public async Task Crear_topico_repetido_devuelve_conflict()
    {
        string topic = NewTopic();
        await CreateTopicAsync(topic);

        (await CreateTopicAsync(topic)).StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task Crear_topico_con_nombre_invalido_devuelve_validation_problem()
    {
        var response = await _client.PostAsJsonAsync("/v1/topics", new CreateTopicRequest("NOPE!"));

        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Publicar_devuelve_offset_creciente()
    {
        string topic = NewTopic();
        await CreateTopicAsync(topic);

        PublishResponse? first = await (await PublishAsync(topic, "a")).Content.ReadFromJsonAsync<PublishResponse>();
        PublishResponse? second = await (await PublishAsync(topic, "b")).Content.ReadFromJsonAsync<PublishResponse>();

        first!.Offset.Should().Be(0);
        second!.Offset.Should().Be(1);
    }

    [Fact]
    public async Task Publicar_en_topico_inexistente_devuelve_not_found()
    {
        (await PublishAsync(NewTopic(), "a")).StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Leer_devuelve_solo_los_mensajes_hasta_el_high_watermark()
    {
        string topic = NewTopic();
        await CreateTopicAsync(topic);
        await PublishAsync(topic, "a", key: "k1");
        await PublishAsync(topic, "b");

        ReadResponse? read = await _client.GetFromJsonAsync<ReadResponse>($"/v1/topics/{topic}/messages?from=0&max=10");

        read!.HighWatermark.Should().Be(1);
        read.NextOffset.Should().Be(2);
        read.Messages.Should().HaveCount(2);
        read.Messages[0].Key.Should().Be("k1");
        read.Messages[0].Value.Should().Be("a");
        read.Messages[1].Value.Should().Be("b");
    }

    [Fact]
    public async Task Leer_salta_los_eventos_de_cola_y_no_los_muestra()
    {
        string topic = NewTopic();
        await CreateTopicAsync(topic);
        await PublishAsync(topic, "a");
        await ReceiveAsync(topic);

        ReadResponse? read = await _client.GetFromJsonAsync<ReadResponse>($"/v1/topics/{topic}/messages?from=0&max=10");

        // El receive escribió un QueueEvent en el mismo log: el historial no debe mostrarlo.
        read!.Messages.Should().ContainSingle().Which.Value.Should().Be("a");
    }

    [Fact]
    public async Task Recibir_devuelve_el_mensaje_y_receive_sin_nada_devuelve_no_content()
    {
        string topic = NewTopic();
        await CreateTopicAsync(topic);
        await PublishAsync(topic, "a");

        var first = await ReceiveAsync(topic);
        DeliveryResponse? delivery = await first.Content.ReadFromJsonAsync<DeliveryResponse>();
        delivery!.Offset.Should().Be(0);
        delivery.Attempt.Should().Be(1);

        (await ReceiveAsync(topic)).StatusCode.Should().Be(HttpStatusCode.NoContent);
    }

    [Fact]
    public async Task Ack_confirma_el_mensaje_y_un_ack_repetido_devuelve_not_found()
    {
        string topic = NewTopic();
        await CreateTopicAsync(topic);
        await PublishAsync(topic, "a");
        await ReceiveAsync(topic);

        (await AckAsync(topic, 0)).StatusCode.Should().Be(HttpStatusCode.OK);
        (await AckAsync(topic, 0)).StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Ack_de_un_mensaje_ya_confirmado_no_lo_vuelve_a_entregar()
    {
        string topic = NewTopic();
        await CreateTopicAsync(topic);
        await PublishAsync(topic, "a");
        await ReceiveAsync(topic);
        await AckAsync(topic, 0);

        (await ReceiveAsync(topic)).StatusCode.Should().Be(HttpStatusCode.NoContent);
    }
}
