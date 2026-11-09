using Microsoft.AspNetCore.Mvc.Testing;

namespace K0sStreams.Broker.Tests;

/// <summary>
/// Prueba que <c>/metrics</c> responde en formato Prometheus con el estado del cluster y con las
/// métricas HTTP que ASP.NET Core ya trae.
/// </summary>
public sealed class MetricsEndpointTests(WebApplicationFactory<Program> factory)
    : IClassFixture<WebApplicationFactory<Program>>
{
    private readonly HttpClient _client = factory.CreateClient();

    [Theory]
    [InlineData("k0sstreams_broker_is_leader")]
    [InlineData("k0sstreams_leader_epoch")]
    public async Task Metrics_expone_el_estado_del_cluster(string metric)
    {
        string body = await ScrapeAsync();

        body.Should().Contain(metric);
    }

    [Fact]
    public async Task Metrics_etiqueta_cada_serie_con_el_nodo()
    {
        string body = await ScrapeAsync();

        body.Should().Contain("node=");
    }

    [Fact]
    public async Task Metrics_expone_la_duracion_de_los_pedidos_http()
    {
        // Un pedido cualquiera antes del scrape: sin tráfico la métrica no tiene nada que informar.
        await _client.GetAsync(new Uri("/health", UriKind.Relative));

        string body = await ScrapeAsync();

        body.Should().Contain("http_server_request_duration_seconds");
    }

    private async Task<string> ScrapeAsync()
    {
        var response = await _client.GetAsync(new Uri("/metrics", UriKind.Relative));

        response.EnsureSuccessStatusCode();
        return await response.Content.ReadAsStringAsync();
    }
}
