using System.Net;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;

namespace K0sStreams.Broker.Tests;

/// <summary>
/// Cada sonda corre solo su comprobación. Lo que se prueba acá es un broker que todavía no sabe
/// quién lidera: tiene que seguir vivo para llegar a liderar, pero no recibir tráfico.
/// </summary>
public sealed class HealthProbesTests : IDisposable
{
    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), $"k0s-probes-{Guid.NewGuid():N}");
    private readonly WebApplicationFactory<Program> _host;
    private readonly HttpClient _client;

    public HealthProbesTests()
    {
        // Seguidor sin dirección del líder: es el estado en el que ReadyHealthCheck falla.
        _host = new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
        {
            builder.UseSetting("Broker:NodeId", "broker-1");
            builder.UseSetting("Broker:DataDir", _dataDir);
            builder.UseSetting("Coordination:LeaderId", "broker-0");
        });
        _client = _host.CreateClient();
    }

    public void Dispose()
    {
        _host.Dispose();
        if (Directory.Exists(_dataDir))
        {
            Directory.Delete(_dataDir, recursive: true);
        }
    }

    [Fact]
    public async Task Health_pasa_aunque_no_se_sepa_quien_lidera()
    {
        // Si /health fallara acá, Kubernetes reiniciaría el broker en cada elección y nunca
        // llegaría a liderar: con un solo broker vivo, eso es un bucle de reinicios.
        var response = await _client.GetAsync(new Uri("/health", UriKind.Relative));

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task Ready_falla_si_no_se_sabe_quien_lidera()
    {
        var response = await _client.GetAsync(new Uri("/ready", UriKind.Relative));

        response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
    }
}
