using System.Net;
using System.Net.Http.Json;
using System.Text;
using K0sStreams.Contracts;
using K0sStreams.Contracts.Api;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;

namespace K0sStreams.Broker.Tests;

/// <summary>
/// Un seguidor no escribe en su log: lo que escribe o usa la cola lo redirige al líder. Si escribiera, su log dejaría
/// de coincidir con el del líder.
/// </summary>
public sealed class LeaderRoutingTests : IDisposable
{
    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), $"k0s-follower-{Guid.NewGuid():N}");
    private readonly List<WebApplicationFactory<Program>> _hosts = [];

    public void Dispose()
    {
        foreach (var host in _hosts)
        {
            host.Dispose();
        }

        if (Directory.Exists(_dataDir))
        {
            Directory.Delete(_dataDir, recursive: true);
        }
    }

    // Con un cuerpo válido: ASP.NET lee el cuerpo antes de correr el filtro que redirige.
    public static TheoryData<string, string, string?> RutasQueEscriben => new()
    {
        { "POST", "/v1/topics", """{"name":"pedidos"}""" },
        { "POST", "/v1/topics/pedidos/messages", """{"value":"a"}""" },
        { "POST", "/v1/topics/pedidos/queues/facturacion/receive", "{}" },
        { "POST", "/v1/topics/pedidos/queues/facturacion/ack/0/7", null },
        { "POST", "/v1/topics/pedidos/queues/facturacion/nack/0/7", "{}" },
        { "GET", "/v1/topics/pedidos/queues/facturacion/dlq", null },
    };

    [Theory]
    [MemberData(nameof(RutasQueEscriben))]
    public async Task En_un_seguidor_lo_que_escribe_se_redirige_al_lider_con_307(string method, string path, string? json)
    {
        HttpClient follower = Follower(leaderAddress: "http://broker-0.broker:9090");
        using var request = new HttpRequestMessage(new HttpMethod(method), path);
        if (json is not null)
        {
            request.Content = new StringContent(json, Encoding.UTF8, "application/json");
        }

        var response = await follower.SendAsync(request);

        response.StatusCode.Should().Be(HttpStatusCode.TemporaryRedirect);
        response.Headers.Location.Should().Be(new Uri("http://broker-0.broker:9090" + path));
    }

    [Fact]
    public async Task La_redireccion_conserva_la_query_string()
    {
        HttpClient follower = Follower(leaderAddress: "http://broker-0.broker:9090");

        var response = await follower.PostAsJsonAsync("/v1/topics/pedidos/messages?trace=1", new PublishRequest(null, "a"));

        response.Headers.Location.Should().Be(new Uri("http://broker-0.broker:9090/v1/topics/pedidos/messages?trace=1"));
    }

    [Fact]
    public async Task En_un_seguidor_sin_lider_conocido_responde_503()
    {
        HttpClient follower = Follower(leaderAddress: null);

        var response = await follower.PostAsJsonAsync("/v1/topics", new CreateTopicRequest("pedidos"));

        response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
    }

    [Fact]
    public async Task En_un_seguidor_leer_el_historial_no_redirige()
    {
        HttpClient follower = Follower(leaderAddress: "http://broker-0.broker:9090");

        var response = await follower.GetAsync(new Uri("/v1/topics/pedidos/partitions/0/messages", UriKind.Relative));

        response.StatusCode.Should().NotBe(HttpStatusCode.TemporaryRedirect);
    }

    [Fact]
    public async Task Si_deja_de_ser_lider_esperando_el_quorum_redirige_al_nuevo_lider()
    {
        var host = new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
        {
            builder.UseSetting("Broker:DataDir", _dataDir);
            builder.UseSetting("Coordination:LeaderAddress", "http://broker-2.broker:9090");
            builder.ConfigureTestServices(services => services.AddSingleton<IReplicator, NotLeaderReplicator>());
        });
        _hosts.Add(host);
        HttpClient leader = host.CreateClient(new WebApplicationFactoryClientOptions { AllowAutoRedirect = false });

        // Crear el tópico también espera el quórum (se guarda en el log), así que ya ahí se entera.
        var response = await leader.PostAsJsonAsync("/v1/topics", new CreateTopicRequest("pedidos"));

        response.StatusCode.Should().Be(HttpStatusCode.TemporaryRedirect);
        response.Headers.Location.Should().Be(new Uri("http://broker-2.broker:9090/v1/topics"));
    }

    private HttpClient Follower(string? leaderAddress)
    {
        var host = new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
        {
            builder.UseSetting("Broker:NodeId", "broker-1");
            builder.UseSetting("Broker:DataDir", _dataDir);
            builder.UseSetting("Coordination:LeaderId", "broker-0");
            if (leaderAddress is not null)
            {
                builder.UseSetting("Coordination:LeaderAddress", leaderAddress);
            }
        });
        _hosts.Add(host);
        return host.CreateClient(new WebApplicationFactoryClientOptions { AllowAutoRedirect = false });
    }
}

/// <summary>Un líder que perdió el liderazgo mientras esperaba el quórum.</summary>
internal sealed class NotLeaderReplicator : IReplicator
{
    public ValueTask WaitForQuorumAsync(string topic, int partition, long offset, CancellationToken ct = default) =>
        ValueTask.FromException(new NotLeaderException("Otro broker es el líder."));
}
