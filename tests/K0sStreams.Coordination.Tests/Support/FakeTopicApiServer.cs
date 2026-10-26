using System.Net;
using System.Text;
using System.Text.Json;

namespace K0sStreams.Coordination.Tests.Support;

/// <summary>
/// API server mínimo que solo sabe listar Topic, con el JSON que manda Kubernetes. Va como handler del cliente real,
/// igual que <see cref="FakeLeaseApiServer"/>.
/// </summary>
internal sealed class FakeTopicApiServer : DelegatingHandler
{
    private readonly List<object> _topics = [];

    /// <summary>Cada pedido recibido, como "MÉTODO /ruta".</summary>
    public List<string> Requests { get; } = [];

    /// <summary>Si no es null, el próximo pedido responde este código (p. ej. 404 si la CRD no está instalada).</summary>
    public HttpStatusCode? NextFailure { get; set; }

    /// <summary>Alguien hizo <c>kubectl apply</c> de un Topic. <paramref name="spec"/> null es un Topic sin spec.</summary>
    public void Applied(string name, object? spec = null) =>
        _topics.Add(spec is null
            ? new { apiVersion = "k0sstreams.io/v1", kind = "Topic", metadata = new { name } }
            : new { apiVersion = "k0sstreams.io/v1", kind = "Topic", metadata = new { name }, spec });

    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        Requests.Add($"{request.Method} {request.RequestUri?.AbsolutePath}");

        HttpResponseMessage response = NextFailure is { } code ? Status(code) : List();
        NextFailure = null;
        response.RequestMessage = request;
        return Task.FromResult(response);
    }

    private HttpResponseMessage List() => new(HttpStatusCode.OK)
    {
        Content = new StringContent(
            JsonSerializer.Serialize(new { apiVersion = "k0sstreams.io/v1", kind = "TopicList", items = _topics }),
            Encoding.UTF8,
            "application/json"),
    };

    private static HttpResponseMessage Status(HttpStatusCode code) => new(code)
    {
        Content = new StringContent(
            $$"""{"kind":"Status","apiVersion":"v1","status":"Failure","code":{{(int)code}}}""",
            Encoding.UTF8,
            "application/json"),
    };
}
