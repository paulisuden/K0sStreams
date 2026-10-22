using System.Globalization;
using System.Net;
using System.Text;
using k8s;
using k8s.Models;

namespace K0sStreams.Coordination.Tests.Support;

/// <summary>
/// API server mínimo en memoria para un solo Lease: GET, POST y PUT, con el mismo compare-and-swap por
/// <c>resourceVersion</c> que Kubernetes. Va como handler del cliente real, así que el test pasa por el mismo HTTP y el
/// mismo JSON que en el cluster.
/// </summary>
internal sealed class FakeLeaseApiServer : DelegatingHandler
{
    private int _lastVersion;

    /// <summary>El Lease guardado, o null si no existe.</summary>
    public V1Lease? Stored { get; private set; }

    /// <summary>Cada pedido recibido, como "MÉTODO /ruta".</summary>
    public List<string> Requests { get; } = [];

    /// <summary>Otro broker escribe el Lease directamente.</summary>
    public void WrittenBy(string? holder, int transitions) =>
        Save(new V1Lease
        {
            Metadata = new V1ObjectMeta { Name = "k0sstreams-leader" },
            Spec = new V1LeaseSpec { HolderIdentity = holder, LeaseTransitions = transitions },
        });

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        // Como HttpClient de verdad, cada respuesta dice a qué pedido responde: el cliente de Kubernetes lo usa.
        HttpResponseMessage response = await RespondAsync(request, cancellationToken);
        response.RequestMessage = request;
        return response;
    }

    private async Task<HttpResponseMessage> RespondAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        Requests.Add($"{request.Method} {request.RequestUri?.AbsolutePath}");

        if (request.Method == HttpMethod.Get)
        {
            return Stored is null ? Status(HttpStatusCode.NotFound) : Json(HttpStatusCode.OK, Stored);
        }

        string body = request.Content is null ? "" : await request.Content.ReadAsStringAsync(cancellationToken);
        V1Lease lease = KubernetesJson.Deserialize<V1Lease>(body);

        if (request.Method == HttpMethod.Post)
        {
            return Stored is not null ? Status(HttpStatusCode.Conflict) : Json(HttpStatusCode.Created, Save(lease));
        }

        if (Stored is null)
        {
            return Status(HttpStatusCode.NotFound);
        }

        // El compare-and-swap: el PUT tiene que traer la versión que está guardada.
        return lease.Metadata.ResourceVersion != Stored.Metadata.ResourceVersion
            ? Status(HttpStatusCode.Conflict)
            : Json(HttpStatusCode.OK, Save(lease));
    }

    private V1Lease Save(V1Lease lease)
    {
        lease.Metadata.ResourceVersion = (++_lastVersion).ToString(CultureInfo.InvariantCulture);
        Stored = lease;
        return lease;
    }

    private static HttpResponseMessage Json(HttpStatusCode code, V1Lease lease) => new(code)
    {
        Content = new StringContent(KubernetesJson.Serialize(lease), Encoding.UTF8, "application/json"),
    };

    private static HttpResponseMessage Status(HttpStatusCode code) => new(code)
    {
        Content = new StringContent(
            $$"""{"kind":"Status","apiVersion":"v1","status":"Failure","code":{{(int)code}}}""",
            Encoding.UTF8,
            "application/json"),
    };
}
