using System.Net;
using K0sStreams.Contracts;
using K0sStreams.Contracts.Fakes;
using K0sStreams.Coordination.Tests.Support;
using k8s;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;

namespace K0sStreams.Coordination.Tests;

public sealed class TopicReconcilerTests : IDisposable
{
    private readonly FakeTopicApiServer _api = new();
    private readonly InMemoryTopicCatalog _catalog = new();
    private readonly Kubernetes _client;

    public TopicReconcilerTests()
    {
        _client = new Kubernetes(new KubernetesClientConfiguration { Host = "http://api-server.test" }, _api);
    }

    public void Dispose()
    {
        _client.Dispose();
        _api.Dispose();
    }

    [Fact]
    public async Task El_lider_crea_los_Topic_declarados_con_su_configuracion()
    {
        _api.Applied("pedidos", new { partitions = 1, maxRetries = 5, retryBackoffSeconds = 10 });

        await Reconciler(isLeader: true).ReconcileAsync();

        _catalog.Find("pedidos").Should().Be(new TopicConfig("pedidos", 1, 5, TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task Lista_los_Topic_del_namespace_del_pod()
    {
        await Reconciler(isLeader: true).ReconcileAsync();

        _api.Requests.Should().Equal("GET /apis/k0sstreams.io/v1/namespaces/k0s/topics");
    }

    [Fact]
    public async Task Un_seguidor_no_lista_ni_crea_nada()
    {
        _api.Applied("pedidos");

        await Reconciler(isLeader: false).ReconcileAsync();

        _api.Requests.Should().BeEmpty();
        _catalog.List().Should().BeEmpty();
    }

    [Fact]
    public async Task Un_Topic_sin_spec_usa_los_mismos_valores_por_defecto_que_la_API()
    {
        _api.Applied("pedidos");

        await Reconciler(isLeader: true).ReconcileAsync();

        _catalog.Find("pedidos").Should().Be(new TopicConfig("pedidos", 1, 3, TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task Un_topico_que_ya_existe_queda_con_su_configuracion()
    {
        var created = new TopicConfig("pedidos", 1, 0, TimeSpan.Zero);
        await _catalog.CreateAsync(created);
        _api.Applied("pedidos", new { partitions = 1, maxRetries = 5, retryBackoffSeconds = 10 });

        await Reconciler(isLeader: true).ReconcileAsync();

        _catalog.Find("pedidos").Should().Be(created);
    }

    [Fact]
    public async Task Un_Topic_invalido_se_saltea_y_los_demas_se_crean()
    {
        _api.Applied(new string('a', 101));
        _api.Applied("pedidos");

        await Reconciler(isLeader: true).ReconcileAsync();

        _catalog.List().Select(topic => topic.Name).Should().Equal("pedidos");
    }

    [Fact]
    public async Task Si_el_API_server_falla_no_se_cae_y_crea_en_la_pasada_siguiente()
    {
        _api.Applied("pedidos");
        _api.NextFailure = HttpStatusCode.NotFound;
        TopicReconciler reconciler = Reconciler(isLeader: true);

        await reconciler.ReconcileAsync();
        _catalog.List().Should().BeEmpty();

        await reconciler.ReconcileAsync();
        _catalog.Find("pedidos").Should().NotBeNull();
    }

    private TopicReconciler Reconciler(bool isLeader) => new(
        _client,
        "k0s",
        _catalog,
        new StaticClusterState("broker-0", isLeader),
        new FakeTimeProvider(),
        NullLogger<TopicReconciler>.Instance);
}
