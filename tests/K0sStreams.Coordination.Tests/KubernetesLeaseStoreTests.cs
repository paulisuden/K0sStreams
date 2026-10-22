using K0sStreams.Coordination.Tests.Support;
using k8s;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;

namespace K0sStreams.Coordination.Tests;

public sealed class KubernetesLeaseStoreTests : IDisposable
{
    private readonly FakeTimeProvider _time = new();
    private readonly FakeLeaseApiServer _api = new();
    private readonly Kubernetes _client;
    private readonly KubernetesLeaseStore _store;

    public KubernetesLeaseStoreTests()
    {
        _client = new Kubernetes(new KubernetesClientConfiguration { Host = "http://api-server.test" }, _api);
        _store = new KubernetesLeaseStore(_client, "k0s", Options.Create(new CoordinationOptions()), _time);
    }

    public void Dispose()
    {
        _client.Dispose();
        _api.Dispose();
    }

    [Fact]
    public async Task Sin_Lease_leer_devuelve_null()
    {
        (await _store.GetAsync()).Should().BeNull();
    }

    [Fact]
    public async Task Habla_con_la_ruta_del_Lease_en_el_namespace_del_pod()
    {
        await _store.GetAsync();

        _api.Requests.Should().Equal("GET /apis/coordination.k8s.io/v1/namespaces/k0s/leases/k0sstreams-leader");
    }

    [Fact]
    public async Task Crear_guarda_titular_epoca_duracion_y_hora_de_renovacion()
    {
        LeaseRecord? created = await _store.TryCreateAsync("broker-0", transitions: 1);

        created.Should().Be(new LeaseRecord("broker-0", 1, "1"));
        _api.Stored?.Spec.HolderIdentity.Should().Be("broker-0");
        _api.Stored?.Spec.LeaseTransitions.Should().Be(1);
        _api.Stored?.Spec.LeaseDurationSeconds.Should().Be(10);
        _api.Stored?.Spec.RenewTime.Should().Be(_time.GetUtcNow().UtcDateTime);
    }

    [Fact]
    public async Task Crear_si_ya_existe_devuelve_null_y_no_lo_toca()
    {
        _api.WrittenBy("broker-0", transitions: 4);

        (await _store.TryCreateAsync("broker-1", transitions: 1)).Should().BeNull();

        _api.Stored?.Spec.HolderIdentity.Should().Be("broker-0");
    }

    [Fact]
    public async Task Leer_devuelve_titular_epoca_y_version()
    {
        _api.WrittenBy("broker-0", transitions: 4);

        (await _store.GetAsync()).Should().Be(new LeaseRecord("broker-0", 4, "1"));
    }

    [Fact]
    public async Task Reemplazar_con_la_version_leida_escribe_y_devuelve_la_version_nueva()
    {
        _api.WrittenBy("broker-0", transitions: 4);
        LeaseRecord read = await ReadAsync();

        LeaseRecord? replaced = await _store.TryReplaceAsync(read, "broker-1", transitions: 5);

        replaced.Should().Be(new LeaseRecord("broker-1", 5, "2"));
    }

    [Fact]
    public async Task Reemplazar_si_otro_escribio_en_el_medio_devuelve_null_y_no_lo_toca()
    {
        _api.WrittenBy(holder: null, transitions: 4);
        LeaseRecord read = await ReadAsync();
        _api.WrittenBy("broker-2", transitions: 5);

        (await _store.TryReplaceAsync(read, "broker-1", transitions: 5)).Should().BeNull();

        _api.Stored?.Spec.HolderIdentity.Should().Be("broker-2");
    }

    [Fact]
    public async Task Reemplazar_con_titular_null_libera_el_Lease()
    {
        LeaseRecord? created = await _store.TryCreateAsync("broker-0", transitions: 1);
        created.Should().NotBeNull();

        await _store.TryReplaceAsync(created ?? throw new InvalidOperationException(), holder: null, transitions: 1);

        _api.Stored?.Spec.HolderIdentity.Should().BeNull();
        _api.Stored?.Spec.LeaseTransitions.Should().Be(1);
    }

    private async Task<LeaseRecord> ReadAsync() =>
        await _store.GetAsync() ?? throw new InvalidOperationException("El Lease tendría que existir.");
}
