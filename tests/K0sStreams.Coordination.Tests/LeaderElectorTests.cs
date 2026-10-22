using K0sStreams.Contracts;
using K0sStreams.Coordination.Tests.Support;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;

namespace K0sStreams.Coordination.Tests;

public sealed class LeaderElectorTests : IDisposable
{
    private readonly FakeTimeProvider _time = new();
    private readonly FakeLeaseStore _store = new();
    private readonly FakePromotion _promotion = new();
    private readonly List<LeaderElector> _electors = [];

    public void Dispose()
    {
        foreach (LeaderElector elector in _electors)
        {
            elector.Dispose();
        }
    }

    [Fact]
    public async Task Sin_Lease_lo_crea_y_lidera_la_epoca_1()
    {
        var (elector, state) = Broker("broker-0");

        await elector.TickAsync();

        _store.Current.Should().BeEquivalentTo(new { HolderIdentity = "broker-0", Transitions = 1 });
        _promotion.Epochs.Should().Equal(1);
        state.IsLeader.Should().BeTrue();
        state.CurrentEpoch.Should().Be(1);
    }

    [Fact]
    public async Task Con_el_Lease_de_otro_broker_es_seguidor_de_esa_epoca()
    {
        _store.WrittenBy("broker-0", transitions: 4);
        var (elector, state) = Broker("broker-1");

        await elector.TickAsync();

        state.IsLeader.Should().BeFalse();
        state.CurrentEpoch.Should().Be(4);
        state.LeaderAddress.Should().Be("http://broker-0.broker:9090");
        _store.Current.Should().BeEquivalentTo(new { HolderIdentity = "broker-0", Transitions = 4 });
        _promotion.Epochs.Should().BeEmpty();
    }

    [Fact]
    public async Task Sin_renovar_por_menos_de_LeaseDuration_no_lo_toma()
    {
        _store.WrittenBy("broker-0", transitions: 4);
        var (elector, _) = Broker("broker-1");
        await elector.TickAsync();

        _time.Advance(TimeSpan.FromSeconds(9));
        await elector.TickAsync();

        _store.Current.Should().BeEquivalentTo(new { HolderIdentity = "broker-0", Transitions = 4 });
    }

    [Fact]
    public async Task Sin_renovar_durante_LeaseDuration_lo_toma_con_la_epoca_siguiente()
    {
        _store.WrittenBy("broker-0", transitions: 4);
        var (elector, state) = Broker("broker-1");
        await elector.TickAsync();

        _time.Advance(TimeSpan.FromSeconds(10));
        await elector.TickAsync();

        _store.Current.Should().BeEquivalentTo(new { HolderIdentity = "broker-1", Transitions = 5 });
        _promotion.Epochs.Should().Equal(5);
        state.IsLeader.Should().BeTrue();
        state.CurrentEpoch.Should().Be(5);
    }

    [Fact]
    public async Task Cada_renovacion_del_lider_reinicia_el_plazo()
    {
        _store.WrittenBy("broker-0", transitions: 4);
        var (elector, _) = Broker("broker-1");
        await elector.TickAsync();

        // Renovar es escribir la misma época: la versión cambia y el seguidor vuelve a contar desde cero.
        _time.Advance(TimeSpan.FromSeconds(6));
        _store.WrittenBy("broker-0", transitions: 4);
        await elector.TickAsync();
        _time.Advance(TimeSpan.FromSeconds(6));
        await elector.TickAsync();

        _store.Current.Should().BeEquivalentTo(new { HolderIdentity = "broker-0", Transitions = 4 });
    }

    [Fact]
    public async Task Un_Lease_liberado_se_toma_sin_esperar()
    {
        _store.WrittenBy(holder: null, transitions: 4);
        var (elector, state) = Broker("broker-1");

        await elector.TickAsync();

        _store.Current.Should().BeEquivalentTo(new { HolderIdentity = "broker-1", Transitions = 5 });
        state.IsLeader.Should().BeTrue();
    }

    [Fact]
    public async Task Al_reiniciarse_retoma_su_propio_Lease_con_una_epoca_nueva()
    {
        // broker-1 lideraba la época 4 y se reinició: el Lease sigue a su nombre.
        _store.WrittenBy("broker-1", transitions: 4);
        var (elector, state) = Broker("broker-1");

        await elector.TickAsync();

        _store.Current.Should().BeEquivalentTo(new { HolderIdentity = "broker-1", Transitions = 5 });
        _promotion.Epochs.Should().Equal(5);
        state.IsLeader.Should().BeTrue();
    }

    [Fact]
    public async Task Si_otro_broker_lo_toma_primero_no_es_lider()
    {
        _store.WrittenBy(holder: null, transitions: 4);
        var (elector, state) = Broker("broker-1");

        // broker-2 escribe entre la lectura y la escritura de broker-1: el compare-and-swap rechaza a broker-1.
        _store.BeforeWrite = () => _store.WrittenBy("broker-2", transitions: 5);
        await elector.TickAsync();

        _store.Current.Should().BeEquivalentTo(new { HolderIdentity = "broker-2", Transitions = 5 });
        _promotion.Epochs.Should().BeEmpty();
        state.IsLeader.Should().BeFalse();
    }

    [Fact]
    public async Task Mientras_se_pone_al_dia_todavia_no_es_lider()
    {
        _promotion.Hold = true;
        var (elector, state) = Broker("broker-0");

        await elector.TickAsync();

        state.IsLeader.Should().BeFalse();
        state.CurrentEpoch.Should().Be(0);

        // El resto de la promoción puede seguir en otro hilo: el test espera a que termine antes de mirar.
        _promotion.Complete();
        await elector.PendingPromotion;

        state.IsLeader.Should().BeTrue();
        state.CurrentEpoch.Should().Be(1);
    }

    [Fact]
    public async Task Si_no_puede_ponerse_al_dia_libera_el_Lease()
    {
        _promotion.FailWith = new NotLeaderException("Otro broker ya está en una época mayor.");
        var (elector, state) = Broker("broker-0");

        await elector.TickAsync();

        _store.Current.Should().BeEquivalentTo(new { HolderIdentity = (string?)null, Transitions = 1 });
        state.IsLeader.Should().BeFalse();
    }

    [Fact]
    public async Task Sin_API_server_no_cambia_nada_y_el_ciclo_no_falla()
    {
        _store.FailWith = new HttpRequestException("API server caído");
        var (elector, state) = Broker("broker-1");

        Func<Task> tick = () => elector.TickAsync();

        await tick.Should().NotThrowAsync();
        state.IsLeader.Should().BeFalse();
        state.CurrentEpoch.Should().Be(0);
    }

    [Fact]
    public async Task Como_seguidor_se_entera_del_cambio_de_lider()
    {
        _store.WrittenBy("broker-0", transitions: 4);
        var (elector, state) = Broker("broker-1");
        var notified = new List<long>();
        state.EpochChanged += notified.Add;
        await elector.TickAsync();

        _store.WrittenBy("broker-2", transitions: 5);
        await elector.TickAsync();

        notified.Should().Equal(4, 5);
        state.IsLeader.Should().BeFalse();
        state.LeaderAddress.Should().Be("http://broker-2.broker:9090");
    }

    [Fact]
    public async Task El_lider_renueva_el_Lease_en_cada_ciclo()
    {
        var (elector, state) = Broker("broker-0");
        await elector.TickAsync();
        string versionBefore = VersionOf(_store.Current);

        _time.Advance(TimeSpan.FromSeconds(2));
        await elector.TickAsync();

        _store.Current.Should().BeEquivalentTo(new { HolderIdentity = "broker-0", Transitions = 1 });
        VersionOf(_store.Current).Should().NotBe(versionBefore);
        state.IsLeader.Should().BeTrue();
    }

    [Fact]
    public async Task Sin_poder_renovar_por_menos_de_RenewDeadline_sigue_siendo_lider()
    {
        var (elector, state) = Broker("broker-0");
        await elector.TickAsync();

        _store.FailWith = new HttpRequestException("API server caído");
        _time.Advance(TimeSpan.FromSeconds(6));
        await elector.TickAsync();

        state.IsLeader.Should().BeTrue();
    }

    [Fact]
    public async Task Sin_poder_renovar_durante_RenewDeadline_deja_de_ser_lider()
    {
        var (elector, state) = Broker("broker-0");
        var notified = new List<long>();
        state.EpochChanged += notified.Add;
        await elector.TickAsync();

        _store.FailWith = new HttpRequestException("API server caído");
        _time.Advance(TimeSpan.FromSeconds(7));
        await elector.TickAsync();

        state.IsLeader.Should().BeFalse();
        notified.Should().Equal(1, 1);
    }

    [Fact]
    public async Task Si_otro_broker_le_saco_el_Lease_deja_de_ser_lider_enseguida()
    {
        var (elector, state) = Broker("broker-0");
        await elector.TickAsync();

        _store.WrittenBy("broker-1", transitions: 2);
        await elector.TickAsync();

        state.IsLeader.Should().BeFalse();
    }

    [Fact]
    public async Task Mientras_se_pone_al_dia_sigue_renovando_el_Lease()
    {
        _promotion.Hold = true;
        var (elector, _) = Broker("broker-0");
        await elector.TickAsync();
        string versionBefore = VersionOf(_store.Current);

        _time.Advance(TimeSpan.FromSeconds(2));
        await elector.TickAsync();

        _store.Current.Should().BeEquivalentTo(new { HolderIdentity = "broker-0", Transitions = 1 });
        VersionOf(_store.Current).Should().NotBe(versionBefore);
    }

    [Fact]
    public async Task Si_pierde_el_Lease_mientras_se_pone_al_dia_cancela_la_promocion_y_no_es_lider()
    {
        _promotion.Hold = true;
        var (elector, state) = Broker("broker-0");
        await elector.TickAsync();

        _store.WrittenBy("broker-1", transitions: 2);
        await elector.TickAsync();
        await elector.PendingPromotion;

        _promotion.Token.IsCancellationRequested.Should().BeTrue();
        state.IsLeader.Should().BeFalse();
    }

    [Fact]
    public async Task Al_apagarse_el_lider_libera_el_Lease()
    {
        var (elector, state) = Broker("broker-0");
        await elector.TickAsync();

        await elector.StartAsync(CancellationToken.None);
        await elector.StopAsync(CancellationToken.None);

        _store.Current.Should().BeEquivalentTo(new { HolderIdentity = (string?)null, Transitions = 1 });
        state.IsLeader.Should().BeFalse();
    }

    private static string VersionOf(LeaseRecord? lease) => lease?.Version ?? "";

    private (LeaderElector Elector, LeaseClusterState State) Broker(string nodeId)
    {
        var state = new LeaseClusterState(nodeId, "http://{0}.broker:9090");
        var elector = new LeaderElector(
            _store,
            state,
            _promotion,
            Options.Create(new CoordinationOptions()),
            _time,
            NullLogger<LeaderElector>.Instance);
        _electors.Add(elector);
        return (elector, state);
    }
}
