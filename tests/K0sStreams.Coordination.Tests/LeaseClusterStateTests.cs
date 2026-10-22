namespace K0sStreams.Coordination.Tests;

public class LeaseClusterStateTests
{
    private const string AddressFormat = "http://{0}.broker:9090";

    [Fact]
    public void Al_arrancar_no_es_lider_ni_conoce_la_epoca_ni_al_lider()
    {
        var state = new LeaseClusterState("broker-1", AddressFormat);

        state.IsLeader.Should().BeFalse();
        state.CurrentEpoch.Should().Be(0);
        state.LeaderAddress.Should().BeNull();
    }

    [Fact]
    public void Como_seguidor_toma_la_epoca_y_la_direccion_del_lider_y_avisa()
    {
        var state = new LeaseClusterState("broker-1", AddressFormat);
        var notified = new List<long>();
        state.EpochChanged += notified.Add;

        state.Follow(4, "broker-0");

        state.IsLeader.Should().BeFalse();
        state.CurrentEpoch.Should().Be(4);
        state.LeaderAddress.Should().Be("http://broker-0.broker:9090");
        notified.Should().Equal(4);
    }

    [Fact]
    public void Como_seguidor_no_avisa_si_la_epoca_no_cambio()
    {
        var state = new LeaseClusterState("broker-1", AddressFormat);
        var notified = new List<long>();
        state.EpochChanged += notified.Add;

        state.Follow(4, "broker-0");
        state.Follow(4, "broker-0");

        notified.Should().Equal(4);
    }

    [Fact]
    public void Sin_formato_de_direccion_no_conoce_la_direccion_del_lider()
    {
        var state = new LeaseClusterState("broker-1", leaderAddressFormat: null);

        state.Follow(4, "broker-0");

        state.LeaderAddress.Should().BeNull();
    }

    [Fact]
    public void Al_terminar_la_promocion_es_lider_de_esa_epoca_y_avisa()
    {
        var state = new LeaseClusterState("broker-1", AddressFormat);
        var notified = new List<long>();
        state.EpochChanged += notified.Add;

        state.BecomeLeader(5);

        state.IsLeader.Should().BeTrue();
        state.CurrentEpoch.Should().Be(5);
        state.LeaderAddress.Should().Be("http://broker-1.broker:9090");
        notified.Should().Equal(5);
    }

    [Fact]
    public void Al_perder_el_Lease_deja_de_ser_lider_y_avisa_con_la_misma_epoca()
    {
        var state = new LeaseClusterState("broker-1", AddressFormat);
        state.BecomeLeader(5);
        var notified = new List<long>();
        state.EpochChanged += notified.Add;

        state.StepDown();

        state.IsLeader.Should().BeFalse();
        state.CurrentEpoch.Should().Be(5);
        state.LeaderAddress.Should().BeNull();
        notified.Should().Equal(5);
    }

    [Fact]
    public void StepDown_siendo_seguidor_no_avisa()
    {
        var state = new LeaseClusterState("broker-1", AddressFormat);
        state.Follow(4, "broker-0");
        var notified = new List<long>();
        state.EpochChanged += notified.Add;

        state.StepDown();

        notified.Should().BeEmpty();
    }

    // La replicación, al recibir el aviso, pregunta IsLeader y CurrentEpoch: tienen que tener ya los valores nuevos.
    [Fact]
    public void Quien_recibe_el_aviso_ya_ve_el_estado_nuevo()
    {
        var state = new LeaseClusterState("broker-1", AddressFormat);
        (bool IsLeader, long Epoch)? seen = null;
        state.EpochChanged += _ => seen = (state.IsLeader, state.CurrentEpoch);

        state.BecomeLeader(5);

        seen.Should().Be((true, 5L));
    }
}
