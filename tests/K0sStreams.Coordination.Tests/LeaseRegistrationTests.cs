using K0sStreams.Contracts;
using K0sStreams.Coordination.Tests.Support;
using k8s;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace K0sStreams.Coordination.Tests;

public class LeaseRegistrationTests
{
    [Fact]
    public void Con_Lease_el_estado_es_uno_solo_y_arranca_como_seguidor()
    {
        using var provider = Provider(new() { ["Coordination:LeaderAddressFormat"] = "http://{0}.broker:9090" });

        var state = provider.GetRequiredService<IClusterState>();

        state.Should().BeSameAs(provider.GetRequiredService<LeaseClusterState>());
        state.NodeId.Should().Be("broker-1");
        state.IsLeader.Should().BeFalse();
    }

    [Fact]
    public void Con_Lease_el_elector_corre_como_servicio_de_fondo()
    {
        using var provider = Provider([]);

        provider.GetServices<IHostedService>().Should().ContainSingle().Which.Should().BeOfType<LeaderElector>();
    }

    [Fact]
    public void Con_Lease_usa_el_Lease_de_Kubernetes_en_el_namespace_del_pod()
    {
        using var provider = Provider([]);

        provider.GetRequiredService<ILeaseStore>().Should().BeOfType<KubernetesLeaseStore>();
    }

    [Fact]
    public void Con_tiempos_que_no_cumplen_RetryPeriod_menor_que_RenewDeadline_menor_que_LeaseDuration_no_arranca()
    {
        using var provider = Provider(new() { ["Coordination:RenewDeadline"] = "00:00:12" });

        var read = () => provider.GetRequiredService<IOptions<CoordinationOptions>>().Value;

        read.Should().Throw<OptionsValidationException>();
    }

    private static ServiceProvider Provider(Dictionary<string, string?> settings)
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(settings).Build();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<ILeaderPromotion, FakePromotion>();

        // Lo que dentro del pod sale de la ServiceAccount: acá, un API server que nadie llama.
        services.AddSingleton(new KubernetesClientConfiguration { Host = "http://api-server.test", Namespace = "k0s" });

        return services.AddLeaseElection(configuration, "broker-1").BuildServiceProvider();
    }
}
