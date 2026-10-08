using K0sStreams.Contracts;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace K0sStreams.Coordination.Tests;

public class LiderFijoTests
{
    [Fact]
    public void Sin_LeaderId_el_broker_es_lider() =>
        ClusterState(new() { ["Broker:NodeId"] = "broker-1" }).IsLeader.Should().BeTrue();

    [Fact]
    public void Con_LeaderId_igual_al_NodeId_el_broker_es_lider() =>
        ClusterState(new() { ["Broker:NodeId"] = "broker-0", ["Coordination:LeaderId"] = "broker-0" })
            .IsLeader.Should().BeTrue();

    [Fact]
    public void Con_otro_LeaderId_el_broker_es_seguidor_y_conoce_la_direccion_del_lider()
    {
        var state = ClusterState(new()
        {
            ["Broker:NodeId"] = "broker-1",
            ["Coordination:LeaderId"] = "broker-0",
            ["Coordination:LeaderAddress"] = "http://broker-0.broker:9090",
        });

        state.NodeId.Should().Be("broker-1");
        state.IsLeader.Should().BeFalse();
        state.CurrentEpoch.Should().Be(1);
        state.LeaderAddress.Should().Be("http://broker-0.broker:9090");
    }

    private static IClusterState ClusterState(Dictionary<string, string?> settings)
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(settings).Build();
        return new ServiceCollection()
            .AddK0sCoordination(configuration)
            .BuildServiceProvider()
            .GetRequiredService<IClusterState>();
    }
}
