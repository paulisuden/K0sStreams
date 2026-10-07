using K0sStreams.Contracts;
using K0sStreams.Replication.Tests.Support;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace K0sStreams.Replication.Tests;

/// <summary>What <c>AddK0sReplication</c> reads from configuration.</summary>
public class RegistrationTests
{
    [Fact]
    public void Peers_come_from_configuration_and_exclude_this_broker()
    {
        using var services = BuildServices(new()
        {
            ["Replication:Peers:broker-0"] = "http://broker-0.broker:9091",
            ["Replication:Peers:broker-1"] = "http://broker-1.broker:9091",
            ["Replication:Peers:broker-2"] = "http://broker-2.broker:9091",
            ["Replication:RpcTimeout"] = "00:00:01",
        });

        services.GetRequiredService<IOptions<ReplicationOptions>>().Value.RpcTimeout.Should().Be(TimeSpan.FromSeconds(1));
        services.GetRequiredService<PeerDirectory>().Peers.Keys.Should().BeEquivalentTo("broker-0", "broker-2");
    }

    [Theory]
    [InlineData("Replication:MaxBatchRecords", "0")]
    [InlineData("Replication:MaxBatchBytes", "999999999")]
    [InlineData("Replication:RpcTimeout", "00:00:00")]
    [InlineData("Replication:Peers:broker-0", "broker-0.broker")]
    public void Out_of_range_settings_are_rejected(string key, string value)
    {
        using var services = BuildServices(new() { [key] = value });

        var act = () => services.GetRequiredService<IOptions<ReplicationOptions>>().Value;

        act.Should().Throw<OptionsValidationException>();
    }

    private static ServiceProvider BuildServices(Dictionary<string, string?> settings)
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(settings).Build();
        return new ServiceCollection()
            .AddLogging()
            .AddSingleton<IClusterState>(new ControllableClusterState("broker-1"))
            .AddK0sReplication(configuration)
            .BuildServiceProvider();
    }
}
