using K0sStreams.Contracts;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace K0sStreams.Replication;

public static class ServiceCollectionExtensions
{
    /// <summary>Registra el replicador y el servicio gRPC. Dueño: C — Replicación.</summary>
    public static IServiceCollection AddK0sReplication(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddOptions<ReplicationOptions>()
            .Bind(configuration.GetSection(ReplicationOptions.SectionName))
            .Validate(
                static options => options.IsValid(),
                "Replication: RpcTimeout, HeartbeatInterval, RetryBackoff, MaxBatchRecords and MaxBatchBytes must be positive, "
                + "MaxRetryBackoff at least RetryBackoff, MaxBatchBytes at most 16 MiB, and every peer an absolute URL.")
            .ValidateOnStart();

        services.TryAddSingleton(TimeProvider.System);
        services.AddSingleton<EpochTracker>();
        services.AddSingleton<AppendHandler>();
        services.AddSingleton<ReplicationNode>();
        services.AddSingleton<PeerDirectory>();
        services.AddGrpc(options =>
        {
            options.MaxReceiveMessageSize = ReplicationOptions.MaxMessageSize;
            options.MaxSendMessageSize = ReplicationOptions.MaxMessageSize;
        });

        // Created after PeerDirectory, so the container stops its loops before closing the channels they use.
        services.AddSingleton<IReplicator>(sp => new QuorumReplicator(
            sp.GetRequiredService<ILog>(),
            sp.GetRequiredService<IClusterState>(),
            sp.GetRequiredService<EpochTracker>(),
            sp.GetRequiredService<PeerDirectory>().Peers,
            sp.GetRequiredService<IOptions<ReplicationOptions>>(),
            sp.GetRequiredService<TimeProvider>(),
            sp.GetRequiredService<ILogger<QuorumReplicator>>()));
        return services;
    }

    /// <summary>Publica el servicio gRPC de replicación (puerto 9091).</summary>
    public static IEndpointRouteBuilder MapK0sReplication(this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapGrpcService<ReplicationGrpcService>();
        return endpoints;
    }
}
