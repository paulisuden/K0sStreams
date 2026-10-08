using K0sStreams.Contracts;
using K0sStreams.Contracts.Fakes;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

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
                "Replication: RpcTimeout, MaxBatchRecords and MaxBatchBytes must be positive, MaxBatchBytes at most 16 MiB, and every peer an absolute URL.")
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

        // Phase 3 replaces this with the quorum replicator. Until then writes are confirmed by the leader alone.
        services.AddSingleton<IReplicator, InstantReplicator>();
        return services;
    }

    /// <summary>Publica el servicio gRPC de replicación (puerto 9091).</summary>
    public static IEndpointRouteBuilder MapK0sReplication(this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapGrpcService<ReplicationGrpcService>();
        return endpoints;
    }
}
