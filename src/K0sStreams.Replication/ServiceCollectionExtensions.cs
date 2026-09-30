using K0sStreams.Contracts;
using K0sStreams.Contracts.Fakes;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace K0sStreams.Replication;

public static class ServiceCollectionExtensions
{
    /// <summary>Registra el replicador y el servicio gRPC. Dueño: C — Replicación.</summary>
    public static IServiceCollection AddK0sReplication(this IServiceCollection services, IConfiguration configuration)
    {
        // Reemplazar por el replicador real y agregar services.AddGrpc().
        services.AddSingleton<IReplicator, InstantReplicator>();
        return services;
    }

    /// <summary>Publica el servicio gRPC de replicación (puerto 9091).</summary>
    public static IEndpointRouteBuilder MapK0sReplication(this IEndpointRouteBuilder endpoints)
    {
        // Mapear acá el servicio gRPC (MapGrpcService) cuando exista.
        return endpoints;
    }
}
