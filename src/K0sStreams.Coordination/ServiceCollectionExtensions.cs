using K0sStreams.Contracts;
using K0sStreams.Contracts.Fakes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace K0sStreams.Coordination;

public static class ServiceCollectionExtensions
{
    /// <summary>Registra el estado del cluster (Lease, épocas). Dueño: D — Plataforma y coordinación.</summary>
    public static IServiceCollection AddK0sCoordination(this IServiceCollection services, IConfiguration configuration)
    {
        // Fases 2 y 3: líder fijo por configuración, época 1. En la fase 4 se reemplaza por el Lease de Kubernetes.
        // Sin Coordination:LeaderId (un solo broker), este broker es el líder.
        string nodeId = configuration["Broker:NodeId"] ?? Environment.MachineName;
        string? leaderId = configuration["Coordination:LeaderId"];
        bool isLeader = leaderId is null || leaderId == nodeId;
        string? leaderAddress = configuration["Coordination:LeaderAddress"];
        services.AddSingleton<IClusterState>(new StaticClusterState(nodeId, isLeader, leaderAddress: leaderAddress));
        return services;
    }
}
