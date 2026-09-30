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
        // Reemplazar por el estado basado en el Lease de Kubernetes. Mientras tanto: "soy líder, época 1".
        string nodeId = configuration["Broker:NodeId"] ?? Environment.MachineName;
        services.AddSingleton<IClusterState>(new StaticClusterState(nodeId));
        return services;
    }
}
