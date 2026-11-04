using K0sStreams.Contracts;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace K0sStreams.Broker;

/// <summary>
/// Comprobación de readiness: el broker puede atender tráfico.
/// Un seguidor sin líder conocido no puede recibir escrituras ni redirigirlas;
/// un broker a mitad de una elección no sabe su rol.
/// </summary>
internal sealed class ReadyHealthCheck(IClusterState cluster) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        // Un seguidor sin líder conocido no puede atender tráfico.
        if (!cluster.IsLeader && cluster.LeaderAddress is null)
        {
            return Task.FromResult(HealthCheckResult.Unhealthy("No hay un líder conocido; el broker no puede atender tráfico."));
        }

        return Task.FromResult(HealthCheckResult.Healthy("El broker está listo para atender tráfico."));
    }
}
