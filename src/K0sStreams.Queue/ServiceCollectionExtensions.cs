using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace K0sStreams.Queue;

public static class ServiceCollectionExtensions
{
    /// <summary>Registra el motor de cola (<c>IQueueEngine</c>). Dueño: B — Cola y API.</summary>
    public static IServiceCollection AddK0sQueue(this IServiceCollection services, IConfiguration configuration)
    {
        // Registrar acá la implementación de IQueueEngine.
        return services;
    }
}
