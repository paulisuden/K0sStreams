using K0sStreams.Contracts;
using K0sStreams.Contracts.Fakes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace K0sStreams.Storage;

public static class ServiceCollectionExtensions
{
    /// <summary>Registra el log y el catálogo de tópicos. Dueño: A — Almacenamiento.</summary>
    public static IServiceCollection AddK0sStorage(this IServiceCollection services, IConfiguration configuration)
    {
        // Reemplazar por las implementaciones reales (la carpeta de datos está en Broker:DataDir).
        services.AddSingleton<ILog, InMemoryLog>(); // cuando alguien pida un ILog, dale un InMemoryLog, y que sea siempre el mismo objeto (Singleton)
        services.AddSingleton<ITopicCatalog, InMemoryTopicCatalog>();
        return services;
    }
}
