using K0sStreams.Contracts;
using K0sStreams.Contracts.Fakes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace K0sStreams.Storage;

public static class ServiceCollectionExtensions
{
    /// <summary>Carpeta de datos cuando no hay configuración: al lado del ejecutable.</summary>
    private const string DefaultDataDirName = "data";

    /// <summary>Registra el log y el catálogo de tópicos. Dueño: A — Almacenamiento.</summary>
    public static IServiceCollection AddK0sStorage(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        services.AddSingleton<ILog>(_ => new SegmentedLog(ResolveDataDir(configuration)));

        // Sigue en memoria: quién persiste los tópicos es un tema abierto del equipo
        // (docs/contexto.md, punto 1), porque crear un tópico también debería replicarse.
        services.AddSingleton<ITopicCatalog, InMemoryTopicCatalog>();
        return services;
    }

    /// <summary>
    /// En el contenedor, el Dockerfile define <c>Broker__DataDir=/data</c>, que es donde se monta el PVC.
    /// Sin configurar nada, los datos van al lado del ejecutable para que <c>dotnet run</c> funcione solo.
    /// </summary>
    private static string ResolveDataDir(IConfiguration configuration) =>
        configuration["Broker:DataDir"] is { Length: > 0 } configured
            ? configured
            : Path.Combine(AppContext.BaseDirectory, DefaultDataDirName);
}
