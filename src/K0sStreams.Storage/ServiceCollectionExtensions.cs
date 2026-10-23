using K0sStreams.Contracts;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace K0sStreams.Storage;

public static class ServiceCollectionExtensions
{
    /// <summary>Carpeta de datos cuando no hay configuración: al lado del ejecutable.</summary>
    private const string DefaultDataDirName = "data";

    /// <summary>Registra el log y el catálogo de tópicos. Dueño: A — Almacenamiento.</summary>
    public static IServiceCollection AddK0sStorage(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        // GetService y no GetRequiredService: si el host no configuró logging, el log arranca sin
        // loguear en lugar de no arrancar. El arranque rápido manda (CLAUDE.md, sondas de Kubernetes).
        services.AddSingleton<ILog>(provider => new SegmentedLog(
            ResolveDataDir(configuration),
            SegmentedLog.DefaultSegmentBytes,
            provider.GetService<ILogger<SegmentedLog>>()));

        // Los tópicos se guardan en el mismo log, replicados por quórum (DEC-003). Necesita IReplicator e IClusterState,
        // que registran Replication y Coordination.
        services.AddSingleton<ITopicCatalog, LogTopicCatalog>();
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
