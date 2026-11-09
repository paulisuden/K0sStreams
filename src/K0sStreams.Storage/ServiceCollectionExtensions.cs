using K0sStreams.Contracts;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace K0sStreams.Storage;

public static class ServiceCollectionExtensions
{
    /// <summary>Carpeta de datos cuando no hay configuración: al lado del ejecutable.</summary>
    private const string DefaultDataDirName = "data";

    /// <summary>Registra el log, su retención y el catálogo de tópicos. Dueño: A — Almacenamiento.</summary>
    public static IServiceCollection AddK0sStorage(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        services.AddOptions<RetentionOptions>()
            .Bind(configuration.GetSection(RetentionOptions.SectionName))
            .Validate(static o => o.IsValid(), "Retention: MaxAge and MaxBytes must be positive if set.")
            .ValidateOnStart();

        // GetService y no GetRequiredService: si el host no configuró logging, el log arranca sin
        // loguear en lugar de no arrancar. El arranque rápido manda (CLAUDE.md, sondas de Kubernetes).
        services.AddSingleton(provider => new SegmentedLog(
            ResolveDataDir(configuration),
            SegmentedLog.DefaultSegmentBytes,
            provider.GetRequiredService<IOptions<RetentionOptions>>().Value,
            provider.GetService<ILogger<SegmentedLog>>()));

        // El tipo concreto además del contrato: la retención es de este log en particular, no de
        // cualquier ILog. Los otros bloques siguen recibiendo ILog y no se enteran.
        services.AddSingleton<ILog>(provider => provider.GetRequiredService<SegmentedLog>());

        // La retención corre como tarea del host y no dentro del log: así el apagado del pod la
        // detiene con su CancellationToken y el log no arranca nada por su cuenta.
        services.AddHostedService<RetentionService>();

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