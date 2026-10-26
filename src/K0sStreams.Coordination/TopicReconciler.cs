using k8s;
using k8s.Models;
using K0sStreams.Contracts;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace K0sStreams.Coordination;

/// <summary>
/// CRD Topic (fase 5): crea en el catálogo los tópicos declarados con <c>kubectl apply -f topic.yaml</c>. Solo actúa
/// en el líder. Cada <see cref="SyncPeriod"/> lista todos los Topic del namespace y crea los que faltan: como compara el
/// estado completo y no eventos sueltos, un líder nuevo se pone al día solo y no hay un watch que se pueda cortar.
/// </summary>
internal sealed partial class TopicReconciler(
    IKubernetes client,
    string namespaceName,
    ITopicCatalog catalog,
    IClusterState cluster,
    TimeProvider time,
    ILogger<TopicReconciler> logger) : BackgroundService
{
    public const string Group = "k0sstreams.io";
    public const string Version = "v1";
    public const string Plural = "topics";

    internal static readonly TimeSpan SyncPeriod = TimeSpan.FromSeconds(5);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(SyncPeriod, time);
        try
        {
            do
            {
                await ReconcileAsync(stoppingToken).ConfigureAwait(false);
            }
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false));
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // El pod se está apagando.
        }
    }

    /// <summary>Una pasada: si es líder, crea los Topic que el catálogo no tiene. Los tests la llaman directo.</summary>
    internal async Task ReconcileAsync(CancellationToken ct = default)
    {
        if (!cluster.IsLeader)
        {
            return;
        }

        try
        {
            TopicResourceList topics = await client.CustomObjects
                .ListNamespacedCustomObjectAsync<TopicResourceList>(Group, Version, namespaceName, Plural, cancellationToken: ct)
                .ConfigureAwait(false);
            foreach (TopicResource topic in topics.Items)
            {
                await CreateIfMissingAsync(topic.ToConfig(), ct).ConfigureAwait(false);
            }
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            // Sin API server, sin la CRD instalada o dejando de ser líder a mitad de camino: se reintenta en la próxima
            // pasada. Una excepción que escapa de un BackgroundService apaga el broker entero.
            LogReconcileFailed(logger, ex);
        }
    }

    private async Task CreateIfMissingAsync(TopicConfig config, CancellationToken ct)
    {
        // Si ya existe (por la API o por una pasada anterior) no se compara la configuración: el catálogo no la cambia.
        if (catalog.Find(config.Name) is not null)
        {
            return;
        }

        // La CRD ya valida al hacer apply; esto cubre lo que su esquema no puede (o un cluster sin reglas CEL).
        if (config.Validate().Count > 0)
        {
            LogInvalid(logger, config.Name);
            return;
        }

        if (await catalog.CreateAsync(config, ct).ConfigureAwait(false))
        {
            LogCreated(logger, config.Name);
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Tópico {Topic} creado desde la CRD Topic.")]
    private static partial void LogCreated(ILogger logger, string topic);

    [LoggerMessage(Level = LogLevel.Warning, Message = "El Topic {Topic} no es un tópico válido; no se crea.")]
    private static partial void LogInvalid(ILogger logger, string topic);

    [LoggerMessage(Level = LogLevel.Warning, Message = "No se pudieron sincronizar los Topic; se reintenta en la próxima pasada.")]
    private static partial void LogReconcileFailed(ILogger logger, Exception exception);
}

/// <summary>Respuesta de <c>GET /apis/k0sstreams.io/v1/namespaces/{ns}/topics</c>.</summary>
internal sealed record TopicResourceList(IReadOnlyList<TopicResource> Items);

/// <summary>Un objeto Topic; el nombre del tópico es el del objeto.</summary>
internal sealed record TopicResource(V1ObjectMeta Metadata, TopicSpec? Spec)
{
    public TopicConfig ToConfig()
    {
        TopicSpec spec = Spec ?? new TopicSpec();
        return new TopicConfig(Metadata.Name, spec.Partitions, spec.MaxRetries, TimeSpan.FromSeconds(spec.RetryBackoffSeconds));
    }
}

/// <summary>Los mismos valores por defecto que <c>POST /v1/topics</c> y que deploy/topic-crd.yaml.</summary>
internal sealed record TopicSpec(int Partitions = 1, int MaxRetries = 3, int RetryBackoffSeconds = 5);
