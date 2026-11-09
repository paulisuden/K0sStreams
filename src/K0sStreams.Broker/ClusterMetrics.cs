using System.Diagnostics.Metrics;
using K0sStreams.Contracts;

namespace K0sStreams.Broker;

/// <summary>
/// Estado del cluster publicado en <c>/metrics</c>: quién lidera, en qué época está y hasta dónde
/// llegó cada tópico en este broker.
/// </summary>
/// <remarks>
/// Son gauges observables: no cuestan nada hasta que Prometheus hace un scrape, y entonces se leen
/// de <see cref="IClusterState"/>, <see cref="ITopicCatalog"/> e <see cref="ILog"/>, que ya están en
/// memoria. Medir no puede cambiar el comportamiento ni tirar excepciones: un tópico que no se puede
/// leer se saltea y se loguea.
/// </remarks>
internal sealed partial class ClusterMetrics
{
    /// <summary>Meter de este proyecto. El broker lo registra en <see cref="MetricsExtensions"/>.</summary>
    public const string MeterName = "K0sStreams.Broker";

    /// <summary>Un tópico es un único log, pero <see cref="ILog"/> todavía pide la partición (DEC-001).</summary>
    private const int SingleLogPartition = 0;

    private readonly IClusterState _cluster;
    private readonly ITopicCatalog _catalog;
    private readonly ILog _log;
    private readonly ILogger<ClusterMetrics> _logger;

    /// <summary>Etiqueta que distingue las series de cada broker. Va en todas las métricas.</summary>
    private readonly KeyValuePair<string, object?> _node;

    public ClusterMetrics(
        IMeterFactory meterFactory,
        IClusterState cluster,
        ITopicCatalog catalog,
        ILog log,
        ILogger<ClusterMetrics> logger)
    {
        ArgumentNullException.ThrowIfNull(meterFactory);
        ArgumentNullException.ThrowIfNull(cluster);

        _cluster = cluster;
        _catalog = catalog;
        _log = log;
        _logger = logger;
        _node = new KeyValuePair<string, object?>("node", cluster.NodeId);

        // El Meter lo crea la fábrica y lo libera el contenedor de servicios al apagarse.
        Meter meter = meterFactory.Create(MeterName);

        meter.CreateObservableGauge(
            "k0sstreams.broker.is_leader",
            () => new Measurement<int>(_cluster.IsLeader ? 1 : 0, _node),
            description: "1 if this broker is the leader, 0 otherwise.");

        meter.CreateObservableGauge(
            "k0sstreams.leader.epoch",
            () => new Measurement<long>(_cluster.CurrentEpoch, _node),
            description: "Epoch this broker believes the cluster is in.");

        meter.CreateObservableGauge(
            "k0sstreams.topic.high_watermark",
            () => ObserveTopics(_log.HighWatermark),
            description: "Last offset confirmed by the quorum, per topic.");

        meter.CreateObservableGauge(
            "k0sstreams.topic.end_offset",
            () => ObserveTopics(_log.EndOffset),
            description: "Last offset on this broker's own disk, per topic.");
    }

    /// <summary>
    /// Lee el mismo offset en cada tópico del catálogo. Si el catálogo o un tópico no se pueden
    /// leer, informa lo que sí pudo leer en lugar de romper el scrape.
    /// </summary>
    private List<Measurement<long>> ObserveTopics(Func<string, int, long> readOffset)
    {
        var measurements = new List<Measurement<long>>();

        IReadOnlyCollection<TopicConfig> topics;
        try
        {
            topics = _catalog.List();
        }
        catch (Exception ex)
        {
            LogCatalogUnreadable(_logger, ex);
            return measurements;
        }

        foreach (TopicConfig topic in topics)
        {
            try
            {
                measurements.Add(new Measurement<long>(
                    readOffset(topic.Name, SingleLogPartition),
                    _node,
                    new KeyValuePair<string, object?>("topic", topic.Name)));
            }
            catch (Exception ex)
            {
                LogTopicUnreadable(_logger, ex, topic.Name);
            }
        }

        return measurements;
    }

    [LoggerMessage(
        Level = LogLevel.Warning,
        Message = "Could not list the topics while exporting metrics; this scrape has no per-topic offsets.")]
    private static partial void LogCatalogUnreadable(ILogger logger, Exception ex);

    [LoggerMessage(
        Level = LogLevel.Warning,
        Message = "Skipped {Topic} while exporting metrics: its offsets could not be read.")]
    private static partial void LogTopicUnreadable(ILogger logger, Exception ex, string topic);
}
