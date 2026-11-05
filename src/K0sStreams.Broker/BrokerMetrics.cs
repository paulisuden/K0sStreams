using System.Diagnostics.Metrics;
using K0sStreams.Contracts;

namespace K0sStreams.Broker;

/// <summary>
/// Gauges que no se pueden mantener en un contador: se leen en cada scrape. El rol, la época, el high watermark
/// de cada partición y el lag de cada seguidor son estado, no eventos, así que se publican como observables.
/// El <see cref="Meter"/> es el estático de <see cref="K0sMetrics"/>: mantiene vivos los instrumentos.
/// </summary>
internal sealed class BrokerMetrics
{
    private readonly IClusterState _cluster;
    private readonly ITopicCatalog _catalog;
    private readonly ILog _log;

    public BrokerMetrics(IClusterState cluster, ITopicCatalog catalog, ILog log)
    {
        _cluster = cluster;
        _catalog = catalog;
        _log = log;

        K0sMetrics.Meter.CreateObservableGauge(
            "k0sstreams.broker.is_leader",
            ObserveLeader,
            unit: null,
            description: "1 si este broker es el líder de la época actual.");

        K0sMetrics.Meter.CreateObservableGauge(
            "k0sstreams.leader.epoch",
            ObserveEpoch,
            unit: null,
            description: "Época del Lease que este broker conoce.");

        K0sMetrics.Meter.CreateObservableGauge(
            "k0sstreams.topic.high_watermark",
            ObserveHighWatermarks,
            unit: null,
            description: "Último offset confirmado por quórum de cada partición.");

        K0sMetrics.Meter.CreateObservableGauge(
            "k0sstreams.replication.follower_lag",
            ObserveFollowerLag,
            unit: null,
            description: "Registros que un seguidor va por detrás del líder.");
    }

    private IEnumerable<Measurement<int>> ObserveLeader() =>
    [
        new(_cluster.IsLeader ? 1 : 0, new KeyValuePair<string, object?>("node", _cluster.NodeId)),
    ];

    private IEnumerable<Measurement<long>> ObserveEpoch() =>
    [
        new(_cluster.CurrentEpoch, new KeyValuePair<string, object?>("node", _cluster.NodeId)),
    ];

    private IEnumerable<Measurement<long>> ObserveHighWatermarks()
    {
        foreach (TopicConfig topic in _catalog.List())
        {
            for (int partition = 0; partition < topic.Partitions; partition++)
            {
                yield return new Measurement<long>(
                    _log.HighWatermark(topic.Name, partition),
                    new KeyValuePair<string, object?>("node", _cluster.NodeId),
                    new KeyValuePair<string, object?>("topic", topic.Name),
                    new KeyValuePair<string, object?>("partition", partition));
            }
        }
    }

    private static IEnumerable<Measurement<long>> ObserveFollowerLag()
    {
        foreach (var (key, lag) in K0sMetrics.FollowerLag)
        {
            yield return new Measurement<long>(
                lag,
                new KeyValuePair<string, object?>("topic", key.Topic),
                new KeyValuePair<string, object?>("partition", key.Partition),
                new KeyValuePair<string, object?>("peer", key.Peer));
        }
    }
}
