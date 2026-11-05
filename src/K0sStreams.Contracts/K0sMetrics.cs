using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace K0sStreams.Contracts;

/// <summary>
/// Instrumentos de <see cref="Meter"/> compartidos por todos los proyectos. Viven en Contracts porque cada
/// proyecto lo referencia: así todos publican en el mismo medidor y el exportador (Prometheus) los ve juntos.
/// Los nombres llevan el prefijo <c>k0sstreams.</c> para no chocar con los de ASP.NET Core.
/// </summary>
public static class K0sMetrics
{
    /// <summary>Nombre del medidor que tiene que registrar el proveedor (Program.cs).</summary>
    public const string MeterName = "K0sStreams";

    /// <summary>Nombre del broker. Lo fija el arranque para que cada serie diga de qué pod viene.</summary>
    public static string NodeId { get; set; } = "unknown";

    public static readonly Meter Meter = new(MeterName, "1.0.0");

    // ---- API de clientes ----
    public static readonly Counter<long> Published = Meter.CreateCounter<long>(
        "k0sstreams.publish.messages", null, "Mensajes publicados y confirmados por quórum.");

    public static readonly Counter<long> PublishErrors = Meter.CreateCounter<long>(
        "k0sstreams.publish.errors", null, "Publicaciones que no se pudieron confirmar.");

    public static readonly Histogram<double> PublishDuration = Meter.CreateHistogram<double>(
        "k0sstreams.publish.duration", "s", "Latencia de una publicación hasta el quórum.");

    public static readonly Counter<long> ReadMessages = Meter.CreateCounter<long>(
        "k0sstreams.read.messages", null, "Mensajes devueltos por la lectura del historial.");

    public static readonly Histogram<double> ReadDuration = Meter.CreateHistogram<double>(
        "k0sstreams.read.duration", "s", "Latencia de una lectura del historial.");

    // ---- Cola ----
    public static readonly Counter<long> QueueReceive = Meter.CreateCounter<long>(
        "k0sstreams.queue.receive", null, "Pedidos de receive, etiquetados por resultado.");

    public static readonly Counter<long> QueueAcked = Meter.CreateCounter<long>(
        "k0sstreams.queue.ack", null, "Mensajes confirmados por un consumidor.");

    public static readonly Counter<long> QueueNacked = Meter.CreateCounter<long>(
        "k0sstreams.queue.nack", null, "Mensajes devueltos para reintento.");

    public static readonly Counter<long> QueueDeadLettered = Meter.CreateCounter<long>(
        "k0sstreams.queue.dlq", null, "Mensajes enviados a la cola de muertos.");

    // ---- Replicación ----
    public static readonly Histogram<double> QuorumWait = Meter.CreateHistogram<double>(
        "k0sstreams.replication.quorum_wait.duration", "s", "Espera hasta que el offset llega al quórum.");

    public static readonly Counter<long> QuorumTimeouts = Meter.CreateCounter<long>(
        "k0sstreams.replication.quorum_timeouts", null, "Escrituras que vencieron esperando quórum.");

    public static readonly Counter<long> AppendFailures = Meter.CreateCounter<long>(
        "k0sstreams.replication.append_failures", null, "Appends rechazados o fallidos contra un seguidor.");

    /// <summary>Último lag conocido de cada seguidor (end offset del líder menos lo replicado).</summary>
    public static readonly ConcurrentDictionary<(string Topic, int Partition, string Peer), long> FollowerLag = new();

    // ---- Almacenamiento ----
    public static readonly Histogram<double> FsyncDuration = Meter.CreateHistogram<double>(
        "k0sstreams.storage.fsync.duration", "s", "Duración de cada fsync del group commit.");

    public static readonly Histogram<long> GroupCommitRecords = Meter.CreateHistogram<long>(
        "k0sstreams.storage.group_commit.records", "records", "Registros cubiertos por cada fsync.");

    public static readonly Counter<long> Fsyncs = Meter.CreateCounter<long>(
        "k0sstreams.storage.fsyncs", null, "Fsync ejecutados por el group commit.");

    /// <summary>Registra una publicación confirmada y su latencia.</summary>
    public static void RecordPublished(string topic, int partition, double seconds)
    {
        TagList tags = default;
        tags.Add("node", NodeId);
        tags.Add("topic", topic);
        tags.Add("partition", partition);
        Published.Add(1, tags);
        PublishDuration.Record(seconds, tags);
    }

    /// <summary>Registra una publicación que no llegó al quórum.</summary>
    public static void RecordPublishError(string topic)
    {
        TagList tags = default;
        tags.Add("node", NodeId);
        tags.Add("topic", topic);
        PublishErrors.Add(1, tags);
    }

    /// <summary>Registra una lectura del historial.</summary>
    public static void RecordRead(string topic, int partition, int count, double seconds)
    {
        TagList tags = default;
        tags.Add("node", NodeId);
        tags.Add("topic", topic);
        tags.Add("partition", partition);
        ReadMessages.Add(count, tags);
        ReadDuration.Record(seconds, tags);
    }

    /// <summary>Registra un pedido de receive. <paramref name="result"/>: hit, empty o not_assigned.</summary>
    public static void RecordQueueReceive(string topic, string queue, string result)
    {
        TagList tags = default;
        tags.Add("node", NodeId);
        tags.Add("topic", topic);
        tags.Add("queue", queue);
        tags.Add("result", result);
        QueueReceive.Add(1, tags);
    }

    /// <summary>Registra un ack confirmado.</summary>
    public static void RecordQueueAck(string topic, string queue)
    {
        TagList tags = default;
        tags.Add("node", NodeId);
        tags.Add("topic", topic);
        tags.Add("queue", queue);
        QueueAcked.Add(1, tags);
    }

    /// <summary>Registra un nack, sea para reintento o para la DLQ.</summary>
    public static void RecordQueueNack(string topic, string queue, bool deadLettered)
    {
        TagList tags = default;
        tags.Add("node", NodeId);
        tags.Add("topic", topic);
        tags.Add("queue", queue);
        QueueNacked.Add(1, tags);
        if (deadLettered)
        {
            QueueDeadLettered.Add(1, tags);
        }
    }

    /// <summary>Registra la espera de quórum de una escritura.</summary>
    public static void RecordQuorumWait(string topic, int partition, double seconds, bool timedOut)
    {
        TagList tags = default;
        tags.Add("node", NodeId);
        tags.Add("topic", topic);
        tags.Add("partition", partition);
        QuorumWait.Record(seconds, tags);
        if (timedOut)
        {
            QuorumTimeouts.Add(1, tags);
        }
    }

    /// <summary>Registra que un seguidor rechazó un append o que la llamada falló.</summary>
    public static void RecordAppendFailure(string topic, int partition, string peer)
    {
        TagList tags = default;
        tags.Add("node", NodeId);
        tags.Add("topic", topic);
        tags.Add("partition", partition);
        tags.Add("peer", peer);
        AppendFailures.Add(1, tags);
    }

    /// <summary>Actualiza el lag de un seguidor en la partición.</summary>
    public static void SetFollowerLag(string topic, int partition, string peer, long lag) =>
        FollowerLag[(topic, partition, peer)] = Math.Max(0, lag);

    /// <summary>Registra un fsync del group commit y cuántos registros cubrió.</summary>
    public static void RecordFsync(string topic, int partition, int files, long records, double seconds)
    {
        TagList tags = default;
        tags.Add("node", NodeId);
        tags.Add("topic", topic);
        tags.Add("partition", partition);
        FsyncDuration.Record(seconds, tags);
        GroupCommitRecords.Record(records, tags);
        Fsyncs.Add(files, tags);
    }
}
