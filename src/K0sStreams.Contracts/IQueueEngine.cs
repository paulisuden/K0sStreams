namespace K0sStreams.Contracts;

/// <summary>Un mensaje entregado a un consumidor de cola, pendiente de ack o nack.</summary>
/// <param name="Partition">Partición del mensaje; hace falta para el ack.</param>
/// <param name="Offset">Offset del mensaje; identifica la entrega.</param>
/// <param name="Message">El registro original.</param>
/// <param name="Attempt">Número de intento, empezando en 1.</param>
/// <param name="VisibleUntil">Si no hay ack antes de esto, el mensaje vuelve a la cola.</param>
public sealed record Delivery(int Partition, long Offset, Record Message, int Attempt, DateTimeOffset VisibleUntil);

/// <summary>Un mensaje que agotó los reintentos.</summary>
public sealed record DeadLetter(int Partition, long Offset, Record Message, int Attempts, string? LastReason);

/// <summary>Asignación de particiones a un consumidor dentro de un grupo.</summary>
/// <param name="ConsumerId">Identificador único del consumidor.</param>
/// <param name="Partitions">Particiones asignadas a este consumidor.</param>
/// <param name="Generation">Generación del grupo (incrementa en cada rebalance).</param>
public sealed record ConsumerAssignment(string ConsumerId, IReadOnlyList<int> Partitions, long Generation);

/// <summary>Estado de un consumidor en el grupo.</summary>
/// <param name="ConsumerId">Identificador único del consumidor.</param>
/// <param name="LastHeartbeat">Último latido recibido.</param>
/// <param name="AssignedPartitions">Particiones actualmente asignadas.</param>
public sealed record ConsumerState(string ConsumerId, DateTimeOffset LastHeartbeat, IReadOnlyList<int> AssignedPartitions);

/// <summary>
/// Semántica de cola (tipo SQS) sobre el log. Cada cola es un grupo de consumidores independiente:
/// dos colas del mismo tópico reciben cada una todos los mensajes.
/// Cada transición se escribe en el log como <see cref="QueueEvent"/>, así un nuevo líder reconstruye el estado.
/// </summary>
public interface IQueueEngine
{
    /// <summary>Toma el próximo mensaje listo de las particiones asignadas al consumidor, o null si no hay. El tópico tiene que existir.</summary>
    ValueTask<Delivery?> ReceiveAsync(string topic, string queue, string consumerId, TimeSpan visibility, CancellationToken ct = default);

    /// <summary>Confirma un mensaje en proceso. Devuelve false si no estaba en proceso en esa cola.</summary>
    ValueTask<bool> AckAsync(string topic, string queue, int partition, long offset, CancellationToken ct = default);

    /// <summary>Devuelve un mensaje en proceso para reintentar (o a la DLQ). False si no estaba en proceso.</summary>
    ValueTask<bool> NackAsync(string topic, string queue, int partition, long offset, string? reason, CancellationToken ct = default);

    /// <summary>Une un consumidor al grupo de la cola. Devuelve la asignación de particiones.</summary>
    ValueTask<ConsumerAssignment> JoinGroupAsync(string topic, string queue, string consumerId, CancellationToken ct = default);

    /// <summary>Saca un consumidor del grupo, liberando sus particiones para rebalance.</summary>
    ValueTask LeaveGroupAsync(string topic, string queue, string consumerId, CancellationToken ct = default);

    /// <summary>Latido del consumidor para mantener su membresía. Devuelve la asignación actual.</summary>
    ValueTask<ConsumerAssignment> HeartbeatAsync(string topic, string queue, string consumerId, CancellationToken ct = default);

    /// <summary>Confirma el offset procesado por partición (commit de offset).</summary>
    ValueTask CommitOffsetsAsync(string topic, string queue, string consumerId, IReadOnlyDictionary<int, long> offsets, CancellationToken ct = default);

    IReadOnlyList<DeadLetter> GetDeadLetters(string topic, string queue);

    /// <summary>Obtiene el estado actual de todos los consumidores en el grupo.</summary>
    IReadOnlyList<ConsumerState> GetConsumerStates(string topic, string queue);

    /// <summary>Aplica un evento de cola leído del log (al replicar o al volverse líder).</summary>
    void Apply(string topic, Record queueEvent);
}