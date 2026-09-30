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

/// <summary>
/// Semántica de cola (tipo SQS) sobre el log. Cada cola es un grupo de consumidores independiente:
/// dos colas del mismo tópico reciben cada una todos los mensajes.
/// Cada transición se escribe en el log como <see cref="QueueEvent"/>, así un nuevo líder reconstruye el estado.
/// </summary>
public interface IQueueEngine
{
    /// <summary>Toma el próximo mensaje listo, o null si no hay. El tópico tiene que existir.</summary>
    ValueTask<Delivery?> ReceiveAsync(string topic, string queue, TimeSpan visibility, CancellationToken ct = default);

    /// <summary>Confirma un mensaje en proceso. Devuelve false si no estaba en proceso en esa cola.</summary>
    ValueTask<bool> AckAsync(string topic, string queue, int partition, long offset, CancellationToken ct = default);

    /// <summary>Devuelve un mensaje en proceso para reintentar (o a la DLQ). False si no estaba en proceso.</summary>
    ValueTask<bool> NackAsync(string topic, string queue, int partition, long offset, string? reason, CancellationToken ct = default);

    IReadOnlyList<DeadLetter> GetDeadLetters(string topic, string queue);

    /// <summary>Aplica un evento de cola leído del log (al replicar o al volverse líder).</summary>
    void Apply(string topic, Record queueEvent);
}
