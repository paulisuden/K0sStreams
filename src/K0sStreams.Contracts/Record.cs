namespace K0sStreams.Contracts;

/// <summary>
/// Un registro del log: un mensaje de usuario o un evento (cola, cambio de época).
/// Ver <see cref="RecordCodec"/> para el formato binario.
/// </summary>
/// <param name="Offset">Posición en la partición. <see cref="Unassigned"/> si todavía no la asignó el log.</param>
/// <param name="Epoch">Época del líder que escribió el registro.</param>
/// <param name="Type">Mensaje o tipo de evento.</param>
/// <param name="Timestamp">Momento de escritura, en ms Unix.</param>
/// <param name="DeliverAt">Momento de entrega para mensajes programados, en ms Unix. 0 si es inmediato.</param>
/// <param name="Key">Clave del mensaje (vacía si no tiene).</param>
/// <param name="Value">Payload del usuario o del evento.</param>
public sealed record Record(
    long Offset,
    long Epoch,
    RecordType Type,
    long Timestamp,
    long DeliverAt,
    ReadOnlyMemory<byte> Key,
    ReadOnlyMemory<byte> Value)
{
    public const long Unassigned = -1;

    public static Record NewMessage(long epoch, long timestamp, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, long deliverAt = 0) =>
        new(Unassigned, epoch, RecordType.Message, timestamp, deliverAt, key, value);
}
