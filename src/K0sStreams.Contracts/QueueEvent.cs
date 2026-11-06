using System.Buffers.Binary;
using System.Text;

namespace K0sStreams.Contracts;

/// <summary>
/// Evento de cola que se guarda en el log como un <see cref="Record"/>.
/// En el registro, <c>Key</c> es el nombre de la cola y <c>Value</c> es:
/// <c>partition int32 | targetOffset int64 | untilMs int64 | reason UTF-8</c>.
/// Lo durable es el cursor confirmado (<see cref="RecordType.OffsetCommit"/>) y las marcas de DLQ;
/// las entregas en proceso y los reintentos viven en memoria.
/// </summary>
/// <param name="Type">OffsetCommit o Nack (marca de DLQ). Los tipos de versiones anteriores se ignoran al releer.</param>
/// <param name="Queue">Cola a la que aplica.</param>
/// <param name="Partition">Partición del mensaje afectado.</param>
/// <param name="TargetOffset">Offset confirmado o mensaje que fue a la DLQ.</param>
/// <param name="UntilMs">Sin uso en los eventos durables; se conserva por compatibilidad de formato.</param>
/// <param name="Reason">Motivo del nack (opcional).</param>
public sealed record QueueEvent(RecordType Type, string Queue, int Partition, long TargetOffset, long UntilMs, string? Reason = null)
{
    private const int FixedSize = 4 + 8 + 8;

    public static bool IsQueueEvent(RecordType type) =>
        type is RecordType.Receive or RecordType.Ack or RecordType.Nack
            or RecordType.Timeout or RecordType.OffsetCommit;

    /// <summary>Evento que confirma el cursor de una cola: todo lo anterior ya se procesó.</summary>
    public static QueueEvent ConfirmarOffset(string queue, int partition, long offset) =>
        new(RecordType.OffsetCommit, queue, partition, offset, UntilMs: 0);

    public Record ToRecord(long epoch, long timestamp)
    {
        if (!IsQueueEvent(Type))
        {
            throw new InvalidOperationException($"{Type} no es un evento de cola.");
        }

        int reasonLength = Reason is null ? 0 : Encoding.UTF8.GetByteCount(Reason);
        var value = new byte[FixedSize + reasonLength];
        BinaryPrimitives.WriteInt32LittleEndian(value, Partition);
        BinaryPrimitives.WriteInt64LittleEndian(value.AsSpan(4), TargetOffset);
        BinaryPrimitives.WriteInt64LittleEndian(value.AsSpan(12), UntilMs);
        if (Reason is not null)
        {
            Encoding.UTF8.GetBytes(Reason, value.AsSpan(FixedSize));
        }

        return new Record(Record.Unassigned, epoch, Type, timestamp, 0, Encoding.UTF8.GetBytes(Queue), value);
    }

    public static QueueEvent FromRecord(Record record)
    {
        ArgumentNullException.ThrowIfNull(record);
        if (!IsQueueEvent(record.Type) || record.Value.Length < FixedSize)
        {
            throw new ArgumentException("El registro no es un evento de cola válido.", nameof(record));
        }

        var value = record.Value.Span;
        return new QueueEvent(
            record.Type,
            Encoding.UTF8.GetString(record.Key.Span),
            BinaryPrimitives.ReadInt32LittleEndian(value),
            BinaryPrimitives.ReadInt64LittleEndian(value[4..]),
            BinaryPrimitives.ReadInt64LittleEndian(value[12..]),
            value.Length > FixedSize ? Encoding.UTF8.GetString(value[FixedSize..]) : null);
    }
}
