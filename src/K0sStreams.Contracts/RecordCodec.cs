using System.Buffers.Binary;
using System.IO.Hashing;

namespace K0sStreams.Contracts;

public enum DecodeStatus
{
    Ok,

    /// <summary>Faltan bytes: típico de una escritura cortada al final del segmento.</summary>
    Incomplete,

    /// <summary>El CRC o los largos no cierran: el registro está dañado.</summary>
    Corrupt,
}

/// <summary>
/// Formato binario de un registro, el mismo en disco y en la réplica gRPC (little-endian):
/// <code>
/// length    int32   tamaño de todo lo que sigue
/// crc32     uint32  sobre todo lo que sigue
/// offset    int64
/// epoch     int64
/// type      byte
/// timestamp int64   ms Unix
/// deliverAt int64   0 si es inmediato
/// keyLen    int32   + key bytes
/// valueLen  int32   + value bytes
/// </code>
/// </summary>
public static class RecordCodec
{
    public const int MaxRecordSize = 16 * 1024 * 1024;

    /// <summary>Tamaño de un registro con clave y valor vacíos.</summary>
    public const int HeaderSize = 49;

    private const int LengthSize = 4;
    private const int CrcEnd = 8;

    public static int GetEncodedSize(Record record)
    {
        ArgumentNullException.ThrowIfNull(record);
        return HeaderSize + record.Key.Length + record.Value.Length;
    }

    public static byte[] Encode(Record record)
    {
        var buffer = new byte[GetEncodedSize(record)];
        Encode(record, buffer);
        return buffer;
    }

    /// <summary>Escribe el registro al principio de <paramref name="destination"/> y devuelve los bytes escritos.</summary>
    public static int Encode(Record record, Span<byte> destination)
    {
        int size = GetEncodedSize(record);
        if (size > MaxRecordSize)
        {
            throw new ArgumentException($"El registro ocupa {size} bytes; el máximo es {MaxRecordSize}.", nameof(record));
        }

        if (destination.Length < size)
        {
            throw new ArgumentException("El buffer de destino es chico.", nameof(destination));
        }

        var span = destination[..size];
        BinaryPrimitives.WriteInt32LittleEndian(span, size - LengthSize);
        int pos = CrcEnd;
        BinaryPrimitives.WriteInt64LittleEndian(span[pos..], record.Offset);
        pos += 8;
        BinaryPrimitives.WriteInt64LittleEndian(span[pos..], record.Epoch);
        pos += 8;
        span[pos++] = (byte)record.Type;
        BinaryPrimitives.WriteInt64LittleEndian(span[pos..], record.Timestamp);
        pos += 8;
        BinaryPrimitives.WriteInt64LittleEndian(span[pos..], record.DeliverAt);
        pos += 8;
        BinaryPrimitives.WriteInt32LittleEndian(span[pos..], record.Key.Length);
        pos += 4;
        record.Key.Span.CopyTo(span[pos..]);
        pos += record.Key.Length;
        BinaryPrimitives.WriteInt32LittleEndian(span[pos..], record.Value.Length);
        pos += 4;
        record.Value.Span.CopyTo(span[pos..]);

        BinaryPrimitives.WriteUInt32LittleEndian(span[LengthSize..], Crc32.HashToUInt32(span[CrcEnd..]));
        return size;
    }

    /// <summary>
    /// Lee un registro del principio de <paramref name="source"/>. Si devuelve <see cref="DecodeStatus.Ok"/>,
    /// <paramref name="consumed"/> indica cuántos bytes ocupaba.
    /// </summary>
    public static DecodeStatus TryDecode(ReadOnlySpan<byte> source, out Record? record, out int consumed)
    {
        record = null;
        consumed = 0;
        if (source.Length < LengthSize)
        {
            return DecodeStatus.Incomplete;
        }

        int length = BinaryPrimitives.ReadInt32LittleEndian(source);
        if (length < HeaderSize - LengthSize || length > MaxRecordSize - LengthSize)
        {
            return DecodeStatus.Corrupt;
        }

        int size = LengthSize + length;
        if (source.Length < size)
        {
            return DecodeStatus.Incomplete;
        }

        var span = source[..size];
        uint crc = BinaryPrimitives.ReadUInt32LittleEndian(span[LengthSize..]);
        if (crc != Crc32.HashToUInt32(span[CrcEnd..]))
        {
            return DecodeStatus.Corrupt;
        }

        int pos = CrcEnd;
        long offset = BinaryPrimitives.ReadInt64LittleEndian(span[pos..]);
        pos += 8;
        long epoch = BinaryPrimitives.ReadInt64LittleEndian(span[pos..]);
        pos += 8;
        var type = (RecordType)span[pos++];
        long timestamp = BinaryPrimitives.ReadInt64LittleEndian(span[pos..]);
        pos += 8;
        long deliverAt = BinaryPrimitives.ReadInt64LittleEndian(span[pos..]);
        pos += 8;
        int keyLength = BinaryPrimitives.ReadInt32LittleEndian(span[pos..]);
        pos += 4;
        if (keyLength < 0 || keyLength > size - HeaderSize)
        {
            return DecodeStatus.Corrupt;
        }

        var key = span.Slice(pos, keyLength).ToArray();
        pos += keyLength;
        int valueLength = BinaryPrimitives.ReadInt32LittleEndian(span[pos..]);
        pos += 4;
        if (valueLength != size - pos || !Enum.IsDefined(type))
        {
            return DecodeStatus.Corrupt;
        }

        record = new Record(offset, epoch, type, timestamp, deliverAt, key, span[pos..].ToArray());
        consumed = size;
        return DecodeStatus.Ok;
    }
}
