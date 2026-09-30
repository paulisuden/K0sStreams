using System.IO.Hashing;

namespace K0sStreams.Contracts;

/// <summary>Elige la partición de un mensaje. Tiene que dar lo mismo en todos los brokers y versiones.</summary>
public static class Partitioner
{
    /// <summary>CRC32 de la clave módulo particiones. Sin clave va a la partición 0.</summary>
    public static int ForKey(ReadOnlySpan<byte> key, int partitions)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(partitions, 1);
        return key.IsEmpty ? 0 : (int)(Crc32.HashToUInt32(key) % (uint)partitions);
    }
}
