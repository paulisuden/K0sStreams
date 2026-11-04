using System.Buffers.Binary;
using System.Collections.Generic;
using System.Text;

namespace K0sStreams.Contracts;

/// <summary>
/// Evento de cola que se guarda en el log como un <see cref="Record"/> (event sourcing).
/// El <c>Key</c> del registro es el nombre de la cola. El <c>Value</c> depende del tipo:
/// <ul>
///   <li><c>Receive</c>, <c>Ack</c>, <c>Nack</c>, <c>Timeout</c>: <c>partition int32 | targetOffset int64 | untilMs int64 | reason UTF-8</c></li>
///   <li><c>OffsetCommit</c>: <c>partition int32 | committedOffset int64</c> (UntilMs=0, Reason=null)</li>
///   <li><c>GroupState</c>: <c>generation int64 | memberCount int32 | para cada miembro: consumerIdLength int32 | consumerId UTF-8 | partitionCount int32 | particiones int32...</c></li>
/// </ul>
/// </summary>
/// <param name="Tipo">Tipo de evento de cola.</param>
/// <param name="Cola">Cola (grupo de consumidores) a la que aplica.</param>
/// <param name="Particion">Partición del mensaje afectado (para OffsetCommit: la partición confirmada).</param>
/// <param name="OffsetObjetivo">Offset del mensaje afectado (para OffsetCommit: el offset confirmado).</param>
/// <param name="HastaMs">Receive: fin del visibility timeout. Nack/Timeout: próximo intento. Otros: 0.</param>
/// <param name="Razon">Motivo de un nack (opcional).</param>
public sealed record QueueEvent(RecordType Tipo, string Cola, int Particion, long OffsetObjetivo, long HastaMs, string? Razon = null)
{
    private const int TamanoFijo = 4 + 8 + 8;

    public static bool EsEventoDeCola(RecordType tipo) =>
        tipo is RecordType.Receive or RecordType.Ack or RecordType.Nack or RecordType.Timeout
        or RecordType.OffsetCommit or RecordType.GroupState;

    /// <summary>Crea un evento <see cref="RecordType.OffsetCommit"/>.</summary>
    public static QueueEvent ConfirmarOffset(string cola, int particion, long offsetConfirmado) =>
        new(RecordType.OffsetCommit, cola, particion, offsetConfirmado, 0, null);

    /// <summary>Crea un evento <see cref="RecordType.GroupState"/> con el estado completo del grupo.</summary>
    public static QueueEvent EstadoDelGrupo(string cola, long generacion, IReadOnlyDictionary<string, IReadOnlyList<int>> miembros)
    {
        return new QueueEvent(RecordType.GroupState, cola, -1, generacion, miembros.Count, null);
    }

    public Record ARegistro(long epoca, long timestamp)
    {
        if (!EsEventoDeCola(Tipo))
        {
            throw new InvalidOperationException($"{Tipo} no es un evento de cola.");
        }

        if (Tipo == RecordType.GroupState)
        {
            // Grupo: serialización personalizada en Value. Los campos Particion/OffsetObjetivo/HastaMs/Razon no se usan.
            using var ms = new MemoryStream();
            using var writer = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);
            writer.Write(OffsetObjetivo); // generation
            writer.Write(HastaMs);        // memberCount
            // Los miembros se serializan por quien llama (ver GroupStatePayload). Aquí solo dejamos el encabezado.
            return new Record(Record.Unassigned, epoca, Tipo, timestamp, 0, Encoding.UTF8.GetBytes(Cola), ms.ToArray());
        }

        int largoRazon = Razon is null ? 0 : Encoding.UTF8.GetByteCount(Razon);
        var valor = new byte[TamanoFijo + largoRazon];
        BinaryPrimitives.WriteInt32LittleEndian(valor, Particion);
        BinaryPrimitives.WriteInt64LittleEndian(valor.AsSpan(4), OffsetObjetivo);
        BinaryPrimitives.WriteInt64LittleEndian(valor.AsSpan(12), HastaMs);
        if (Razon is not null)
        {
            Encoding.UTF8.GetBytes(Razon, valor.AsSpan(TamanoFijo));
        }

        return new Record(Record.Unassigned, epoca, Tipo, timestamp, 0, Encoding.UTF8.GetBytes(Cola), valor);
    }

    /// <summary>
    /// Serializa el estado completo de un grupo de consumidores en el formato de valor de un registro GroupState.
    /// </summary>
    public static byte[] SerializarEstadoDelGrupo(long generacion, IReadOnlyDictionary<string, IReadOnlyList<int>> miembros)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);
        writer.Write(generacion);
        writer.Write(miembros.Count);
        foreach (var (consumerId, particiones) in miembros)
        {
            var idBytes = Encoding.UTF8.GetBytes(consumerId);
            writer.Write(idBytes.Length);
            writer.Write(idBytes);
            writer.Write(particiones.Count);
            foreach (var p in particiones)
            {
                writer.Write(p);
            }
        }
        return ms.ToArray();
    }

    public static QueueEvent DesdeRegistro(Record registro)
    {
        ArgumentNullException.ThrowIfNull(registro);
        if (!EsEventoDeCola(registro.Type))
        {
            throw new ArgumentException("El registro no es un evento de cola válido.", nameof(registro));
        }

        var valor = registro.Value.Span;
        return registro.Type switch
        {
            RecordType.Receive or RecordType.Ack or RecordType.Nack or RecordType.Timeout
                => DesdeRegistroDeEvento(registro, valor),
            RecordType.OffsetCommit
                => DesdeRegistroDeConfirmacion(registro, valor),
            RecordType.GroupState
                => DesdeRegistroDeGrupo(registro, valor),
            _ => throw new ArgumentException($"Tipo de evento de cola no soportado: {registro.Type}", nameof(registro))
        };
    }

    private static QueueEvent DesdeRegistroDeEvento(Record registro, ReadOnlySpan<byte> valor)
    {
        if (valor.Length < TamanoFijo)
        {
            throw new ArgumentException("El registro no es un evento de cola válido.", nameof(registro));
        }

        return new QueueEvent(
            registro.Type,
            Encoding.UTF8.GetString(registro.Key.Span),
            BinaryPrimitives.ReadInt32LittleEndian(valor),
            BinaryPrimitives.ReadInt64LittleEndian(valor[4..]),
            BinaryPrimitives.ReadInt64LittleEndian(valor[12..]),
            valor.Length > TamanoFijo ? Encoding.UTF8.GetString(valor[TamanoFijo..]) : null);
    }

    private static QueueEvent DesdeRegistroDeConfirmacion(Record registro, ReadOnlySpan<byte> valor)
    {
        if (valor.Length < 12)
        {
            throw new ArgumentException("El registro de confirmación de offset no es válido.", nameof(registro));
        }

        return new QueueEvent(
            registro.Type,
            Encoding.UTF8.GetString(registro.Key.Span),
            BinaryPrimitives.ReadInt32LittleEndian(valor),
            BinaryPrimitives.ReadInt64LittleEndian(valor[4..]),
            0,
            null);
    }

    private static QueueEvent DesdeRegistroDeGrupo(Record registro, ReadOnlySpan<byte> valor)
    {
        // GroupState: Value tiene generación + memberCount + miembros serializados.
        // Los miembros se reconstruyen por quien consume el evento (QueueEngine.ReplayAsync)
        // usando DeserializarEstadoDelGrupo.
        return new QueueEvent(
            registro.Type,
            Encoding.UTF8.GetString(registro.Key.Span),
            -1,
            0, // generation se lee del valor por quien lo consume
            0, // memberCount se lee del valor por quien lo consume
            null);
    }

    /// <summary>
    /// Deserializa el payload de un registro GroupState en un diccionario de consumidor → particiones.
    /// Devuelve null si el valor no tiene datos de grupo (encabezado vacío).
    /// </summary>
    public static IReadOnlyDictionary<string, IReadOnlyList<int>>? DeserializarEstadoDelGrupo(ReadOnlySpan<byte> valor, out long generacion)
    {
        generacion = 0;
        if (valor.Length < 12)
        {
            return null;
        }

        generacion = BinaryPrimitives.ReadInt64LittleEndian(valor);
        int miembroCount = BinaryPrimitives.ReadInt32LittleEndian(valor[8..]);
        int offset = 12;
        var miembros = new Dictionary<string, List<int>>(StringComparer.Ordinal);

        for (int i = 0; i < miembroCount; i++)
        {
            if (offset + 4 > valor.Length)
            {
                return null;
            }
            int idLargo = BinaryPrimitives.ReadInt32LittleEndian(valor[offset..]);
            offset += 4;
            if (offset + idLargo > valor.Length)
            {
                return null;
            }
            string consumerId = Encoding.UTF8.GetString(valor.Slice(offset, idLargo));
            offset += idLargo;

            if (offset + 4 > valor.Length)
            {
                return null;
            }
            int particionCount = BinaryPrimitives.ReadInt32LittleEndian(valor[offset..]);
            offset += 4;

            var particiones = new List<int>(particionCount);
            for (int j = 0; j < particionCount; j++)
            {
                if (offset + 4 > valor.Length)
                {
                    return null;
                }
                particiones.Add(BinaryPrimitives.ReadInt32LittleEndian(valor[offset..]));
                offset += 4;
            }

            miembros[consumerId] = particiones;
        }

        return miembros.ToDictionary(kvp => kvp.Key, kvp => (IReadOnlyList<int>)kvp.Value, StringComparer.Ordinal);
    }
}
