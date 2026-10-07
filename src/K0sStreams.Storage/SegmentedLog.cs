using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Globalization;
using System.Runtime.CompilerServices;
using K0sStreams.Contracts;

namespace K0sStreams.Storage;

/// <summary>
/// Log append-only en disco. Reemplaza a <c>InMemoryLog</c>.
/// Cada tópico y partición vive en <c>{DataDir}/{topic}/{partition}/</c>, hoy con un único
/// segmento <c>00000000000000000000.log</c>.
/// </summary>
/// <remarks>
/// Por ahora simplemente escribe y lee de disco de verdad, pero todavía no esya completo
public sealed class SegmentedLog : ILog
{
    private const string LogExtension = ".log";

    /// <summary>Nombre del segmento: el primer offset que contiene, con 20 dígitos.</summary>
    private const string SegmentNameFormat = "D20";

    private readonly ConcurrentDictionary<(string Topic, int Partition), PartitionLog> _partitions = new();

    /// <param name="dataDir">Carpeta raíz de los datos. En Docker es <c>/data</c> (configuración <c>Broker:DataDir</c>).</param>
    public SegmentedLog(string dataDir)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dataDir);
        DataDir = dataDir;
    }

    /// <summary>Carpeta raíz donde se guardan los segmentos.</summary>
    public string DataDir { get; }

    /// ESCRIBIR
    public ValueTask<long> AppendAsync(string topic, int partition, Record record, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(record);
        ct.ThrowIfCancellationRequested();

        var log = Get(topic, partition);
        lock (log.Gate)
        {
            // El offset que toca es la cantidad de registros que ya hay: los offsets
            // empiezan en 0 y son contiguos, así que el índice de la lista ES el offset.
            long next = log.Positions.Count;
            //si la replica erro, se rechaza
            if (record.Offset != Record.Unassigned && record.Offset != next)
            {
                throw new ArgumentException($"Se esperaba el offset {next} y llegó {record.Offset}.", nameof(record));
            }

            //el offset viaja adentro de los bytes del registro. Por eso al releer, 
            // cada registro sabe cuál es su propio offset y no hay que deducirlo de la posición.
            byte[] bytes = RecordCodec.Encode(record with { Offset = next }); //paso a bytes

            Directory.CreateDirectory(log.DirectoryPath);
            using (var stream = new FileStream(log.LogFile, FileMode.Append, FileAccess.Write, FileShare.Read))
            {
                stream.Write(bytes); //lo escribo al final del archivo
            }

            // Recién ahora se actualiza el índice: si la escritura falla, el estado en memoria no miente.
            log.Positions.Add(log.Length); //actualizo donde quedo
            log.Length += bytes.Length;
            return ValueTask.FromResult(next);
        }
    }

    /// LEER
    public async IAsyncEnumerable<Record> ReadAsync(
        string topic, int partition, long fromOffset, int max, [EnumeratorCancellation] CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(fromOffset);
        ArgumentOutOfRangeException.ThrowIfNegative(max);

        var log = Get(topic, partition);

        // Se mira el índice una sola vez, bajo candado, y después se suelta. leer de disco
        // puede tardar y no hay razón para bloquear a quien quiera escribir mientras tanto.
        long startPosition;
        int count;

        // calcula desde donde y cuantos
        lock (log.Gate)
        {
            int start = (int)Math.Min(fromOffset, log.Positions.Count);
            count = Math.Min(max, log.Positions.Count - start);
            startPosition = count == 0 ? 0 : log.Positions[start];
        }

        if (count == 0)
        {
            yield break;
        }

        using var stream = new FileStream(
            log.LogFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, bufferSize: 8192, useAsync: true);
        stream.Seek(startPosition, SeekOrigin.Begin);

        // Los registros están uno pegado al otro, así que desde la posición del primero
        // alcanza con leerlos en orden. Cada uno arranca con su propio largo.
        var header = new byte[sizeof(int)];
        for (int i = 0; i < count; i++)
        {
            ct.ThrowIfCancellationRequested();

            await stream.ReadExactlyAsync(header, ct).ConfigureAwait(false);
            int payloadLength = BinaryPrimitives.ReadInt32LittleEndian(header);
            if (payloadLength < 0 || payloadLength > RecordCodec.MaxRecordSize)
            {
                throw new InvalidDataException(
                    $"Largo de registro inválido ({payloadLength}) en {log.LogFile}, posición {stream.Position - header.Length}.");
            }

            var buffer = new byte[header.Length + payloadLength];
            header.CopyTo(buffer.AsSpan());
            await stream.ReadExactlyAsync(buffer.AsMemory(header.Length), ct).ConfigureAwait(false);

            if (RecordCodec.TryDecode(buffer, out var decoded, out _) != DecodeStatus.Ok || decoded is null)
            {
                throw new InvalidDataException(
                    $"Registro ilegible en {log.LogFile}, posición {stream.Position - buffer.Length}.");
            }

            yield return decoded;
        }
    }

    /// <inheritdoc />
    public long HighWatermark(string topic, int partition)
    {
        var log = Get(topic, partition);
        lock (log.Gate)
        {
            return log.HighWatermark;
        }
    }

    /// <inheritdoc />
    public long EndOffset(string topic, int partition)
    {
        var log = Get(topic, partition);
        lock (log.Gate)
        {
            return log.Positions.Count - 1;
        }
    }

    /// <inheritdoc />
    public void AdvanceHighWatermark(string topic, int partition, long offset)
    {
        var log = Get(topic, partition);
        lock (log.Gate)
        {
            long end = log.Positions.Count - 1;
            ArgumentOutOfRangeException.ThrowIfGreaterThan(offset, end);
            log.HighWatermark = Math.Max(log.HighWatermark, offset);
        }
    }

    /// BORRAR LO NO CONFIRMADO
    public ValueTask TruncateAsync(string topic, int partition, long toOffset, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(toOffset, Record.Unassigned);
        ct.ThrowIfCancellationRequested();

        var log = Get(topic, partition);
        lock (log.Gate)
        {
            if (toOffset < log.HighWatermark)
            {
                throw new InvalidOperationException(
                    $"No se puede truncar a {toOffset}: el high watermark es {log.HighWatermark}.");
            }

            int keep = (int)Math.Min(toOffset + 1, log.Positions.Count);
            if (keep == log.Positions.Count)
            {
                return ValueTask.CompletedTask;
            }

            // Si quiero quedarme con los offsets 0 y 1, el primero que se va es el 2
            // y la posición donde arranca el registro 2 es exactamente el nuevo tamaño del archivo
            // Recortar el archivo ahí borra el 2 y todo lo que venga después, de una
            long newLength = log.Positions[keep];
            log.Positions.RemoveRange(keep, log.Positions.Count - keep);
            log.Length = newLength;

            using var stream = new FileStream(log.LogFile, FileMode.Open, FileAccess.Write, FileShare.Read);
            stream.SetLength(newLength);
        }

        return ValueTask.CompletedTask;
    }

    private PartitionLog Get(string topic, int partition)
    {
        // El nombre del tópico se usa como nombre de carpeta Y validarlo evita rutas tipo "../".
        if (!TopicConfig.IsValidName(topic))
        {
            throw new ArgumentException($"Nombre de tópico inválido: '{topic}'.", nameof(topic));
        }

        ArgumentOutOfRangeException.ThrowIfNegative(partition);
        return _partitions.GetOrAdd(
            (topic, partition), key => new PartitionLog(DataDir, key.Topic, key.Partition));
    }

    /// <summary>Estado de una partición: dónde están sus archivos y qué hay escrito.</summary>
    private sealed class PartitionLog
    {
        public PartitionLog(string dataDir, string topic, int partition)
        {
            DirectoryPath = Path.Combine(dataDir, topic, partition.ToString(CultureInfo.InvariantCulture));
            string segment = 0L.ToString(SegmentNameFormat, CultureInfo.InvariantCulture) + LogExtension;
            LogFile = Path.Combine(DirectoryPath, segment);
        }

        public Lock Gate { get; } = new();

        public string DirectoryPath { get; }

        public string LogFile { get; }

        /// <summary>Índice en memoria: la posición <c>i</c> guarda en qué byte del archivo empieza el offset <c>i</c>.</summary>
        public List<long> Positions { get; } = [];

        /// <summary>Bytes escritos en el segmento, o sea dónde va a caer el próximo registro.</summary>
        public long Length { get; set; }

        public long HighWatermark { get; set; } = -1;
    }
}
