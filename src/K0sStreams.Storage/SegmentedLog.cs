using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Globalization;
using System.Runtime.CompilerServices;
using K0sStreams.Contracts;

namespace K0sStreams.Storage;

/// <summary>
/// Log append-only en disco. Reemplaza a <c>InMemoryLog</c>.
/// Cada tópico vive en <c>{DataDir}/{topic}/</c>, hoy con un único segmento
/// <c>00000000000000000000.log</c>.
/// </summary>
/// <remarks>
/// Estado: Escribe con <c>fsync</c> y reconstruye su índice al arrancar.
/// Todavía no rota segmentos, no tiene índice en disco,
/// no agrupa los <c>fsync</c> y el high watermark vive solo en memoria
/// </remarks>
public sealed class SegmentedLog : ILog
{
    /// <summary>
    /// un tópico es un único log, sin particiones. <see cref="ILog"/> todavía lleva el
    /// parámetro, así que se fija acá en un solo lugar; cuando el contrato lo pierda, se borra esto.
    /// </summary>
    private const int SingleLogPartition = 0;

    private const string LogExtension = ".log";

    /// <summary>Nombre del segmento: el primer offset que contiene, con 20 dígitos.</summary>
    private const string SegmentNameFormat = "D20";

    private readonly ConcurrentDictionary<string, TopicLog> _topics = new(StringComparer.Ordinal);

    /// <param name="dataDir">Carpeta raíz de los datos. En Docker es <c>/data</c> (configuración <c>Broker:DataDir</c>).</param>
    public SegmentedLog(string dataDir)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dataDir);
        DataDir = dataDir;
    }

    /// <summary>Carpeta raíz donde se guardan los segmentos.</summary>
    public string DataDir { get; }

    /// <inheritdoc />
    public ValueTask<long> AppendAsync(string topic, int partition, Record record, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(record);
        ct.ThrowIfCancellationRequested();

        var log = Get(topic, partition);
        lock (log.Gate)
        {
            // El offset que toca es la cantidad de registros que ya hay: los offsets empiezan en 0
            // y son contiguos, así que el índice de la lista ES el offset.
            long next = log.Positions.Count;
            if (record.Offset != Record.Unassigned && record.Offset != next)
            {
                throw new ArgumentException($"Se esperaba el offset {next} y llegó {record.Offset}.", nameof(record));
            }

            byte[] bytes = RecordCodec.Encode(record with { Offset = next });

            bool creating = !File.Exists(log.LogFile);
            Directory.CreateDirectory(log.DirectoryPath);
            using (var stream = new FileStream(log.LogFile, FileMode.Append, FileAccess.Write, FileShare.Read))
            {
                stream.Write(bytes);

                // El contrato de ILog dice que el offset se devuelve cuando el registro está EN DISCO.
                // Sin este true los bytes quedarían en el caché del sistema operativo y un corte de luz
                // los perdería. Es la línea que sostiene "no se pierden mensajes confirmados".
                stream.Flush(flushToDisk: true);
            }

            if (creating)
            {
                // Sincronizar el archivo no alcanza la primera vez: si su entrada de directorio sigue
                // en memoria, tras un corte el contenido está a salvo pero el archivo no existe.
                DirectorySync.Flush(log.DirectoryPath);
                DirectorySync.Flush(DataDir);
            }

            // Recién ahora se actualiza el índice: si la escritura falla, el estado en memoria no miente.
            log.Positions.Add(log.Length);
            log.Length += bytes.Length;
            return ValueTask.FromResult(next);
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<Record> ReadAsync(
        string topic, int partition, long fromOffset, int max, [EnumeratorCancellation] CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(fromOffset);
        ArgumentOutOfRangeException.ThrowIfNegative(max);

        var log = Get(topic, partition);

        // Se mira el índice una sola vez, bajo candado, y después se suelta: leer de disco puede
        // tardar y no hay razón para bloquear a quien quiera escribir mientras tanto.
        long startPosition;
        int count;
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

        // Los registros están uno pegado al otro, así que desde la posición del primero alcanza
        // con leerlos en orden. Cada uno arranca con su propio largo.
        var header = new byte[sizeof(int)];
        for (int i = 0; i < count; i++)
        {
            ct.ThrowIfCancellationRequested();

            await stream.ReadExactlyAsync(header, ct).ConfigureAwait(false);
            int payloadLength = BinaryPrimitives.ReadInt32LittleEndian(header);
            if (!IsPlausibleLength(payloadLength))
            {
                throw Damaged(log.LogFile, stream.Position - header.Length, $"el largo {payloadLength} no tiene sentido");
            }

            var buffer = new byte[header.Length + payloadLength];
            header.CopyTo(buffer.AsSpan());
            await stream.ReadExactlyAsync(buffer.AsMemory(header.Length), ct).ConfigureAwait(false);

            if (RecordCodec.TryDecode(buffer, out var decoded, out _) != DecodeStatus.Ok || decoded is null)
            {
                throw Damaged(log.LogFile, stream.Position - buffer.Length, "el CRC no cierra");
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

    /// <inheritdoc />
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

            // La posición del primer registro que se va es exactamente el nuevo largo del archivo.
            long newLength = log.Positions[keep];
            log.Positions.RemoveRange(keep, log.Positions.Count - keep);
            log.Length = newLength;
            Cut(log.LogFile, newLength);
        }

        return ValueTask.CompletedTask;
    }

    /// <summary>Recorta el archivo y confirma el recorte en disco.</summary>
    private static void Cut(string file, long length)
    {
        using var stream = new FileStream(file, FileMode.Open, FileAccess.Write, FileShare.Read);
        stream.SetLength(length);
        stream.Flush(flushToDisk: true);
    }

    /// <summary>Un largo de registro que podría ser real; cualquier otro indica bytes dañados.</summary>
    private static bool IsPlausibleLength(int payloadLength) =>
        payloadLength >= RecordCodec.HeaderSize - sizeof(int) && payloadLength <= RecordCodec.MaxRecordSize - sizeof(int);

    private static InvalidDataException Damaged(string file, long position, string detalle) =>
        new($"Registro dañado en '{file}', byte {position}: {detalle}.");

    private TopicLog Get(string topic, int partition)
    {
        // El nombre del tópico se usa como nombre de carpeta: validarlo evita rutas tipo "../".
        if (!TopicConfig.IsValidName(topic))
        {
            throw new ArgumentException($"Nombre de tópico inválido: '{topic}'.", nameof(topic));
        }

        // no hay particiones. Se rechaza cualquier otra en lugar de ignorarla en silencio.
        ArgumentOutOfRangeException.ThrowIfNotEqual(partition, SingleLogPartition);

        var log = _topics.GetOrAdd(topic, name => new TopicLog(DataDir, name));
        lock (log.Gate)
        {
            if (!log.Recovered)
            {
                RebuildIndex(log);
                log.Recovered = true;
            }
        }

        return log;
    }

    /// <summary>
    /// Reconstruye el índice en memoria leyendo el segmento. Corre una sola vez por tópico, la
    /// primera vez que alguien lo usa; hacerlo al arrancar el proceso lo demoraría y las sondas de
    /// Kubernetes matan un pod que tarda.
    /// </summary>
    private static void RebuildIndex(TopicLog log)
    {
        if (!File.Exists(log.LogFile))
        {
            return;
        }

        long valid;
        long total;
        using (var stream = new FileStream(log.LogFile, FileMode.Open, FileAccess.Read, FileShare.Read))
        {
            total = stream.Length;
            valid = ScanRecords(log, stream);
        }

        log.Length = valid;
        if (valid < total)
        {
            // Lo que sobra es una escritura que quedó por la mitad: el proceso murió escribiéndola.
            // Se descarta sin culpa, porque ese AppendAsync nunca llegó a devolver un offset.
            Cut(log.LogFile, valid);
        }
    }

    /// <summary>Recorre el segmento anotando cada registro y devuelve hasta dónde llegó lo válido.</summary>
    private static long ScanRecords(TopicLog log, FileStream stream)
    {
        var header = new byte[sizeof(int)];
        long position = 0;

        while (stream.Length - position >= header.Length)
        {
            stream.Position = position;
            stream.ReadExactly(header);
            int payloadLength = BinaryPrimitives.ReadInt32LittleEndian(header);
            if (!IsPlausibleLength(payloadLength))
            {
                throw Damaged(log.LogFile, position, $"el largo {payloadLength} no tiene sentido");
            }

            int size = header.Length + payloadLength;
            if (stream.Length - position < size)
            {
                break; // Falta la cola del registro: escritura cortada.
            }

            var buffer = new byte[size];
            stream.Position = position;
            stream.ReadExactly(buffer);

            var status = RecordCodec.TryDecode(buffer, out var decoded, out int consumed);
            if (status == DecodeStatus.Incomplete)
            {
                break;
            }

            if (status != DecodeStatus.Ok || decoded is null)
            {
                // CRC que no cierra: no es una escritura cortada, es daño sobre bytes ya guardados.
                // Se falla fuerte en vez de descartar: perder datos en silencio es peor que no arrancar.
                throw Damaged(log.LogFile, position, "el CRC no cierra");
            }

            if (decoded.Offset != log.Positions.Count)
            {
                throw Damaged(
                    log.LogFile, position, $"dice offset {decoded.Offset} y le corresponde {log.Positions.Count}");
            }

            log.Positions.Add(position);
            position += consumed;
        }

        return position;
    }

    /// <summary>Estado de un tópico: dónde están sus archivos y qué hay escrito.</summary>
    private sealed class TopicLog
    {
        public TopicLog(string dataDir, string topic)
        {
            DirectoryPath = Path.Combine(dataDir, topic);
            string segment = 0L.ToString(SegmentNameFormat, CultureInfo.InvariantCulture) + LogExtension;
            LogFile = Path.Combine(DirectoryPath, segment);
        }

        public Lock Gate { get; } = new();

        public string DirectoryPath { get; }

        public string LogFile { get; }

        /// <summary>Índice en memoria: la posición <c>i</c> guarda en qué byte del archivo empieza el offset <c>i</c>.</summary>
        public List<long> Positions { get; } = [];

        /// <summary>Bytes válidos del segmento, o sea dónde va a caer el próximo registro.</summary>
        public long Length { get; set; }

        public long HighWatermark { get; set; } = -1;

        /// <summary>Si ya se leyó el archivo para reconstruir <see cref="Positions"/>.</summary>
        public bool Recovered { get; set; }
    }
}
