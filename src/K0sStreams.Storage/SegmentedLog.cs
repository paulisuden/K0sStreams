using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Globalization;
using System.Runtime.CompilerServices;
using K0sStreams.Contracts;

namespace K0sStreams.Storage;

/// <summary>
/// Log append-only en disco. Reemplaza a <c>InMemoryLog</c>.
/// Cada tópico vive en <c>{DataDir}/{topic}/</c>, partido en <see cref="Segment"/> de tamaño
/// acotado, cada uno con su <see cref="SparseIndex"/>, más un archivo <c>checkpoint</c> con el
/// high watermark.
/// </summary>
/// <remarks>
/// Estado: escribe con <c>fsync</c>, rota segmentos, los indexa en disco de forma dispersa y
/// recuerda el high watermark entre reinicios. Todavía no agrupa los <c>fsync</c> y no borra
/// segmentos viejos.
/// </remarks>
public sealed class SegmentedLog : ILog
{
    /// <summary>Tamaño al que se cierra un segmento y se abre otro, si no se indica otro.</summary>
    public const long DefaultSegmentBytes = 64L * 1024 * 1024;

    /// <summary>
    /// Tope de un segmento: las posiciones del índice disperso son int32, y hay que dejarle lugar
    /// al registro más grande, que se escribe aunque pase el límite.
    /// </summary>
    public const long MaxSegmentBytes = int.MaxValue - RecordCodec.MaxRecordSize;

    /// <summary>
    /// un tópico es un único log, sin particiones. <see cref="ILog"/> todavía lleva el
    /// parámetro, así que se fija acá en un solo lugar; cuando el contrato lo pierda, se borra esto.
    /// </summary>
    private const int SingleLogPartition = 0;

    /// <summary>Archivo con el high watermark del tópico, al lado de los segmentos.</summary>
    private const string CheckpointFileName = "checkpoint";

    private readonly ConcurrentDictionary<string, TopicLog> _topics = new(StringComparer.Ordinal);
    private readonly long _segmentBytes;

    /// <param name="dataDir">Carpeta raíz de los datos. En Docker es <c>/data</c> (configuración <c>Broker:DataDir</c>).</param>
    /// <param name="segmentBytes">Tamaño máximo de un segmento. Los tests lo bajan para generar varios.</param>
    public SegmentedLog(string dataDir, long segmentBytes = DefaultSegmentBytes)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dataDir);
        ArgumentOutOfRangeException.ThrowIfLessThan(segmentBytes, RecordCodec.HeaderSize);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(segmentBytes, MaxSegmentBytes);
        DataDir = dataDir;
        _segmentBytes = segmentBytes;
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
            long next = log.NextOffset;
            if (record.Offset != Record.Unassigned && record.Offset != next)
            {
                throw new ArgumentException($"Se esperaba el offset {next} y llegó {record.Offset}.", nameof(record));
            }

            byte[] bytes = RecordCodec.Encode(record with { Offset = next });

            Directory.CreateDirectory(log.DirectoryPath);
            var segment = SegmentFor(log, bytes.Length);
            bool creating = !File.Exists(segment.FilePath);

            using (var stream = new FileStream(segment.FilePath, FileMode.Append, FileAccess.Write, FileShare.Read))
            {
                stream.Write(bytes);

                // El contrato de ILog dice que el offset se devuelve cuando el registro está EN DISCO.
                // Sin este true los bytes quedarían en el caché del sistema operativo y un corte de luz
                // los perdería. Es la línea que sostiene "no se pierden mensajes confirmados".
                stream.Flush(flushToDisk: true);
            }

            if (creating)
            {
                // Vale para cada segmento nuevo, no solo para el primero: si su entrada de directorio
                // sigue en memoria, tras un corte el contenido está a salvo pero el archivo no existe.
                DirectorySync.Flush(log.DirectoryPath);
                DirectorySync.Flush(DataDir);
            }

            // Recién ahora se actualiza el índice: si la escritura falla, el estado en memoria no miente.
            segment.Append(segment.Length, bytes.Length);
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

        // Se arma el recorrido una sola vez bajo candado y después se suelta: leer de disco puede
        // tardar y no hay razón para bloquear a quien quiera escribir mientras tanto.
        var tramos = PlanRead(log, fromOffset, max);

        foreach (var tramo in tramos)
        {
            using var stream = new FileStream(
                tramo.FilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, bufferSize: 8192, useAsync: true);
            stream.Seek(tramo.Start, SeekOrigin.Begin);

            // Dentro de un segmento los registros están pegados uno al otro, así que desde la posición
            // del primero alcanza con leerlos en orden. Cada uno arranca con su propio largo.
            var header = new byte[sizeof(int)];

            // El índice es disperso: deja parado en un registro anterior al pedido. Los que sobran
            // se saltean leyendo solo su largo, sin decodificarlos, porque no se van a devolver.
            for (long salteados = 0; salteados < tramo.Skip; salteados++)
            {
                ct.ThrowIfCancellationRequested();

                int saltar = await ReadSizeAsync(stream, header, tramo.FilePath, ct).ConfigureAwait(false);
                stream.Seek(saltar - header.Length, SeekOrigin.Current);
            }

            for (int i = 0; i < tramo.Count; i++)
            {
                ct.ThrowIfCancellationRequested();

                int size = await ReadSizeAsync(stream, header, tramo.FilePath, ct).ConfigureAwait(false);
                var buffer = new byte[size];
                header.CopyTo(buffer.AsSpan());
                await stream.ReadExactlyAsync(buffer.AsMemory(header.Length), ct).ConfigureAwait(false);

                if (RecordCodec.TryDecode(buffer, out var decoded, out _) != DecodeStatus.Ok || decoded is null)
                {
                    throw Damaged(tramo.FilePath, stream.Position - buffer.Length, "el CRC no cierra");
                }

                yield return decoded;
            }
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
            return log.NextOffset - 1;
        }
    }

    /// <inheritdoc />
    public void AdvanceHighWatermark(string topic, int partition, long offset)
    {
        var log = Get(topic, partition);
        lock (log.Gate)
        {
            ArgumentOutOfRangeException.ThrowIfGreaterThan(offset, log.NextOffset - 1);

            long advanced = Math.Max(log.HighWatermark, offset);
            if (advanced == log.HighWatermark)
            {
                return;
            }

            // Primero el disco y después la memoria, igual que con el índice: si la escritura
            // falla, el valor en memoria no afirma algo que el disco no respalda.
            WriteCheckpoint(log, advanced);
            log.HighWatermark = advanced;
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

            long firstGone = toOffset + 1;
            if (firstGone >= log.NextOffset)
            {
                return ValueTask.CompletedTask;
            }

            // Los segmentos enteramente posteriores al corte se borran; del que lo contiene se
            // recorta la cola. Si el corte cae justo en su primer offset, queda vacío en lugar de
            // borrarse: su offset base ya es el próximo a escribir, así que se llena solo.
            int cutIndex = log.IndexOf(firstGone);
            for (int i = log.Segments.Count - 1; i > cutIndex; i--)
            {
                File.Delete(log.Segments[i].FilePath);
                log.Segments[i].Index.Discard();
                log.Segments.RemoveAt(i);
            }

            var cut = log.Segments[cutIndex];
            long newLength = ResolvePosition(cut, firstGone);
            cut.DropFrom(firstGone);
            cut.Length = newLength;
            Cut(cut.FilePath, newLength);
        }

        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Dónde entra el próximo registro. Rota si el segmento activo se pasaría del límite, salvo que
    /// esté vacío: un registro más grande que el límite tiene que poder guardarse igual, o no habría
    /// segmento que lo aceptara nunca.
    /// </summary>
    private Segment SegmentFor(TopicLog log, int recordSize)
    {
        if (log.Segments.Count == 0)
        {
            return log.Add(0);
        }

        var active = log.Segments[^1];
        return active.IsEmpty || active.Length + recordSize <= _segmentBytes
            ? active
            : log.Add(active.NextOffset);
    }

    /// <summary>
    /// Qué archivo abrir, desde qué byte, cuántos registros saltear desde ahí y cuántos leer. Se
    /// arma solo con el índice en memoria, sin tocar disco, para poder soltar el candado antes.
    /// </summary>
    private static List<(string FilePath, long Start, long Skip, int Count)> PlanRead(
        TopicLog log, long fromOffset, int max)
    {
        var tramos = new List<(string FilePath, long Start, long Skip, int Count)>();
        lock (log.Gate)
        {
            long from = Math.Min(fromOffset, log.NextOffset);
            long remaining = Math.Min(max, log.NextOffset - from);

            for (int i = log.IndexOf(from); remaining > 0 && i >= 0 && i < log.Segments.Count; i++)
            {
                var segment = log.Segments[i];
                int take = (int)Math.Min(remaining, segment.NextOffset - from);
                if (take <= 0)
                {
                    continue;
                }

                var anchor = segment.Anchor(from);
                tramos.Add((segment.FilePath, anchor.Position, from - anchor.Offset, take));
                from += take;
                remaining -= take;
            }
        }

        return tramos;
    }

    /// <summary>
    /// Byte exacto donde empieza ese offset. El índice es disperso, así que deja parado en un
    /// registro anterior y de ahí se avanza leyendo largos. Toca disco, pero solo al truncar, que
    /// ya borra archivos y hace <c>fsync</c>.
    /// </summary>
    private static long ResolvePosition(Segment segment, long offset)
    {
        var anchor = segment.Anchor(offset);
        using var stream = new FileStream(segment.FilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        stream.Position = anchor.Position;

        var header = new byte[sizeof(int)];
        for (long current = anchor.Offset; current < offset; current++)
        {
            // En dos pasos a propósito: ReadSize ya dejó la posición después del largo, y meterlo
            // dentro del += la leería de antes y se saltearía esos cuatro bytes.
            int size = ReadSize(stream, header, segment.FilePath);
            stream.Position += size - header.Length;
        }

        return stream.Position;
    }

    /// <summary>Lee el largo con el que arranca un registro y devuelve cuánto ocupa en total.</summary>
    private static int ReadSize(FileStream stream, byte[] header, string file)
    {
        long position = stream.Position;
        stream.ReadExactly(header);
        return PlausibleSize(BinaryPrimitives.ReadInt32LittleEndian(header), file, position);
    }

    /// <inheritdoc cref="ReadSize" />
    private static async ValueTask<int> ReadSizeAsync(
        FileStream stream, byte[] header, string file, CancellationToken ct)
    {
        long position = stream.Position;
        await stream.ReadExactlyAsync(header, ct).ConfigureAwait(false);
        return PlausibleSize(BinaryPrimitives.ReadInt32LittleEndian(header), file, position);
    }

    /// <summary>Recorta el archivo y confirma el recorte en disco.</summary>
    private static void Cut(string file, long length)
    {
        using var stream = new FileStream(file, FileMode.Open, FileAccess.Write, FileShare.Read);
        stream.SetLength(length);
        stream.Flush(flushToDisk: true);
    }

    /// <summary>
    /// Guarda el high watermark. A diferencia del log, no lleva <c>fsync</c>: se llama una vez por
    /// escritura y pagarlo duplicaría el costo. Perder la última actualización ante un corte es seguro,
    /// porque deja al broker creyendo que tiene menos confirmado de lo que tiene, nunca más.
    /// </summary>
    private static void WriteCheckpoint(TopicLog log, long offset) =>
        File.WriteAllText(log.CheckpointFile, offset.ToString(CultureInfo.InvariantCulture));

    /// <summary>
    /// Lee el high watermark guardado. Un archivo que falta, vacío o ilegible vale -1: es un número
    /// recalculable, no datos perdidos, y la replicación lo vuelve a subir en la próxima ronda.
    /// </summary>
    private static long ReadCheckpoint(TopicLog log) =>
        File.Exists(log.CheckpointFile)
        && long.TryParse(File.ReadAllText(log.CheckpointFile), NumberStyles.Integer, CultureInfo.InvariantCulture, out long saved)
            ? saved
            : Record.Unassigned;

    /// <summary>
    /// Tamaño total del registro a partir del largo que trae adelante, si ese largo podría ser
    /// real. Cualquier otro valor son bytes dañados.
    /// </summary>
    private static int PlausibleSize(int payloadLength, string file, long position) =>
        payloadLength >= RecordCodec.HeaderSize - sizeof(int) && payloadLength <= RecordCodec.MaxRecordSize - sizeof(int)
            ? sizeof(int) + payloadLength
            : throw Damaged(file, position, $"el largo {payloadLength} no tiene sentido");

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
    /// Reconstruye el índice en memoria leyendo todos los segmentos en orden. Corre una sola vez por
    /// tópico, la primera vez que alguien lo usa; hacerlo al arrancar el proceso lo demoraría y las
    /// sondas de Kubernetes matan un pod que tarda.
    /// </summary>
    private static void RebuildIndex(TopicLog log)
    {
        if (Directory.Exists(log.DirectoryPath))
        {
            var baseOffsets = SegmentBaseOffsets(log.DirectoryPath);
            for (int i = 0; i < baseOffsets.Count; i++)
            {
                var segment = new Segment(log.DirectoryPath, baseOffsets[i]);

                // Los offsets son contiguos: cada segmento tiene que empezar donde terminó el anterior.
                if (segment.BaseOffset != log.NextOffset)
                {
                    throw Damaged(segment.FilePath, 0, $"debería empezar en el offset {log.NextOffset}");
                }

                log.Segments.Add(segment);
                ScanSegment(log, segment, isLast: i == baseOffsets.Count - 1);
            }
        }

        // El high watermark se guarda aparte. Se acota al end offset por si el checkpoint quedó
        // adelantado: nunca se puede dar por confirmado algo que no está escrito.
        log.HighWatermark = Math.Min(ReadCheckpoint(log), log.NextOffset - 1);
    }

    /// <summary>Offsets base de los segmentos que hay en la carpeta, de menor a mayor.</summary>
    private static List<long> SegmentBaseOffsets(string directoryPath)
    {
        var baseOffsets = new List<long>();
        foreach (string path in Directory.EnumerateFiles(directoryPath, "*" + Segment.Extension))
        {
            if (Segment.BaseOffsetFromName(path) is long baseOffset)
            {
                baseOffsets.Add(baseOffset);
            }
        }

        baseOffsets.Sort();
        return baseOffsets;
    }

    private static void ScanSegment(TopicLog log, Segment segment, bool isLast)
    {
        long total;
        long valid;
        using (var stream = new FileStream(segment.FilePath, FileMode.Open, FileAccess.Read, FileShare.Read))
        {
            total = stream.Length;
            valid = Scan(log, segment, stream);
        }

        segment.Length = valid;
        if (valid == total)
        {
            return;
        }

        // Una escritura solo puede quedar cortada en el segmento activo: la rotación ocurre entre
        // registros, así que un segmento cerrado siempre termina justo donde termina uno.
        if (!isLast)
        {
            throw Damaged(segment.FilePath, valid, "un segmento cerrado no puede terminar a mitad de un registro");
        }

        // El proceso murió escribiendo. Se descarta sin culpa: ese AppendAsync nunca devolvió un offset.
        Cut(segment.FilePath, valid);

        // Y el recorte puede haber dejado un ancla apuntando a un registro que ya no está.
        segment.Index.DropFrom(segment.RecordCount);
    }

    /// <summary>
    /// Recorre el segmento desde donde haga falta y devuelve hasta dónde llega lo válido. Con un
    /// índice que valida alcanza leer desde su última entrada: todo lo anterior ya está descrito
    /// ahí, y es lo que hace que arrancar no cueste releer el log completo.
    /// </summary>
    private static long Scan(TopicLog log, Segment segment, FileStream stream)
    {
        if (segment.Index.TryLoad(stream.Length))
        {
            var from = segment.Index.Last;
            try
            {
                long valid = ScanRecords(log, segment, stream, from);

                // No haber podido leer ni un registro significa que la última entrada del índice no
                // cae donde empieza uno. El que está mal es el índice, no el log: aceptar ese
                // resultado haría recortar el log ahí, y eso sí perdería datos buenos.
                if (valid > from.Position)
                {
                    return valid;
                }
            }
            catch (InvalidDataException)
            {
                // Lo mismo, detectado un paso antes: donde apunta el índice no hay un registro.
            }
        }

        // El índice no sirve o no está: se rehace recorriendo el log, la única fuente de verdad.
        segment.Index.Discard();
        return ScanRecords(log, segment, stream, from: (0, 0));
    }

    /// <summary>
    /// Recorre un segmento desde un punto conocido anotando cada registro, y devuelve hasta dónde
    /// llegó lo válido.
    /// </summary>
    private static long ScanRecords(
        TopicLog log, Segment segment, FileStream stream, (long RelativeOffset, long Position) from)
    {
        // El punto de partida ya tiene registros detrás y el índice sabe cuántos. Sin esto, los
        // offsets que vienen no coincidirían con los que el recorrido espera.
        segment.RecordCount = from.RelativeOffset;

        var header = new byte[sizeof(int)];
        long position = from.Position;

        while (stream.Length - position >= header.Length)
        {
            stream.Position = position;
            int size = ReadSize(stream, header, segment.FilePath);
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
                throw Damaged(segment.FilePath, position, "el CRC no cierra");
            }

            if (decoded.Offset != log.NextOffset)
            {
                throw Damaged(
                    segment.FilePath, position, $"dice offset {decoded.Offset} y le corresponde {log.NextOffset}");
            }

            segment.Append(position, consumed);
            position += consumed;
        }

        return position;
    }

    /// <summary>Estado de un tópico: sus segmentos en orden y hasta dónde está confirmado.</summary>
    private sealed class TopicLog
    {
        public TopicLog(string dataDir, string topic)
        {
            DirectoryPath = Path.Combine(dataDir, topic);
            CheckpointFile = Path.Combine(DirectoryPath, CheckpointFileName);
        }

        public Lock Gate { get; } = new();

        public string DirectoryPath { get; }

        /// <summary>Archivo donde sobrevive el high watermark entre reinicios.</summary>
        public string CheckpointFile { get; }

        /// <summary>Segmentos ordenados por offset base. El último es donde se escribe.</summary>
        public List<Segment> Segments { get; } = [];

        /// <summary>Último offset confirmado por quórum. Se refleja en <see cref="CheckpointFile"/>.</summary>
        public long HighWatermark { get; set; } = -1;

        /// <summary>Si ya se leyeron los segmentos para reconstruir el índice.</summary>
        public bool Recovered { get; set; }

        /// <summary>Offset que le toca al próximo registro. En un log vacío es 0.</summary>
        public long NextOffset => Segments.Count == 0 ? 0 : Segments[^1].NextOffset;

        public Segment Add(long baseOffset)
        {
            var segment = new Segment(DirectoryPath, baseOffset);
            Segments.Add(segment);
            return segment;
        }

        /// <summary>Índice del segmento que contiene ese offset, o -1 si no está en ninguno.</summary>
        public int IndexOf(long offset)
        {
            for (int i = Segments.Count - 1; i >= 0; i--)
            {
                if (Segments[i].Contains(offset))
                {
                    return i;
                }
            }

            return -1;
        }
    }
}
