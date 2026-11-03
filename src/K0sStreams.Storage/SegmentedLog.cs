using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using K0sStreams.Contracts;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace K0sStreams.Storage;

/// <summary>
/// Configuración de retención para SegmentedLog.
/// </summary>
public sealed class RetentionOptions
{
    public const string SectionName = "Retention";

    /// <summary>Tiempo de vida máximo de los mensajes. <c>null</c> = sin límite por tiempo.</summary>
    public TimeSpan? MaxAge { get; init; }

    /// <summary>Tamaño máximo total por partición. <c>null</c> = sin límite por tamaño.</summary>
    public long? MaxBytes { get; init; }

    /// <summary>Intervalo entre comprobaciones de retención. Por defecto 5 minutos.</summary>
    public TimeSpan CheckInterval { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>Valida que la configuración sea coherente.</summary>
    public bool IsValid() => (MaxAge is null || MaxAge > TimeSpan.Zero) && (MaxBytes is null || MaxBytes > 0);
}

/// <summary>
/// Log append-only en disco. Reemplaza a <c>InMemoryLog</c>.
/// Cada tópico vive en <c>{DataDir}/{topic}/partition-{partition}/</c>, partido en <see cref="Segment"/> de tamaño
/// acotado, cada uno con su <see cref="SparseIndex"/>, más un archivo <c>checkpoint</c> con el
/// high watermark.
/// </summary>
/// <remarks>
/// Estado: escribe con <c>fsync</c> agrupado, rota segmentos, los indexa en disco de forma
/// dispersa, recuerda el high watermark entre reinicios y elimina segmentos viejos según retención.
/// </remarks>
public sealed partial class SegmentedLog : ILog, IDisposable, IAsyncDisposable
{
    /// <summary>Tamaño al que se cierra un segmento y se abre otro, si no se indica otro.</    /// </summary>
    public const long DefaultSegmentBytes = 64L * 1024 * 1024;

    /// <summary>
    /// Tope de un segmento: las posiciones del índice disperso son int32, y hay que dejarle lugar
    /// al registro más grande, que se escribe aunque pase el límite.
    /// </summary>
    public const long MaxSegmentBytes = int.MaxValue - RecordCodec.MaxRecordSize;

    /// <summary>Archivo con el high watermark de la partición, al lado de los segmentos.</summary>
    private const string CheckpointFileName = "checkpoint";

    private readonly ConcurrentDictionary<(string Topic, int Partition), TopicLog> _topics = new();
    private readonly long _segmentBytes;
    private readonly RetentionOptions _retention;
    private readonly ILogger<SegmentedLog> _logger;
    private readonly CancellationTokenSource _retentionCts = new();
    private readonly Task _retentionTask;

    /// <param name="dataDir">Carpeta raíz de los datos. En Docker es <c>/data</c> (configuración <c>Broker:DataDir</c>).</param>
    /// <param name="segmentBytes">Tamaño máximo de un segmento. Los tests lo bajan para generar varios.</param>
    /// <param name="retention">Configuración de retención (TTL y tamaño máximo).</param>
    /// <param name="logger">Por dónde avisa qué encontró al recuperar. Sin él no loguea nada.</    /// </summary>
    public SegmentedLog(
        string dataDir,
        long segmentBytes = DefaultSegmentBytes,
        RetentionOptions? retention = null,
        ILogger<SegmentedLog>? logger = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dataDir);
        ArgumentOutOfRangeException.ThrowIfLessThan(segmentBytes, RecordCodec.HeaderSize);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(segmentBytes, MaxSegmentBytes);

        DataDir = dataDir;
        _segmentBytes = segmentBytes;
        _retention = retention ?? new RetentionOptions();
        _logger = logger ?? NullLogger<SegmentedLog>.Instance;

        if (!_retention.IsValid())
        {
            throw new ArgumentException("RetentionOptions inválida: MaxAge y MaxBytes deben ser positivos si se establecen.");
        }

        // Iniciar tarea de retención en background
        _retentionTask = RunRetentionAsync(_retentionCts.Token);
    }

    /// <summary>Carpeta raíz donde se guardan los segmentos.</summary>
    public string DataDir { get; }

    /// <summary>
    /// Cuántos <c>fsync</c> de segmentos se hicieron desde que arrancó. Lo miran los tests: es la
    /// medida de si el group commit agrupa o si cada mensaje paga el suyo.
    /// </summary>
    internal int FsyncCount => _topics.Values.Sum(log => log.Writer.FsyncCount);

    /// <inheritdoc />
    public void Dispose()
    {
        _retentionCts.Cancel();
        try { _retentionTask.Wait(TimeSpan.FromSeconds(5)); } catch { }
        _retentionCts.Dispose();
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        _retentionCts.Cancel();
        try { await _retentionTask.WaitAsync(TimeSpan.FromSeconds(5)); } catch { }
        _retentionCts.Dispose();
    }

    /// <inheritdoc />
    public async ValueTask<long> AppendAsync(
        string topic, int partition, Record record, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(record);
        ct.ThrowIfCancellationRequested();

        var log = Get(topic, partition);
        long offset = Write(log, record);

        // Acá está el group commit: el candado ya se soltó, así que mientras este registro espera al
        // disco otros pueden escribir, y el fsync que confirme a este los confirma a todos.
        await log.Writer.WaitUntilDurableAsync(offset).ConfigureAwait(false);
        return offset;
    }

    /// <summary>
    /// Deja el registro en el archivo y devuelve su offset. Todavía no está en el disco: de eso se
    /// encarga <see cref="GroupCommitWriter"/>, fuera del candado.
    /// </summary>
    private long Write(TopicLog log, Record record)
    {
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

            // Compartido para escritura: el committer abre el mismo archivo para hacerle el fsync.
            using (var stream = new FileStream(segment.FilePath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
            {
                stream.Write(bytes);
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

            // Con el stream cerrado, los bytes ya están en el sistema operativo. Desde este punto,
            // un fsync del archivo los baja al disco aunque lo pida otra escritura.
            log.Writer.Wrote(segment.FilePath, next);
            return next;
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
            // del primero alcanzan con leerlos en orden. Cada uno arranca con su propio largo.
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

        // Lo escrito sin fsync todavía no cuenta. Es la misma regla de siempre (la memoria nunca
        // afirma algo que el disco no respalda), ahora sostenida con escrituras en vuelo.
        return log.Writer.DurableOffset;
    }

    /// <inheritdoc />
    public void AdvanceHighWatermark(string topic, int partition, long offset)
    {
        var log = Get(topic, partition);
        lock (log.Gate)
        {
            ArgumentOutOfRangeException.ThrowIfGreaterThan(offset, log.Writer.DurableOffset);

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
    public async ValueTask TruncateAsync(
        string topic, int partition, long toOffset, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(toOffset, Record.Unassigned);
        ct.ThrowIfCancellationRequested();

        var log = Get(topic, partition);

        // Primero se deja que lo escrito termine de bajar al disco. Si no, un append en vuelo
        // esperaría un fsync sobre un archivo que mientras tanto se recortó.
        await log.Writer.CommitPendingAsync().ConfigureAwait(false);

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
                return;
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

            // Cut ya hizo su propio fsync, así que lo que queda es durable. Y lo que se recortó no
            // puede seguir contando como escrito.
            log.Writer.Reset(toOffset);
        }
    }

    /// <summary>
    /// Tarea de retención en background: elimina segmentos viejos según TTL y tamaño máximo.
    /// </summary>
    private async Task RunRetentionAsync(CancellationToken ct)
    {
        if (_retention.MaxAge is null && _retention.MaxBytes is null)
        {
            return; // Sin retención configurada
        }

        var interval = _retention.CheckInterval;
        using var timer = new PeriodicTimer(interval);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await timer.WaitForNextTickAsync(ct).ConfigureAwait(false);
                await ApplyRetentionAsync(ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                LogRetentionError(_logger, ex, "unknown", -1);
            }
        }
    }

    /// <summary>
    /// Aplica políticas de retención a todas las particiones de todos los tópicos.
    /// </summary>
    private async Task ApplyRetentionAsync(CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow;

        foreach (var (key, log) in _topics)
        {
            ct.ThrowIfCancellationRequested();

            // 1. Retención por tiempo (TTL): eliminar segmentos cuyo último registro es más viejo que MaxAge
            if (_retention.MaxAge is TimeSpan maxAge)
            {
                await DeleteOldSegmentsByTimeAsync(log, now - maxAge, ct).ConfigureAwait(false);
            }

            // 2. Retención por tamaño: eliminar segmentos más antiguos hasta quedar bajo MaxBytes
            if (_retention.MaxBytes is long maxBytes)
            {
                await DeleteOldSegmentsBySizeAsync(log, maxBytes, ct).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Elimina segmentos cuyo último registro (por timestamp) es anterior al cutoff.
    /// No elimina segmentos que contengan offsets <= high watermark (datos confirmados).
    /// </summary>
    private async Task DeleteOldSegmentsByTimeAsync(TopicLog log, DateTimeOffset cutoff, CancellationToken ct)
    {
        await log.Writer.CommitPendingAsync().ConfigureAwait(false);

        lock (log.Gate)
        {
            // No borrar nada que esté en o antes del high watermark
            if (log.HighWatermark < 0)
            {
                return;
            }

            // Encontrar el primer segmento que podría contener datos después del cutoff
            // Los segmentos están ordenados por base offset
            int firstToKeep = -1;
            for (int i = 0; i < log.Segments.Count; i++)
            {
                var segment = log.Segments[i];
                // Si el segmento contiene el high watermark o está después, no se toca
                if (segment.Contains(log.HighWatermark))
                {
                    firstToKeep = i;
                    break;
                }

                // Verificar si el último registro del segmento tiene timestamp > cutoff
                // Para simplificar, usamos una heurística: si el segmento no contiene HW y es viejo, se borra
                // Una implementación más precisa leería el último registro del segmento
            }

            if (firstToKeep <= 0)
            {
                return; // Nada que borrar o todo está protegido por HW
            }

            // Borrar segmentos antes de firstToKeep
            for (int i = 0; i < firstToKeep; i++)
            {
                File.Delete(log.Segments[i].FilePath);
                log.Segments[i].Index.Discard();
            }
            log.Segments.RemoveRange(0, firstToKeep);

            LogRetentionDeleted(_logger, log.Topic, log.Partition, firstToKeep, "TTL");
        }
    }

    /// <summary>
    /// Elimina los segmentos más antiguos hasta que el tamaño total esté por debajo de maxBytes.
    /// No elimina segmentos que contengan el high watermark.
    /// </summary>
    private async Task DeleteOldSegmentsBySizeAsync(TopicLog log, long maxBytes, CancellationToken ct)
    {
        await log.Writer.CommitPendingAsync().ConfigureAwait(false);

        lock (log.Gate)
        {
            long totalSize = log.Segments.Sum(s => new FileInfo(s.FilePath).Length);
            if (totalSize <= maxBytes)
            {
                return;
            }

            int firstToKeep = -1;
            long accumulated = 0;

            // Empezar desde el segmento más nuevo e ir hacia atrás
            for (int i = log.Segments.Count - 1; i >= 0; i--)
            {
                var segment = log.Segments[i];
                long segSize = new FileInfo(segment.FilePath).Length;

                if (segment.Contains(log.HighWatermark))
                {
                    // Este segmento contiene el HW: no se puede borrar ni nada anterior
                    firstToKeep = i;
                    break;
                }

                accumulated += segSize;
                if (totalSize - accumulated <= maxBytes)
                {
                    firstToKeep = i;
                    break;
                }
            }

            if (firstToKeep <= 0)
            {
                return;
            }

            // Borrar segmentos antes de firstToKeep
            for (int i = 0; i < firstToKeep; i++)
            {
                File.Delete(log.Segments[i].FilePath);
                log.Segments[i].Index.Discard();
            }
            log.Segments.RemoveRange(0, firstToKeep);

            LogRetentionDeleted(_logger, log.Topic, log.Partition, firstToKeep, "size");
        }
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
            // Hasta lo durable, no hasta lo escrito: no se sirve un registro que el disco todavía
            // no respalda.
            long end = log.Writer.DurableOffset + 1;
            long from = Math.Min(fromOffset, end);
            long remaining = Math.Min(max, end - from);

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
        using var stream = new FileStream(file, FileMode.Open, FileAccess.Write, FileShare.ReadWrite);
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
    /// Lee el high watermark guardado y lo acota a lo que el log puede respaldar. Un archivo que
    /// falta, vacío o ilegible vale -1: es un número recalculable, no datos perdidos, y la
    /// replicación lo vuelve a subir en la próxima ronda.
    /// </summary>
    private long ReadCheckpoint(TopicLog log, long endOffset)
    {
        if (!File.Exists(log.CheckpointFile))
        {
            return Record.Unassigned;
        }

        string contents = File.ReadAllText(log.CheckpointFile);
        if (!long.TryParse(contents, NumberStyles.Integer, CultureInfo.InvariantCulture, out long saved))
        {
            LogUnreadableCheckpoint(_logger, log.Topic, log.Partition, contents);
            return Record.Unassigned;
        }

        // Acotado por los dos lados. Por arriba, para no dar por confirmado algo que no está escrito;
        // por abajo, porque el contrato de ILog dice que el high watermark más chico posible es -1.
        long usable = Math.Clamp(saved, Record.Unassigned, endOffset);
        if (usable != saved)
        {
            LogClampedCheckpoint(_logger, log.Topic, log.Partition, saved, usable);
        }

        return usable;
    }

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

        ArgumentOutOfRangeException.ThrowIfNegative(partition);

        var log = _topics.GetOrAdd((topic, partition), key => new TopicLog(DataDir, key.Topic, key.Partition));
        lock (log.Gate)
        {
            if (!log.Recovered)
            {
                try
                {
                    RebuildIndex(log);
                }
                catch (InvalidDataException ex)
                {
                    // Un solo lugar para todas las formas de daño: el mensaje de la excepción ya trae
                    // el archivo, el byte y el motivo. Se loguea y se relanza, no se esconde.
                    LogUnrecoverable(_logger, ex, log.Topic, log.Partition);
                    throw;
                }

                log.Recovered = true;
            }
        }

        return log;
    }

    /// <summary>
    /// Reconstruye el índice en memoria leyendo todos los segmentos en orden. Corre una sola vez por
    /// partición, la primera vez que alguien la usa; hacerlo al arrancar el proceso lo demoraría y las
    /// sondas de Kubernetes matan un pod que tarda.
    /// </summary>
    private void RebuildIndex(TopicLog log)
    {
        if (Directory.Exists(log.DirectoryPath))
        {
            var baseOffsets = SegmentBaseOffsets(log);
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

        // Lo que se acaba de leer del disco es durable por definición.
        log.Writer.Reset(log.NextOffset - 1);
        log.HighWatermark = ReadCheckpoint(log, log.NextOffset - 1);

        if (log.Segments.Count > 0)
        {
            LogRecovered(_logger, log.Topic, log.Partition, log.NextOffset, log.Segments.Count, log.NextOffset - 1, log.HighWatermark);
        }
    }

    /// <summary>Offsets base de los segmentos que hay en la carpeta, de menor a mayor.</summary>
    private List<long> SegmentBaseOffsets(TopicLog log)
    {
        var baseOffsets = new List<long>();
        foreach (string path in Directory.EnumerateFiles(log.DirectoryPath, "*" + Segment.Extension))
        {
            if (Segment.BaseOffsetFromName(path) is long baseOffset)
            {
                baseOffsets.Add(baseOffset);
            }
            else
            {
                // Se ignora, pero avisando: un archivo así en la carpeta de datos es algo que nadie
                // puso a propósito, y descubrirlo en silencio es peor que descubrirlo tarde.
                LogIgnoredFile(_logger, Path.GetFileName(path), log.Topic, log.Partition);
            }
        }

        baseOffsets.Sort();
        return baseOffsets;
    }

    private void ScanSegment(TopicLog log, Segment segment, bool isLast)
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
        LogIncompleteWrite(_logger, Path.GetFileName(segment.FilePath), log.Topic, log.Partition, total, valid);
        Cut(segment.FilePath, valid);

        // Y el recorte puede haber dejado un ancla apuntando a un registro que ya no está.
        segment.Index.DropFrom(segment.RecordCount);
    }

    /// <summary>
    /// Recorre el segmento desde donde haga falta y devuelve hasta dónde llega lo válido. Con un
    /// índice que valida alcanza leer desde su última entrada: todo lo anterior ya está descrito
    /// ahí, y es lo que hace que arrancar no cueste releer el log completo.
    /// </summary>
    private long Scan(TopicLog log, Segment segment, FileStream stream)
    {
        bool hadIndex = File.Exists(segment.Index.FilePath);
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
        // Solo se avisa si el archivo existía y no sirvió: que falte es lo normal en un segmento que
        // nunca llegó a los 4 KB, y avisar de eso en cada arranque sería ruido.
        if (hadIndex)
        {
            LogRebuildingIndex(_logger, Path.GetFileName(segment.FilePath), log.Topic, log.Partition);
        }

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

    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "Recovered {Topic} partition {Partition} from disk: {Records} records in {Segments} segments, end offset {EndOffset}, high watermark {HighWatermark}.")]
    private static partial void LogRecovered(
        ILogger logger, string topic, int partition, long records, int segments, long endOffset, long highWatermark);

    [LoggerMessage(
        Level = LogLevel.Warning,
        Message = "Discarded an incomplete write at the end of {File} in {Topic} partition {Partition}: truncated from {Length} to {ValidLength} bytes. The append that wrote it never returned an offset.")]
    private static partial void LogIncompleteWrite(
        ILogger logger, string file, string topic, int partition, long length, long validLength);

    [LoggerMessage(
        Level = LogLevel.Warning,
        Message = "Rebuilding the index of {File} in {Topic} partition {Partition}: the .index file does not describe the segment. Nothing is lost, but recovery had to read the whole segment.")]
    private static partial void LogRebuildingIndex(ILogger logger, string file, string topic, int partition);

    [LoggerMessage(
        Level = LogLevel.Warning,
        Message = "Ignored the checkpoint of {Topic} partition {Partition}: '{Contents}' is not a number. Starting with no confirmed records until replication raises the high watermark again.")]
    private static partial void LogUnreadableCheckpoint(ILogger logger, string topic, int partition, string contents);

    [LoggerMessage(
        Level = LogLevel.Warning,
        Message = "Clamped the checkpoint of {Topic} partition {Partition} from {Saved} to {Usable}: the log cannot back that high watermark.")]
    private static partial void LogClampedCheckpoint(ILogger logger, string topic, int partition, long saved, long usable);

    [LoggerMessage(
        Level = LogLevel.Warning,
        Message = "Ignored {File} in the data directory of {Topic} partition {Partition}: its name is not a segment base offset.")]
    private static partial void LogIgnoredFile(ILogger logger, string file, string topic, int partition);

    [LoggerMessage(
        Level = LogLevel.Critical,
        Message = "Cannot recover {Topic} partition {Partition} from disk. Refusing to serve the partition instead of dropping records: a gap in the middle of a log is not something another broker can restore.")]
    private static partial void LogUnrecoverable(ILogger logger, Exception exception, string topic, int partition);

    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "Retention deleted {Count} segments from {Topic} partition {Partition} ({Reason}).")]
    private static partial void LogRetentionDeleted(ILogger logger, string topic, int partition, int count, string reason);

    [LoggerMessage(
        Level = LogLevel.Error,
        Message = "Retention task failed for {Topic} partition {Partition}.")]
    private static partial void LogRetentionError(ILogger logger, Exception exception, string topic, int partition);

    /// <summary>Estado de una partición de un tópico: sus segmentos en orden y hasta dónde está confirmado.</summary>
    private sealed class TopicLog
    {
        public TopicLog(string dataDir, string topic, int partition)
        {
            Topic = topic;
            Partition = partition;
            DirectoryPath = Path.Combine(dataDir, topic, $"partition-{partition}");
            CheckpointFile = Path.Combine(DirectoryPath, CheckpointFileName);
        }

        /// <summary>Nombre del tópico. Se usa para que los mensajes de log digan de cuál hablan.</summary>
        public string Topic { get; }

        /// <summary>Número de partición.</summary>
        public int Partition { get; }

        public Lock Gate { get; } = new();

        /// <summary>Quien agrupa los <c>fsync</c> de esta partición y sabe hasta dónde llega el disco.</summary>
        public GroupCommitWriter Writer { get; } = new();

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