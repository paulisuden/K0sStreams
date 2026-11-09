using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Globalization;
using K0sStreams.Contracts;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace K0sStreams.Storage;

/// <summary>
/// Log append-only en disco. Reemplaza a <c>InMemoryLog</c>.
/// Cada tópico vive en <c>{DataDir}/{topic}/</c>, partido en <see cref="Segment"/> de tamaño
/// acotado, cada uno con su <see cref="SparseIndex"/>, más un archivo <c>checkpoint</c> con el
/// high watermark.
/// </summary>
/// <remarks>
/// Estado: escribe con <c>fsync</c> agrupado, rota segmentos, los indexa en disco de forma
/// dispersa, recuerda el high watermark entre reinicios y borra los segmentos viejos cuando
/// <see cref="RetentionService"/> se lo pide.
/// <para>
/// La clase está partida en varios archivos, uno por camino: este tiene el contrato
/// <see cref="ILog"/>, la escritura y el high watermark; <c>SegmentedLog.Read.cs</c> la lectura,
/// <c>SegmentedLog.Recovery.cs</c> la recuperación al arrancar, <c>SegmentedLog.Retention.cs</c> el
/// borrado de segmentos viejos y <c>SegmentedLog.TopicLog.cs</c> el estado en memoria de un tópico.
/// </para>
/// </remarks>
public sealed partial class SegmentedLog : ILog
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

    /// <summary>Archivo con el offset donde empieza el log, si la retención borró los primeros segmentos.</summary>
    private const string LogStartFileName = "log-start";

    /// <summary>Donde se arma el archivo de arriba antes de renombrarlo sobre el definitivo.</summary>
    private const string LogStartTempFileName = "log-start.tmp";

    private readonly ConcurrentDictionary<string, TopicLog> _topics = new(StringComparer.Ordinal);
    private readonly long _segmentBytes;
    private readonly RetentionOptions _retention;
    private readonly ILogger<SegmentedLog> _logger;

    /// <param name="dataDir">Carpeta raíz de los datos. En Docker es <c>/data</c> (configuración <c>Broker:DataDir</c>).</param>
    /// <param name="segmentBytes">Tamaño máximo de un segmento. Los tests lo bajan para generar varios.</param>
    /// <param name="retention">Hasta cuándo se guardan los segmentos. Lo aplica <see cref="RetentionService"/>.</param>
    /// <param name="logger">Por dónde avisa qué encontró al recuperar. Sin él no loguea nada.</param>
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
            throw new ArgumentException(
                "RetentionOptions inválida: MaxAge y MaxBytes tienen que ser positivos si se definen.", nameof(retention));
        }
    }

    /// <summary>Carpeta raíz donde se guardan los segmentos.</summary>
    public string DataDir { get; }

    /// <summary>
    /// Cuántos <c>fsync</c> de segmentos se hicieron desde que arrancó. Lo miran los tests: es la
    /// medida de si el group commit agrupa o si cada mensaje paga el suyo.
    /// </summary>
    internal int FsyncCount => _topics.Values.Sum(log => log.Writer.FsyncCount);

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
            segment.Append(segment.Length, bytes.Length, record.Timestamp);

            // Con el stream cerrado, los bytes ya están en el sistema operativo. Desde este punto,
            // un fsync del archivo los baja al disco aunque lo pida otra escritura.
            log.Writer.Wrote(segment.FilePath, next);
            return next;
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
            return log.Add(log.NextOffset);
        }

        var active = log.Segments[^1];
        return active.IsEmpty || active.Length + recordSize <= _segmentBytes
            ? active
            : log.Add(active.NextOffset);
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
            LogUnreadableCheckpoint(_logger, log.Topic, contents);
            return Record.Unassigned;
        }

        // Acotado por los dos lados. Por arriba, para no dar por confirmado algo que no está escrito.
        // Por abajo, por el inicio del log: la retención solo borra lo que ya estaba confirmado, así
        // que un checkpoint anterior al inicio es uno que quedó viejo (no lleva fsync). En un log sin
        // retención el piso sigue siendo -1, que es el high watermark más chico que admite ILog.
        long usable = Math.Clamp(saved, log.LogStart - 1, endOffset);
        if (usable != saved)
        {
            LogClampedCheckpoint(_logger, log.Topic, saved, usable);
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

    [LoggerMessage(
        Level = LogLevel.Warning,
        Message = "Ignored the checkpoint of {Topic}: '{Contents}' is not a number. Starting with no confirmed records until replication raises the high watermark again.")]
    private static partial void LogUnreadableCheckpoint(ILogger logger, string topic, string contents);

    [LoggerMessage(
        Level = LogLevel.Warning,
        Message = "Clamped the checkpoint of {Topic} from {Saved} to {Usable}: the log cannot back that high watermark.")]
    private static partial void LogClampedCheckpoint(ILogger logger, string topic, long saved, long usable);
}
