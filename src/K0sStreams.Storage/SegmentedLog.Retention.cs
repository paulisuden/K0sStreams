using System.Globalization;
using System.Text;
using Microsoft.Extensions.Logging;

namespace K0sStreams.Storage;

// Retención: borrar los segmentos más viejos por antigüedad o por tamaño. Acá vive toda la historia
// del archivo log-start, tanto quien lo escribe antes de borrar como quien lo lee al recuperar: son
// las dos mitades de la misma garantía y se leen juntas.
public sealed partial class SegmentedLog
{
    /// <summary>
    /// Aplica la retención una vez a los tópicos que este broker tiene abiertos: borra los
    /// segmentos más viejos que pasaron <see cref="RetentionOptions.MaxAge"/> y, si hace falta, los
    /// que sobran para entrar en <see cref="RetentionOptions.MaxBytes"/>.
    /// </summary>
    /// <remarks>
    /// La hora llega de afuera, de <see cref="RetentionService"/>, que es quien tiene el
    /// <see cref="TimeProvider"/>: el log no mira el reloj del sistema y los tests pueden mover el
    /// tiempo a mano.
    /// </remarks>
    internal void ApplyRetention(DateTimeOffset now, CancellationToken ct = default)
    {
        if (_retention.MaxAge is null && _retention.MaxBytes is null)
        {
            return;
        }

        foreach (var log in _topics.Values)
        {
            ct.ThrowIfCancellationRequested();

            lock (log.Gate)
            {
                // Los dos criterios se resuelven y se aplican dentro del mismo candado: así quien
                // lee ve el log antes o después del borrado, nunca a mitad de camino.
                int deletable = Deletable(log);
                int byAge = _retention.MaxAge is TimeSpan maxAge
                    ? SegmentsOlderThan(log, (now - maxAge).ToUnixTimeMilliseconds(), deletable)
                    : 0;
                int bySize = _retention.MaxBytes is long maxBytes
                    ? SegmentsOverSize(log, maxBytes, deletable)
                    : 0;

                DeleteOldest(log, Math.Max(byAge, bySize));
            }
        }
    }

    /// <summary>
    /// Cuántos segmentos, contando desde el más viejo, se pueden borrar. El que contiene el high
    /// watermark no se toca, y como es anterior o igual al activo, protegerlo protege también al
    /// activo y a todo lo que venga después. Sin nada confirmado no se puede borrar nada.
    /// </summary>
    private static int Deletable(TopicLog log)
    {
        // IndexOf devuelve -1 si el high watermark no cae en ningún segmento, que es lo que pasa
        // cuando un checkpoint viejo quedó por debajo del inicio del log. Ahí tampoco se borra nada.
        int holder = log.HighWatermark < 0 ? -1 : log.IndexOf(log.HighWatermark);
        return Math.Max(holder, 0);
    }

    /// <summary>
    /// Cuántos de los segmentos más viejos tienen su registro más nuevo antes del corte. Se para en
    /// el primero que no lo cumple: se borra siempre del más viejo al más nuevo, sin dejar huecos.
    /// </summary>
    private static int SegmentsOlderThan(TopicLog log, long cutoff, int deletable)
    {
        int count = 0;
        while (count < deletable && log.Segments[count].NewestTimestamp < cutoff)
        {
            count++;
        }

        return count;
    }

    /// <summary>
    /// Cuántos de los segmentos más viejos hay que borrar para que el tópico entre en
    /// <paramref name="maxBytes"/>. Usa el largo que cada segmento ya tiene en memoria: es el mismo
    /// número que el del archivo y no cuesta una consulta al sistema de archivos por segmento.
    /// </summary>
    private static int SegmentsOverSize(TopicLog log, long maxBytes, int deletable)
    {
        long total = 0;
        foreach (var segment in log.Segments)
        {
            total += segment.Length;
        }

        int count = 0;
        while (count < deletable && total > maxBytes)
        {
            total -= log.Segments[count].Length;
            count++;
        }

        return count;
    }

    /// <summary>
    /// Borra los <paramref name="count"/> segmentos más viejos. Antes de tocar un solo archivo deja
    /// anotado en disco dónde va a empezar el log, así un corte en el medio se puede terminar al
    /// recuperar en lugar de parecer un log con un agujero adelante.
    /// </summary>
    private void DeleteOldest(TopicLog log, int count)
    {
        if (count <= 0)
        {
            return;
        }

        long start = log.Segments[count].BaseOffset;
        WriteLogStart(log, start);

        // Y antes de borrar, el estado en memoria: una lectura que ya tenía su plan armado va a
        // replanificar desde el inicio nuevo en lugar de abrir un archivo que está por desaparecer.
        log.LogStart = start;
        var deleted = log.Segments.GetRange(0, count);
        log.Segments.RemoveRange(0, count);

        foreach (var segment in deleted)
        {
            File.Delete(segment.FilePath);
            segment.Index.Discard();
        }

        LogRetentionDeleted(_logger, log.Topic, count, start);
    }

    /// <summary>
    /// Deja en disco el offset donde empieza el log. Se escribe aparte y se renombra porque un
    /// rename es atómico: la recuperación ve el valor viejo o el nuevo, nunca uno a medio escribir.
    /// Y lleva <c>fsync</c>, al revés del checkpoint: si se perdiera, la recuperación tomaría por
    /// daño unos segmentos que la retención borró a propósito.
    /// </summary>
    private static void WriteLogStart(TopicLog log, long offset)
    {
        using (var stream = new FileStream(log.LogStartTempFile, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            stream.Write(Encoding.UTF8.GetBytes(offset.ToString(CultureInfo.InvariantCulture)));
            stream.Flush(flushToDisk: true);
        }

        File.Move(log.LogStartTempFile, log.LogStartFile, overwrite: true);

        // El rename también hay que bajarlo al disco: si no, el contenido estaría a salvo pero la
        // entrada de directorio podría seguir apuntando al archivo viejo.
        DirectorySync.Flush(log.DirectoryPath);
    }

    /// <summary>
    /// En qué offset empieza el log según el disco, y de paso termina un borrado que haya quedado a
    /// medias. Sin archivo <c>log-start</c> el log empieza en 0, que es la regla de siempre: un
    /// primer segmento que no arranca ahí es daño. Con él, el inicio es el que dice, y los
    /// segmentos que quedaron enteros por debajo se borran ahora y salen de
    /// <paramref name="baseOffsets"/>.
    /// </summary>
    private long ResolveLogStart(TopicLog log, List<long> baseOffsets)
    {
        if (!File.Exists(log.LogStartFile))
        {
            return 0;
        }

        string contents = File.ReadAllText(log.LogStartFile);
        if (!long.TryParse(contents, NumberStyles.Integer, CultureInfo.InvariantCulture, out long start) || start < 0)
        {
            // El archivo existe, así que hubo retención. El nombre del primer segmento que quedó
            // dice dónde empieza el log; negarse a servir el tópico trataría como daño un borrado
            // hecho a propósito, y lo que falta abajo no lo puede devolver nadie.
            LogUnreadableLogStart(_logger, log.Topic, contents);
            return baseOffsets.Count > 0 ? baseOffsets[0] : 0;
        }

        // Un corte entre anotar el inicio y borrar los archivos deja segmentos que ya no cuentan.
        // Se los reconoce porque el que les sigue también arranca en o antes del inicio: nunca se
        // borra el segmento activo, así que siempre queda al menos uno detrás de los interrumpidos.
        int unfinished = 0;
        while (unfinished + 1 < baseOffsets.Count && baseOffsets[unfinished + 1] <= start)
        {
            DeleteSegmentFiles(log, baseOffsets[unfinished]);
            unfinished++;
        }

        if (unfinished > 0)
        {
            baseOffsets.RemoveRange(0, unfinished);
            LogFinishedRetention(_logger, log.Topic, unfinished, start);
        }

        return start;
    }

    /// <summary>Borra el <c>.log</c> y su <c>.index</c>, por si el proceso murió entre los dos.</summary>
    private static void DeleteSegmentFiles(TopicLog log, long baseOffset)
    {
        string file = Path.Combine(log.DirectoryPath, Segment.NameFor(baseOffset));
        File.Delete(file);
        File.Delete(Path.ChangeExtension(file, SparseIndex.Extension));
    }

    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "Retention deleted the {Count} oldest segments of {Topic}: the log now starts at offset {LogStart}.")]
    private static partial void LogRetentionDeleted(ILogger logger, string topic, int count, long logStart);

    [LoggerMessage(
        Level = LogLevel.Warning,
        Message = "Finished a retention delete of {Topic} that was interrupted: removed {Count} segments below offset {LogStart}. Nothing is lost, they were already deleted on purpose.")]
    private static partial void LogFinishedRetention(ILogger logger, string topic, int count, long logStart);

    [LoggerMessage(
        Level = LogLevel.Warning,
        Message = "Ignored the log start of {Topic}: '{Contents}' is not an offset. Taking the first segment on disk as the start of the log.")]
    private static partial void LogUnreadableLogStart(ILogger logger, string topic, string contents);
}
