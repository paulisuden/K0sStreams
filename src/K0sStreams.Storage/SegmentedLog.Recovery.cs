using K0sStreams.Contracts;
using Microsoft.Extensions.Logging;

namespace K0sStreams.Storage;

// Recuperación al arrancar: leer la carpeta de un tópico y rearmar en memoria lo que el disco dice,
// una sola vez por tópico y la primera vez que alguien lo usa. Acá se decide qué es una escritura
// cortada (se descarta) y qué es daño (se deja de servir el tópico).
public sealed partial class SegmentedLog
{
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
                try
                {
                    RebuildIndex(log);
                }
                catch (InvalidDataException ex)
                {
                    // Un solo lugar para todas las formas de daño: el mensaje de la excepción ya trae
                    // el archivo, el byte y el motivo. Se loguea y se relanza, no se esconde.
                    LogUnrecoverable(_logger, ex, topic);
                    throw;
                }

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
    private void RebuildIndex(TopicLog log)
    {
        if (Directory.Exists(log.DirectoryPath))
        {
            var baseOffsets = SegmentBaseOffsets(log);

            // Antes de comparar offsets hay que saber dónde empieza el log: si la retención borró
            // los primeros segmentos, el primero que queda no arranca en 0 y eso no es daño.
            log.LogStart = ResolveLogStart(log, baseOffsets);

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
            LogRecovered(_logger, log.Topic, log.NextOffset, log.Segments.Count, log.NextOffset - 1, log.HighWatermark);
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
                LogIgnoredFile(_logger, Path.GetFileName(path), log.Topic);
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
        LogIncompleteWrite(_logger, Path.GetFileName(segment.FilePath), log.Topic, total, valid);
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
            LogRebuildingIndex(_logger, Path.GetFileName(segment.FilePath), log.Topic);
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

            segment.Append(position, consumed, decoded.Timestamp);
            position += consumed;
        }

        return position;
    }

    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "Recovered {Topic} from disk: {Records} records in {Segments} segments, end offset {EndOffset}, high watermark {HighWatermark}.")]
    private static partial void LogRecovered(
        ILogger logger, string topic, long records, int segments, long endOffset, long highWatermark);

    [LoggerMessage(
        Level = LogLevel.Warning,
        Message = "Discarded an incomplete write at the end of {File} in {Topic}: truncated from {Length} to {ValidLength} bytes. The append that wrote it never returned an offset.")]
    private static partial void LogIncompleteWrite(
        ILogger logger, string file, string topic, long length, long validLength);

    [LoggerMessage(
        Level = LogLevel.Warning,
        Message = "Rebuilding the index of {File} in {Topic}: the .index file does not describe the segment. Nothing is lost, but recovery had to read the whole segment.")]
    private static partial void LogRebuildingIndex(ILogger logger, string file, string topic);

    [LoggerMessage(
        Level = LogLevel.Warning,
        Message = "Ignored {File} in the data directory of {Topic}: its name is not a segment base offset.")]
    private static partial void LogIgnoredFile(ILogger logger, string file, string topic);

    [LoggerMessage(
        Level = LogLevel.Critical,
        Message = "Cannot recover {Topic} from disk. Refusing to serve the topic instead of dropping records: a gap in the middle of a log is not something another broker can restore.")]
    private static partial void LogUnrecoverable(ILogger logger, Exception exception, string topic);
}
