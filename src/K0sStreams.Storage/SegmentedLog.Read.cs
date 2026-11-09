using System.Runtime.CompilerServices;
using K0sStreams.Contracts;

namespace K0sStreams.Storage;

// Camino de lectura: armar el recorrido con el índice en memoria, abrir cada segmento y decodificar.
// Va aparte porque es el único camino que lee mientras la retención borra, y esa convivencia tiene
// sus propias reglas: ver OpenSegment y Replan.
public sealed partial class SegmentedLog
{
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

        // Lo que todavía falta entregar. Hace falta para poder rearmar el plan si la retención
        // borra un segmento entre que se planificó y que se abre el archivo.
        long siguiente = fromOffset;
        int faltan = max;

        int indice = 0;
        while (indice < tramos.Count)
        {
            var tramo = tramos[indice];
            var abierto = OpenSegment(tramo.FilePath);
            if (abierto is null)
            {
                tramos = Replan(log, tramo.FilePath, siguiente, faltan);
                indice = 0;
                continue;
            }

            using (abierto)
            {
                await foreach (var record in ReadSegmentAsync(abierto, tramo, ct).ConfigureAwait(false))
                {
                    yield return record;
                    siguiente = record.Offset + 1;
                    faltan--;
                }
            }

            indice++;
        }
    }

    /// <summary>
    /// Los registros de un tramo, sobre un segmento ya abierto. Dentro de un segmento los registros
    /// están pegados uno al otro, así que desde la posición del primero alcanza con leerlos en
    /// orden: cada uno arranca con su propio largo.
    /// </summary>
    private static async IAsyncEnumerable<Record> ReadSegmentAsync(
        FileStream stream,
        (string FilePath, long Start, long Skip, int Count) tramo,
        [EnumeratorCancellation] CancellationToken ct)
    {
        stream.Seek(tramo.Start, SeekOrigin.Begin);
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

    /// <summary>
    /// Abre un segmento para leerlo, o <c>null</c> si ya no está. Una vez abierto, la retención lo
    /// puede borrar sin molestar: en Linux un archivo abierto se sigue leyendo hasta que se cierra.
    /// Lo que no se puede es abrirlo después de que lo borraron.
    /// </summary>
    private static FileStream? OpenSegment(string file)
    {
        try
        {
            return new FileStream(
                file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, bufferSize: 8192, useAsync: true);
        }
        catch (FileNotFoundException)
        {
            return null;
        }
    }

    /// <summary>
    /// Vuelve a planificar lo que falta leer, después de encontrar que un segmento del plan ya no
    /// está. El plan nuevo se arma bajo el candado, así que ya refleja el borrado.
    /// </summary>
    private static List<(string FilePath, long Start, long Skip, int Count)> Replan(
        TopicLog log, string missing, long fromOffset, int max)
    {
        var tramos = PlanRead(log, fromOffset, max);

        // Si el plan nuevo sigue apuntando al mismo archivo, no fue la retención: algo le pasó al
        // disco, y ahí sí corresponde fallar en lugar de girar en falso.
        return tramos.Count > 0 && tramos[0].FilePath == missing
            ? throw Damaged(missing, 0, "el segmento no está en disco")
            : tramos;
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

            // Y desde el inicio del log, no desde donde pidieron: pedir un offset que la retención
            // ya borró no es un error de quien lee, así que se le devuelve desde donde se puede.
            long from = Math.Clamp(fromOffset, log.LogStart, end);
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
}
