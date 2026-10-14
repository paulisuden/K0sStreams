using K0sStreams.Contracts;

namespace K0sStreams.Storage;

/// <summary>
/// Junta en un solo <c>fsync</c> las escrituras que llegaron mientras el anterior corría. Hay uno
/// por tópico, y es el único que baja al disco los <c>.log</c>.
/// </summary>
/// <remarks>
/// No espera a nadie de más: no hay ventana de tiempo. El primero que llega arranca su <c>fsync</c>
/// y los que vienen detrás se cuelgan del que ya está corriendo, que los cubre sin costo. Por eso
/// la latencia de cada escritura no cambia y el throughput sí.
/// </remarks>
internal sealed class GroupCommitWriter
{
    /// <summary>Protege lo de abajo. Es aparte del candado del tópico, que no se sostiene en un await.</summary>
    private readonly Lock _state = new();

    /// <summary>Archivos con bytes que todavía no bajaron al disco. Casi siempre uno; dos al rotar.</summary>
    private readonly HashSet<string> _dirtyFiles = new(StringComparer.Ordinal);

    /// <summary>El <c>fsync</c> en curso, o uno ya terminado si no hay ninguno.</summary>
    private Task _commit = Task.CompletedTask;

    private long _writtenOffset = Record.Unassigned;
    private long _durableOffset = Record.Unassigned;
    private int _fsyncCount;

    /// <summary>Último offset que el disco ya respalda. Es lo único que el log admite hacia afuera.</summary>
    public long DurableOffset => Volatile.Read(ref _durableOffset);

    /// <summary>Cuántos <c>fsync</c> se hicieron. Es la medida de si el group commit agrupa.</summary>
    public int FsyncCount => Volatile.Read(ref _fsyncCount);

    /// <summary>
    /// Anota un registro ya escrito en el archivo. Se llama con el stream <b>cerrado</b>: desde ese
    /// momento sus bytes están en el sistema operativo y cualquier <c>fsync</c> del archivo los cubre.
    /// </summary>
    public void Wrote(string filePath, long offset)
    {
        lock (_state)
        {
            _dirtyFiles.Add(filePath);
            _writtenOffset = offset;
        }
    }

    /// <summary>Espera a que el disco respalde ese offset.</summary>
    public async ValueTask WaitUntilDurableAsync(long offset)
    {
        while (DurableOffset < offset)
        {
            Task commit;
            lock (_state)
            {
                if (_durableOffset >= offset)
                {
                    // Mientras se esperaba, un fsync ajeno ya cubrió este registro. Acá es donde se
                    // gana: cuantas más escrituras esperan, más confirma un solo fsync.
                    return;
                }

                // Si no hay ninguno corriendo, este arranca uno; si hay, se cuelga del que ya está.
                // Va al pool de hilos y no al que llama: si el fsync corriera acá mismo, el primer
                // append bloquearía los milisegundos del disco antes de devolver el control, los que
                // vienen detrás no alcanzarían a escribir, y no habría nada que agrupar.
                if (_commit.IsCompleted)
                {
                    _commit = Task.Run(Commit);
                }

                commit = _commit;
            }

            // Sin cancelación: los bytes ya están escritos y el registro va a quedar, así que
            // contestar "cancelado" sería mentir. Un fsync tarda milisegundos, no cuelga el apagado.
            await commit.ConfigureAwait(false);
        }
    }

    /// <summary>Fuerza el <c>fsync</c> de todo lo escrito y espera a que termine.</summary>
    public ValueTask CommitPendingAsync()
    {
        long written;
        lock (_state)
        {
            written = _writtenOffset;
        }

        return WaitUntilDurableAsync(written);
    }

    /// <summary>
    /// Fija hasta dónde llega el disco sin pasar por un <c>fsync</c>. Lo usan la recuperación al
    /// arrancar (lo que se leyó del disco ya es durable) y el truncado (recorta y confirma él mismo).
    /// </summary>
    public void Reset(long durableOffset)
    {
        lock (_state)
        {
            _dirtyFiles.Clear();
            _writtenOffset = durableOffset;
            Volatile.Write(ref _durableOffset, durableOffset);
        }
    }

    /// <summary>Un <c>fsync</c> por todo lo que haya pendiente, sin importar quién lo escribió.</summary>
    private void Commit()
    {
        var (files, offset) = TakePending();
        if (files.Length == 0)
        {
            return;
        }

        try
        {
            FlushToDisk(files);
        }
        catch
        {
            // Falló el fsync: esos archivos siguen sucios y el próximo commit tiene que reintentarlos.
            Restore(files);
            throw;
        }

        lock (_state)
        {
            // Acotado a lo escrito: si entremedio hubo un truncado, lo que se recortó no revive.
            Volatile.Write(ref _durableOffset, Math.Min(offset, _writtenOffset));
        }
    }

    /// <summary>Se queda con lo pendiente: eso es exactamente lo que va a cubrir el <c>fsync</c>.</summary>
    private (string[] Files, long Offset) TakePending()
    {
        lock (_state)
        {
            string[] files = [.. _dirtyFiles];
            _dirtyFiles.Clear();
            return (files, _writtenOffset);
        }
    }

    private void Restore(string[] files)
    {
        lock (_state)
        {
            foreach (string file in files)
            {
                _dirtyFiles.Add(file);
            }
        }
    }

    private void FlushToDisk(string[] files)
    {
        foreach (string file in files)
        {
            // El fsync es del archivo, no del descriptor: baja al disco lo que escribió cualquiera.
            // Por eso cada append puede abrir y cerrar el suyo, sin dejar ninguno abierto.
            using var stream = new FileStream(file, FileMode.Open, FileAccess.Write, FileShare.ReadWrite);
            stream.Flush(flushToDisk: true);
            Interlocked.Increment(ref _fsyncCount);
        }
    }
}
