using System.Buffers.Binary;

namespace K0sStreams.Storage;

/// <summary>
/// Índice disperso de un segmento: un archivo <c>.index</c> al lado del <c>.log</c> con pares
/// (offset, posición), uno cada <see cref="IntervalBytes"/> escritos.
/// </summary>
/// <remarks>
/// No tiene una entrada por registro, así que no sabe dónde empieza cualquier offset: sabe dónde
/// empieza uno <em>anterior</em>, y desde ahí hay que avanzar leyendo. En cambio ocupa una fracción
/// de la memoria y permite arrancar sin releer el log completo.
/// </remarks>
internal sealed class SparseIndex
{
    public const string Extension = ".index";

    /// <summary>
    /// Bytes de log entre dos entradas. Más chico es más memoria y menos lectura secuencial;
    /// más grande, al revés. 4 KB es lo que fija <c>docs/ARQUITECTURA.md</c>.
    /// </summary>
    public const int IntervalBytes = 4096;

    /// <summary>Una entrada: dos int32 little-endian, el mismo criterio que <c>RecordCodec</c>.</summary>
    private const int EntrySize = 2 * sizeof(int);

    private readonly List<Entry> _entries = [];

    public SparseIndex(string logFilePath) => FilePath = Path.ChangeExtension(logFilePath, Extension);

    public string FilePath { get; }

    /// <summary>La última entrada, o el principio del segmento si todavía no hay ninguna.</summary>
    public (long RelativeOffset, long Position) Last => At(_entries.Count - 1);

    /// <summary>
    /// Anota el registro si pasaron <see cref="IntervalBytes"/> desde la última entrada. El primero
    /// del segmento nunca se anota: el byte 0 ya es un comienzo de registro conocido.
    /// </summary>
    public void Note(long relativeOffset, long position)
    {
        if (position - Last.Position < IntervalBytes)
        {
            return;
        }

        var entry = new Entry((int)relativeOffset, (int)position);
        _entries.Add(entry);

        // Sin fsync, a diferencia del .log: esto es un acelerador recalculable, no datos. Y se
        // escribe DESPUÉS de que el registro está en disco, así que el índice puede quedar
        // atrasado pero nunca adelantado, y un índice atrasado solo cuesta leer un poco más.
        Span<byte> bytes = stackalloc byte[EntrySize];
        WriteEntry(entry, bytes);
        using var stream = new FileStream(FilePath, FileMode.Append, FileAccess.Write, FileShare.Read);
        stream.Write(bytes);
    }

    /// <summary>
    /// Último punto conocido en o antes de ese offset, o el principio del segmento si no hay
    /// ninguno. Desde ahí hasta el offset buscado hay menos de <see cref="IntervalBytes"/>.
    /// </summary>
    public (long RelativeOffset, long Position) AnchorFor(long relativeOffset) => At(LastAtOrBefore(relativeOffset));

    /// <summary>
    /// Lee el archivo. Devuelve <c>false</c> si falta o si no es consistente con un <c>.log</c> de
    /// <paramref name="logLength"/> bytes, y en ese caso no se usa nada de lo leído. No es un
    /// error: el índice se reconstruye recorriendo el log, que es la única fuente de verdad.
    /// </summary>
    public bool TryLoad(long logLength)
    {
        _entries.Clear();
        if (!File.Exists(FilePath))
        {
            return false;
        }

        byte[] bytes = File.ReadAllBytes(FilePath);
        if (bytes.Length == 0 || bytes.Length % EntrySize != 0)
        {
            return false;
        }

        for (int start = 0; start < bytes.Length; start += EntrySize)
        {
            var span = bytes.AsSpan(start);
            var entry = new Entry(
                BinaryPrimitives.ReadInt32LittleEndian(span),
                BinaryPrimitives.ReadInt32LittleEndian(span[sizeof(int)..]));

            // Las dos series crecen juntas y toda posición cae dentro del log. El centinela de la
            // primera entrada no es (0, 0) sino el último valor imposible: ninguna entrada puede
            // apuntar al offset 0 ni a un byte anterior a los 4 KB, porque esas no se anotan.
            var previous = _entries.Count == 0 ? new Entry(0, IntervalBytes - 1) : _entries[^1];
            if (entry.RelativeOffset <= previous.RelativeOffset
                || entry.Position <= previous.Position
                || entry.Position >= logLength)
            {
                _entries.Clear();
                return false;
            }

            _entries.Add(entry);
        }

        return true;
    }

    /// <summary>Borra las entradas desde ese offset en adelante, en memoria y en el archivo.</summary>
    public void DropFrom(long relativeOffset)
    {
        int keep = LastAtOrBefore(relativeOffset - 1) + 1;
        if (keep == _entries.Count)
        {
            return;
        }

        _entries.RemoveRange(keep, _entries.Count - keep);
        Save();
    }

    /// <summary>Lo deja como si nunca hubiera existido, para rehacerlo desde cero.</summary>
    public void Discard()
    {
        _entries.Clear();
        File.Delete(FilePath);
    }

    private (long RelativeOffset, long Position) At(int position) =>
        position < 0 ? (0, 0) : (_entries[position].RelativeOffset, _entries[position].Position);

    /// <summary>Índice de la última entrada con offset menor o igual, o -1 si no hay ninguna.</summary>
    private int LastAtOrBefore(long relativeOffset)
    {
        int low = 0;
        int high = _entries.Count - 1;
        int found = -1;
        while (low <= high)
        {
            int middle = low + ((high - low) / 2);
            if (_entries[middle].RelativeOffset <= relativeOffset)
            {
                found = middle;
                low = middle + 1;
            }
            else
            {
                high = middle - 1;
            }
        }

        return found;
    }

    /// <summary>Reescribe el archivo completo. Hace falta al recortar: un append no puede borrar.</summary>
    private void Save()
    {
        if (_entries.Count == 0)
        {
            File.Delete(FilePath);
            return;
        }

        var bytes = new byte[_entries.Count * EntrySize];
        for (int i = 0; i < _entries.Count; i++)
        {
            WriteEntry(_entries[i], bytes.AsSpan(i * EntrySize));
        }

        File.WriteAllBytes(FilePath, bytes);
    }

    private static void WriteEntry(Entry entry, Span<byte> destination)
    {
        BinaryPrimitives.WriteInt32LittleEndian(destination, entry.RelativeOffset);
        BinaryPrimitives.WriteInt32LittleEndian(destination[sizeof(int)..], entry.Position);
    }

    private readonly record struct Entry(int RelativeOffset, int Position);
}
