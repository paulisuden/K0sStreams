using System.Globalization;

namespace K0sStreams.Storage;

/// <summary>
/// Un tramo del log en su propio archivo. El nombre del archivo es el primer offset que contiene,
/// con 20 dígitos, así que mirando la carpeta se sabe qué archivo abrir para un offset dado.
/// </summary>
internal sealed class Segment
{
    public const string Extension = ".log";

    private const string NameFormat = "D20";

    public Segment(string directoryPath, long baseOffset)
    {
        BaseOffset = baseOffset;
        FilePath = Path.Combine(directoryPath, NameFor(baseOffset));
        Index = new SparseIndex(FilePath);
    }

    /// <summary>Primer offset que contiene, o que va a contener si todavía está vacío.</summary>
    public long BaseOffset { get; }

    public string FilePath { get; }

    /// <summary>Su índice disperso, el archivo <c>.index</c> de al lado.</summary>
    public SparseIndex Index { get; }

    /// <summary>
    /// Cuántos registros contiene. Como los offsets son contiguos y sin huecos, este número solo
    /// alcanza para saber hasta dónde llega el segmento.
    /// </summary>
    public long RecordCount { get; set; }

    /// <summary>Bytes válidos del archivo, o sea dónde va a caer el próximo registro.</summary>
    public long Length { get; set; }

    /// <summary>Offset que sigue al último que contiene. En un segmento vacío es <see cref="BaseOffset"/>.</summary>
    public long NextOffset => BaseOffset + RecordCount;

    public bool IsEmpty => RecordCount == 0;

    public bool Contains(long offset) => offset >= BaseOffset && offset < NextOffset;

    /// <summary>
    /// Punto de partida para llegar a ese offset: el comienzo del registro anterior más cercano que
    /// el índice conoce. De ahí al offset pedido hay menos de <see cref="SparseIndex.IntervalBytes"/>.
    /// </summary>
    public (long Offset, long Position) Anchor(long offset)
    {
        var anchor = Index.AnchorFor(offset - BaseOffset);
        return (BaseOffset + anchor.RelativeOffset, anchor.Position);
    }

    /// <summary>
    /// Suma un registro al final: lo cuenta, corre el largo y se lo ofrece al índice, que decide si
    /// le toca anotarlo. Lo usan tanto una escritura nueva como la recuperación al arrancar, así que
    /// el índice queda igual por los dos caminos.
    /// </summary>
    public void Append(long position, int size)
    {
        Index.Note(RecordCount, position);
        RecordCount++;
        Length = position + size;
    }

    /// <summary>Borra del índice los offsets desde <paramref name="fromOffset"/> en adelante.</summary>
    public void DropFrom(long fromOffset)
    {
        RecordCount = fromOffset - BaseOffset;
        Index.DropFrom(RecordCount);
    }

    public static string NameFor(long baseOffset) =>
        baseOffset.ToString(NameFormat, CultureInfo.InvariantCulture) + Extension;

    /// <summary>Offset base leído del nombre del archivo, o <c>null</c> si no tiene esa forma.</summary>
    public static long? BaseOffsetFromName(string path) =>
        long.TryParse(
            Path.GetFileNameWithoutExtension(path), NumberStyles.None, CultureInfo.InvariantCulture, out long baseOffset)
            ? baseOffset
            : null;
}
