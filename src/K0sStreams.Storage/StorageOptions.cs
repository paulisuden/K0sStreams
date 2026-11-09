using K0sStreams.Contracts;

namespace K0sStreams.Storage;

/// <summary>
/// Configuración del log en disco, sección <c>Storage</c>. Sin configurar nada, los valores por
/// defecto son los que usa el broker en producción.
/// </summary>
public sealed class StorageOptions
{
    public const string SectionName = "Storage";

    /// <summary>Tamaño al que se cierra un segmento y se abre otro. Por defecto 64 MiB.</summary>
    /// <remarks>
    /// Es configurable para poder ejercitar la retención: con 64 MiB hay que escribir más de 128 MiB
    /// en un tópico para que llegue a borrar un segmento, así que con el valor por defecto no se la
    /// puede mostrar en una demo ni en la suite de caos.
    /// </remarks>
    public long SegmentBytes { get; init; } = SegmentedLog.DefaultSegmentBytes;

    /// <summary>Valida que el tamaño entre en lo que el formato de segmento admite.</summary>
    public bool IsValid() =>
        SegmentBytes >= RecordCodec.HeaderSize && SegmentBytes <= SegmentedLog.MaxSegmentBytes;
}
