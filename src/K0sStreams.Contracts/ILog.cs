namespace K0sStreams.Contracts;

/// <summary>
/// Log append-only por tópico y partición. Es la única fuente de verdad del broker.
/// Los offsets empiezan en 0 y son contiguos. En un log vacío, <see cref="EndOffset"/> y
/// <see cref="HighWatermark"/> valen -1.
/// </summary>
public interface ILog
{
    /// <summary>
    /// Agrega un registro y devuelve su offset cuando está en disco (fsync, con group commit).
    /// Si <c>record.Offset</c> es <see cref="Record.Unassigned"/>, el log asigna el siguiente (líder).
    /// Si no, tiene que ser exactamente <c>EndOffset + 1</c> (réplica); si no lo es, lanza <see cref="ArgumentException"/>.
    /// </summary>
    ValueTask<long> AppendAsync(string topic, int partition, Record record, CancellationToken ct = default);

    /// <summary>
    /// Lee hasta <paramref name="max"/> registros desde <paramref name="fromOffset"/> hasta <see cref="EndOffset"/>.
    /// No corta en el high watermark: eso lo decide quien llama (la API sí corta; la réplica no).
    /// </summary>
    IAsyncEnumerable<Record> ReadAsync(string topic, int partition, long fromOffset, int max, CancellationToken ct = default);

    /// <summary>Último offset confirmado por quórum. Solo esto se muestra a los clientes.</summary>
    long HighWatermark(string topic, int partition);

    /// <summary>Último offset escrito localmente.</summary>
    long EndOffset(string topic, int partition);

    /// <summary>
    /// Sube el high watermark (nunca lo baja). Lo llama la replicación al lograr quórum.
    /// Lanza <see cref="ArgumentOutOfRangeException"/> si <paramref name="offset"/> supera <see cref="EndOffset"/>.
    /// </summary>
    void AdvanceHighWatermark(string topic, int partition, long offset);

    /// <summary>
    /// Borra todos los registros con offset mayor que <paramref name="toOffset"/>. Se usa al cambiar de época.
    /// Lanza <see cref="InvalidOperationException"/> si se intenta borrar algo confirmado (por debajo del high watermark).
    /// </summary>
    ValueTask TruncateAsync(string topic, int partition, long toOffset, CancellationToken ct = default);
}
