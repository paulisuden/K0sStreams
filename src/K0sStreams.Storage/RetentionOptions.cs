namespace K0sStreams.Storage;

/// <summary>
/// Hasta cuándo se guardan los segmentos de un tópico. Con los dos límites sin valor la retención
/// queda apagada y el log crece sin tope.
/// </summary>
/// <remarks>
/// El nombre de la sección y de las tres claves los usan el chart de Helm y
/// <c>appsettings.k8s.json</c> (como <c>Retention__MaxAge</c> y compañía): no se renombran.
/// </remarks>
public sealed class RetentionOptions
{
    public const string SectionName = "Retention";

    /// <summary>
    /// Edad máxima de un segmento: se borra cuando su registro más nuevo pasó este tiempo.
    /// <c>null</c> = sin límite por tiempo.
    /// </summary>
    public TimeSpan? MaxAge { get; init; }

    /// <summary>Tamaño máximo de los segmentos de un tópico. <c>null</c> = sin límite por tamaño.</summary>
    public long? MaxBytes { get; init; }

    /// <summary>Intervalo entre comprobaciones de retención. Por defecto 5 minutos.</summary>
    public TimeSpan CheckInterval { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>Valida que la configuración sea coherente.</summary>
    public bool IsValid() => (MaxAge is null || MaxAge > TimeSpan.Zero) && (MaxBytes is null || MaxBytes > 0);
}
