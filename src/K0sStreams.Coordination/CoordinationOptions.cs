namespace K0sStreams.Coordination;

/// <summary>Configuración de la elección de líder, sección <c>Coordination</c>.</summary>
public sealed class CoordinationOptions
{
    public const string SectionName = "Coordination";

    /// <summary>Nombre del Lease de Kubernetes que hace de candado del líder, en el namespace del pod.</summary>
    public string LeaseName { get; set; } = "k0sstreams-leader";

    /// <summary>Cuánto vale el Lease sin renovarse. Pasado ese tiempo, otro broker lo puede tomar.</summary>
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Si el líder no logra renovar durante este tiempo, deja de ser líder. Es menor que <see cref="LeaseDuration"/>: así
    /// se baja antes de que otro broker pueda tomar el Lease.
    /// </summary>
    public TimeSpan RenewDeadline { get; set; } = TimeSpan.FromSeconds(7);

    /// <summary>Cada cuánto el líder renueva el Lease y un seguidor lo mira.</summary>
    public TimeSpan RetryPeriod { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// URL HTTP de un broker a partir de su NodeId, p. ej. <c>http://{0}.broker:9090</c>. Sin configurar no se conoce la
    /// dirección del líder, y la API no puede redirigirle las escrituras.
    /// </summary>
    public string? LeaderAddressFormat { get; set; }

    internal bool IsValid() =>
        LeaseName.Length > 0
        && RetryPeriod > TimeSpan.Zero
        && RetryPeriod < RenewDeadline
        && RenewDeadline < LeaseDuration;
}
