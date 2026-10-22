namespace K0sStreams.Coordination;

/// <summary>Lo que la elección necesita de un Lease: quién lo tiene, cuántas veces cambió de dueño (la época) y su versión.</summary>
/// <param name="HolderIdentity">NodeId del titular, o null si está libre.</param>
/// <param name="Transitions">Cuántas veces cambió de dueño. Es la época del titular.</param>
/// <param name="Version">El <c>resourceVersion</c> de Kubernetes: cambia en cada escritura, incluida cada renovación.</param>
internal sealed record LeaseRecord(string? HolderIdentity, int Transitions, string Version);

/// <summary>
/// El Lease del líder. Cada escritura es un compare-and-swap: se aplica solo si nadie escribió desde la versión que se
/// leyó. Así, de dos brokers que intentan tomarlo a la vez, gana uno solo.
/// </summary>
internal interface ILeaseStore
{
    /// <summary>El Lease, o null si todavía no existe.</summary>
    Task<LeaseRecord?> GetAsync(CancellationToken ct = default);

    /// <summary>Crea el Lease a nombre de <paramref name="holder"/>. Devuelve null si ya existía.</summary>
    Task<LeaseRecord?> TryCreateAsync(string holder, int transitions, CancellationToken ct = default);

    /// <summary>
    /// Escribe <paramref name="holder"/> (null lo libera) y <paramref name="transitions"/>, con la hora actual como
    /// renovación, solo si el Lease sigue en la versión de <paramref name="expected"/>. Devuelve null si otro escribió en
    /// el medio.
    /// </summary>
    Task<LeaseRecord?> TryReplaceAsync(LeaseRecord expected, string? holder, int transitions, CancellationToken ct = default);
}
