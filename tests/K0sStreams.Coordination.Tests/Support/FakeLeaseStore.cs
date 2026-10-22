using System.Globalization;

namespace K0sStreams.Coordination.Tests.Support;

/// <summary>Lease en memoria con el mismo compare-and-swap que Kubernetes: cada escritura cambia la versión.</summary>
internal sealed class FakeLeaseStore : ILeaseStore
{
    private int _lastVersion;

    public LeaseRecord? Current { get; private set; }

    /// <summary>Si no es null, toda operación falla con esta excepción, como con el API server caído.</summary>
    public Exception? FailWith { get; set; }

    /// <summary>Corre una vez, justo antes de la próxima escritura: otro broker que escribe entre la lectura y la escritura.</summary>
    public Action? BeforeWrite { get; set; }

    /// <summary>Otro broker escribe el Lease: lo toma, lo renueva o lo libera (<paramref name="holder"/> null).</summary>
    public void WrittenBy(string? holder, int transitions) =>
        Current = new LeaseRecord(holder, transitions, (++_lastVersion).ToString(CultureInfo.InvariantCulture));

    public Task<LeaseRecord?> GetAsync(CancellationToken ct = default) =>
        FailWith is null ? Task.FromResult(Current) : Task.FromException<LeaseRecord?>(FailWith);

    public Task<LeaseRecord?> TryCreateAsync(string holder, int transitions, CancellationToken ct = default) =>
        Write(expectedVersion: null, holder, transitions);

    public Task<LeaseRecord?> TryReplaceAsync(LeaseRecord expected, string? holder, int transitions, CancellationToken ct = default) =>
        Write(expected.Version, holder, transitions);

    private Task<LeaseRecord?> Write(string? expectedVersion, string? holder, int transitions)
    {
        if (FailWith is not null)
        {
            return Task.FromException<LeaseRecord?>(FailWith);
        }

        Action? beforeWrite = BeforeWrite;
        BeforeWrite = null;
        beforeWrite?.Invoke();

        // Crear espera que no exista (versión null); reemplazar, que nadie haya escrito desde la versión leída.
        if (Current?.Version != expectedVersion)
        {
            return Task.FromResult<LeaseRecord?>(null);
        }

        WrittenBy(holder, transitions);
        return Task.FromResult(Current);
    }
}
