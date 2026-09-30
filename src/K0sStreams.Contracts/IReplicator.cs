namespace K0sStreams.Contracts;

public interface IReplicator
{
    /// <summary>
    /// Espera a que <paramref name="offset"/> esté en disco en la mayoría (2 de 3) y sube el high watermark del log.
    /// Si este broker deja de ser líder mientras espera, lanza <see cref="NotLeaderException"/>.
    /// </summary>
    ValueTask WaitForQuorumAsync(string topic, int partition, long offset, CancellationToken ct = default);
}
