namespace K0sStreams.Contracts;

public interface IReplicator
{
    /// <summary>
    /// Espera a que <paramref name="offset"/> esté en disco en la mayoría (2 de 3) y sube el high watermark del log.
    /// Si este broker deja de ser líder mientras espera, lanza <see cref="NotLeaderException"/>.
    /// Si <paramref name="timeout"/> se agota antes de alcanzar el quórum, lanza <see cref="QuorumLostException"/>.
    /// </summary>
    ValueTask WaitForQuorumAsync(string topic, int partition, long offset, TimeSpan? timeout = null, CancellationToken ct = default);
}
