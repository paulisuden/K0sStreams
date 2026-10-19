namespace K0sStreams.Contracts.Fakes;

/// <summary>Confirma al instante, como si hubiera quórum: un solo broker sin réplicas.</summary>
public sealed class InstantReplicator(ILog log) : IReplicator
{
    public ValueTask WaitForQuorumAsync(string topic, long offset, CancellationToken ct = default)
    {
        log.AdvanceHighWatermark(topic, offset);
        return ValueTask.CompletedTask;
    }
}
