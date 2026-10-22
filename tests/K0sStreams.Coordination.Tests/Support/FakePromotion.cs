using K0sStreams.Contracts;

namespace K0sStreams.Coordination.Tests.Support;

/// <summary>Promoción controlable: anota cada época y puede quedar en curso o fallar.</summary>
internal sealed class FakePromotion : ILeaderPromotion
{
    private TaskCompletionSource? _pending;

    public List<long> Epochs { get; } = [];

    /// <summary>Si es true, cada promoción queda en curso hasta <see cref="Complete"/>.</summary>
    public bool Hold { get; set; }

    public Exception? FailWith { get; set; }

    /// <summary>El token de la última promoción, para ver si el elector la canceló.</summary>
    public CancellationToken Token { get; private set; }

    public ValueTask PromoteAsync(long newEpoch, CancellationToken ct = default)
    {
        Epochs.Add(newEpoch);
        Token = ct;
        if (FailWith is not null)
        {
            return ValueTask.FromException(FailWith);
        }

        if (!Hold)
        {
            return ValueTask.CompletedTask;
        }

        _pending = new TaskCompletionSource();
        return new ValueTask(_pending.Task.WaitAsync(ct));
    }

    public void Complete() => _pending?.SetResult();
}
