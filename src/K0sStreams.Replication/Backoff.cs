namespace K0sStreams.Replication;

/// <summary>
/// The growing wait between retries: <see cref="ReplicationOptions.RetryBackoff"/> first, doubling up to
/// <see cref="ReplicationOptions.MaxRetryBackoff"/>. One per retry loop; not thread-safe.
/// </summary>
internal sealed class Backoff(ReplicationOptions options, TimeProvider time)
{
    private TimeSpan _current;

    public Task WaitAsync(CancellationToken ct)
    {
        _current = _current == TimeSpan.Zero
            ? options.RetryBackoff
            : TimeSpan.FromTicks(Math.Min(_current.Ticks * 2, options.MaxRetryBackoff.Ticks));
        return Task.Delay(_current, time, ct);
    }
}
