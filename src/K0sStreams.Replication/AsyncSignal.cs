namespace K0sStreams.Replication;

/// <summary>
/// Wakes up one waiting loop. <see cref="Set"/> releases the current <see cref="WaitAsync"/>, or the next one if nobody
/// is waiting yet; several sets before a wait count as one.
/// </summary>
internal sealed class AsyncSignal
{
    private readonly Lock _gate = new();
    private TaskCompletionSource? _waiter;
    private bool _set;

    public void Set()
    {
        TaskCompletionSource? waiter;
        lock (_gate)
        {
            waiter = _waiter;
            _waiter = null;
            _set = waiter is null;
        }

        waiter?.TrySetResult();
    }

    public Task WaitAsync()
    {
        lock (_gate)
        {
            if (_set)
            {
                _set = false;
                return Task.CompletedTask;
            }

            _waiter ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            return _waiter.Task;
        }
    }
}
