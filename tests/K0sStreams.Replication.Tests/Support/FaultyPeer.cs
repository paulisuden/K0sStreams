using System.Runtime.CompilerServices;
using Grpc.Core;
using K0sStreams.Contracts.Grpc;

namespace K0sStreams.Replication.Tests.Support;

/// <summary>
/// The network between the leader and one follower, with faults the test can switch on: the follower unreachable,
/// calls stuck until resumed, or a response lost after the follower already handled the call.
/// </summary>
internal sealed class FaultyPeer(IReplicationPeer inner) : IReplicationPeer
{
    private IReplicationPeer _inner = inner;
    private TaskCompletionSource? _pause;
    private int _dropNextResponse;
    private volatile bool _disconnected;

    /// <summary>The follower behind this link. Replace it to simulate a restart.</summary>
    public IReplicationPeer Inner
    {
        get => Volatile.Read(ref _inner);
        set => Volatile.Write(ref _inner, value);
    }

    public void Disconnect() => _disconnected = true;

    public void Reconnect() => _disconnected = false;

    public void Pause() => Volatile.Write(ref _pause, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));

    public void Resume() => Interlocked.Exchange(ref _pause, null)?.TrySetResult();

    public void DropNextResponse() => Volatile.Write(ref _dropNextResponse, 1);

    public async Task<AppendResponse> AppendAsync(AppendRequest request, CancellationToken ct = default)
    {
        await PassAsync(ct);
        var response = await Inner.AppendAsync(request, ct);
        if (Interlocked.Exchange(ref _dropNextResponse, 0) == 1)
        {
            throw Unavailable("the response was lost");
        }

        return response;
    }

    public async IAsyncEnumerable<RecordBatch> FetchAsync(FetchRequest request, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await PassAsync(ct);
        await foreach (var batch in Inner.FetchAsync(request, ct))
        {
            yield return batch;
        }
    }

    public async Task<StateResponse> GetStateAsync(StateRequest request, CancellationToken ct = default)
    {
        await PassAsync(ct);
        return await Inner.GetStateAsync(request, ct);
    }

    private async Task PassAsync(CancellationToken ct)
    {
        if (Volatile.Read(ref _pause) is { } pause)
        {
            await pause.Task.WaitAsync(ct);
        }

        if (_disconnected)
        {
            throw Unavailable("the follower is unreachable");
        }
    }

    private static RpcException Unavailable(string detail) => new(new Status(StatusCode.Unavailable, detail));
}
