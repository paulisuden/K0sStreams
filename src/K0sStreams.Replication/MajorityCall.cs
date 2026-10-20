using Grpc.Core;
using K0sStreams.Contracts;
using Microsoft.Extensions.Options;

namespace K0sStreams.Replication;

/// <summary>An answer from one broker, with how to reach it again.</summary>
internal readonly record struct PeerAnswer<T>(string NodeId, IReplicationPeer Peer, T Answer);

/// <summary>
/// Asks several brokers the same thing in parallel and returns as soon as enough of them answered. A broker that fails,
/// or doesn't answer within <see cref="ReplicationOptions.RpcTimeout"/>, is asked again after a backoff until enough
/// answered or the caller gives up. A <see cref="NotLeaderException"/> from any answer stops everything: another broker
/// already leads a newer epoch.
/// </summary>
internal sealed class MajorityCall(IOptions<ReplicationOptions> options, TimeProvider time)
{
    private readonly ReplicationOptions _options = options.Value;

    public async Task<IReadOnlyList<PeerAnswer<T>>> AskAsync<T>(
        IReadOnlyList<KeyValuePair<string, IReplicationPeer>> peers,
        int needed,
        Func<IReplicationPeer, CancellationToken, Task<T>> ask,
        CancellationToken ct)
    {
        var answers = new List<PeerAnswer<T>>(needed);
        if (needed == 0)
        {
            return answers;
        }

        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var pending = peers.Select(peer => AskUntilAnsweredAsync(peer, ask, stop.Token)).ToList();
        try
        {
            while (answers.Count < needed)
            {
                var finished = await Task.WhenAny(pending).ConfigureAwait(false);
                pending.Remove(finished);
                answers.Add(await finished.ConfigureAwait(false));
            }

            return answers;
        }
        finally
        {
            // The brokers still being asked aren't needed anymore.
            await stop.CancelAsync().ConfigureAwait(false);
            await Task.WhenAll(pending.Cast<Task>()).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }
    }

    private async Task<PeerAnswer<T>> AskUntilAnsweredAsync<T>(
        KeyValuePair<string, IReplicationPeer> peer, Func<IReplicationPeer, CancellationToken, Task<T>> ask, CancellationToken ct)
    {
        var backoff = new Backoff(_options, time);
        while (true)
        {
            using (var deadline = new CancellationTokenSource(_options.RpcTimeout, time))
            using (var call = CancellationTokenSource.CreateLinkedTokenSource(ct, deadline.Token))
            {
                try
                {
                    return new PeerAnswer<T>(peer.Key, peer.Value, await ask(peer.Value, call.Token).ConfigureAwait(false));
                }
                catch (Exception ex) when (ex is RpcException or OperationCanceledException && !ct.IsCancellationRequested)
                {
                    // Unreachable, failing or too slow: ask again after the backoff.
                }
            }

            await backoff.WaitAsync(ct).ConfigureAwait(false);
        }
    }
}
