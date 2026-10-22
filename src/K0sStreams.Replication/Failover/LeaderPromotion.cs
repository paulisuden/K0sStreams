using Grpc.Core;
using K0sStreams.Contracts;
using K0sStreams.Replication.Common;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace K0sStreams.Replication.Failover;

/// <summary>
/// Makes this broker ready to lead a new epoch without losing a confirmed record (docs/replication.md §10.1). For every
/// topic, three steps, each in its own class:
/// <list type="number">
/// <item><see cref="PeerSurvey"/> announces the epoch to a majority and finds where each log ends;</item>
/// <item><see cref="LogCatchUp"/> copies what the most complete of those logs has and this one lacks;</item>
/// <item><see cref="EpochChangeWriter"/> marks the start of the epoch.</item>
/// </list>
/// Any confirmed record is on a majority, and any two majorities share a broker, so the survey always sees it.
/// </summary>
internal sealed partial class LeaderPromotion(
    IClusterState cluster,
    EpochTracker epochs,
    PeerSurvey survey,
    LogCatchUp catchUp,
    EpochChangeWriter epochChange,
    IOptions<ReplicationOptions> options,
    TimeProvider time,
    ILogger<LeaderPromotion> logger) : ILeaderPromotion, IDisposable
{
    private readonly SemaphoreSlim _oneAtATime = new(1, 1);

    public async ValueTask PromoteAsync(long newEpoch, CancellationToken ct = default)
    {
        if (cluster.IsLeader)
        {
            // While IsLeader is true, records stamped with the old epoch could land after the new epoch's records.
            throw new InvalidOperationException("Promote before announcing leadership: IsLeader must still be false.");
        }

        await _oneAtATime.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (newEpoch < epochs.Current)
            {
                throw new NotLeaderException($"Epoch {newEpoch} is older than this broker's {epochs.Current}.");
            }

            epochs.Observe(newEpoch);
            LogPromoting(logger, newEpoch);
            foreach (string topic in await survey.ListTopicsAsync(newEpoch, ct).ConfigureAwait(false))
            {
                await PromoteTopicAsync(topic, newEpoch, ct).ConfigureAwait(false);
            }

            LogReady(logger, newEpoch);
        }
        finally
        {
            _oneAtATime.Release();
        }
    }

    public void Dispose() => _oneAtATime.Dispose();

    private async Task PromoteTopicAsync(string topic, long epoch, CancellationToken ct)
    {
        var backoff = new Backoff(options.Value, time);
        while (true)
        {
            var best = PeerSurvey.MostComplete(await survey.SurveyAsync(topic, epoch, ct).ConfigureAwait(false));
            if (best.Tip.IsEmpty)
            {
                // No broker has a record of this topic: there's nothing to lead.
                return;
            }

            if (best.NodeId == cluster.NodeId || await TryCatchUpAsync(topic, epoch, best, ct).ConfigureAwait(false))
            {
                await epochChange.WriteAsync(topic, epoch, ct).ConfigureAwait(false);
                return;
            }

            await backoff.WaitAsync(ct).ConfigureAwait(false);
        }
    }

    /// <summary>False if the copy has to start over from a new survey: the source failed or its log changed.</summary>
    private async Task<bool> TryCatchUpAsync(string topic, long epoch, SurveyAnswer source, CancellationToken ct)
    {
        LogCatchingUp(logger, topic, source.NodeId, source.Tip.EndOffset);
        try
        {
            return await catchUp.TryCopyAsync(topic, epoch, source, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is RpcException or OperationCanceledException && !ct.IsCancellationRequested)
        {
            LogCatchUpFailed(logger, ex, topic, source.NodeId);
            return false;
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Getting ready to lead epoch {Epoch}.")]
    private static partial void LogPromoting(ILogger logger, long epoch);

    [LoggerMessage(Level = LogLevel.Information, Message = "Ready to lead epoch {Epoch}.")]
    private static partial void LogReady(ILogger logger, long epoch);

    [LoggerMessage(Level = LogLevel.Information, Message = "Copying {Topic} up to offset {EndOffset} from {Peer}, whose log is more complete.")]
    private static partial void LogCatchingUp(ILogger logger, string topic, string peer, long endOffset);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Copying {Topic} from {Peer} failed; surveying again.")]
    private static partial void LogCatchUpFailed(ILogger logger, Exception exception, string topic, string peer);
}
