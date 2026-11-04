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
            var answers = await survey.SurveyAsync(topic, epoch, ct).ConfigureAwait(false);
            if (answers.Count == 0)
            {
                // No broker has a record of this topic: there's nothing to lead.
                return;
            }

            // Check if any partition has data to promote
            bool anyData = false;
            for (int i = 0; i < answers.Count; i++)
            {
                var tips = answers[i].PartitionTips;
                for (int j = 0; j < tips.Count; j++)
                {
                    if (!tips[j].Tip.IsEmpty)
                    {
                        anyData = true;
                        break;
                    }
                }
                if (anyData)
                {
                    break;
                }
            }
            if (!anyData)
            {
                return;
            }

            // Try to catch up each partition from its best source
            bool allCaughtUp = true;
            int partitionCount = answers[0].PartitionTips.Count;
            for (int partition = 0; partition < partitionCount; partition++)
            {
                var bestTip = PeerSurvey.MostCompleteForPartition(answers, partition);
                if (bestTip.Tip.IsEmpty)
                {
                    // No data for this partition
                    continue;
                }

                SurveyAnswer? bestSource = null;
                for (int i = 0; i < answers.Count; i++)
                {
                    var tips = answers[i].PartitionTips;
                    for (int j = 0; j < tips.Count; j++)
                    {
                        if (tips[j].Partition == partition && tips[j].Tip == bestTip.Tip)
                        {
                            bestSource = answers[i];
                            break;
                        }
                    }
                    if (bestSource != null)
                    {
                        break;
                    }
                }

                if (bestSource != null && bestSource.NodeId != cluster.NodeId)
                {
                    var caughtUp = await TryCatchUpPartitionAsync(topic, epoch, bestSource, partition, ct).ConfigureAwait(false);
                    if (!caughtUp)
                    {
                        allCaughtUp = false;
                        break;
                    }
                }
            }

            if (!allCaughtUp)
            {
                await backoff.WaitAsync(ct).ConfigureAwait(false);
                continue;
            }

            // All partitions caught up, write epoch change to all
            await epochChange.WriteAsync(topic, epoch, ct).ConfigureAwait(false);
            return;
        }
    }

    /// <summary>False if the copy has to start over from a new survey: the source failed or its log changed.</summary>
    private async Task<bool> TryCatchUpPartitionAsync(string topic, long epoch, SurveyAnswer source, int partition, CancellationToken ct)
    {
        PartitionTip sourceTip = new(partition, LogTip.Empty);
        var tips = source.PartitionTips;
        for (int i = 0; i < tips.Count; i++)
        {
            if (tips[i].Partition == partition)
            {
                sourceTip = tips[i];
                break;
            }
        }
        LogCatchingUp(logger, topic, partition, source.NodeId, sourceTip.Tip.EndOffset);
        try
        {
            // Create a survey answer list with just this source for the specific partition
            var surveyAnswers = new List<SurveyAnswer> { source };
            return await catchUp.TryCopyAsync(topic, epoch, surveyAnswers, ct).ConfigureAwait(false);
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

    [LoggerMessage(Level = LogLevel.Information, Message = "Copying {topic} partition {partition} up to offset {endOffset} from {peer}, whose log is more complete.")]
    private static partial void LogCatchingUp(ILogger logger, string topic, int partition, string peer, long endOffset);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Copying {topic} from {peer} failed; surveying again.")]
    private static partial void LogCatchUpFailed(ILogger logger, Exception exception, string topic, string peer);
}