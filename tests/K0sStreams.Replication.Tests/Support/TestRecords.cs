using System.Text;
using Google.Protobuf;
using K0sStreams.Contracts;
using K0sStreams.Contracts.Grpc;
using Record = K0sStreams.Contracts.Record;

namespace K0sStreams.Replication.Tests.Support;

internal static class TestRecords
{
    public const string Topic = "orders";

    public static Record Message(long offset, long epoch, string value = "") =>
        new(offset, epoch, RecordType.Message, 0, 0, ReadOnlyMemory<byte>.Empty, Encoding.UTF8.GetBytes(value));

    /// <summary>Records with offsets 0, 1, 2… and the given epochs.</summary>
    public static Record[] Sequence(params long[] epochs) => [.. epochs.Select((epoch, offset) => Message(offset, epoch, $"m{offset}"))];

    public static AppendRequest Append(long epoch, long prevOffset, long prevEpoch, long leaderHw, params Record[] records)
    {
        var request = new AppendRequest
        {
            Epoch = epoch,
            Topic = Topic,
            PrevOffset = prevOffset,
            PrevEpoch = prevEpoch,
            LeaderHw = leaderHw,
            LeaderId = "broker-0",
        };
        request.Records.AddRange(records.Select(record => ByteString.CopyFrom(RecordCodec.Encode(record))));
        return request;
    }

    public static async Task SeedAsync(ILog log, params long[] epochs)
    {
        foreach (var record in Sequence(epochs))
        {
            await log.AppendAsync(Topic, record);
        }
    }

    public static async Task<List<long>> EpochsAsync(ILog log) =>
        await log.ReadAsync(Topic, 0, int.MaxValue).Select(record => record.Epoch).ToListAsync();

    /// <summary>The log as base64-encoded records, to compare two logs byte for byte.</summary>
    public static async Task<List<string>> EncodedAsync(ILog log) =>
        await log.ReadAsync(Topic, 0, int.MaxValue).Select(record => Convert.ToBase64String(RecordCodec.Encode(record))).ToListAsync();
}
