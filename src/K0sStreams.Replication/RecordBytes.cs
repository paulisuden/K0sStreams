using Google.Protobuf;
using K0sStreams.Contracts;

namespace K0sStreams.Replication;

/// <summary>Records as they travel between brokers: the same bytes as on disk (<see cref="RecordCodec"/>).</summary>
internal static class RecordBytes
{
    /// <summary>The array is new and never modified afterwards, so it backs the ByteString without a copy.</summary>
    public static ByteString Encode(Record record) => UnsafeByteOperations.UnsafeWrap(RecordCodec.Encode(record));

    public static Record Decode(ByteString bytes)
    {
        if (RecordCodec.TryDecode(bytes.Span, out var record, out int consumed) is not DecodeStatus.Ok || record is null || consumed != bytes.Length)
        {
            throw new InvalidDataException("A record from another broker doesn't decode.");
        }

        return record;
    }
}
