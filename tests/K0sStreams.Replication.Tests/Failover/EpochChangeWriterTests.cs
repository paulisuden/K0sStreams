using System.Text;
using K0sStreams.Contracts;
using K0sStreams.Replication.Common;
using K0sStreams.Replication.Failover;
using K0sStreams.Replication.Tests.Support;
using Microsoft.Extensions.Time.Testing;
using static K0sStreams.Replication.Tests.Support.TestRecords;

namespace K0sStreams.Replication.Tests.Failover;

/// <summary>Step 3 of a promotion: marking the start of the new epoch in a topic.</summary>
public sealed class EpochChangeWriterTests : IDisposable
{
    private readonly TestNode _broker = new("broker-2");
    private readonly EpochChangeWriter _writer;

    public EpochChangeWriterTests()
    {
        _broker.Epochs.Observe(2);
        _writer = new EpochChangeWriter(_broker.Log, _broker.Node, _broker.Cluster, new FakeTimeProvider());
    }

    public void Dispose() => _broker.Dispose();

    [Fact]
    public async Task The_mark_goes_at_the_end_of_the_log_in_the_new_epoch()
    {
        await SeedAsync(_broker.Log, 1, 1);

        await _writer.WriteAsync(Topic, 2, CancellationToken.None);

        var mark = await _broker.Log.ReadAsync(Topic, 2, 1).SingleAsync();
        mark.Type.Should().Be(RecordType.EpochChange);
        mark.Epoch.Should().Be(2);
        Encoding.UTF8.GetString(mark.Value.Span).Should().Be("broker-2");
    }

    [Fact]
    public async Task Marking_again_at_the_same_epoch_changes_nothing()
    {
        await SeedAsync(_broker.Log, 1);

        await _writer.WriteAsync(Topic, 2, CancellationToken.None);
        await _writer.WriteAsync(Topic, 2, CancellationToken.None);

        (await EpochsAsync(_broker.Log)).Should().Equal(1, 2);
    }

    [Fact]
    public async Task A_newer_epoch_seen_meanwhile_stops_the_mark()
    {
        await SeedAsync(_broker.Log, 1);
        _broker.Epochs.Observe(3);

        await FluentActions.Awaiting(() => _writer.WriteAsync(Topic, 2, CancellationToken.None)).Should().ThrowAsync<NotLeaderException>();

        (await EpochsAsync(_broker.Log)).Should().Equal(1);
    }
}
