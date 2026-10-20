using K0sStreams.Contracts;
using K0sStreams.Replication.Tests.Support;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using static K0sStreams.Replication.Tests.Support.TestRecords;

namespace K0sStreams.Replication.Tests;

/// <summary>Step 2 of a promotion: copying what a more complete log has after this broker's confirmed prefix.</summary>
public sealed class LogCatchUpTests : IDisposable
{
    private const long Epoch = 3;

    private readonly TestNode _self = new("broker-2");
    private readonly TestNode _source = new("broker-1");
    private readonly LogCatchUp _catchUp;

    public LogCatchUpTests()
    {
        // The promotion adopts the new epoch before copying anything.
        _self.Epochs.Observe(Epoch);
        _catchUp = new LogCatchUp(_self.Log, _self.Node, _self.Cluster, Options.Create(new ReplicationOptions()), new FakeTimeProvider());
    }

    public void Dispose()
    {
        _self.Dispose();
        _source.Dispose();
    }

    [Fact]
    public async Task It_copies_what_comes_after_the_confirmed_prefix_and_adopts_the_sources_high_watermark()
    {
        await SeedAsync(_self.Log, 1, 1);
        _self.Log.AdvanceHighWatermark(Topic, 1);
        await SeedAsync(_source.Log, 1, 1, 1, 1);
        _source.Log.AdvanceHighWatermark(Topic, 2);

        (await CopyAsync(new LogTip(3, 1))).Should().BeTrue();

        (await EncodedAsync(_self.Log)).Should().Equal(await EncodedAsync(_source.Log));
        _self.Log.HighWatermark(Topic).Should().Be(2);
    }

    [Fact]
    public async Task It_copies_from_the_start_when_nothing_is_confirmed()
    {
        await SeedAsync(_source.Log, 1, 1);

        (await CopyAsync(new LogTip(1, 1))).Should().BeTrue();

        (await EpochsAsync(_self.Log)).Should().Equal(1, 1);
    }

    [Fact]
    public async Task It_replaces_a_tail_that_was_never_confirmed()
    {
        await SeedAsync(_self.Log, 1, 1, 1); // 1 and 2 came from a leader that died before confirming them
        _self.Log.AdvanceHighWatermark(Topic, 0);
        await SeedAsync(_source.Log, 1, 2, 2, 2);

        (await CopyAsync(new LogTip(3, 2))).Should().BeTrue();

        (await EpochsAsync(_self.Log)).Should().Equal(1, 2, 2, 2);
    }

    [Fact]
    public async Task It_stops_where_the_source_ended_in_the_survey()
    {
        await SeedAsync(_source.Log, 1, 1, 1, 1); // the survey saw it end at 1; it grew afterwards

        (await CopyAsync(new LogTip(1, 1))).Should().BeTrue();

        _self.Log.EndOffset(Topic).Should().Be(1);
    }

    [Fact]
    public async Task It_reports_a_source_whose_log_changed_since_the_survey()
    {
        await SeedAsync(_source.Log, 1, 1); // the survey saw it end at 3

        (await CopyAsync(new LogTip(3, 1))).Should().BeFalse();
    }

    [Fact]
    public async Task It_never_overwrites_a_confirmed_record()
    {
        await SeedAsync(_self.Log, 1, 1);
        _self.Log.AdvanceHighWatermark(Topic, 1);
        await SeedAsync(_source.Log, 1, 2, 2); // disagrees on offset 1, which this broker counts as confirmed

        await FluentActions.Awaiting(() => CopyAsync(new LogTip(2, 2))).Should().ThrowAsync<InvalidOperationException>();

        (await EpochsAsync(_self.Log)).Should().Equal(1, 1);
    }

    [Fact]
    public async Task A_newer_epoch_seen_meanwhile_stops_the_copy()
    {
        await SeedAsync(_source.Log, 1, 1);
        _self.Epochs.Observe(Epoch + 1); // another broker took over meanwhile

        await FluentActions.Awaiting(() => CopyAsync(new LogTip(1, 1))).Should().ThrowAsync<NotLeaderException>();

        _self.Log.EndOffset(Topic).Should().Be(-1);
    }

    private Task<bool> CopyAsync(LogTip surveyed) =>
        _catchUp.TryCopyAsync(Topic, Epoch, new SurveyAnswer("broker-1", _source.Node, surveyed), CancellationToken.None);
}
