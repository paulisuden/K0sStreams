using K0sStreams.Contracts;
using K0sStreams.Contracts.Grpc;
using K0sStreams.Replication.Tests.Support;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using static K0sStreams.Replication.Tests.Support.TestRecords;

namespace K0sStreams.Replication.Tests;

/// <summary>Asking several brokers the same thing and returning once enough of them answered.</summary>
public sealed class MajorityCallTests : IDisposable
{
    private readonly FakeTimeProvider _time = new();
    private readonly TestNode[] _brokers = [new("broker-1"), new("broker-2")];
    private readonly FaultyPeer[] _links;
    private readonly MajorityCall _majority;

    public MajorityCallTests()
    {
        _links = [.. _brokers.Select(broker => new FaultyPeer(broker.Node))];
        _majority = new MajorityCall(Options.Create(new ReplicationOptions()), _time);
    }

    private KeyValuePair<string, IReplicationPeer>[] Peers =>
        [.. _links.Select((link, i) => KeyValuePair.Create(_brokers[i].Cluster.NodeId, (IReplicationPeer)link))];

    public void Dispose()
    {
        foreach (var broker in _brokers)
        {
            broker.Dispose();
        }
    }

    [Fact]
    public async Task It_returns_as_soon_as_enough_brokers_answered()
    {
        _links[1].Pause();

        var answers = await _majority.AskAsync(Peers, needed: 1, AskStateAsync, CancellationToken.None);

        answers.Should().ContainSingle().Which.NodeId.Should().Be("broker-1");
    }

    [Fact]
    public async Task An_unreachable_broker_is_asked_again_until_it_answers()
    {
        _links[0].Disconnect();
        var ask = _majority.AskAsync(Peers[..1], needed: 1, AskStateAsync, CancellationToken.None);
        await _time.AdvanceSlowlyAsync(TimeSpan.FromSeconds(3));
        ask.IsCompleted.Should().BeFalse();

        _links[0].Reconnect();

        await _time.EventuallyAsync(() => ask.IsCompleted, "the broker to answer");
        (await ask).Should().ContainSingle();
    }

    [Fact]
    public async Task A_broker_that_does_not_answer_in_time_is_asked_again()
    {
        var hangsOnce = new HangsOncePeer(_brokers[0].Node);
        var ask = _majority.AskAsync([KeyValuePair.Create("broker-1", (IReplicationPeer)hangsOnce)], needed: 1, AskStateAsync, CancellationToken.None);

        await _time.EventuallyAsync(() => ask.IsCompleted, "the second attempt to answer");

        (await ask).Should().ContainSingle();
        hangsOnce.Calls.Should().Be(2);
    }

    [Fact]
    public async Task A_broker_at_a_newer_epoch_stops_everything()
    {
        Task<StateResponse> Ask(IReplicationPeer peer, CancellationToken ct) =>
            peer == _links[1] ? Task.FromException<StateResponse>(new NotLeaderException("Newer epoch.")) : AskStateAsync(peer, ct);
        _links[0].Pause();

        await FluentActions.Awaiting(() => _majority.AskAsync(Peers, needed: 1, Ask, CancellationToken.None))
            .Should().ThrowAsync<NotLeaderException>();
    }

    [Fact]
    public async Task Cancelling_stops_the_asking()
    {
        _links[0].Disconnect();
        _links[1].Disconnect();
        using var cts = new CancellationTokenSource();
        var ask = _majority.AskAsync(Peers, needed: 1, AskStateAsync, cts.Token);
        await _time.AdvanceSlowlyAsync(TimeSpan.FromSeconds(1));

        await cts.CancelAsync();

        await FluentActions.Awaiting(() => ask).Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task Needing_no_answers_returns_at_once()
    {
        _links[0].Disconnect();
        _links[1].Disconnect();

        (await _majority.AskAsync(Peers, needed: 0, AskStateAsync, CancellationToken.None)).Should().BeEmpty();
    }

    private static Task<StateResponse> AskStateAsync(IReplicationPeer peer, CancellationToken ct) =>
        peer.GetStateAsync(new StateRequest { Topic = Topic }, ct);

    /// <summary>Its first GetState never answers (until cancelled); later calls go through.</summary>
    private sealed class HangsOncePeer(IReplicationPeer inner) : IReplicationPeer
    {
        private int _calls;

        public int Calls => Volatile.Read(ref _calls);

        public async Task<StateResponse> GetStateAsync(StateRequest request, CancellationToken ct = default)
        {
            if (Interlocked.Increment(ref _calls) == 1)
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            }

            return await inner.GetStateAsync(request, ct);
        }

        public Task<AppendResponse> AppendAsync(AppendRequest request, CancellationToken ct = default) => inner.AppendAsync(request, ct);

        public IAsyncEnumerable<RecordBatch> FetchAsync(FetchRequest request, CancellationToken ct = default) => inner.FetchAsync(request, ct);

        public Task<ListTopicsResponse> ListTopicsAsync(ListTopicsRequest request, CancellationToken ct = default) => inner.ListTopicsAsync(request, ct);
    }
}
