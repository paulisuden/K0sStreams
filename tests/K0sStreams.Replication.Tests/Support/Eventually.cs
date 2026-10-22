using System.Diagnostics;
using Microsoft.Extensions.Time.Testing;

namespace K0sStreams.Replication.Tests.Support;

/// <summary>
/// Waiting for background loops. With a <see cref="FakeTimeProvider"/>, time only moves while a test waits for
/// something: each poll advances it a little, so backoffs and heartbeats happen without real waiting.
/// </summary>
internal static class Eventually
{
    private static readonly TimeSpan Step = TimeSpan.FromMilliseconds(50);
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(15);

    /// <summary>Polls <paramref name="condition"/>, advancing fake time between polls. Fails after 15 s of real time.</summary>
    public static async Task EventuallyAsync(this FakeTimeProvider time, Func<bool> condition, string what)
    {
        var watch = Stopwatch.StartNew();
        while (!condition())
        {
            if (watch.Elapsed > Patience)
            {
                throw new TimeoutException($"Timed out waiting for {what}.");
            }

            time.Advance(Step);
            await Task.Delay(1);
        }
    }

    /// <summary>Advances fake time by <paramref name="total"/> in small steps, letting the loops run between them.</summary>
    public static async Task AdvanceSlowlyAsync(this FakeTimeProvider time, TimeSpan total)
    {
        for (var elapsed = TimeSpan.Zero; elapsed < total; elapsed += Step)
        {
            time.Advance(Step);
            await Task.Delay(1);
        }
    }

    /// <summary>Polls <paramref name="condition"/> in real time, for tests with real servers.</summary>
    public static async Task UntilAsync(Func<bool> condition, string what)
    {
        var watch = Stopwatch.StartNew();
        while (!condition())
        {
            if (watch.Elapsed > Patience)
            {
                throw new TimeoutException($"Timed out waiting for {what}.");
            }

            await Task.Delay(20);
        }
    }
}
