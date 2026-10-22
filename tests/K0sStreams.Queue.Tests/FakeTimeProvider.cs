namespace K0sStreams.Queue.Tests;

/// <summary>Reloj controlable para probar el visibility timeout sin esperar de verdad.</summary>
public sealed class FakeTimeProvider(DateTimeOffset start) : TimeProvider
{
    public DateTimeOffset Now { get; private set; } = start;

    public override DateTimeOffset GetUtcNow() => Now;

    public void Advance(TimeSpan by) => Now += by;
}
