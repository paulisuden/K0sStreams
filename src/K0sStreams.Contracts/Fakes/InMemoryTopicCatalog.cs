using System.Collections.Concurrent;

namespace K0sStreams.Contracts.Fakes;

/// <summary>Catálogo de tópicos en memoria. Se pierde al reiniciar.</summary>
public sealed class InMemoryTopicCatalog : ITopicCatalog
{
    private readonly ConcurrentDictionary<string, TopicConfig> _topics = new(StringComparer.Ordinal);

    public ValueTask<bool> CreateAsync(TopicConfig config, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(config);
        return ValueTask.FromResult(_topics.TryAdd(config.Name, config));
    }

    public TopicConfig? Find(string name) => _topics.GetValueOrDefault(name);

    public IReadOnlyCollection<TopicConfig> List() => [.. _topics.Values.OrderBy(t => t.Name, StringComparer.Ordinal)];
}
