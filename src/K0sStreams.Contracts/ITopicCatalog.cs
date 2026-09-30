namespace K0sStreams.Contracts;

/// <summary>Tópicos existentes y su configuración.</summary>
public interface ITopicCatalog
{
    /// <summary>Crea el tópico. Devuelve false si ya existía.</summary>
    ValueTask<bool> CreateAsync(TopicConfig config, CancellationToken ct = default);

    TopicConfig? Find(string name);

    IReadOnlyCollection<TopicConfig> List();
}
