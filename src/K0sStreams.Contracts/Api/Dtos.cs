namespace K0sStreams.Contracts.Api;

// Cuerpos JSON de la API REST (puerto 9090). Claves y valores viajan como texto UTF-8.

public sealed record CreateTopicRequest(string Name, int MaxRetries = 3, TimeSpan? RetryBackoff = null)
{
    public static readonly TimeSpan DefaultRetryBackoff = TimeSpan.FromSeconds(5);

    public TopicConfig ToConfig() => new(Name, MaxRetries, RetryBackoff ?? DefaultRetryBackoff);
}

public sealed record TopicResponse(string Name, int MaxRetries, TimeSpan RetryBackoff)
{
    public static TopicResponse From(TopicConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);
        return new(config.Name, config.MaxRetries, config.RetryBackoff);
    }
}

/// <param name="Key">Opcional. Clasifica el mensaje; viaja con él.</param>
/// <param name="Value">Contenido del mensaje.</param>
/// <param name="DeliverAt">Opcional. Para colas: no se entrega antes de este momento.</param>
public sealed record PublishRequest(string? Key, string Value, DateTimeOffset? DeliverAt = null);

public sealed record PublishResponse(long Offset);

public sealed record MessageResponse(long Offset, string? Key, string Value, DateTimeOffset Timestamp, DateTimeOffset? DeliverAt);

/// <param name="Messages">Mensajes leídos (los eventos internos de cola no se muestran).</param>
/// <param name="HighWatermark">Último offset confirmado del tópico.</param>
/// <param name="NextOffset">Valor de <c>from</c> para seguir leyendo.</param>
public sealed record ReadResponse(IReadOnlyList<MessageResponse> Messages, long HighWatermark, long NextOffset);

/// <param name="VisibilitySeconds">Tiempo para hacer ack antes de que el mensaje vuelva a la cola. Por defecto 30.</param>
public sealed record ReceiveRequest(int? VisibilitySeconds = null);

public sealed record DeliveryResponse(long Offset, string? Key, string Value, int Attempt, DateTimeOffset VisibleUntil);

public sealed record NackRequest(string? Reason = null);

public sealed record DeadLetterResponse(long Offset, string? Key, string Value, int Attempts, string? LastReason);
