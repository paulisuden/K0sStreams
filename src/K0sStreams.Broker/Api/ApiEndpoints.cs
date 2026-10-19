using System.Text;
using K0sStreams.Contracts;
using K0sStreams.Contracts.Api;

namespace K0sStreams.Broker.Api;

/// <summary>
/// Endpoints REST de clientes (puerto 9090). Dueño: B — Cola y API.
/// Rutas según DEC-001 (docs/decisions.md): el tópico es un solo log, sin particiones.
/// </summary>
internal static class ApiEndpoints
{
    /// <summary>Visibilidad por defecto del receive, en segundos, si el cuerpo no la trae.</summary>
    private const int DefaultVisibilitySeconds = 30;

    public static IEndpointRouteBuilder MapK0sApi(this IEndpointRouteBuilder endpoints)
    {
        RouteGroupBuilder topics = endpoints.MapGroup("/v1/topics");

        topics.MapPost("/", CreateTopicAsync);
        topics.MapPost("/{topic}/messages", PublishAsync);
        topics.MapGet("/{topic}/messages", ReadAsync);
        topics.MapPost("/{topic}/queues/{queue}/receive", ReceiveAsync);
        topics.MapPost("/{topic}/queues/{queue}/ack/{offset:long}", AckAsync);

        return endpoints;
    }

    private static async Task<IResult> CreateTopicAsync(
        CreateTopicRequest request,
        ITopicCatalog catalog,
        CancellationToken ct)
    {
        TopicConfig config = request.ToConfig();
        Dictionary<string, string[]> errors = config.Validate();
        if (errors.Count > 0)
        {
            return Results.ValidationProblem(errors);
        }

        bool created = await catalog.CreateAsync(config, ct).ConfigureAwait(false);
        return created
            ? Results.Created($"/v1/topics/{config.Name}", TopicResponse.From(config))
            : Results.Conflict(new { error = $"El tópico '{config.Name}' ya existe." });
    }

    private static async Task<IResult> PublishAsync(
        string topic,
        PublishRequest request,
        ILog log,
        ITopicCatalog catalog,
        IReplicator replicator,
        IClusterState cluster,
        TimeProvider time,
        CancellationToken ct)
    {
        if (string.IsNullOrEmpty(request.Value))
        {
            return Results.ValidationProblem(new Dictionary<string, string[]>
            {
                [nameof(request.Value)] = ["El valor no puede estar vacío."],
            });
        }

        if (catalog.Find(topic) is null)
        {
            return Results.NotFound(new { error = $"El tópico '{topic}' no existe." });
        }

        ReadOnlyMemory<byte> key = request.Key is null ? ReadOnlyMemory<byte>.Empty : Encoding.UTF8.GetBytes(request.Key);
        long deliverAt = request.DeliverAt?.ToUnixTimeMilliseconds() ?? 0;
        var record = Record.NewMessage(cluster.CurrentEpoch, time.GetUtcNow().ToUnixTimeMilliseconds(), key, Encoding.UTF8.GetBytes(request.Value), deliverAt);

        // Solo el líder escribe. La replicación sube el high watermark cuando hay quórum.
        long offset = await log.AppendAsync(topic, record, ct).ConfigureAwait(false);
        await replicator.WaitForQuorumAsync(topic, offset, ct).ConfigureAwait(false);

        return Results.Ok(new PublishResponse(offset));
    }

    private static async Task<IResult> ReadAsync(
        string topic,
        ILog log,
        ITopicCatalog catalog,
        long? from,
        int? max,
        CancellationToken ct)
    {
        if (catalog.Find(topic) is null)
        {
            return Results.NotFound(new { error = $"El tópico '{topic}' no existe." });
        }

        long fromOffset = from ?? 0;
        if (fromOffset < 0)
        {
            return Results.ValidationProblem(new Dictionary<string, string[]>
            {
                [nameof(from)] = ["No puede ser negativo."],
            });
        }

        int limit = max ?? 100;
        long highWatermark = log.HighWatermark(topic);
        var messages = new List<MessageResponse>();
        long nextOffset = fromOffset;

        // Solo hasta el high watermark: no se muestra un mensaje que todavía podría perderse.
        await foreach (var record in log.ReadAsync(topic, fromOffset, limit, ct).ConfigureAwait(false))
        {
            nextOffset = record.Offset + 1;
            if (record.Offset > highWatermark)
            {
                break;
            }

            // En el mismo log conviven mensajes y eventos de cola; el historial solo muestra mensajes.
            if (record.Type != RecordType.Message)
            {
                continue;
            }

            messages.Add(new MessageResponse(
                record.Offset,
                record.Key.IsEmpty ? null : Encoding.UTF8.GetString(record.Key.Span),
                Encoding.UTF8.GetString(record.Value.Span),
                DateTimeOffset.FromUnixTimeMilliseconds(record.Timestamp),
                record.DeliverAt == 0 ? null : DateTimeOffset.FromUnixTimeMilliseconds(record.DeliverAt)));
        }

        return Results.Ok(new ReadResponse(messages, highWatermark, nextOffset));
    }

    private static async Task<IResult> ReceiveAsync(
        string topic,
        string queue,
        ReceiveRequest? request,
        IQueueEngine engine,
        CancellationToken ct)
    {
        int seconds = request?.VisibilitySeconds ?? DefaultVisibilitySeconds;
        if (seconds <= 0)
        {
            return Results.ValidationProblem(new Dictionary<string, string[]>
            {
                [nameof(ReceiveRequest.VisibilitySeconds)] = ["Tiene que ser mayor que cero."],
            });
        }

        Delivery? delivery = await engine.ReceiveAsync(topic, queue, TimeSpan.FromSeconds(seconds), ct).ConfigureAwait(false);
        if (delivery is null)
        {
            return Results.NoContent();
        }

        return Results.Ok(new DeliveryResponse(
            delivery.Offset,
            delivery.Message.Key.IsEmpty ? null : Encoding.UTF8.GetString(delivery.Message.Key.Span),
            Encoding.UTF8.GetString(delivery.Message.Value.Span),
            delivery.Attempt,
            delivery.VisibleUntil));
    }

    private static async Task<IResult> AckAsync(
        string topic,
        string queue,
        long offset,
        IQueueEngine engine,
        CancellationToken ct)
    {
        bool acked = await engine.AckAsync(topic, queue, offset, ct).ConfigureAwait(false);
        return acked
            ? Results.Ok(new { topic, queue, offset })
            : Results.NotFound(new { error = "El mensaje no estaba en proceso en esa cola." });
    }
}
