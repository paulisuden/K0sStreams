using System.Text;
using K0sStreams.Contracts;
using K0sStreams.Contracts.Api;

namespace K0sStreams.Broker.Api;

internal static class ApiEndpoints
{
    /// <summary>Visibilidad por defecto del receive, en segundos, si el cuerpo no la trae.</summary>
    private const int DefaultVisibilitySeconds = 30;

    /// <summary>Endpoints REST de clientes (puerto 9090). Dueño: B — Cola y API.</summary>
    public static IEndpointRouteBuilder MapK0sApi(this IEndpointRouteBuilder endpoints)
    {
        RouteGroupBuilder topics = endpoints.MapGroup("/v1/topics");

        // Lo que escribe en el log o usa el estado de la cola lo atiende el líder. Leer el historial no: los seguidores
        // tienen el log replicado hasta el high watermark.
        topics.MapPost("/", CreateTopicAsync).AddEndpointFilter(LeaderOnlyAsync);
        topics.MapPost("/{topic}/messages", PublishAsync).AddEndpointFilter(LeaderOnlyAsync);
        topics.MapGet("/{topic}/partitions/{partition:int}/messages", ReadAsync);
        topics.MapPost("/{topic}/queues/{queue}/receive", ReceiveAsync).AddEndpointFilter(LeaderOnlyAsync);
        topics.MapPost("/{topic}/queues/{queue}/ack/{partition:int}/{offset:long}", AckAsync).AddEndpointFilter(LeaderOnlyAsync);
        topics.MapPost("/{topic}/queues/{queue}/nack/{partition:int}/{offset:long}", NackAsync).AddEndpointFilter(LeaderOnlyAsync);
        topics.MapGet("/{topic}/queues/{queue}/dlq", GetDeadLettersAsync).AddEndpointFilter(LeaderOnlyAsync);

        return endpoints;
    }

    /// <summary>En un seguidor, no ejecuta la ruta: redirige el pedido al líder.</summary>
    private static async ValueTask<object?> LeaderOnlyAsync(EndpointFilterInvocationContext context, EndpointFilterDelegate next)
    {
        IClusterState cluster = context.HttpContext.RequestServices.GetRequiredService<IClusterState>();
        return cluster.IsLeader
            ? await next(context).ConfigureAwait(false)
            : RedirectToLeader(cluster, context.HttpContext.Request);
    }

    /// <summary>
    /// 307 conserva el método y el cuerpo, así que el cliente repite el mismo pedido en el líder. Sin líder conocido (por
    /// ejemplo, en medio de un failover) responde 503 para que el cliente reintente.
    /// </summary>
    private static IResult RedirectToLeader(IClusterState cluster, HttpRequest request) =>
        cluster.LeaderAddress is null
            ? Results.Problem("No hay un líder conocido; reintentar en unos segundos.", statusCode: StatusCodes.Status503ServiceUnavailable)
            : Results.Redirect(cluster.LeaderAddress.TrimEnd('/') + request.Path + request.QueryString, permanent: false, preserveMethod: true);

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
        HttpRequest httpRequest,
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

        // Cada tópico es un solo log (DEC-001): todo va a la partición 0, la única que acepta la replicación.
        const int partition = 0;

        // Solo el líder escribe. La replicación sube el high watermark cuando hay quórum.
        long offset = await log.AppendAsync(topic, partition, record, ct).ConfigureAwait(false);
        try
        {
            await replicator.WaitForQuorumAsync(topic, partition, offset, ct).ConfigureAwait(false);
        }
        catch (NotLeaderException)
        {
            // Dejó de ser líder mientras esperaba: el mensaje no quedó confirmado y el cliente lo reintenta en el nuevo.
            return RedirectToLeader(cluster, httpRequest);
        }

        return Results.Ok(new PublishResponse(partition, offset));
    }

    private static async Task<IResult> ReadAsync(
        string topic,
        int partition,
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
        long highWatermark = log.HighWatermark(topic, partition);
        var messages = new List<MessageResponse>();
        long nextOffset = fromOffset;

        // Solo hasta el high watermark: no se muestra un mensaje que todavía podría perderse.
        await foreach (var record in log.ReadAsync(topic, partition, fromOffset, limit, ct).ConfigureAwait(false))
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
            delivery.Partition,
            delivery.Offset,
            delivery.Message.Key.IsEmpty ? null : Encoding.UTF8.GetString(delivery.Message.Key.Span),
            Encoding.UTF8.GetString(delivery.Message.Value.Span),
            delivery.Attempt,
            delivery.VisibleUntil));
    }

    private static async Task<IResult> AckAsync(
        string topic,
        string queue,
        int partition,
        long offset,
        IQueueEngine engine,
        CancellationToken ct)
    {
        bool acked = await engine.AckAsync(topic, queue, partition, offset, ct).ConfigureAwait(false);
        return acked
            ? Results.Ok(new { topic, queue, partition, offset })
            : Results.NotFound(new { error = "El mensaje no estaba en proceso en esa cola." });
    }

    private static async Task<IResult> NackAsync(
        string topic,
        string queue,
        int partition,
        long offset,
        NackRequest? request,
        IQueueEngine engine,
        CancellationToken ct)
    {
        bool nacked = await engine.NackAsync(topic, queue, partition, offset, request?.Reason, ct).ConfigureAwait(false);
        return nacked
            ? Results.Ok(new { topic, queue, partition, offset })
            : Results.NotFound(new { error = "El mensaje no estaba en proceso en esa cola." });
    }

    private static IResult GetDeadLettersAsync(
        string topic,
        string queue,
        IQueueEngine engine)
    {
        IReadOnlyList<DeadLetter> deadLetters = engine.GetDeadLetters(topic, queue);
        var response = deadLetters.Select(dl => new DeadLetterResponse(
            dl.Partition,
            dl.Offset,
            dl.Message.Key.IsEmpty ? null : Encoding.UTF8.GetString(dl.Message.Key.Span),
            Encoding.UTF8.GetString(dl.Message.Value.Span),
            dl.Attempts,
            dl.LastReason)).ToArray();
        return Results.Ok(response);
    }
}
