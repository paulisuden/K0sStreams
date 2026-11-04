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
        
        // Queue endpoints with consumer group support
        topics.MapPost("/{topic}/queues/{queue}/join", JoinGroupAsync).AddEndpointFilter(LeaderOnlyAsync);
        topics.MapPost("/{topic}/queues/{queue}/leave", LeaveGroupAsync).AddEndpointFilter(LeaderOnlyAsync);
        topics.MapPost("/{topic}/queues/{queue}/heartbeat", HeartbeatAsync).AddEndpointFilter(LeaderOnlyAsync);
        topics.MapPost("/{topic}/queues/{queue}/receive", ReceiveAsync).AddEndpointFilter(LeaderOnlyAsync);
        topics.MapPost("/{topic}/queues/{queue}/ack/{partition:int}/{offset:long}", AckAsync).AddEndpointFilter(LeaderOnlyAsync);
        topics.MapPost("/{topic}/queues/{queue}/nack/{partition:int}/{offset:long}", NackAsync).AddEndpointFilter(LeaderOnlyAsync);
        topics.MapPost("/{topic}/queues/{queue}/offsets", CommitOffsetsAsync).AddEndpointFilter(LeaderOnlyAsync);
        topics.MapGet("/{topic}/queues/{queue}/consumers", GetConsumerStatesAsync).AddEndpointFilter(LeaderOnlyAsync);
        topics.MapGet("/{topic}/queues/{queue}/dlq", GetDeadLettersAsync).AddEndpointFilter(LeaderOnlyAsync);

        return endpoints;
    }

    /// <summary>En un seguidor, no ejecuta la ruta: redirige el pedido al líder.</summary>
    private static async ValueTask<object?> LeaderOnlyAsync(EndpointFilterInvocationContext context, EndpointFilterDelegate next)
    {
        IClusterState cluster = context.HttpContext.RequestServices.GetRequiredService<IClusterState>();
        if (!cluster.IsLeader)
        {
            return RedirectToLeader(cluster, context.HttpContext.Request);
        }

        try
        {
            return await next(context).ConfigureAwait(false);
        }
        catch (NotLeaderException)
        {
            // Dejó de ser líder esperando el quórum: lo escrito no quedó confirmado y el cliente lo reintenta en el nuevo.
            return RedirectToLeader(cluster, context.HttpContext.Request);
        }
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

        // Elige la partición por clave (CRC32 módulo particiones). Sin clave va a la partición 0.
        TopicConfig topicConfig = catalog.Find(topic)!;
        int partition = Partitioner.ForKey(key.Span, topicConfig.Partitions);

        // Solo el líder escribe. La replicación sube el high watermark cuando hay quórum.
        long offset = await log.AppendAsync(topic, partition, record, ct).ConfigureAwait(false);
        try
        {
            // 5 s de espera: si ambos seguidores están caídos, no se bloquea para siempre.
            await replicator.WaitForQuorumAsync(topic, partition, offset, TimeSpan.FromSeconds(5), ct).ConfigureAwait(false);
        }
        catch (QuorumLostException)
        {
            // Seguimos siendo líder, pero no hay suficientes seguidores para confirmar.
            return Results.Problem("No se alcanzó el quórum para confirmar la escritura; reintentar.", statusCode: StatusCodes.Status503ServiceUnavailable);
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
            if (record.Offset > highWatermark)
            {
                break;
            }

            nextOffset = record.Offset + 1;

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

    private static async Task<IResult> JoinGroupAsync(
        string topic,
        string queue,
        JoinGroupRequest request,
        IQueueEngine engine,
        CancellationToken ct)
    {
        if (string.IsNullOrEmpty(request.ConsumerId))
        {
            return Results.ValidationProblem(new Dictionary<string, string[]>
            {
                [nameof(JoinGroupRequest.ConsumerId)] = ["ConsumerId es requerido."],
            });
        }

        try
        {
            var assignment = await engine.JoinGroupAsync(topic, queue, request.ConsumerId, ct).ConfigureAwait(false);
            return Results.Ok(new JoinGroupResponse(
                assignment.ConsumerId,
                assignment.Partitions,
                assignment.Generation));
        }
        catch (InvalidOperationException ex)
        {
            return Results.NotFound(new { error = ex.Message });
        }
    }

    private static async Task<IResult> LeaveGroupAsync(
        string topic,
        string queue,
        LeaveGroupRequest request,
        IQueueEngine engine,
        CancellationToken ct)
    {
        if (string.IsNullOrEmpty(request.ConsumerId))
        {
            return Results.ValidationProblem(new Dictionary<string, string[]>
            {
                [nameof(LeaveGroupRequest.ConsumerId)] = ["ConsumerId es requerido."],
            });
        }

        await engine.LeaveGroupAsync(topic, queue, request.ConsumerId, ct).ConfigureAwait(false);
        return Results.Ok(new { topic, queue, consumerId = request.ConsumerId });
    }

    private static async Task<IResult> HeartbeatAsync(
        string topic,
        string queue,
        HeartbeatRequest request,
        IQueueEngine engine,
        CancellationToken ct)
    {
        if (string.IsNullOrEmpty(request.ConsumerId))
        {
            return Results.ValidationProblem(new Dictionary<string, string[]>
            {
                [nameof(HeartbeatRequest.ConsumerId)] = ["ConsumerId es requerido."],
            });
        }

        try
        {
            var assignment = await engine.HeartbeatAsync(topic, queue, request.ConsumerId, ct).ConfigureAwait(false);
            return Results.Ok(new HeartbeatResponse(
                assignment.ConsumerId,
                assignment.Partitions,
                assignment.Generation));
        }
        catch (InvalidOperationException ex)
        {
            return Results.NotFound(new { error = ex.Message });
        }
    }

    private static async Task<IResult> ReceiveAsync(
        string topic,
        string queue,
        ReceiveRequest request,
        IQueueEngine engine,
        CancellationToken ct)
    {
        if (string.IsNullOrEmpty(request.ConsumerId))
        {
            return Results.ValidationProblem(new Dictionary<string, string[]>
            {
                [nameof(ReceiveRequest.ConsumerId)] = ["ConsumerId es requerido."],
            });
        }

        int seconds = request.VisibilitySeconds ?? DefaultVisibilitySeconds;
        if (seconds <= 0)
        {
            return Results.ValidationProblem(new Dictionary<string, string[]>
            {
                [nameof(ReceiveRequest.VisibilitySeconds)] = ["Tiene que ser mayor que cero."],
            });
        }

        Delivery? delivery = await engine.ReceiveAsync(topic, queue, request.ConsumerId, TimeSpan.FromSeconds(seconds), ct).ConfigureAwait(false);
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

    private static async Task<IResult> CommitOffsetsAsync(
        string topic,
        string queue,
        CommitOffsetsRequest request,
        IQueueEngine engine,
        CancellationToken ct)
    {
        if (string.IsNullOrEmpty(request.ConsumerId))
        {
            return Results.ValidationProblem(new Dictionary<string, string[]>
            {
                [nameof(CommitOffsetsRequest.ConsumerId)] = ["ConsumerId es requerido."],
            });
        }

        if (request.Offsets == null || request.Offsets.Count == 0)
        {
            return Results.ValidationProblem(new Dictionary<string, string[]>
            {
                [nameof(CommitOffsetsRequest.Offsets)] = ["Al menos un offset es requerido."],
            });
        }

        try
        {
            await engine.CommitOffsetsAsync(topic, queue, request.ConsumerId, request.Offsets, ct).ConfigureAwait(false);
            return Results.Ok(new { topic, queue, consumerId = request.ConsumerId });
        }
        catch (InvalidOperationException ex)
        {
            return Results.NotFound(new { error = ex.Message });
        }
    }

    private static async Task<IResult> GetConsumerStatesAsync(
        string topic,
        string queue,
        IQueueEngine engine,
        CancellationToken ct)
    {
        var states = engine.GetConsumerStates(topic, queue);
        var response = states.Select(s => new ConsumerStateResponse(
            s.ConsumerId,
            s.LastHeartbeat,
            s.AssignedPartitions)).ToArray();
        return Results.Ok(response);
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