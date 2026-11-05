namespace K0sStreams.Contracts.Api;

// Cuerpos JSON de la API REST (puerto 9090). Claves y valores viajan como texto UTF-8.

public sealed record PublishRequest(string? Key, string Value, DateTimeOffset? DeliverAt = null);

public sealed record PublishResponse(int Partition, long Offset);

public sealed record MessageResponse(long Offset, string? Key, string Value, DateTimeOffset Timestamp, DateTimeOffset? DeliverAt);

/// <param name="Messages">Mensajes leídos (los eventos internos de cola no se muestran).</param>
/// <param name="HighWatermark">Último offset confirmado de la partición.</param>
/// <param name="NextOffset">Valor de <c>from</c> para seguir leyendo.</param>
public sealed record ReadResponse(IReadOnlyList<MessageResponse> Messages, long HighWatermark, long NextOffset);

/// <param name="ConsumerId">Identificador único del consumidor.</param>
/// <param name="VisibilitySeconds">Tiempo para hacer ack antes de que el mensaje vuelva a la cola. Por defecto 30.</param>
public sealed record ReceiveRequest(string ConsumerId, int? VisibilitySeconds = null);

public sealed record DeliveryResponse(int Partition, long Offset, string? Key, string Value, int Attempt, DateTimeOffset VisibleUntil);

public sealed record NackRequest(string? Reason = null);

public sealed record DeadLetterResponse(int Partition, long Offset, string? Key, string Value, int Attempts, string? LastReason);

// Consumer Group API DTOs

/// <param name="ConsumerId">Identificador único del consumidor.</param>
public sealed record JoinGroupRequest(string ConsumerId);

/// <param name="ConsumerId">Identificador único del consumidor.</param>
/// <param name="Partitions">Particiones asignadas a este consumidor.</param>
/// <param name="Generation">Generación del grupo (incrementa en cada rebalance).</param>
public sealed record JoinGroupResponse(string ConsumerId, IReadOnlyList<int> Partitions, long Generation);

/// <param name="ConsumerId">Identificador único del consumidor.</param>
public sealed record LeaveGroupRequest(string ConsumerId);

/// <param name="ConsumerId">Identificador único del consumidor.</param>
public sealed record HeartbeatRequest(string ConsumerId);

/// <param name="ConsumerId">Identificador único del consumidor.</param>
/// <param name="Partitions">Particiones asignadas a este consumidor.</param>
/// <param name="Generation">Generación actual del grupo.</param>
public sealed record HeartbeatResponse(string ConsumerId, IReadOnlyList<int> Partitions, long Generation);

/// <param name="ConsumerId">Identificador único del consumidor.</param>
/// <param name="Offsets">Diccionario partición -> offset confirmado.</param>
public sealed record CommitOffsetsRequest(string ConsumerId, IReadOnlyDictionary<int, long> Offsets);

/// <param name="ConsumerId">Identificador único del consumidor.</param>
/// <param name="LastHeartbeat">Último latido recibido.</param>
/// <param name="AssignedPartitions">Particiones actualmente asignadas.</param>
public sealed record ConsumerStateResponse(string ConsumerId, DateTimeOffset LastHeartbeat, IReadOnlyList<int> AssignedPartitions);