using Grpc.Core;
using K0sStreams.Contracts;
using K0sStreams.Contracts.Grpc;
using K0sStreams.Replication.Common;
using K0sStreams.Replication.Network;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace K0sStreams.Replication.Follower;

/// <summary>
/// Pone al día a este broker cuando es seguidor (docs/replication.md, §5.4): le pregunta al líder dónde termina cada
/// tópico+partición y, si este log está atrás, se trae la diferencia con <c>Fetch</c> y la agrega por su propio
/// <c>Append</c>.
/// </summary>
/// <remarks>
/// El líder normalmente empuja con <c>Append</c> y retrocede cuando el seguidor le contesta <c>LOG_MISMATCH</c>, pero eso
/// solo pasa mientras tenga un <see cref="Leader.PeerReplicator"/> vivo para esa partición. Un pod que se reinicia no
/// tiene forma de avisar que volvió: hasta que el líder tenga algo nuevo que mandar (o que escribir, porque recién ahí
/// arranca el bucle de cada partición) el seguidor queda sirviendo un log atrasado. Por eso el que vuelve es el que
/// pide, que es para lo que existe <c>Fetch</c>.
///
/// Los registros entran por <see cref="ReplicationNode.AppendAsync"/>, igual que los que empuja el líder: pasan por el
/// fencing de época, la continuidad, el CRC, el truncado de una cola divergente y la regla del high watermark.
///
/// <para>
/// Cuándo corre: cuando hay una razón para creer que este log está atrás, no cada tanto porque sí. Un seguidor puede
/// atrasarse por lo que pasó mientras no estaba (arranca el proceso) o porque hay un líder nuevo con más historia (cambia
/// la época). Esas dos cosas lo despiertan; mientras quede algo por traer reintenta con espera creciente, y cuando una
/// pasada no encuentra diferencias se duerme. El temporizador solo queda como red de seguridad por si el empuje del
/// líder se callara sin dar error.
/// </para>
/// </remarks>
internal sealed partial class FollowerCatchUp(
    ILog log,
    ITopicCatalog catalog,
    IClusterState cluster,
    ReplicationNode self,
    IReadOnlyDictionary<string, IReplicationPeer> peers,
    IOptions<ReplicationOptions> options,
    TimeProvider time,
    ILogger<FollowerCatchUp> logger) : BackgroundService
{
    private readonly ReplicationOptions _options = options.Value;

    /// <summary>Se libera cuando cambia la época: hay un líder nuevo y puede tener más historia que este log.</summary>
    private readonly SemaphoreSlim _newEpoch = new(0, 1);

    /// <summary>El líder de la última pasada; null si todavía no se sabe o si dejó de serlo.</summary>
    private string? _leaderId;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        cluster.EpochChanged += OnEpochChanged;
        try
        {
            // Arranca de cero: un proceso nuevo no sabe qué se escribió mientras no estaba.
            var backoff = new Backoff(_options, time);
            while (!stoppingToken.IsCancellationRequested)
            {
                if (await ReconcileAsync(stoppingToken).ConfigureAwait(false) == CatchUpResult.CaughtUp)
                {
                    // Al día: lo que venga lo empuja el líder. Se vuelve a mirar cuando cambie la época, o pasado
                    // CatchUpInterval por si el empuje se quedó callado.
                    backoff = new Backoff(_options, time);
                    await _newEpoch.WaitAsync(_options.CatchUpInterval, stoppingToken).ConfigureAwait(false);
                }
                else
                {
                    // Falta algo, o todavía no se sabe quién lidera: reintentar, esperando cada vez un poco más.
                    await backoff.WaitAsync(stoppingToken).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // El pod se está apagando.
        }
        finally
        {
            cluster.EpochChanged -= OnEpochChanged;
        }
    }

    /// <summary>
    /// Una pasada sobre todos los tópicos del catálogo. Los tests la llaman directo. Devuelve si quedó algo por traer,
    /// que es lo que decide si el bucle reintenta enseguida o se duerme hasta el próximo aviso.
    /// </summary>
    internal async Task<CatchUpResult> ReconcileAsync(CancellationToken ct = default)
    {
        // Si este broker lidera, no hay nada que traerse: lo que le falte lo resuelve la promoción.
        if (cluster.IsLeader)
        {
            return CatchUpResult.CaughtUp;
        }

        IReadOnlyCollection<TopicConfig> topics = catalog.List();
        if (topics.Count == 0)
        {
            return CatchUpResult.CaughtUp;
        }

        try
        {
            IReplicationPeer? leader = await FindLeaderAsync(topics, ct).ConfigureAwait(false);
            if (leader is null)
            {
                return CatchUpResult.Unknown;
            }

            var result = CatchUpResult.CaughtUp;
            foreach (TopicConfig topic in topics)
            {
                for (int partition = 0; partition < topic.Partitions; partition++)
                {
                    result = result.Combine(await CatchUpPartitionAsync(leader, topic.Name, partition, ct).ConfigureAwait(false));
                }
            }

            return result;
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            // Sin red, sin el líder o dejando de ser seguidor a mitad de camino: se reintenta en la próxima pasada. Una
            // excepción que escape de un BackgroundService apaga el broker entero.
            LogReconcileFailed(logger, ex);
            return CatchUpResult.Unknown;
        }
    }

    /// <summary>En la hebra de la coordinación: solo despierta al bucle, nunca hace trabajo acá.</summary>
    private void OnEpochChanged(long epoch)
    {
        // Varios avisos antes de que el bucle vuelva a esperar cuentan como uno solo.
        if (_newEpoch.CurrentCount == 0)
        {
            _newEpoch.Release();
        }
    }

    /// <summary>
    /// Quién lidera ahora, de los otros brokers del cluster: cada uno lo dice en
    /// <c>GetState</c>, así que no hace falta que el NodeId venga de la coordinación ni de la dirección HTTP del líder.
    /// </summary>
    private async Task<IReplicationPeer?> FindLeaderAsync(IReadOnlyCollection<TopicConfig> topics, CancellationToken ct)
    {
        IReadOnlyDictionary<string, IReplicationPeer> known = peers;
        if (_leaderId is not null && known.TryGetValue(_leaderId, out IReplicationPeer? cached))
        {
            return cached;
        }

        _leaderId = null;
        var request = new StateRequest { Topic = topics.First().Name, Partition = 0 };
        foreach ((string nodeId, IReplicationPeer peer) in known)
        {
            StateResponse state;
            try
            {
                state = await peer.GetStateAsync(request, ct).ConfigureAwait(false);
            }
            catch (RpcException ex)
            {
                LogPeerUnreachable(logger, ex, nodeId);
                continue;
            }

            if (state.IsLeader)
            {
                _leaderId = nodeId;
                return peer;
            }
        }

        return null;
    }

    /// <summary>Trae lo que le falte a este log de un tópico+partición, comparando contra el estado del líder.</summary>
    private async Task<CatchUpResult> CatchUpPartitionAsync(IReplicationPeer leader, string topic, int partition, CancellationToken ct)
    {
        StateResponse state;
        try
        {
            state = await leader.GetStateAsync(
                new StateRequest { Topic = topic, Partition = partition }, ct).ConfigureAwait(false);
        }
        catch (RpcException ex)
        {
            LogStateFailed(logger, ex, topic, partition);
            _leaderId = null;
            return CatchUpResult.Unknown;
        }

        if (!state.IsLeader)
        {
            // Cambió el líder: la próxima pasada lo busca de nuevo.
            _leaderId = null;
            return CatchUpResult.Unknown;
        }

        long localEnd = log.EndOffset(topic, partition);
        long localHighWatermark = log.HighWatermark(topic, partition);

        // Nada nuevo que traer: lo confirmado coincide con lo del líder.
        if (state.EndOffset <= localEnd && state.HighWatermark <= localHighWatermark)
        {
            return CatchUpResult.CaughtUp;
        }

        // Desde lo último confirmado acá, no desde el final del log: así el primer registro del lote verifica que los
        // dos logs coinciden en todo lo confirmado, y una cola divergente se trunca sola en el Append.
        long from = Math.Max(localHighWatermark, 0);
        if (state.EndOffset < from)
        {
            // El líder termina antes de lo que este broker ya tiene confirmado. Eso no lo arregla un seguidor: si el
            // log de acá perdió registros, se entera el líder al empujar (y la promoción al hacer la encuesta).
            // Nada que un seguidor pueda hacer: no se reintenta, se espera a que el líder (o la promoción) lo resuelva.
            LogLeaderBehind(logger, topic, partition, state.EndOffset, localHighWatermark);
            return CatchUpResult.CaughtUp;
        }

        long wanted = Math.Min(state.EndOffset - from + 1, int.MaxValue);
        var request = new FetchRequest
        {
            Epoch = state.Epoch,
            Topic = topic,
            Partition = partition,
            FromOffset = from,
            MaxRecords = checked((int)wanted),
        };

        LogCatchingUp(logger, topic, partition, state.NodeId, state.EndOffset, localEnd);

        // Un plazo por lote, no por la copia entera: una copia larga está bien, una conexión colgada no.
        using var idle = new CancellationTokenSource(_options.RpcTimeout, time);
        using var call = CancellationTokenSource.CreateLinkedTokenSource(ct, idle.Token);
        var cursor = new LogTip(localHighWatermark, 0);
        bool atConfirmedRecord = localHighWatermark >= 0;
        try
        {
            await foreach (RecordBatch batch in leader.FetchAsync(request, call.Token).ConfigureAwait(false))
            {
                idle.CancelAfter(_options.RpcTimeout);
                var append = new AppendRequest
                {
                    Epoch = state.Epoch,
                    Topic = topic,
                    Partition = partition,
                    LeaderHw = batch.LeaderHw,
                    LeaderId = state.NodeId,
                };

                LogTip batchEnd = cursor;
                foreach (Google.Protobuf.ByteString bytes in batch.Records)
                {
                    Record record = RecordBytes.Decode(bytes);
                    if (atConfirmedRecord)
                    {
                        if (record.Offset != localHighWatermark)
                        {
                            // El log del líder cambió entre el GetState y el Fetch. Se reintenta enseguida.
                            return CatchUpResult.Behind;
                        }

                        cursor = batchEnd = new LogTip(record.Offset, record.Epoch);
                        atConfirmedRecord = false;
                        continue;
                    }

                    append.Records.Add(bytes);
                    batchEnd = new LogTip(record.Offset, record.Epoch);
                }

                if (append.Records.Count > 0)
                {
                    append.PrevOffset = cursor.EndOffset;
                    append.PrevEpoch = cursor.LastEpoch;
                    if (!await AppendToSelfAsync(append, topic, partition, ct).ConfigureAwait(false))
                    {
                        return CatchUpResult.Behind;
                    }

                    cursor = batchEnd;
                }
            }
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            LogStalled(logger, topic, partition, state.NodeId);
            return CatchUpResult.Behind;
        }

        // Solo está al día si llegó hasta donde el líder terminaba al empezar la pasada.
        return cursor.EndOffset >= state.EndOffset ? CatchUpResult.CaughtUp : CatchUpResult.Behind;
    }

    /// <summary>Agrega el lote al log propio. False si hay que dejar la partición para la próxima pasada.</summary>
    private async Task<bool> AppendToSelfAsync(AppendRequest append, string topic, int partition, CancellationToken ct)
    {
        AppendResponse response = await self.AppendAsync(append, ct).ConfigureAwait(false);
        if (response.Ok)
        {
            return true;
        }

        if (response.Code == AppendError.StaleEpoch)
        {
            // Este broker ya está en una época más nueva (se está promoviendo, o vio a otro líder): lo que traiga el
            // líder viejo no entra y la coordinación decide quién manda.
            LogStaleEpoch(logger, topic, partition, response.Epoch);
            return false;
        }

        // Cualquier otro rechazo es un problema de la copia, no del log: no se arregla reintentando a ciegas.
        LogRejected(logger, topic, partition, response.Code, response.Error);
        return false;
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Getting {Topic} partition {Partition} from {Peer}: it ends at offset {PeerEnd} and this log at {LocalEnd}.")]
    private static partial void LogCatchingUp(ILogger logger, string topic, int partition, string peer, long peerEnd, long localEnd);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Broker {Peer} is unreachable while looking for the leader.")]
    private static partial void LogPeerUnreachable(ILogger logger, Exception exception, string peer);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Could not read the state of {Topic} partition {Partition} from the leader.")]
    private static partial void LogStateFailed(ILogger logger, Exception exception, string topic, int partition);

    [LoggerMessage(Level = LogLevel.Debug, Message = "The leader ends {Topic} partition {Partition} at offset {PeerEnd}, before the {HighWatermark} this broker has confirmed; only the leader can resolve that.")]
    private static partial void LogLeaderBehind(ILogger logger, string topic, int partition, long peerEnd, long highWatermark);

    [LoggerMessage(Level = LogLevel.Warning, Message = "The copy of {Topic} partition {Partition} from {Peer} stalled; retrying on the next pass.")]
    private static partial void LogStalled(ILogger logger, string topic, int partition, string peer);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Dropping the copy of {Topic} partition {Partition}: this broker already adopted epoch {Epoch}.")]
    private static partial void LogStaleEpoch(ILogger logger, string topic, int partition, long epoch);

    [LoggerMessage(Level = LogLevel.Warning, Message = "The leader rejected {Topic} partition {Partition} with {Code}: {Error}")]
    private static partial void LogRejected(ILogger logger, string topic, int partition, AppendError code, string error);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not catch up with the leader; retrying on the next pass.")]
    private static partial void LogReconcileFailed(ILogger logger, Exception exception);

    /// <summary>Cómo quedó una pasada, que es lo que decide si el bucle reintenta o se duerme.</summary>
    internal enum CatchUpResult
    {
        /// <summary>Lo que hay en este log cubre hasta donde termina el líder.</summary>
        CaughtUp,

        /// <summary>Quedó algo por traer: se reintenta.</summary>
        Behind,

        /// <summary>No se sabe quién lidera, o no contesta: puede faltar algo, así que también se reintenta.</summary>
        Unknown,
    }
}

internal static class CatchUpResultExtensions
{
    /// <summary>Lo peor de las dos: si una partición quedó atrás, la pasada entera quedó atrás.</summary>
    public static FollowerCatchUp.CatchUpResult Combine(this FollowerCatchUp.CatchUpResult left, FollowerCatchUp.CatchUpResult right) =>
        left > right ? left : right;
}
