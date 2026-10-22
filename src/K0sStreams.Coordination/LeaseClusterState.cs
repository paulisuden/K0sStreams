using System.Globalization;
using K0sStreams.Contracts;

namespace K0sStreams.Coordination;

/// <summary>
/// Rol de este broker según el Lease (fase 4): si es líder, en qué época y quién lidera. Lo actualiza el bucle de
/// elección; el resto del broker solo lo lee y escucha <see cref="EpochChanged"/>.
/// </summary>
/// <param name="leaderAddressFormat">URL HTTP de un broker a partir de su NodeId, p. ej. <c>http://{0}.broker:9090</c>.</param>
internal sealed class LeaseClusterState(string nodeId, string? leaderAddressFormat) : IClusterState
{
    private readonly object _gate = new();
    private bool _isLeader;
    private long _epoch;
    private string? _leaderId;

    public string NodeId => nodeId;

    public bool IsLeader
    {
        get
        {
            lock (_gate)
            {
                return _isLeader;
            }
        }
    }

    public long CurrentEpoch
    {
        get
        {
            lock (_gate)
            {
                return _epoch;
            }
        }
    }

    public string? LeaderAddress
    {
        get
        {
            string? leaderId;
            lock (_gate)
            {
                leaderId = _leaderId;
            }

            return leaderId is null || leaderAddressFormat is null
                ? null
                : string.Format(CultureInfo.InvariantCulture, leaderAddressFormat, leaderId);
        }
    }

    public event Action<long>? EpochChanged;

    /// <summary>
    /// Este broker es seguidor y el Lease está en <paramref name="epoch"/>, a nombre de <paramref name="leaderId"/>
    /// (null si no hay líder). Avisa solo si la época cambió.
    /// </summary>
    public void Follow(long epoch, string? leaderId)
    {
        bool changed;
        lock (_gate)
        {
            changed = epoch != _epoch;
            _epoch = epoch;
            _leaderId = leaderId;
        }

        if (changed)
        {
            EpochChanged?.Invoke(epoch);
        }
    }

    /// <summary>La promoción terminó: este broker lidera <paramref name="epoch"/>.</summary>
    public void BecomeLeader(long epoch)
    {
        lock (_gate)
        {
            _isLeader = true;
            _epoch = epoch;
            _leaderId = nodeId;
        }

        EpochChanged?.Invoke(epoch);
    }

    /// <summary>
    /// Perdió el Lease o se está apagando: deja de aceptar escrituras ya mismo. Avisa con la misma época, porque quien
    /// escucha (la replicación) mira <see cref="IsLeader"/> para saber que tiene que frenar.
    /// </summary>
    public void StepDown()
    {
        long epoch;
        lock (_gate)
        {
            if (!_isLeader)
            {
                return;
            }

            _isLeader = false;
            _leaderId = null;
            epoch = _epoch;
        }

        EpochChanged?.Invoke(epoch);
    }
}
