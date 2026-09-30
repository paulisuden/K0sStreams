namespace K0sStreams.Contracts;

/// <summary>Rol de este broker según el Lease de Kubernetes y la época actual.</summary>
public interface IClusterState
{
    /// <summary>Identificador de este broker (el nombre del pod, p. ej. broker-0).</summary>
    string NodeId { get; }

    bool IsLeader { get; }

    long CurrentEpoch { get; }

    /// <summary>URL base HTTP del líder (p. ej. http://broker-0.broker:9090), o null si no se conoce.</summary>
    string? LeaderAddress { get; }

    /// <summary>Se dispara con la nueva época cuando cambia el líder.</summary>
    event Action<long>? EpochChanged;
}

/// <summary>La operación necesita al líder y este broker no lo es (o dejó de serlo).</summary>
public sealed class NotLeaderException : Exception
{
    public NotLeaderException()
    {
    }

    public NotLeaderException(string message)
        : base(message)
    {
    }

    public NotLeaderException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
