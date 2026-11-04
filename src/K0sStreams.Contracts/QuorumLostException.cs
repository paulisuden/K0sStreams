namespace K0sStreams.Contracts;

/// <summary>
/// El líder no pudo alcanzar el quórum para un offset dentro del tiempo de espera.
/// Distinto de <see cref="NotLeaderException"/>: este broker sigue siendo líder, pero
/// no tiene suficientes seguidores para confirmar el registro (por ejemplo, ambos
/// seguidores están caídos o son inalcanzables).
/// </summary>
[Serializable]
public sealed class QuorumLostException : Exception
{
    public QuorumLostException()
    {
    }

    public QuorumLostException(string message)
        : base(message)
    {
    }

    public QuorumLostException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
