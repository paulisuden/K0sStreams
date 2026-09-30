namespace K0sStreams.Contracts;

/// <summary>Tipo de registro del log. El valor numérico es el byte que se guarda en disco: no cambiarlo.</summary>
public enum RecordType : byte
{
    Message = 0,
    Receive = 1,
    Ack = 2,
    Nack = 3,
    Timeout = 4,
    EpochChange = 5,
}
