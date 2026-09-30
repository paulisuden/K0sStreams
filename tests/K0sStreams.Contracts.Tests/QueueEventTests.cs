using K0sStreams.Contracts;

namespace K0sStreams.Contracts.Tests;

public class QueueEventTests
{
    [Fact]
    public void Evento_de_cola_ida_y_vuelta_por_record()
    {
        var evt = new QueueEvent(RecordType.Nack, "facturacion", 2, 150, 1_790_000_000_000, "timeout del proveedor");

        var record = evt.ToRecord(epoch: 4, timestamp: 1_789_999_999_000);

        record.Type.Should().Be(RecordType.Nack);
        record.Offset.Should().Be(Record.Unassigned);
        QueueEvent.FromRecord(record).Should().Be(evt);
    }

    [Fact]
    public void Evento_sin_motivo_conserva_null()
    {
        var evt = new QueueEvent(RecordType.Ack, "q", 0, 7, 0);

        QueueEvent.FromRecord(evt.ToRecord(1, 0)).Reason.Should().BeNull();
    }

    [Fact]
    public void Un_mensaje_no_es_evento_de_cola()
    {
        var evt = new QueueEvent(RecordType.Message, "q", 0, 0, 0);

        evt.Invoking(e => e.ToRecord(1, 0)).Should().Throw<InvalidOperationException>();
    }
}
