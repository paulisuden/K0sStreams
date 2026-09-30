using System.Text;
using K0sStreams.Contracts;

namespace K0sStreams.Contracts.Tests;

public class RecordCodecTests
{
    private static readonly Record Sample = new(
        Offset: 42,
        Epoch: 3,
        Type: RecordType.Message,
        Timestamp: 1_790_000_000_000,
        DeliverAt: 1_790_000_060_000,
        Key: Encoding.UTF8.GetBytes("pedido-1"),
        Value: Encoding.UTF8.GetBytes("hola"));

    [Fact]
    public void Encode_y_decode_devuelven_el_mismo_registro()
    {
        var bytes = RecordCodec.Encode(Sample);

        RecordCodec.TryDecode(bytes, out var decoded, out int consumed).Should().Be(DecodeStatus.Ok);

        consumed.Should().Be(bytes.Length);
        decoded!.Offset.Should().Be(42);
        decoded.Epoch.Should().Be(3);
        decoded.Type.Should().Be(RecordType.Message);
        decoded.Timestamp.Should().Be(Sample.Timestamp);
        decoded.DeliverAt.Should().Be(Sample.DeliverAt);
        decoded.Key.ToArray().Should().Equal(Sample.Key.ToArray());
        decoded.Value.ToArray().Should().Equal(Sample.Value.ToArray());
    }

    [Fact]
    public void Registro_con_clave_y_valor_vacios_ocupa_el_header()
    {
        var empty = Sample with { Key = ReadOnlyMemory<byte>.Empty, Value = ReadOnlyMemory<byte>.Empty };

        var bytes = RecordCodec.Encode(empty);

        bytes.Should().HaveCount(RecordCodec.HeaderSize);
        RecordCodec.TryDecode(bytes, out var decoded, out _).Should().Be(DecodeStatus.Ok);
        decoded!.Key.IsEmpty.Should().BeTrue();
        decoded.Value.IsEmpty.Should().BeTrue();
    }

    [Fact]
    public void Un_byte_cambiado_se_detecta_como_corrupto()
    {
        var bytes = RecordCodec.Encode(Sample);
        bytes[^1] ^= 0xFF;

        RecordCodec.TryDecode(bytes, out var decoded, out int consumed).Should().Be(DecodeStatus.Corrupt);
        decoded.Should().BeNull();
        consumed.Should().Be(0);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(3)]
    [InlineData(20)]
    public void Escritura_cortada_se_detecta_como_incompleta(int cut)
    {
        var bytes = RecordCodec.Encode(Sample);

        RecordCodec.TryDecode(bytes.AsSpan(0, bytes.Length - 1 - cut), out _, out _)
            .Should().Be(DecodeStatus.Incomplete);
    }

    [Fact]
    public void Largo_imposible_se_detecta_como_corrupto()
    {
        var bytes = RecordCodec.Encode(Sample);
        bytes[0] = 1;
        bytes[1] = bytes[2] = bytes[3] = 0;

        RecordCodec.TryDecode(bytes, out _, out _).Should().Be(DecodeStatus.Corrupt);
    }

    [Fact]
    public void Se_pueden_leer_varios_registros_seguidos()
    {
        var first = RecordCodec.Encode(Sample);
        var second = RecordCodec.Encode(Sample with { Offset = 43 });
        byte[] buffer = [.. first, .. second];

        RecordCodec.TryDecode(buffer, out var a, out int consumed).Should().Be(DecodeStatus.Ok);
        RecordCodec.TryDecode(buffer.AsSpan(consumed), out var b, out _).Should().Be(DecodeStatus.Ok);

        a!.Offset.Should().Be(42);
        b!.Offset.Should().Be(43);
    }
}
