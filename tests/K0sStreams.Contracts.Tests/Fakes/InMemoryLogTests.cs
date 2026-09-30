using K0sStreams.Contracts;
using K0sStreams.Contracts.Fakes;

namespace K0sStreams.Contracts.Tests.Fakes;

public class InMemoryLogTests
{
    private readonly InMemoryLog _log = new();

    private static Record Msg(string value) => Record.NewMessage(1, 0, ReadOnlyMemory<byte>.Empty, System.Text.Encoding.UTF8.GetBytes(value));

    [Fact]
    public void Log_vacio_tiene_offsets_en_menos_uno()
    {
        _log.EndOffset("t", 0).Should().Be(-1);
        _log.HighWatermark("t", 0).Should().Be(-1);
    }

    [Fact]
    public async Task Append_asigna_offsets_contiguos_desde_cero()
    {
        (await _log.AppendAsync("t", 0, Msg("a"))).Should().Be(0);
        (await _log.AppendAsync("t", 0, Msg("b"))).Should().Be(1);
        (await _log.AppendAsync("t", 1, Msg("c"))).Should().Be(0);

        _log.EndOffset("t", 0).Should().Be(1);
    }

    [Fact]
    public async Task Replica_con_offset_equivocado_se_rechaza()
    {
        await _log.AppendAsync("t", 0, Msg("a") with { Offset = 0 });

        await _log.Invoking(l => l.AppendAsync("t", 0, Msg("b") with { Offset = 5 }).AsTask())
            .Should().ThrowAsync<ArgumentException>();
    }

    [Fact]
    public async Task Read_devuelve_desde_el_offset_pedido()
    {
        for (int i = 0; i < 5; i++)
        {
            await _log.AppendAsync("t", 0, Msg($"m{i}"));
        }

        var offsets = await _log.ReadAsync("t", 0, 2, 2).Select(r => r.Offset).ToListAsync();

        offsets.Should().Equal(2, 3);
    }

    [Fact]
    public async Task High_watermark_solo_sube_y_no_supera_el_end_offset()
    {
        await _log.AppendAsync("t", 0, Msg("a"));
        await _log.AppendAsync("t", 0, Msg("b"));

        _log.AdvanceHighWatermark("t", 0, 1);
        _log.AdvanceHighWatermark("t", 0, 0);

        _log.HighWatermark("t", 0).Should().Be(1);
        _log.Invoking(l => l.AdvanceHighWatermark("t", 0, 2)).Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public async Task Truncate_borra_lo_no_confirmado_pero_no_lo_confirmado()
    {
        for (int i = 0; i < 4; i++)
        {
            await _log.AppendAsync("t", 0, Msg($"m{i}"));
        }

        _log.AdvanceHighWatermark("t", 0, 1);

        await _log.TruncateAsync("t", 0, 1);
        _log.EndOffset("t", 0).Should().Be(1);

        await _log.Invoking(l => l.TruncateAsync("t", 0, 0).AsTask()).Should().ThrowAsync<InvalidOperationException>();
    }
}
