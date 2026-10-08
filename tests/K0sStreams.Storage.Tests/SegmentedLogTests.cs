using System.Text;
using K0sStreams.Contracts;

// xUnit trae su propia clase Xunit.Record (global using en tests/Directory.Build.props),
// así que hay que decir explícitamente de cuál hablamos.
using Record = K0sStreams.Contracts.Record;

namespace K0sStreams.Storage.Tests;

/// <summary>
/// Los mismos seis casos que <c>InMemoryLogTests</c>, pero contra el log real en disco.
/// Son el contrato de <see cref="ILog"/>: si pasan en las dos implementaciones, son intercambiables.
/// </summary>
public sealed class SegmentedLogTests : IDisposable
{   
    // carpeta temporal con nombre al azar (Guid). xUnit crea un objeto nuevo de la clase de tests por cada test. 
    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "k0sstreams-tests", Guid.NewGuid().ToString("N"));
    private readonly SegmentedLog _log;

    public SegmentedLogTests() => _log = new SegmentedLog(_dataDir);

    // Dispose es el método de limpieza. xUnit lo llama cuando el test termina, pase o falle y borra la carpeta temporal
    public void Dispose()
    {
        if (Directory.Exists(_dataDir))
        {
            Directory.Delete(_dataDir, recursive: true);
        }
    }

    private static Record Msg(string value) =>
        Record.NewMessage(1, 0, ReadOnlyMemory<byte>.Empty, Encoding.UTF8.GetBytes(value));

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
