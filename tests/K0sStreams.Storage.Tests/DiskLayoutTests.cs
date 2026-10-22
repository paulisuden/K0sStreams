using System.Text;
using K0sStreams.Contracts;

using Record = K0sStreams.Contracts.Record;

namespace K0sStreams.Storage.Tests;

/// <summary>
/// La otra mitad del criterio de la iteración 1: que lo que queda en disco sea exactamente
/// lo que dice el formato de <see cref="RecordCodec"/>, sin relleno ni basura.
/// </summary>
public sealed class DiskLayoutTests : IDisposable
{
    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "k0sstreams-tests", Guid.NewGuid().ToString("N"));
    private readonly SegmentedLog _log;

    public DiskLayoutTests() => _log = new SegmentedLog(_dataDir);

    /// <summary>Donde el log tiene que haber dejado el único segmento del tópico "pedidos" (DEC-001: sin particiones).</summary>
    private string Segmento => Path.Combine(_dataDir, "pedidos", "00000000000000000000.log");

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
    public async Task Append_crea_la_carpeta_y_el_segmento_con_bytes_adentro()
    {
        await _log.AppendAsync("pedidos", 0, Msg("hola"));

        File.Exists(Segmento).Should().BeTrue("el append tiene que crear el segmento");
        new FileInfo(Segmento).Length.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task Leer_sin_haber_escrito_no_crea_nada_en_disco()
    {
        var leidos = await _log.ReadAsync("pedidos", 0, 0, 10).ToListAsync();

        leidos.Should().BeEmpty();
        Directory.Exists(_dataDir).Should().BeFalse("una lectura no debería crear carpetas");
    }

    [Fact]
    public async Task El_segmento_pesa_exactamente_la_suma_de_los_registros()
    {
        var mensajes = new[] { Msg("a"), Msg("mensaje mas largo"), Msg("otro") };
        long esperado = 0;
        foreach (var mensaje in mensajes)
        {
            await _log.AppendAsync("pedidos", 0, mensaje);
            esperado += RecordCodec.GetEncodedSize(mensaje);
        }

        new FileInfo(Segmento).Length.Should().Be(esperado);
    }

    [Fact]
    public async Task Truncate_recorta_el_archivo_en_disco()
    {
        for (int i = 0; i < 4; i++)
        {
            await _log.AppendAsync("pedidos", 0, Msg($"m{i}"));
        }

        long completo = new FileInfo(Segmento).Length;

        await _log.TruncateAsync("pedidos", 0, 1);

        new FileInfo(Segmento).Length.Should().BeLessThan(completo);
        (await _log.ReadAsync("pedidos", 0, 0, 10).CountAsync()).Should().Be(2);
    }

    [Theory]
    [InlineData("../escape")]
    [InlineData("/etc/passwd")]
    [InlineData("CON MAYUSCULAS")]
    public void Un_nombre_de_topico_invalido_no_llega_a_tocar_el_disco(string topico)
    {
        // El nombre del tópico es el nombre de la carpeta: sin esta validación, un "../"
        // dejaría escribir fuera de DataDir.
        _log.Invoking(l => l.EndOffset(topico, 0)).Should().Throw<ArgumentException>();
        Directory.Exists(_dataDir).Should().BeFalse();
    }
}
