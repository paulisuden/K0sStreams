using System.Globalization;
using System.Text;
using K0sStreams.Contracts;

using Record = K0sStreams.Contracts.Record;

namespace K0sStreams.Storage.Tests;

/// <summary>
/// Iteración 4: un tópico deja de ser un archivo que crece sin límite y pasa a partirse en segmentos.
/// El riesgo está en los bordes entre uno y otro, así que ahí van casi todos los casos.
/// </summary>
public sealed class RotationTests : IDisposable
{
    private const string Topico = "pedidos";

    /// <summary>Con mensajes de 3 bytes, cada registro pesa 52 y entran exactamente 3 por segmento.</summary>
    private const long LimiteChico = 200;

    private const int PorSegmento = 3;

    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "k0sstreams-tests", Guid.NewGuid().ToString("N"));

    private SegmentedLog Reiniciar(long limite = LimiteChico) => new(_dataDir, limite);

    public void Dispose()
    {
        if (Directory.Exists(_dataDir))
        {
            Directory.Delete(_dataDir, recursive: true);
        }
    }

    private static Record Msg(int i) =>
        Record.NewMessage(1, 0, ReadOnlyMemory<byte>.Empty, Encoding.UTF8.GetBytes($"m{i:D2}"));

    private string Segmento(long baseOffset) =>
        Path.Combine(_dataDir, Topico, baseOffset.ToString("D20", CultureInfo.InvariantCulture) + ".log");

    /// <summary>Nombres de los segmentos que hay en disco, ordenados.</summary>
    private string[] SegmentosEnDisco() =>
        [.. Directory.GetFiles(Path.Combine(_dataDir, Topico), "*.log").Select(Path.GetFileName).Order()!];

    private static async Task<SegmentedLog> Escribir(SegmentedLog log, int cuantos)
    {
        for (int i = 0; i < cuantos; i++)
        {
            await log.AppendAsync(Topico, 0, Msg(i));
        }

        return log;
    }

    [Fact]
    public void La_cuenta_de_los_tests_es_la_que_dice_el_codec()
    {
        // Si esto falla, los demás tests de este archivo dejan de probar lo que creen probar.
        RecordCodec.GetEncodedSize(Msg(0)).Should().Be(52);
        (PorSegmento * 52).Should().BeLessThanOrEqualTo((int)LimiteChico);
        ((PorSegmento + 1) * 52).Should().BeGreaterThan((int)LimiteChico);
    }

    [Fact]
    public async Task Se_rota_al_pasar_el_limite()
    {
        await Escribir(Reiniciar(), 7);

        // 7 mensajes de a 3: segmentos que empiezan en los offsets 0, 3 y 6.
        SegmentosEnDisco().Should().Equal(
            "00000000000000000000.log", "00000000000000000003.log", "00000000000000000006.log");
    }

    [Fact]
    public async Task El_nombre_del_archivo_es_su_primer_offset()
    {
        await Escribir(Reiniciar(), 4);

        // El segundo segmento arranca en el offset 3 y tiene un solo registro.
        new FileInfo(Segmento(3)).Length.Should().Be(52);
        new FileInfo(Segmento(0)).Length.Should().Be(PorSegmento * 52);
    }

    [Fact]
    public async Task Un_registro_mas_grande_que_el_limite_se_escribe_igual()
    {
        var log = Reiniciar(limite: 100);
        var gordo = Record.NewMessage(1, 0, ReadOnlyMemory<byte>.Empty, Encoding.UTF8.GetBytes(new string('x', 300)));

        // Sin la excepción del segmento vacío, este registro no entraría en ningún segmento nunca.
        (await log.AppendAsync(Topico, 0, gordo)).Should().Be(0);
        (await log.AppendAsync(Topico, 0, Msg(1))).Should().Be(1);

        SegmentosEnDisco().Should().Equal("00000000000000000000.log", "00000000000000000001.log");
    }

    [Fact]
    public async Task Leer_cruzando_el_borde_devuelve_todo_en_orden()
    {
        var log = await Escribir(Reiniciar(), 7);

        var leidos = await log.ReadAsync(Topico, 0, 0, 100).ToListAsync();

        leidos.Select(r => r.Offset).Should().Equal(0, 1, 2, 3, 4, 5, 6);
        Encoding.UTF8.GetString(leidos[4].Value.Span).Should().Be("m04");
    }

    [Fact]
    public async Task Leer_empezando_justo_en_el_primer_offset_de_un_segmento()
    {
        var log = await Escribir(Reiniciar(), 7);

        var leidos = await log.ReadAsync(Topico, 0, 3, 100).Select(r => r.Offset).ToListAsync();

        leidos.Should().Equal(3, 4, 5, 6);
    }

    [Fact]
    public async Task Leer_terminando_justo_en_el_ultimo_offset_de_un_segmento()
    {
        var log = await Escribir(Reiniciar(), 7);

        var leidos = await log.ReadAsync(Topico, 0, 0, PorSegmento).Select(r => r.Offset).ToListAsync();

        leidos.Should().Equal(0, 1, 2);
    }

    [Fact]
    public async Task Recuperar_con_varios_segmentos_reconstruye_todo()
    {
        await Escribir(Reiniciar(), 7);

        var despues = Reiniciar();

        despues.EndOffset(Topico, 0).Should().Be(6);
        (await despues.ReadAsync(Topico, 0, 0, 100).Select(r => r.Offset).ToListAsync())
            .Should().Equal(0, 1, 2, 3, 4, 5, 6);

        // Y sigue escribiendo donde iba, sin abrir un segmento de más.
        (await despues.AppendAsync(Topico, 0, Msg(7))).Should().Be(7);
        SegmentosEnDisco().Should().HaveCount(3);
    }

    [Fact]
    public async Task Una_escritura_cortada_en_el_ultimo_segmento_se_descarta()
    {
        await Escribir(Reiniciar(), 7);
        byte[] aMedias = RecordCodec.Encode(Msg(99));
        await File.AppendAllBytesAsync(Segmento(6), aMedias[..(aMedias.Length / 2)]);

        var despues = Reiniciar();

        despues.EndOffset(Topico, 0).Should().Be(6);
        new FileInfo(Segmento(6)).Length.Should().Be(52);
    }

    [Fact]
    public async Task Truncar_borra_los_segmentos_posteriores()
    {
        var log = await Escribir(Reiniciar(), 7);

        await log.TruncateAsync(Topico, 0, 4);

        log.EndOffset(Topico, 0).Should().Be(4);
        SegmentosEnDisco().Should().Equal("00000000000000000000.log", "00000000000000000003.log");
        new FileInfo(Segmento(3)).Length.Should().Be(2 * 52, "quedan los offsets 3 y 4");
    }

    [Fact]
    public async Task Truncar_justo_en_el_borde_deja_el_segmento_vacio()
    {
        var log = await Escribir(Reiniciar(), 7);

        await log.TruncateAsync(Topico, 0, 2);

        log.EndOffset(Topico, 0).Should().Be(2);
        new FileInfo(Segmento(3)).Length.Should().Be(0, "su offset base ya es el próximo a escribir");

        // Y se llena solo con el siguiente mensaje, sin abrir otro segmento.
        (await log.AppendAsync(Topico, 0, Msg(3))).Should().Be(3);
        SegmentosEnDisco().Should().Equal("00000000000000000000.log", "00000000000000000003.log");
    }

    [Fact]
    public async Task El_high_watermark_sigue_bloqueando_el_truncado_entre_segmentos()
    {
        var log = await Escribir(Reiniciar(), 7);
        log.AdvanceHighWatermark(Topico, 0, 4);

        await log.Invoking(l => l.TruncateAsync(Topico, 0, 3).AsTask())
            .Should().ThrowAsync<InvalidOperationException>();

        await log.TruncateAsync(Topico, 0, 4);
        log.EndOffset(Topico, 0).Should().Be(4);
    }
}
