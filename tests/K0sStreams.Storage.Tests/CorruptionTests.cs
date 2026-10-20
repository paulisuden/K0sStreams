using System.Buffers.Binary;
using System.Globalization;
using System.Text;
using K0sStreams.Contracts;
using K0sStreams.Storage.Tests.Support;
using Microsoft.Extensions.Logging;

using Record = K0sStreams.Contracts.Record;

namespace K0sStreams.Storage.Tests;

/// <summary>
/// Iteración 7: qué hace el log cuando los archivos están dañados. Es lo que la fase 5 (caos) va a
/// exigir, y cada caso verifica las dos cosas: que el comportamiento sea predecible <b>y</b> que
/// haya quedado un mensaje que explique qué se encontró.
/// </summary>
/// <remarks>
/// El criterio que ordena todo el archivo: un registro <b>cortado</b> es un corte normal y se
/// descarta; un registro <b>dañado</b> son datos que ya estaban bien guardados, y ahí el tópico no
/// arranca. Perder datos en silencio es lo peor que puede hacer un broker.
/// </remarks>
public sealed class CorruptionTests : IDisposable
{
    private const string Topico = "pedidos";

    /// <summary>Con un valor de 3 bytes, cada registro pesa 52 y entran 3 por segmento chico.</summary>
    private const int TamanoRegistro = 52;

    private const long LimiteChico = 200;

    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "k0sstreams-tests", Guid.NewGuid().ToString("N"));
    private readonly CapturingLogger _logger = new();

    private string Carpeta => Path.Combine(_dataDir, Topico);

    private SegmentedLog Reiniciar(long limite = SegmentedLog.DefaultSegmentBytes) =>
        new(_dataDir, limite, _logger);

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
        Path.Combine(Carpeta, baseOffset.ToString("D20", CultureInfo.InvariantCulture) + ".log");

    private static async Task<SegmentedLog> Escribir(SegmentedLog log, int cuantos)
    {
        for (int i = 0; i < cuantos; i++)
        {
            await log.AppendAsync(Topico, 0, Msg(i));
        }

        return log;
    }

    /// <summary>Lo que el log dijo en ese nivel. Vacío si no dijo nada.</summary>
    private string Dijo(LogLevel nivel) => _logger.Texto(nivel);

    [Fact]
    public void La_cuenta_de_los_tests_es_la_que_dice_el_codec()
    {
        // Si esto falla, los recortes a mano de este archivo dejan de caer donde creen caer.
        RecordCodec.GetEncodedSize(Msg(0)).Should().Be(TamanoRegistro);
        (3 * TamanoRegistro).Should().BeLessThanOrEqualTo((int)LimiteChico);
        (4 * TamanoRegistro).Should().BeGreaterThan((int)LimiteChico);
    }

    // ----------------------------------------------------------------- daño que impide arrancar

    [Fact]
    public async Task Un_segmento_cerrado_cortado_a_mitad_de_registro_no_arranca()
    {
        await Escribir(Reiniciar(LimiteChico), 7);

        // Se recorta el PRIMER segmento, que está cerrado. La rotación ocurre entre registros, así
        // que un segmento cerrado siempre termina donde termina uno: esto es daño, no un corte.
        using (var stream = new FileStream(Segmento(0), FileMode.Open, FileAccess.Write))
        {
            stream.SetLength((2 * TamanoRegistro) + 20);
        }

        var despues = Reiniciar(LimiteChico);

        despues.Invoking(l => l.EndOffset(Topico, 0)).Should().Throw<InvalidDataException>()
            .WithMessage("*segmento cerrado*");
        Dijo(LogLevel.Critical).Should().Contain("Cannot recover").And.Contain(Topico);
    }

    [Fact]
    public async Task Un_hueco_entre_segmentos_no_arranca()
    {
        await Escribir(Reiniciar(LimiteChico), 7);
        File.Delete(Segmento(3));

        var despues = Reiniciar(LimiteChico);

        // El segmento que empieza en 6 no puede seguir a uno que terminó en 2.
        despues.Invoking(l => l.EndOffset(Topico, 0)).Should().Throw<InvalidDataException>()
            .WithMessage("*debería empezar en el offset 3*");
        Dijo(LogLevel.Critical).Should().Contain("Cannot recover");
    }

    [Fact]
    public async Task Un_registro_con_el_offset_equivocado_no_arranca()
    {
        await Escribir(Reiniciar(), 3);

        // Un registro con el CRC bien pero el offset que no le toca: se reemplaza el segundo por uno
        // que dice ser el offset 7. Sin este control, un archivo reordenado pasaría inadvertido.
        byte[] impostor = RecordCodec.Encode(Msg(9) with { Offset = 7 });
        impostor.Length.Should().Be(TamanoRegistro, "tiene que ocupar lo mismo que el que reemplaza");
        using (var stream = new FileStream(Segmento(0), FileMode.Open, FileAccess.Write))
        {
            stream.Position = TamanoRegistro;
            stream.Write(impostor);
        }

        var despues = Reiniciar();

        despues.Invoking(l => l.EndOffset(Topico, 0)).Should().Throw<InvalidDataException>()
            .WithMessage("*dice offset 7 y le corresponde 1*");
        Dijo(LogLevel.Critical).Should().Contain("Cannot recover");
    }

    [Fact]
    public async Task Un_largo_de_registro_imposible_no_arranca()
    {
        Directory.CreateDirectory(Carpeta);

        // Cuatro bytes que anuncian un registro de 2 GB. El tope real es MaxRecordSize (16 MB).
        byte[] largoAbsurdo = new byte[sizeof(int)];
        BinaryPrimitives.WriteInt32LittleEndian(largoAbsurdo, int.MaxValue);
        await File.WriteAllBytesAsync(Segmento(0), largoAbsurdo);

        var log = Reiniciar();

        log.Invoking(l => l.EndOffset(Topico, 0)).Should().Throw<InvalidDataException>()
            .WithMessage("*no tiene sentido*");
        Dijo(LogLevel.Critical).Should().Contain("Cannot recover");
    }

    [Fact]
    public async Task Basura_al_final_del_segmento_activo_no_se_descarta_en_silencio()
    {
        await Escribir(Reiniciar(), 3);
        long sano = new FileInfo(Segmento(0)).Length;

        // Distinto de una escritura cortada: un corte deja el PREFIJO de un registro, con su largo
        // correcto y bytes de menos. Esto son bytes arbitrarios, o sea un disco que devolvió basura.
        await File.AppendAllBytesAsync(Segmento(0), Enumerable.Repeat((byte)0xAA, 60).ToArray());

        var despues = Reiniciar();

        despues.Invoking(l => l.EndOffset(Topico, 0)).Should().Throw<InvalidDataException>();
        Dijo(LogLevel.Critical).Should().Contain("Cannot recover");

        // Y no se tocó el archivo: ante daño no se recorta nada.
        new FileInfo(Segmento(0)).Length.Should().Be(sano + 60);
    }

    [Fact]
    public async Task Un_segmento_vacio_que_no_es_el_ultimo_no_arranca()
    {
        Directory.CreateDirectory(Carpeta);
        await File.WriteAllBytesAsync(Segmento(0), []);
        await File.WriteAllBytesAsync(Segmento(3), RecordCodec.Encode(Msg(3) with { Offset = 3 }));

        var log = Reiniciar(LimiteChico);

        // Un segmento vacío adelante es un hueco: faltan los offsets 0, 1 y 2.
        log.Invoking(l => l.EndOffset(Topico, 0)).Should().Throw<InvalidDataException>()
            .WithMessage("*debería empezar en el offset 0*");
        Dijo(LogLevel.Critical).Should().Contain("Cannot recover");
    }

    // ----------------------------------------------------------------- daño que se tolera

    [Fact]
    public async Task Un_archivo_con_nombre_raro_en_la_carpeta_se_ignora()
    {
        await Escribir(Reiniciar(), 3);
        await File.WriteAllTextAsync(Path.Combine(Carpeta, "basura.log"), "esto no es un segmento");
        await File.WriteAllTextAsync(Path.Combine(Carpeta, "00000000000000000000.txt"), "tampoco");

        var despues = Reiniciar();

        despues.EndOffset(Topico, 0).Should().Be(2, "el log sigue andando");
        Dijo(LogLevel.Warning).Should().Contain("basura.log").And.Contain("not a segment base offset");
        Dijo(LogLevel.Critical).Should().BeEmpty("no es daño de datos, es un archivo que sobra");
    }

    [Fact]
    public async Task Un_indice_huerfano_no_molesta()
    {
        await Escribir(Reiniciar(), 3);

        // Un .index de un segmento que ya no existe: solo se mira la lista de .log, así que sobra.
        await File.WriteAllBytesAsync(
            Path.Combine(Carpeta, "00000000000000000099.index"), [1, 2, 3, 4, 5, 6, 7, 8]);

        var despues = Reiniciar();

        despues.EndOffset(Topico, 0).Should().Be(2);
        (await despues.ReadAsync(Topico, 0, 0, 10).CountAsync()).Should().Be(3);
        Dijo(LogLevel.Critical).Should().BeEmpty();
    }

    [Theory]
    [InlineData("-5", -1)]   // el contrato dice que el mínimo es -1
    [InlineData("99", 0)]    // adelantado: se acota al end offset
    public async Task Un_checkpoint_con_un_numero_absurdo_se_acota(string guardado, long esperado)
    {
        var log = await Escribir(Reiniciar(), 1);
        log.AdvanceHighWatermark(Topico, 0, 0);
        await File.WriteAllTextAsync(Path.Combine(Carpeta, "checkpoint"), guardado);

        var despues = Reiniciar();

        despues.HighWatermark(Topico, 0).Should().Be(esperado);
        Dijo(LogLevel.Warning).Should().Contain("Clamped the checkpoint").And.Contain(guardado);
    }

    [Fact]
    public async Task Un_checkpoint_ilegible_queda_en_el_log()
    {
        var log = await Escribir(Reiniciar(), 1);
        log.AdvanceHighWatermark(Topico, 0, 0);
        await File.WriteAllTextAsync(Path.Combine(Carpeta, "checkpoint"), "no-es-un-numero");

        var despues = Reiniciar();

        despues.HighWatermark(Topico, 0).Should().Be(-1);
        Dijo(LogLevel.Warning).Should().Contain("Ignored the checkpoint").And.Contain("no-es-un-numero");
    }

    // ----------------------------------------------------------------- lo que el log cuenta

    [Fact]
    public async Task Descartar_una_escritura_cortada_queda_en_el_log()
    {
        await Escribir(Reiniciar(), 3);
        byte[] aMedias = RecordCodec.Encode(Msg(99));
        await File.AppendAllBytesAsync(Segmento(0), aMedias[..(aMedias.Length / 2)]);

        var despues = Reiniciar();

        despues.EndOffset(Topico, 0).Should().Be(2);
        Dijo(LogLevel.Warning).Should().Contain("Discarded an incomplete write")
            .And.Contain("never returned an offset");
    }

    [Fact]
    public async Task Rehacer_el_indice_queda_en_el_log()
    {
        await Escribir(Reiniciar(), 3);

        // Un .index que existe pero no describe nada: largo que no es múltiplo de una entrada.
        await File.WriteAllBytesAsync(Path.Combine(Carpeta, "00000000000000000000.index"), [9, 9, 9]);

        var despues = Reiniciar();

        despues.EndOffset(Topico, 0).Should().Be(2);
        Dijo(LogLevel.Warning).Should().Contain("Rebuilding the index").And.Contain("Nothing is lost");
    }

    [Fact]
    public async Task Que_falte_el_indice_no_se_avisa()
    {
        // Un segmento que nunca llegó a los 4 KB no tiene .index, y eso es lo normal: avisarlo en
        // cada arranque sería ruido que taparía los avisos que importan.
        await Escribir(Reiniciar(), 3);
        File.Exists(Path.Combine(Carpeta, "00000000000000000000.index")).Should().BeFalse();

        Reiniciar().EndOffset(Topico, 0).Should().Be(2);

        Dijo(LogLevel.Warning).Should().NotContain("Rebuilding the index");
    }

    [Fact]
    public async Task Un_crc_roto_queda_en_el_log_como_critico()
    {
        await Escribir(Reiniciar(), 3);
        byte[] bytes = await File.ReadAllBytesAsync(Segmento(0));
        bytes[^3] ^= 0xFF;
        await File.WriteAllBytesAsync(Segmento(0), bytes);

        var despues = Reiniciar();

        despues.Invoking(l => l.EndOffset(Topico, 0)).Should().Throw<InvalidDataException>();

        // El mensaje tiene que alcanzar para saber qué pasó sin abrir un depurador: el motivo, el
        // archivo, el byte exacto, y por qué el broker prefiere no arrancar.
        Dijo(LogLevel.Critical).Should().Contain("CRC").And.Contain("byte 104")
            .And.Contain("Refusing to serve");
    }

    [Fact]
    public async Task Recuperar_un_topico_queda_en_el_log()
    {
        await Escribir(Reiniciar(LimiteChico), 7);

        Reiniciar(LimiteChico).EndOffset(Topico, 0).Should().Be(6);

        // La línea que se muestra en la demo de caos: el pod arrancó y esto es lo que encontró.
        Dijo(LogLevel.Information).Should().Contain("Recovered").And.Contain(Topico)
            .And.Contain("7 records").And.Contain("3 segments");
    }
}
