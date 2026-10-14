using System.Buffers.Binary;
using System.Text;
using K0sStreams.Contracts;

using Record = K0sStreams.Contracts.Record;

namespace K0sStreams.Storage.Tests;

/// <summary>
/// Iteración 5: cada segmento pasa a tener un <c>.index</c> con pares (offset, posición), uno cada
/// 4 KB. Deja de haber una entrada por registro, así que leer un offset ya no es un salto directo:
/// es un salto al registro anterior más cercano y una lectura corta desde ahí. Casi todos los casos
/// miran qué pasa cuando ese índice falta, está dañado o quedó atrasado.
/// </summary>
/// <remarks>
/// Los registros son de 500 bytes a propósito: con 9 por intervalo, 120 mensajes ya dan 13 anclas.
/// Con registros chicos harían falta 1000, y cada escritura cuesta un <c>fsync</c> hasta que llegue
/// el group commit de la iteración 6.
/// </remarks>
public sealed class SparseIndexTests : IDisposable
{
    private const string Topico = "pedidos";

    /// <summary>Con un valor de 451 bytes, cada registro pesa 500.</summary>
    private const int TamanoRegistro = 500;

    private const int TamanoValor = 451;

    /// <summary>Cada entrada del índice son dos int32.</summary>
    private const int EntradaIndice = 8;

    /// <summary>Registros entre dos entradas del índice: los que hacen falta para pasar los 4 KB.</summary>
    private const int PorAncla = 9;

    private const int Mensajes = 120;

    /// <summary>24 registros por segmento: 12 KB, varios intervalos, así que cada uno tiene anclas.</summary>
    private const long LimitePorSegmento = 24 * TamanoRegistro;

    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "k0sstreams-tests", Guid.NewGuid().ToString("N"));

    private string Carpeta => Path.Combine(_dataDir, Topico);

    private string Log => Path.Combine(Carpeta, "00000000000000000000.log");

    private string Indice => Path.Combine(Carpeta, "00000000000000000000.index");

    private SegmentedLog Reiniciar(long limite = SegmentedLog.DefaultSegmentBytes) => new(_dataDir, limite);

    public void Dispose()
    {
        if (Directory.Exists(_dataDir))
        {
            Directory.Delete(_dataDir, recursive: true);
        }
    }

    /// <summary>El mensaje número <paramref name="i"/>: arranca con su número y se rellena con equis.</summary>
    private static Record Msg(int i) =>
        Record.NewMessage(
            1, 0, ReadOnlyMemory<byte>.Empty, Encoding.UTF8.GetBytes($"{i:D3}".PadRight(TamanoValor, 'x')));

    private static string NumeroDe(Record record) => Encoding.UTF8.GetString(record.Value.Span)[..3];

    private static async Task<SegmentedLog> Escribir(SegmentedLog log, int cuantos)
    {
        for (int i = 0; i < cuantos; i++)
        {
            await log.AppendAsync(Topico, 0, Msg(i));
        }

        return log;
    }

    private long Anclas() => new FileInfo(Indice).Length / EntradaIndice;

    /// <summary>Nombres de los segmentos que tienen índice, ordenados.</summary>
    private string[] Indices() =>
        [.. Directory.GetFiles(Carpeta, "*.index").Select(Path.GetFileNameWithoutExtension).Order()!];

    [Fact]
    public void La_cuenta_de_los_tests_es_la_que_dice_el_codec()
    {
        // Si esto falla, los demás tests de este archivo dejan de probar lo que creen probar.
        RecordCodec.GetEncodedSize(Msg(0)).Should().Be(TamanoRegistro);

        // La primera ancla es el primer registro que cae en o después de los 4 KB, y a partir de
        // ahí el paso se repite igual, porque 9 registros pesan más que el intervalo.
        (PorAncla * TamanoRegistro).Should().BeGreaterThanOrEqualTo(4096);
        ((PorAncla - 1) * TamanoRegistro).Should().BeLessThan(4096);
    }

    [Fact]
    public async Task Leer_un_offset_del_medio_da_lo_mismo_con_indice_y_sin_indice()
    {
        // El criterio de "terminado" de la iteración: el índice es una optimización, no cambia ni
        // un byte de lo que se devuelve. El offset 60 cae entre las anclas 54 y 63.
        await Escribir(Reiniciar(), Mensajes);
        File.Exists(Indice).Should().BeTrue();

        var conIndice = await Reiniciar().ReadAsync(Topico, 0, 60, 10).ToListAsync();

        File.Delete(Indice);
        var sinIndice = await Reiniciar().ReadAsync(Topico, 0, 60, 10).ToListAsync();

        conIndice.Select(r => r.Offset).Should().Equal(Enumerable.Range(60, 10).Select(i => (long)i));
        sinIndice.Select(r => r.Offset).Should().Equal(conIndice.Select(r => r.Offset));
        sinIndice.Select(NumeroDe).Should().Equal(conIndice.Select(NumeroDe));
    }

    [Theory]
    [InlineData(0)]             // antes de la primera entrada: arranca del byte 0
    [InlineData(PorAncla - 1)]  // el registro anterior a un ancla
    [InlineData(PorAncla)]      // justo en un ancla
    [InlineData(PorAncla + 1)]  // el siguiente a un ancla
    [InlineData(60)]            // entre dos anclas, en el medio del log
    [InlineData(Mensajes - 1)]  // el último
    public async Task Leer_desde_cualquier_offset_cae_en_el_registro_correcto(int desde)
    {
        await Escribir(Reiniciar(), Mensajes);

        var leidos = await Reiniciar().ReadAsync(Topico, 0, desde, 3).ToListAsync();

        leidos.Select(r => r.Offset).Should().Equal(
            Enumerable.Range(desde, Math.Min(3, Mensajes - desde)).Select(i => (long)i));
        NumeroDe(leidos[0]).Should().Be($"{desde:D3}");
    }

    [Fact]
    public async Task El_indice_es_mucho_mas_chico_que_el_log()
    {
        await Escribir(Reiniciar(), Mensajes);

        new FileInfo(Log).Length.Should().Be(Mensajes * TamanoRegistro);
        (new FileInfo(Indice).Length % EntradaIndice).Should().Be(0, "son entradas de ancho fijo");
        new FileInfo(Indice).Length.Should().BeLessThan(new FileInfo(Log).Length / 100);
    }

    [Fact]
    public async Task Hay_una_entrada_cada_cuatro_kilobytes()
    {
        await Escribir(Reiniciar(), Mensajes);

        // Lo que hace al índice disperso: no hay una entrada por registro, hay una por intervalo.
        Anclas().Should().Be((Mensajes - 1) / PorAncla);
        Anclas().Should().BeLessThan(Mensajes);
    }

    [Fact]
    public async Task Arrancar_no_vuelve_a_leer_los_registros_viejos()
    {
        await Escribir(Reiniciar(), Mensajes);

        // Se da vuelta un byte del registro 2: el CRC deja de cerrar. Queda muy antes de la última
        // entrada del índice, que es desde donde ahora arranca la recuperación.
        byte[] bytes = await File.ReadAllBytesAsync(Log);
        bytes[(2 * TamanoRegistro) + 20] ^= 0xFF;
        await File.WriteAllBytesAsync(Log, bytes);

        var despues = Reiniciar();

        despues.EndOffset(Topico, 0).Should().Be(
            Mensajes - 1, "el arranque solo lee desde la última entrada del índice");

        // Pero el daño no se sirve en silencio: salta al leer ese offset.
        await despues.Invoking(l => l.ReadAsync(Topico, 0, 2, 1).ToListAsync().AsTask())
            .Should().ThrowAsync<InvalidDataException>().WithMessage("*CRC*");
    }

    [Fact]
    public async Task Un_indice_borrado_se_reconstruye_solo()
    {
        await Escribir(Reiniciar(), Mensajes);
        long sanas = Anclas();
        File.Delete(Indice);

        var despues = Reiniciar();

        despues.EndOffset(Topico, 0).Should().Be(Mensajes - 1);
        Anclas().Should().Be(sanas, "se rehace igual recorriendo el log");
    }

    [Theory]
    [InlineData(3)]  // un largo que no es múltiplo de una entrada
    [InlineData(8)]  // una entrada entera, pero en ceros: nadie anota el offset 0
    public async Task Un_indice_con_basura_se_reconstruye_solo(int basura)
    {
        await Escribir(Reiniciar(), Mensajes);
        long sanas = Anclas();
        await File.WriteAllBytesAsync(Indice, new byte[basura]);

        var despues = Reiniciar();

        despues.EndOffset(Topico, 0).Should().Be(Mensajes - 1);
        (await despues.ReadAsync(Topico, 0, 60, 1).Select(r => r.Offset).ToListAsync()).Should().Equal(60);
        Anclas().Should().Be(sanas);
    }

    [Fact]
    public async Task Un_indice_que_apunta_mas_alla_del_log_se_reconstruye_solo()
    {
        await Escribir(Reiniciar(), Mensajes);
        long sanas = Anclas();
        await File.WriteAllBytesAsync(Indice, Entrada(PorAncla, int.MaxValue));

        var despues = Reiniciar();

        despues.EndOffset(Topico, 0).Should().Be(Mensajes - 1);
        Anclas().Should().Be(sanas);
    }

    [Fact]
    public async Task Un_indice_que_apunta_al_medio_de_un_registro_no_hace_recortar_el_log()
    {
        await Escribir(Reiniciar(), Mensajes);
        long sanas = Anclas();
        long entero = new FileInfo(Log).Length;

        // Una entrada que pasa todas las validaciones de forma pero cae dos bytes dentro del
        // registro 9. Sin la red de seguridad, el arranque leería basura ahí, la tomaría por una
        // escritura cortada y recortaría el log: 110 registros buenos a la basura.
        await File.WriteAllBytesAsync(Indice, Entrada(PorAncla, (PorAncla * TamanoRegistro) + 2));

        var despues = Reiniciar();

        despues.EndOffset(Topico, 0).Should().Be(Mensajes - 1);
        new FileInfo(Log).Length.Should().Be(entero);
        Anclas().Should().Be(sanas, "el índice era el sospechoso, así que se rehizo");
    }

    [Fact]
    public async Task Un_ancla_donde_no_cabe_un_registro_no_hace_recortar_el_log()
    {
        await Escribir(Reiniciar(), Mensajes);
        long sanas = Anclas();
        long entero = new FileInfo(Log).Length;

        // Una entrada que cae a un byte del final: ahí no entra ni el largo de un registro, así que
        // el recorrido no lee nada y no tiene de qué quejarse. Sin la red de seguridad ese "no leí
        // nada" se tomaría por una escritura cortada justo en el ancla, y el log se recortaría ahí.
        await File.WriteAllBytesAsync(Indice, Entrada(PorAncla, entero - 1));

        var despues = Reiniciar();

        despues.EndOffset(Topico, 0).Should().Be(Mensajes - 1);
        new FileInfo(Log).Length.Should().Be(entero);
        Anclas().Should().Be(sanas);
    }

    [Fact]
    public async Task Un_indice_atrasado_se_completa_al_arrancar()
    {
        // El .index no lleva fsync, así que un corte puede dejarlo con menos entradas que el log.
        // Es inofensivo por diseño: puede quedar atrasado, nunca adelantado.
        await Escribir(Reiniciar(), Mensajes);
        long sanas = Anclas();
        byte[] bytes = await File.ReadAllBytesAsync(Indice);
        await File.WriteAllBytesAsync(Indice, bytes[..EntradaIndice]);

        var despues = Reiniciar();

        despues.EndOffset(Topico, 0).Should().Be(Mensajes - 1);
        (await despues.ReadAsync(Topico, 0, 100, 2).Select(r => r.Offset).ToListAsync()).Should().Equal(100, 101);
        Anclas().Should().Be(sanas, "el arranque lo completa desde su última entrada");
    }

    [Fact]
    public async Task El_indice_sobrevive_al_reinicio_y_sigue_creciendo()
    {
        await Escribir(Reiniciar(), 60);
        Anclas().Should().Be((60 - 1) / PorAncla);

        var despues = Reiniciar();
        for (int i = 60; i < Mensajes; i++)
        {
            await despues.AppendAsync(Topico, 0, Msg(i));
        }

        // El reinicio no reinicia la cuenta: el índice sigue desde donde había quedado.
        Anclas().Should().Be((Mensajes - 1) / PorAncla);
        despues.EndOffset(Topico, 0).Should().Be(Mensajes - 1);
        (await despues.ReadAsync(Topico, 0, Mensajes - 1, 1).Select(r => r.Offset).ToListAsync())
            .Should().Equal(Mensajes - 1);
    }

    [Fact]
    public async Task Truncar_recorta_tambien_el_indice()
    {
        var log = await Escribir(Reiniciar(), Mensajes);

        await log.TruncateAsync(Topico, 0, 24);

        log.EndOffset(Topico, 0).Should().Be(24);
        new FileInfo(Log).Length.Should().Be(25 * TamanoRegistro);

        // Solo quedan las anclas que apuntan a registros que todavía existen: las de 9 y 18.
        Anclas().Should().Be(24 / PorAncla);
        (await log.ReadAsync(Topico, 0, 23, 5).Select(r => r.Offset).ToListAsync()).Should().Equal(23, 24);
    }

    [Fact]
    public async Task Truncar_borra_el_indice_de_los_segmentos_borrados()
    {
        var log = await Escribir(Reiniciar(LimitePorSegmento), Mensajes);
        Indices().Should().HaveCount(5, "un .index por segmento");

        await log.TruncateAsync(Topico, 0, 30);

        // Los tres segmentos posteriores al corte se borraron con su índice. Al que contiene el
        // corte le quedan 7 registros, menos que un intervalo, así que se queda sin anclas y su
        // archivo también se va: no quedan .index que describan algo que no está.
        Directory.GetFiles(Carpeta, "*.log").Should().HaveCount(2);
        Indices().Should().Equal("00000000000000000000");

        var despues = Reiniciar(LimitePorSegmento);
        despues.EndOffset(Topico, 0).Should().Be(30);
        (await despues.ReadAsync(Topico, 0, 27, 10).Select(r => r.Offset).ToListAsync())
            .Should().Equal(27, 28, 29, 30);
    }

    [Fact]
    public async Task Cada_segmento_tiene_su_indice_y_leer_los_cruza()
    {
        await Escribir(Reiniciar(LimitePorSegmento), Mensajes);

        var despues = Reiniciar(LimitePorSegmento);
        var leidos = await despues.ReadAsync(Topico, 0, 18, 60).Select(r => r.Offset).ToListAsync();

        // Arranca en el medio del primer segmento, lo termina y sigue por los tres siguientes.
        leidos.Should().Equal(Enumerable.Range(18, 60).Select(i => (long)i));
        Indices().Should().HaveCount(5);
    }

    /// <summary>Una entrada de índice armada a mano, para poder ensuciar el archivo a propósito.</summary>
    private static byte[] Entrada(int relativeOffset, long position)
    {
        var bytes = new byte[EntradaIndice];
        BinaryPrimitives.WriteInt32LittleEndian(bytes, relativeOffset);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(sizeof(int)), (int)position);
        return bytes;
    }
}
