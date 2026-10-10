using System.Text;
using K0sStreams.Contracts;

using Record = K0sStreams.Contracts.Record;

namespace K0sStreams.Storage.Tests;

/// <summary>
/// Iteración 2: que los datos sobrevivan a que maten el proceso.
/// Cada "reinicio" se simula creando un <see cref="SegmentedLog"/> nuevo sobre la misma carpeta,
/// que es justo lo que pasa cuando Kubernetes vuelve a levantar el pod sobre su PVC.
/// </summary>
public sealed class RecoveryTests : IDisposable
{
    private const string Topico = "pedidos";

    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "k0sstreams-tests", Guid.NewGuid().ToString("N"));

    private string Segmento => Path.Combine(_dataDir, Topico, "00000000000000000000.log");

    /// <summary>Un broker que arranca de cero sobre los datos que ya están en la carpeta.</summary>
    private SegmentedLog Reiniciar() => new(_dataDir);

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
    public async Task Los_mensajes_sobreviven_a_reiniciar_el_log()
    {
        var antes = Reiniciar();
        for (int i = 0; i < 100; i++)
        {
            await antes.AppendAsync(Topico, 0, Msg($"m{i}"));
        }

        var despues = Reiniciar();

        despues.EndOffset(Topico, 0).Should().Be(99);
        var leidos = await despues.ReadAsync(Topico, 0, 0, 1000).ToListAsync();
        leidos.Should().HaveCount(100);
        Encoding.UTF8.GetString(leidos[42].Value.Span).Should().Be("m42");
    }

    [Fact]
    public async Task Despues_de_reiniciar_los_offsets_siguen_donde_estaban()
    {
        var antes = Reiniciar();
        await antes.AppendAsync(Topico, 0, Msg("uno"));
        await antes.AppendAsync(Topico, 0, Msg("dos"));

        var despues = Reiniciar();

        // Sin recuperación esto daba 0 y pisaba el registro que ya existía con ese offset.
        (await despues.AppendAsync(Topico, 0, Msg("tres"))).Should().Be(2);
        despues.EndOffset(Topico, 0).Should().Be(2);
    }

    [Fact]
    public async Task Una_escritura_cortada_al_final_se_descarta()
    {
        var antes = Reiniciar();
        for (int i = 0; i < 3; i++)
        {
            await antes.AppendAsync(Topico, 0, Msg($"m{i}"));
        }

        // Simula que el proceso murió a mitad de escribir el cuarto registro.
        byte[] aMedias = RecordCodec.Encode(Msg("se corto"));
        await File.AppendAllBytesAsync(Segmento, aMedias[..(aMedias.Length / 2)]);

        var despues = Reiniciar();

        despues.EndOffset(Topico, 0).Should().Be(2);
        (await despues.ReadAsync(Topico, 0, 0, 10).CountAsync()).Should().Be(3);
    }

    [Fact]
    public async Task El_archivo_queda_recortado_despues_de_recuperar()
    {
        var antes = Reiniciar();
        await antes.AppendAsync(Topico, 0, Msg("entero"));
        long sano = new FileInfo(Segmento).Length;

        await File.AppendAllBytesAsync(Segmento, new byte[] { 1, 2, 3 });
        new FileInfo(Segmento).Length.Should().Be(sano + 3);

        var despues = Reiniciar();
        despues.EndOffset(Topico, 0).Should().Be(0);

        // El recorte tiene que ser real en disco, no solo en el índice en memoria.
        new FileInfo(Segmento).Length.Should().Be(sano);
    }

    [Fact]
    public async Task Despues_de_recuperar_se_puede_seguir_escribiendo_sobre_lo_recortado()
    {
        var antes = Reiniciar();
        await antes.AppendAsync(Topico, 0, Msg("entero"));
        await File.AppendAllBytesAsync(Segmento, new byte[] { 9, 9, 9 });

        var despues = Reiniciar();
        (await despues.AppendAsync(Topico, 0, Msg("siguiente"))).Should().Be(1);

        var leidos = await despues.ReadAsync(Topico, 0, 0, 10).ToListAsync();
        leidos.Should().HaveCount(2);
        Encoding.UTF8.GetString(leidos[1].Value.Span).Should().Be("siguiente");
    }

    [Fact]
    public async Task Un_registro_danado_se_detecta_y_no_se_descarta_en_silencio()
    {
        var antes = Reiniciar();
        await antes.AppendAsync(Topico, 0, Msg("primero"));
        await antes.AppendAsync(Topico, 0, Msg("segundo"));

        // Da vuelta un byte del contenido: el largo sigue bien, pero el CRC deja de cerrar.
        byte[] bytes = await File.ReadAllBytesAsync(Segmento);
        bytes[^3] ^= 0xFF;
        await File.WriteAllBytesAsync(Segmento, bytes);

        var despues = Reiniciar();

        despues.Invoking(l => l.EndOffset(Topico, 0)).Should().Throw<InvalidDataException>()
            .WithMessage("*CRC*");
    }

    [Fact]
    public void Una_carpeta_vacia_no_rompe()
    {
        Directory.CreateDirectory(Path.Combine(_dataDir, Topico));

        var log = Reiniciar();

        log.EndOffset(Topico, 0).Should().Be(-1);
    }

    [Fact]
    public async Task Un_segmento_de_cero_bytes_no_rompe()
    {
        Directory.CreateDirectory(Path.Combine(_dataDir, Topico));
        await File.WriteAllBytesAsync(Segmento, []);

        var log = Reiniciar();

        log.EndOffset(Topico, 0).Should().Be(-1);
        (await log.AppendAsync(Topico, 0, Msg("primero"))).Should().Be(0);
    }

    [Fact]
    public async Task El_high_watermark_arranca_en_menos_uno_despues_de_reiniciar()
    {
        var antes = Reiniciar();
        await antes.AppendAsync(Topico, 0, Msg("uno"));
        antes.AdvanceHighWatermark(Topico, 0, 0);

        var despues = Reiniciar();

        // El high watermark todavía no se guarda en disco: lo vuelve a subir la replicación.
        // Queda documentado acá porque es la pregunta abierta de la iteración 3.
        despues.HighWatermark(Topico, 0).Should().Be(-1);
        despues.EndOffset(Topico, 0).Should().Be(0);
    }
}
