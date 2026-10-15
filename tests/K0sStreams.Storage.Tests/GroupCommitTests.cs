using System.Text;
using K0sStreams.Contracts;

using Record = K0sStreams.Contracts.Record;

namespace K0sStreams.Storage.Tests;

/// <summary>
/// Iteración 6: varias escrituras simultáneas comparten un <c>fsync</c> en lugar de pagar uno cada
/// una. Lo que no cambia es la garantía: <c>AppendAsync</c> sigue devolviendo el offset recién
/// cuando el registro está en el disco.
/// </summary>
/// <remarks>
/// Los appends concurrentes se disparan con <c>Task.Run</c> para que salgan de hilos distintos, que
/// es lo que hace un broker con varios clientes. Si salieran todos del mismo hilo, el primero
/// bloquearía al resto y no habría concurrencia que agrupar.
/// </remarks>
public sealed class GroupCommitTests : IDisposable
{
    private const string Topico = "pedidos";

    private const int EnParalelo = 100;

    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "k0sstreams-tests", Guid.NewGuid().ToString("N"));

    private SegmentedLog Reiniciar() => new(_dataDir);

    public void Dispose()
    {
        if (Directory.Exists(_dataDir))
        {
            Directory.Delete(_dataDir, recursive: true);
        }
    }

    private static Record Msg(int i) =>
        Record.NewMessage(1, 0, ReadOnlyMemory<byte>.Empty, Encoding.UTF8.GetBytes($"m{i:D4}"));

    /// <summary>Dispara <paramref name="cuantos"/> appends a la vez y devuelve los offsets que dieron.</summary>
    private static Task<long[]> EnParaleloSobre(SegmentedLog log, string topico, int cuantos) =>
        Task.WhenAll([.. Enumerable.Range(0, cuantos).Select(i =>
            Task.Run(() => log.AppendAsync(topico, 0, Msg(i)).AsTask()))]);

    [Fact]
    public async Task Cien_appends_en_paralelo_dan_offsets_distintos_y_contiguos()
    {
        var log = Reiniciar();

        long[] offsets = await EnParaleloSobre(log, Topico, EnParalelo);

        // Sin repetidos y sin huecos: el offset se sigue asignando bajo el candado, uno por uno.
        offsets.Order().Should().Equal(Enumerable.Range(0, EnParalelo).Select(i => (long)i));
        log.EndOffset(Topico, 0).Should().Be(EnParalelo - 1);
    }

    [Fact]
    public async Task Cien_appends_en_paralelo_cuestan_muchos_menos_fsync_que_cien()
    {
        var log = Reiniciar();

        await EnParaleloSobre(log, Topico, EnParalelo);

        // El punto de la iteración. Cuántos exactamente depende de cómo el sistema operativo reparta
        // los hilos, así que el test pide una cota amplia: lo que no puede pasar es uno por mensaje.
        log.FsyncCount.Should().BeLessThan(EnParalelo / 2);
        log.FsyncCount.Should().BeGreaterThan(0, "algo tiene que haber bajado al disco");
    }

    [Fact]
    public async Task Un_append_solo_sigue_costando_un_fsync()
    {
        var log = Reiniciar();

        await log.AppendAsync(Topico, 0, Msg(0));

        // Sin nadie con quien agruparse, no hay nada que ahorrar. Que siga siendo exactamente uno es
        // la prueba de que el group commit no agrega fsync de más.
        log.FsyncCount.Should().Be(1);
    }

    [Fact]
    public async Task Escrituras_secuenciales_pagan_un_fsync_cada_una()
    {
        var log = Reiniciar();

        for (int i = 0; i < 10; i++)
        {
            await log.AppendAsync(Topico, 0, Msg(i));
        }

        // Es lo esperado, no una falla: quien espera la respuesta antes de mandar el próximo mensaje
        // no tiene con quién compartir el fsync. El group commit solo ayuda con concurrencia.
        log.FsyncCount.Should().Be(10);
    }

    [Fact]
    public async Task El_offset_vuelve_recien_cuando_el_registro_esta_en_disco()
    {
        var log = Reiniciar();

        for (int i = 0; i < 5; i++)
        {
            long offset = await log.AppendAsync(Topico, 0, Msg(i));

            // El contrato de ILog: cuando devuelve, el registro ya está. Si EndOffset se quedara
            // atrás, el offset habría vuelto antes del fsync.
            log.EndOffset(Topico, 0).Should().Be(offset);
        }
    }

    [Fact]
    public async Task El_end_offset_no_adelanta_registros_sin_fsync()
    {
        var log = Reiniciar();

        // Se mira el end offset mientras hay 100 escrituras en vuelo: en ningún momento puede pasar
        // del último offset durable, aunque en el archivo ya haya bytes más adelante.
        var escribiendo = EnParaleloSobre(log, Topico, EnParalelo);
        while (!escribiendo.IsCompleted)
        {
            log.EndOffset(Topico, 0).Should().BeLessThan(EnParalelo);
        }

        await escribiendo;
        log.EndOffset(Topico, 0).Should().Be(EnParalelo - 1);
    }

    [Fact]
    public async Task El_high_watermark_no_acepta_un_offset_sin_fsync()
    {
        var log = Reiniciar();
        await log.AppendAsync(Topico, 0, Msg(0));

        // La validación mira lo durable, no lo escrito. El offset 1 no existe todavía.
        log.Invoking(l => l.AdvanceHighWatermark(Topico, 0, 1)).Should().Throw<ArgumentOutOfRangeException>();
        log.AdvanceHighWatermark(Topico, 0, 0);
        log.HighWatermark(Topico, 0).Should().Be(0);
    }

    [Fact]
    public async Task Cien_appends_en_paralelo_se_recuperan_todos_al_reiniciar()
    {
        await EnParaleloSobre(Reiniciar(), Topico, EnParalelo);

        // La prueba de que el fsync agrupado es un fsync de verdad: si alguno de los 100 se hubiera
        // quedado en el caché del sistema operativo, acá faltaría.
        var despues = Reiniciar();

        despues.EndOffset(Topico, 0).Should().Be(EnParalelo - 1);
        var leidos = await despues.ReadAsync(Topico, 0, 0, EnParalelo * 2).Select(r => r.Offset).ToListAsync();
        leidos.Should().Equal(Enumerable.Range(0, EnParalelo).Select(i => (long)i));
    }

    [Fact]
    public async Task Appends_en_paralelo_sobre_varios_topicos_no_se_pisan()
    {
        var log = Reiniciar();
        string[] topicos = ["pedidos", "pagos", "envios"];

        await Task.WhenAll([.. topicos.Select(t => EnParaleloSobre(log, t, 20))]);

        // Cada tópico tiene su propio candado y su propio escritor: los offsets de uno no dependen
        // de cuántos mensajes recibió otro.
        foreach (string topico in topicos)
        {
            log.EndOffset(topico, 0).Should().Be(19);
        }
    }

    [Fact]
    public async Task Truncar_despues_de_escrituras_en_paralelo_deja_el_log_consistente()
    {
        var log = Reiniciar();
        await EnParaleloSobre(log, Topico, EnParalelo);

        await log.TruncateAsync(Topico, 0, 49);

        log.EndOffset(Topico, 0).Should().Be(49);

        // Y lo que quedó está en disco de verdad, no solo en memoria.
        var despues = Reiniciar();
        despues.EndOffset(Topico, 0).Should().Be(49);
        (await despues.ReadAsync(Topico, 0, 48, 10).Select(r => r.Offset).ToListAsync()).Should().Equal(48, 49);
    }

    [Fact]
    public async Task Una_replica_puede_escribir_offsets_explicitos_en_paralelo()
    {
        var log = Reiniciar();

        // El caso de la réplica: el offset viene dado y tiene que ser EndOffset + 1. Con escrituras
        // en paralelo, todas menos la que le toca tienen que ser rechazadas, no escribirse mal.
        var intentos = await Task.WhenAll([.. Enumerable.Range(0, 10).Select(_ =>
            Task.Run(async () =>
            {
                try
                {
                    return await log.AppendAsync(Topico, 0, Msg(0) with { Offset = 0 });
                }
                catch (ArgumentException)
                {
                    return -1L;
                }
            }))]);

        intentos.Count(o => o == 0).Should().Be(1, "solo una puede quedarse con el offset 0");
        log.EndOffset(Topico, 0).Should().Be(0);
    }
}
