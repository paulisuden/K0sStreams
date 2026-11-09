using System.Globalization;
using System.Text;
using K0sStreams.Contracts;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;

using Record = K0sStreams.Contracts.Record;

namespace K0sStreams.Storage.Tests;

/// <summary>
/// Retención: borrar segmentos viejos por antigüedad o por tamaño sin perder nada confirmado y sin
/// dejar el tópico inservible. El riesgo no está en borrar, está en lo que pasa después: un
/// reinicio, una lectura por debajo de lo borrado, o una lectura en curso cuando se borra.
/// </summary>
public sealed class RetentionTests : IDisposable
{
    private const string Topico = "pedidos";

    /// <summary>Con mensajes de 3 bytes, cada registro pesa 52 y entran exactamente 3 por segmento.</summary>
    private const long LimiteChico = 200;

    private const int PorSegmento = 3;

    private const long BytesPorSegmento = PorSegmento * 52;

    private static readonly TimeSpan UnaHora = TimeSpan.FromHours(1);

    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "k0sstreams-tests", Guid.NewGuid().ToString("N"));

    /// <summary>El reloj del test. Arranca en una fecha fija para que los mensajes de fallo se lean.</summary>
    private readonly FakeTimeProvider _tiempo = new(new DateTimeOffset(2026, 11, 7, 12, 0, 0, TimeSpan.Zero));

    private string Carpeta => Path.Combine(_dataDir, Topico);

    /// <summary>El archivo donde la retención deja en qué offset empieza el log.</summary>
    private string ArchivoLogStart => Path.Combine(Carpeta, "log-start");

    public void Dispose()
    {
        if (Directory.Exists(_dataDir))
        {
            Directory.Delete(_dataDir, recursive: true);
        }
    }

    /// <summary>Un broker que arranca de cero sobre los datos que ya están en la carpeta.</summary>
    private SegmentedLog Reiniciar(RetentionOptions? retencion = null) =>
        new(_dataDir, LimiteChico, retencion ?? new RetentionOptions());

    private static Record Msg(DateTimeOffset cuando) =>
        Record.NewMessage(1, cuando.ToUnixTimeMilliseconds(), ReadOnlyMemory<byte>.Empty, Encoding.UTF8.GetBytes("abc"));

    private string Segmento(long baseOffset) =>
        Path.Combine(Carpeta, baseOffset.ToString("D20", CultureInfo.InvariantCulture) + Segment.Extension);

    /// <summary>Nombres de los segmentos que quedan en disco para ese tópico, ordenados.</summary>
    private string[] SegmentosEnDisco(string topico = Topico) =>
        [.. Directory.GetFiles(Path.Combine(_dataDir, topico), "*" + Segment.Extension)
            .Select(Path.GetFileName).Order()!];

    /// <summary>
    /// Deja <paramref name="cuantos"/> segmentos llenos, cada uno escrito una hora después del
    /// anterior, y el high watermark en <paramref name="confirmado"/> (-1 = nada confirmado).
    /// El segmento número <c>k</c> queda con el offset <c>3k</c> y su registro más nuevo es de
    /// <c>t0 + k horas</c>.
    /// </summary>
    private async Task<SegmentedLog> ConSegmentos(int cuantos, long confirmado, RetentionOptions? retencion = null)
    {
        var log = Reiniciar(retencion);
        for (int segmento = 0; segmento < cuantos; segmento++)
        {
            for (int i = 0; i < PorSegmento; i++)
            {
                await log.AppendAsync(Topico, 0, Msg(_tiempo.GetUtcNow()));
            }

            _tiempo.Advance(UnaHora);
        }

        if (confirmado >= 0)
        {
            log.AdvanceHighWatermark(Topico, 0, confirmado);
        }

        return log;
    }

    [Fact]
    public void La_cuenta_de_los_tests_es_la_que_dice_el_codec()
    {
        // Si esto falla, los demás tests de este archivo dejan de probar lo que creen probar.
        RecordCodec.GetEncodedSize(Msg(DateTimeOffset.UnixEpoch)).Should().Be(52);
        (PorSegmento * 52).Should().BeLessThanOrEqualTo((int)LimiteChico);
        ((PorSegmento + 1) * 52).Should().BeGreaterThan((int)LimiteChico);
    }

    [Fact]
    public async Task Un_segmento_mas_nuevo_que_MaxAge_no_se_borra()
    {
        var log = await ConSegmentos(3, confirmado: 8, new RetentionOptions { MaxAge = TimeSpan.FromHours(24) });

        log.ApplyRetention(_tiempo.GetUtcNow());

        SegmentosEnDisco().Should().HaveCount(3);
        File.Exists(ArchivoLogStart).Should().BeFalse("no se borró nada, así que el log sigue empezando en 0");
    }

    [Fact]
    public async Task Un_segmento_cuyo_registro_mas_nuevo_es_mas_viejo_que_MaxAge_se_borra()
    {
        // El reloj quedó en t0 + 3 h. Con un corte en t0 + 30 min, solo el primer segmento (t0) es viejo.
        var log = await ConSegmentos(3, confirmado: 8, new RetentionOptions { MaxAge = TimeSpan.FromMinutes(150) });

        log.ApplyRetention(_tiempo.GetUtcNow());

        SegmentosEnDisco().Should().Equal("00000000000000000003.log", "00000000000000000006.log");
        File.ReadAllText(ArchivoLogStart).Should().Be("3");
        (await log.ReadAsync(Topico, 0, 3, 100).Select(r => r.Offset).ToListAsync()).Should().Equal(3, 4, 5, 6, 7, 8);
    }

    [Fact]
    public async Task No_se_borra_el_segmento_del_high_watermark_ni_los_posteriores()
    {
        // Todo es viejo para la retención, pero el high watermark está en el segundo segmento.
        var log = await ConSegmentos(3, confirmado: 4, new RetentionOptions { MaxAge = TimeSpan.FromMinutes(1) });

        log.ApplyRetention(_tiempo.GetUtcNow() + TimeSpan.FromHours(100));

        SegmentosEnDisco().Should().Equal(
            "00000000000000000003.log", "00000000000000000006.log");
    }

    [Fact]
    public async Task Lo_no_confirmado_no_se_borra_aunque_sea_viejo()
    {
        var log = await ConSegmentos(3, confirmado: -1, new RetentionOptions { MaxAge = TimeSpan.FromMinutes(1) });

        log.ApplyRetention(_tiempo.GetUtcNow() + TimeSpan.FromHours(100));

        SegmentosEnDisco().Should().HaveCount(3, "sin quórum no hay nada que se pueda dar por perdido");
    }

    [Fact]
    public async Task Por_tamanio_se_borran_los_mas_viejos_hasta_quedar_bajo_MaxBytes()
    {
        // Cuatro segmentos llenos pesan 624 bytes; el tope deja lugar para dos.
        var log = await ConSegmentos(4, confirmado: 11, new RetentionOptions { MaxBytes = 2 * BytesPorSegmento });

        log.ApplyRetention(_tiempo.GetUtcNow());

        SegmentosEnDisco().Should().Equal("00000000000000000006.log", "00000000000000000009.log");
        File.ReadAllText(ArchivoLogStart).Should().Be("6");
    }

    [Fact]
    public async Task La_retencion_por_tamanio_no_toca_lo_protegido()
    {
        // Un tope imposible de cumplir: aun así, el segmento del high watermark y el activo quedan.
        var log = await ConSegmentos(3, confirmado: 4, new RetentionOptions { MaxBytes = 1 });

        log.ApplyRetention(_tiempo.GetUtcNow());

        SegmentosEnDisco().Should().Equal("00000000000000000003.log", "00000000000000000006.log");
    }

    [Fact]
    public async Task Despues_de_borrar_y_reiniciar_el_topico_sigue_sirviendo()
    {
        var log = await ConSegmentos(4, confirmado: 11, new RetentionOptions { MaxBytes = 2 * BytesPorSegmento });
        log.ApplyRetention(_tiempo.GetUtcNow());

        var despues = Reiniciar();

        despues.EndOffset(Topico, 0).Should().Be(11);
        despues.HighWatermark(Topico, 0).Should().Be(11);
        (await despues.ReadAsync(Topico, 0, 6, 100).Select(r => r.Offset).ToListAsync())
            .Should().Equal(6, 7, 8, 9, 10, 11);

        // Y los offsets nuevos siguen donde estaban, sin volver a empezar en 0.
        (await despues.AppendAsync(Topico, 0, Msg(_tiempo.GetUtcNow()))).Should().Be(12);
    }

    [Fact]
    public async Task Un_borrado_interrumpido_se_termina_al_recuperar()
    {
        await ConSegmentos(3, confirmado: 8);

        // Corte de luz justo después de anotar el inicio nuevo y antes de borrar los archivos.
        await File.WriteAllTextAsync(ArchivoLogStart, "3");

        var despues = Reiniciar();

        despues.EndOffset(Topico, 0).Should().Be(8);
        File.Exists(Segmento(0)).Should().BeFalse("la recuperación termina el borrado que quedó a medias");
        File.Exists(Path.ChangeExtension(Segmento(0), SparseIndex.Extension)).Should().BeFalse();
        (await despues.ReadAsync(Topico, 0, 3, 100).Select(r => r.Offset).ToListAsync()).Should().Equal(3, 4, 5, 6, 7, 8);
        (await despues.AppendAsync(Topico, 0, Msg(_tiempo.GetUtcNow()))).Should().Be(9);
    }

    [Fact]
    public async Task Un_hueco_real_entre_segmentos_sigue_siendo_dano()
    {
        var log = await ConSegmentos(4, confirmado: 11, new RetentionOptions { MaxBytes = 3 * BytesPorSegmento });
        log.ApplyRetention(_tiempo.GetUtcNow());
        SegmentosEnDisco().Should().HaveCount(3, "el primer segmento se borró por retención");

        // Y ahora desaparece uno del medio, que la retención nunca habría borrado.
        File.Delete(Segmento(6));

        var despues = Reiniciar();

        despues.Invoking(l => l.EndOffset(Topico, 0)).Should().Throw<InvalidDataException>()
            .WithMessage("*offset 6*");
    }

    [Fact]
    public async Task Leer_desde_un_offset_ya_borrado_devuelve_desde_el_inicio_del_log()
    {
        var log = await ConSegmentos(3, confirmado: 8, new RetentionOptions { MaxBytes = 2 * BytesPorSegmento });
        log.ApplyRetention(_tiempo.GetUtcNow());

        var leidos = await log.ReadAsync(Topico, 0, 0, 100).Select(r => r.Offset).ToListAsync();

        leidos.Should().Equal(
            new long[] { 3, 4, 5, 6, 7, 8 }, "pedir el offset 0 devuelve desde el inicio, no vacío");
    }

    [Fact]
    public async Task Sin_MaxAge_ni_MaxBytes_no_se_borra_nada()
    {
        var log = await ConSegmentos(3, confirmado: 8);

        log.ApplyRetention(_tiempo.GetUtcNow() + TimeSpan.FromDays(365));

        SegmentosEnDisco().Should().HaveCount(3);
        File.Exists(ArchivoLogStart).Should().BeFalse();
    }

    [Fact]
    public async Task Una_lectura_que_choca_con_un_borrado_no_falla()
    {
        var log = await ConSegmentos(4, confirmado: 11, new RetentionOptions { MaxBytes = BytesPorSegmento });

        var leidos = new List<long>();
        await using var lectura = log.ReadAsync(Topico, 0, 0, 100).GetAsyncEnumerator();

        // Se consume el primer segmento entero: el plan ya está armado y apunta a los otros tres.
        for (int i = 0; i < PorSegmento; i++)
        {
            (await lectura.MoveNextAsync()).Should().BeTrue();
            leidos.Add(lectura.Current.Offset);
        }

        log.ApplyRetention(_tiempo.GetUtcNow());

        while (await lectura.MoveNextAsync())
        {
            leidos.Add(lectura.Current.Offset);
        }

        // Lo que sobrevivió al borrado se sigue entregando; lo que no, se saltea sin tirar error.
        leidos.Should().Equal(0, 1, 2, 9, 10, 11);
    }

    [Fact]
    public async Task La_retencion_limpia_un_topico_que_nadie_toco_desde_el_arranque()
    {
        await ConSegmentos(4, confirmado: 11);

        // Un broker que reinicia y sirve otra cosa: su lista de tópicos en memoria arranca vacía, y
        // este tópico igual tiene que limpiarse o el disco se llena con algo que nadie va a pedir.
        var despues = Reiniciar(new RetentionOptions { MaxBytes = 2 * BytesPorSegmento });
        despues.ApplyRetention(_tiempo.GetUtcNow());

        SegmentosEnDisco().Should().Equal("00000000000000000006.log", "00000000000000000009.log");
    }

    [Fact]
    public async Task El_catalogo_de_topicos_no_se_poda_nunca()
    {
        // Con límites imposibles de cumplir: el catálogo es metadato y no caduca. Si se le borrara
        // el primer segmento, los tópicos viejos darían 404 con los mensajes sanos en disco.
        var log = Reiniciar(new RetentionOptions { MaxAge = TimeSpan.FromMinutes(1), MaxBytes = 1 });
        for (int i = 0; i < 4 * PorSegmento; i++)
        {
            await log.AppendAsync(LogTopicCatalog.TopicsLog, 0, Msg(_tiempo.GetUtcNow()));
        }

        log.AdvanceHighWatermark(LogTopicCatalog.TopicsLog, 0, (4 * PorSegmento) - 1);

        log.ApplyRetention(_tiempo.GetUtcNow() + TimeSpan.FromDays(365));

        SegmentosEnDisco(LogTopicCatalog.TopicsLog).Should().HaveCount(4);
    }

    [Fact]
    public async Task Un_topico_de_un_solo_segmento_no_se_poda_nunca()
    {
        // Es lo que salva al catálogo incluso sin la exclusión explícita: su único segmento contiene
        // el high watermark. Vale fijarlo, porque bajar el tamaño de segmento lo rompería.
        var log = await ConSegmentos(
            1, confirmado: 2, new RetentionOptions { MaxAge = TimeSpan.FromMinutes(1), MaxBytes = 1 });

        log.ApplyRetention(_tiempo.GetUtcNow() + TimeSpan.FromDays(365));

        SegmentosEnDisco().Should().HaveCount(1);
    }

    [Fact]
    public async Task El_tamanio_de_segmento_sale_de_la_configuracion()
    {
        // Sin esto la retención no se puede mostrar: con los 64 MiB por defecto hay que escribir más
        // de 128 MiB en un tópico para que llegue a borrar un segmento.
        using var services = new ServiceCollection()
            .AddK0sStorage(new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Broker:DataDir"] = _dataDir,
                ["Storage:SegmentBytes"] = LimiteChico.ToString(CultureInfo.InvariantCulture),
            }).Build())
            .BuildServiceProvider();

        var log = services.GetRequiredService<ILog>();
        for (int i = 0; i < PorSegmento + 1; i++)
        {
            await log.AppendAsync(Topico, 0, Msg(_tiempo.GetUtcNow()));
        }

        SegmentosEnDisco().Should().HaveCount(2, "con ese límite entran tres registros por segmento");
    }

    [Fact]
    public void Sin_configurar_el_tamanio_de_segmento_son_64_MiB()
    {
        using var services = new ServiceCollection()
            .AddK0sStorage(new ConfigurationBuilder().Build())
            .BuildServiceProvider();

        services.GetRequiredService<IOptions<StorageOptions>>().Value.SegmentBytes
            .Should().Be(SegmentedLog.DefaultSegmentBytes);
    }

    [Fact]
    public void Una_configuracion_vacia_deja_la_retencion_apagada()
    {
        // Así llega del chart de Helm cuando se la quiere apagar: Retention__MaxAge="".
        var retencion = Configurar(new() { ["Retention:MaxAge"] = "", ["Retention:MaxBytes"] = "" });

        retencion.MaxAge.Should().BeNull();
        retencion.MaxBytes.Should().BeNull();
        retencion.CheckInterval.Should().Be(TimeSpan.FromMinutes(5));
    }

    [Fact]
    public void El_MaxAge_del_chart_se_lee_como_24_dias_y_no_como_24_horas()
    {
        // Los valores son los que pone el chart de Helm. "24:00:00" NO son 24 horas: .NET lo lee
        // como d:hh:mm, o sea 24 días, porque 24 no es una hora válida. Veinticuatro horas se
        // escriben "1.00:00:00". El valor está en deploy/ y en appsettings.k8s.json, que son de
        // otros bloques, así que acá solo queda fijado lo que el log entiende hoy.
        var retencion = Configurar(new()
        {
            ["Retention:MaxAge"] = "24:00:00",
            ["Retention:MaxBytes"] = "1073741824",
            ["Retention:CheckInterval"] = "00:05:00",
        });

        retencion.MaxAge.Should().Be(TimeSpan.FromDays(24));
        retencion.MaxBytes.Should().Be(1024L * 1024 * 1024);
        retencion.CheckInterval.Should().Be(TimeSpan.FromMinutes(5));
    }

    [Fact]
    public void La_retencion_corre_como_tarea_del_host()
    {
        // Y no adentro del log: si el log arrancara la tarea en su constructor, el apagado del pod
        // no la podría detener.
        using var services = new ServiceCollection()
            .AddK0sStorage(new ConfigurationBuilder().Build())
            .AddLogging()
            .AddSingleton(TimeProvider.System)
            .BuildServiceProvider();

        services.GetServices<IHostedService>().Should().ContainSingle()
            .Which.Should().BeOfType<RetentionService>();
    }

    private static RetentionOptions Configurar(Dictionary<string, string?> settings)
    {
        using var services = new ServiceCollection()
            .AddK0sStorage(new ConfigurationBuilder().AddInMemoryCollection(settings).Build())
            .BuildServiceProvider();

        return services.GetRequiredService<IOptions<RetentionOptions>>().Value;
    }
}
