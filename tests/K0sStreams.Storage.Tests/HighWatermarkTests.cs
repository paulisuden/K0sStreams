using System.Text;
using K0sStreams.Contracts;

using Record = K0sStreams.Contracts.Record;

namespace K0sStreams.Storage.Tests;

/// <summary>
/// Iteración 3: que el high watermark sobreviva a un reinicio.
/// Sin esto, un broker que vuelve a arrancar no le muestra nada a sus clientes hasta que
/// llegue tráfico nuevo, aunque los datos estén sanos en disco.
/// </summary>
public sealed class HighWatermarkTests : IDisposable
{
    private const string Topico = "pedidos";

    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "k0sstreams-tests", Guid.NewGuid().ToString("N"));

    private string Checkpoint => Path.Combine(_dataDir, Topico, "checkpoint");

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

    /// <summary>Deja un log con <paramref name="cuantos"/> mensajes y el high watermark en <paramref name="confirmado"/>.</summary>
    private async Task<SegmentedLog> ConUnLogDe(int cuantos, long confirmado)
    {
        var log = Reiniciar();
        for (int i = 0; i < cuantos; i++)
        {
            await log.AppendAsync(Topico, 0, Msg($"m{i}"));
        }

        if (confirmado >= 0)
        {
            log.AdvanceHighWatermark(Topico, 0, confirmado);
        }

        return log;
    }

    [Fact]
    public async Task El_high_watermark_sobrevive_al_reinicio()
    {
        await ConUnLogDe(cuantos: 4, confirmado: 2);

        var despues = Reiniciar();

        despues.HighWatermark(Topico, 0).Should().Be(2);
        despues.EndOffset(Topico, 0).Should().Be(3);
    }

    [Fact]
    public async Task El_checkpoint_queda_escrito_en_disco()
    {
        await ConUnLogDe(cuantos: 3, confirmado: 1);

        (await File.ReadAllTextAsync(Checkpoint)).Should().Be("1");
    }

    [Fact]
    public async Task Sin_checkpoint_el_high_watermark_arranca_en_menos_uno()
    {
        // Un log escrito antes de que existiera el checkpoint tiene que seguir abriéndose.
        await ConUnLogDe(cuantos: 3, confirmado: -1);
        File.Exists(Checkpoint).Should().BeFalse();

        var despues = Reiniciar();

        despues.HighWatermark(Topico, 0).Should().Be(-1);
        despues.EndOffset(Topico, 0).Should().Be(2);
    }

    [Fact]
    public async Task Un_checkpoint_ilegible_arranca_en_menos_uno()
    {
        await ConUnLogDe(cuantos: 3, confirmado: 1);
        await File.WriteAllTextAsync(Checkpoint, "no soy un numero");

        var despues = Reiniciar();

        // Es un número recalculable, no datos perdidos: se arranca conservador y la replicación lo vuelve a subir.
        despues.HighWatermark(Topico, 0).Should().Be(-1);
        despues.EndOffset(Topico, 0).Should().Be(2);
    }

    [Fact]
    public async Task Un_checkpoint_adelantado_se_acota_al_end_offset()
    {
        await ConUnLogDe(cuantos: 2, confirmado: 1);
        await File.WriteAllTextAsync(Checkpoint, "99");

        var despues = Reiniciar();

        // Nunca se puede dar por confirmado algo que no está escrito.
        despues.HighWatermark(Topico, 0).Should().Be(1);
    }

    [Fact]
    public async Task Despues_de_reiniciar_sigue_sin_poder_truncarse_lo_confirmado()
    {
        await ConUnLogDe(cuantos: 4, confirmado: 1);

        var despues = Reiniciar();

        // La replicación (AppendHandler) depende de esta excepción para detectar que el líder
        // contradice algo ya confirmado. Sin checkpoint, un reinicio borraba esa red de seguridad.
        await despues.Invoking(l => l.TruncateAsync(Topico, 0, 0).AsTask())
            .Should().ThrowAsync<InvalidOperationException>();

        // Y lo que está por encima del high watermark se sigue pudiendo truncar.
        await despues.TruncateAsync(Topico, 0, 1);
        despues.EndOffset(Topico, 0).Should().Be(1);
    }
}
