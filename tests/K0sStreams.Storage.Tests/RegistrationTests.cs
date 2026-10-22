using K0sStreams.Contracts;
using K0sStreams.Contracts.Fakes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace K0sStreams.Storage.Tests;

/// <summary>Qué deja registrado <c>AddK0sStorage</c> y de dónde saca la carpeta de datos.</summary>
public class RegistrationTests
{
    [Fact]
    public void El_broker_recibe_el_log_en_disco_y_no_el_fake()
    {
        using var services = Build([]);

        services.GetRequiredService<ILog>().Should().BeOfType<SegmentedLog>();
    }

    [Fact]
    public void La_carpeta_de_datos_sale_de_la_configuracion()
    {
        // En el contenedor esto llega como la variable de entorno Broker__DataDir=/data.
        using var services = Build(new() { ["Broker:DataDir"] = "/data" });

        services.GetRequiredService<ILog>().Should().BeOfType<SegmentedLog>()
            .Which.DataDir.Should().Be("/data");
    }

    [Fact]
    public void Sin_configuracion_la_carpeta_queda_al_lado_del_ejecutable()
    {
        using var services = Build([]);

        services.GetRequiredService<ILog>().Should().BeOfType<SegmentedLog>()
            .Which.DataDir.Should().Be(Path.Combine(AppContext.BaseDirectory, "data"));
    }

    [Fact]
    public void Pedir_el_log_no_toca_el_disco()
    {
        string dataDir = Path.Combine(Path.GetTempPath(), "k0sstreams-tests", Guid.NewGuid().ToString("N"));
        using var services = Build(new() { ["Broker:DataDir"] = dataDir });

        services.GetRequiredService<ILog>();

        // El arranque del broker no debe crear carpetas: recién se crean al escribir el primer mensaje.
        Directory.Exists(dataDir).Should().BeFalse();
    }

    [Fact]
    public void El_catalogo_de_topicos_se_guarda_en_el_log()
    {
        using var services = Build([]);

        services.GetRequiredService<ITopicCatalog>().Should().BeOfType<LogTopicCatalog>();
    }

    // El catálogo necesita lo que en el broker registran el host, Replication y Coordination.
    private static ServiceProvider Build(Dictionary<string, string?> settings) =>
        new ServiceCollection()
            .AddK0sStorage(new ConfigurationBuilder().AddInMemoryCollection(settings).Build())
            .AddSingleton(TimeProvider.System)
            .AddSingleton<IReplicator>(provider => new InstantReplicator(provider.GetRequiredService<ILog>()))
            .AddSingleton<IClusterState>(new StaticClusterState("broker-0"))
            .BuildServiceProvider();
}
