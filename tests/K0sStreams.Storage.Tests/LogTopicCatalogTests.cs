using K0sStreams.Contracts;
using K0sStreams.Contracts.Fakes;
using Record = K0sStreams.Contracts.Record;

namespace K0sStreams.Storage.Tests;

/// <summary>El catálogo de tópicos vive en el log: sobrevive a reinicios y lo ve cualquier broker que tenga el log.</summary>
public sealed class LogTopicCatalogTests : IDisposable
{
    private static readonly TopicConfig Pedidos = new("pedidos", 1, MaxRetries: 5, TimeSpan.FromSeconds(7));

    private readonly InMemoryLog _log = new();
    private readonly List<LogTopicCatalog> _catalogs = [];

    public void Dispose()
    {
        foreach (var catalog in _catalogs)
        {
            catalog.Dispose();
        }
    }

    [Fact]
    public async Task Crear_un_topico_lo_escribe_en_el_log_y_lo_encuentra()
    {
        var catalog = Catalog();

        (await catalog.CreateAsync(Pedidos)).Should().BeTrue();

        catalog.Find("pedidos").Should().Be(Pedidos);
        _log.EndOffset(LogTopicCatalog.TopicsLog, 0).Should().Be(0);
    }

    [Fact]
    public async Task Crear_un_topico_que_ya_existe_devuelve_false()
    {
        var catalog = Catalog();
        await catalog.CreateAsync(Pedidos);

        (await catalog.CreateAsync(Pedidos with { MaxRetries = 1 })).Should().BeFalse();

        catalog.Find("pedidos")!.MaxRetries.Should().Be(5);
    }

    [Fact]
    public async Task El_nombre_del_topico_interno_esta_reservado()
    {
        var catalog = Catalog();

        (await catalog.CreateAsync(Pedidos with { Name = LogTopicCatalog.TopicsLog })).Should().BeFalse();

        catalog.List().Should().BeEmpty();
    }

    [Fact]
    public async Task Un_catalogo_nuevo_sobre_el_mismo_log_conoce_los_topicos_con_su_configuracion()
    {
        // Como un broker que se reinicia, o un líder nuevo que tiene el log replicado.
        await Catalog().CreateAsync(Pedidos);
        await Catalog().CreateAsync(Pedidos with { Name = "facturas" });

        var restarted = Catalog();

        restarted.Find("pedidos").Should().Be(Pedidos);
        restarted.List().Select(topic => topic.Name).Should().Equal("facturas", "pedidos");
    }

    [Fact]
    public async Task Un_topico_existe_recien_cuando_su_creacion_tiene_quorum()
    {
        // El líder escribió la creación, pero todavía no hay quórum: el high watermark no la incluye.
        await Catalog(new NoQuorumYet()).CreateAsync(Pedidos);
        var follower = Catalog();

        follower.Find("pedidos").Should().BeNull();

        _log.AdvanceHighWatermark(LogTopicCatalog.TopicsLog, 0, 0);

        follower.Find("pedidos").Should().Be(Pedidos);
    }

    [Fact]
    public async Task Si_deja_de_ser_lider_esperando_el_quorum_falla_y_el_topico_no_existe()
    {
        var catalog = Catalog(new NotLeaderReplicator());

        var create = async () => await catalog.CreateAsync(Pedidos);

        await create.Should().ThrowAsync<NotLeaderException>();
        catalog.Find("pedidos").Should().BeNull();
    }

    [Fact]
    public async Task Ignora_los_registros_que_no_son_topicos()
    {
        var catalog = Catalog();
        await catalog.CreateAsync(Pedidos);

        // Al promoverse, un líder escribe un EpochChange en cada tópico, también en el interno.
        long offset = await _log.AppendAsync(
            LogTopicCatalog.TopicsLog,
            0,
            new Record(Record.Unassigned, 2, RecordType.EpochChange, 0, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty));
        _log.AdvanceHighWatermark(LogTopicCatalog.TopicsLog, 0, offset);
        await catalog.CreateAsync(Pedidos with { Name = "facturas" });

        Catalog().List().Select(topic => topic.Name).Should().Equal("facturas", "pedidos");
    }

    [Fact]
    public async Task Creaciones_concurrentes_del_mismo_topico_crean_uno_solo()
    {
        var catalog = Catalog();

        bool[] created = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => catalog.CreateAsync(Pedidos).AsTask()));

        created.Should().ContainSingle(result => result);
        _log.EndOffset(LogTopicCatalog.TopicsLog, 0).Should().Be(0);
    }

    private LogTopicCatalog Catalog(IReplicator? replicator = null)
    {
        var catalog = new LogTopicCatalog(_log, replicator ?? new InstantReplicator(_log), new StaticClusterState("broker-0"), TimeProvider.System);
        _catalogs.Add(catalog);
        return catalog;
    }

    /// <summary>Un líder que todavía espera a los seguidores: no sube el high watermark.</summary>
    private sealed class NoQuorumYet : IReplicator
    {
        public ValueTask WaitForQuorumAsync(string topic, int partition, long offset, CancellationToken ct = default) =>
            ValueTask.CompletedTask;
    }

    /// <summary>Un líder que perdió el liderazgo mientras esperaba el quórum.</summary>
    private sealed class NotLeaderReplicator : IReplicator
    {
        public ValueTask WaitForQuorumAsync(string topic, int partition, long offset, CancellationToken ct = default) =>
            ValueTask.FromException(new NotLeaderException("Otro broker es el líder."));
    }
}
