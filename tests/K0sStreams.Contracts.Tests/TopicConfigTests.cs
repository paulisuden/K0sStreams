using K0sStreams.Contracts;

namespace K0sStreams.Contracts.Tests;

public class TopicConfigTests
{
    [Theory]
    [InlineData("pedidos")]
    [InlineData("pedidos.v1")]
    [InlineData("a-b_c.9")]
    public void Nombres_validos(string name) => TopicConfig.IsValidName(name).Should().BeTrue();

    [Theory]
    [InlineData("")]
    [InlineData("Pedidos")]
    [InlineData("../etc")]
    [InlineData(".oculto")]
    [InlineData("con espacio")]
    [InlineData(null)]
    public void Nombres_invalidos(string? name) => TopicConfig.IsValidName(name).Should().BeFalse();

    [Fact]
    public void Configuracion_valida_no_tiene_errores() =>
        new TopicConfig("pedidos", 1, 3, TimeSpan.FromSeconds(5)).Validate().Should().BeEmpty();

    [Fact]
    public void Configuracion_invalida_reporta_cada_campo() =>
        new TopicConfig("MAL", 0, -1, TimeSpan.FromSeconds(-1)).Validate().Keys
            .Should().BeEquivalentTo("Name", "Partitions", "MaxRetries", "RetryBackoff");
}
