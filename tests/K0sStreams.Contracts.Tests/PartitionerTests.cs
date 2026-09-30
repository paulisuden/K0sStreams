using System.Text;
using K0sStreams.Contracts;

namespace K0sStreams.Contracts.Tests;

public class PartitionerTests
{
    [Fact]
    public void Sin_clave_va_a_la_particion_cero() =>
        Partitioner.ForKey([], 8).Should().Be(0);

    [Fact]
    public void Misma_clave_misma_particion_y_dentro_del_rango()
    {
        for (int i = 0; i < 200; i++)
        {
            var key = Encoding.UTF8.GetBytes($"cliente-{i}");
            int p = Partitioner.ForKey(key, 7);
            p.Should().BeInRange(0, 6);
            Partitioner.ForKey(key, 7).Should().Be(p);
        }
    }

    [Theory]
    [InlineData("pedido-1", 16, 7)]
    [InlineData("cliente-42", 7, 4)]
    public void Valores_fijos_para_detectar_cambios_de_algoritmo(string key, int partitions, int expected) =>
        // Si esto cambia, los mensajes de una clave pasarían a otra partición: no cambiar el algoritmo.
        Partitioner.ForKey(Encoding.UTF8.GetBytes(key), partitions).Should().Be(expected);
}
