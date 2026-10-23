using System.Net;
using K0sStreams.Contracts;
using Microsoft.AspNetCore.Mvc.Testing;

namespace K0sStreams.Broker.Tests;

public sealed class HostTests(WebApplicationFactory<Program> factory) : IClassFixture<WebApplicationFactory<Program>>
{
    private readonly HttpClient _client = factory.CreateClient();

    [Theory]
    [InlineData("/health")]
    [InlineData("/ready")]
    [InlineData("/swagger/v1/swagger.json")]
    public async Task Sondas_y_swagger_responden(string path) =>
        (await _client.GetAsync(new Uri(path, UriKind.Relative))).StatusCode.Should().Be(HttpStatusCode.OK);

    [Theory]
    [InlineData(typeof(ILog))]
    [InlineData(typeof(ITopicCatalog))]
    [InlineData(typeof(IQueueEngine))]
    [InlineData(typeof(IReplicator))]
    [InlineData(typeof(ILeaderPromotion))]
    [InlineData(typeof(IClusterState))]
    public void Cada_contrato_tiene_una_implementacion_registrada(Type contract) =>
        factory.Services.GetService(contract).Should().NotBeNull();
}
