using System.Net;
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
}
