namespace K0sStreams.Broker.Api;

internal static class ApiEndpoints
{
    /// <summary>Endpoints REST de clientes (puerto 9090). Dueño: B — Cola y API.</summary>
    public static IEndpointRouteBuilder MapK0sApi(this IEndpointRouteBuilder endpoints)
    {
        // Mapear acá las rutas /v1/topics... de docs/ARQUITECTURA.md.
        return endpoints;
    }
}
