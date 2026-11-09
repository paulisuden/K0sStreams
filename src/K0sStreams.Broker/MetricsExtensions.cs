using OpenTelemetry.Metrics;

namespace K0sStreams.Broker;

/// <summary>
/// Publica las métricas del broker en <c>/metrics</c>, en formato Prometheus, sobre el mismo puerto
/// que la API (9090). Cada bloque mide lo suyo con su propio Meter; acá se registran todos.
/// </summary>
internal static class MetricsExtensions
{
    /// <summary>
    /// Meter que ASP.NET Core ya trae: duración de cada pedido por ruta, método y código de
    /// respuesta. Alcanza para ver los 200, 307 y 503 de la API sin contadores propios.
    /// </summary>
    private const string AspNetCoreMeterName = "Microsoft.AspNetCore.Hosting";

    /// <summary>Registra el exportador Prometheus y los Meter de cada bloque.</summary>
    public static IServiceCollection AddK0sMetrics(this IServiceCollection services)
    {
        services.AddSingleton<ClusterMetrics>();
        services.AddOpenTelemetry().WithMetrics(metrics => metrics
            .AddMeter(ClusterMetrics.MeterName)
            .AddMeter(AspNetCoreMeterName)
            .AddPrometheusExporter());
        return services;
    }

    /// <summary>Mapea <c>/metrics</c> y deja creados los gauges del estado del cluster.</summary>
    public static WebApplication MapK0sMetrics(this WebApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);

        // Pedir el servicio es lo que crea los gauges observables: nadie más lo pide.
        app.Services.GetRequiredService<ClusterMetrics>();
        app.MapPrometheusScrapingEndpoint();
        return app;
    }
}
