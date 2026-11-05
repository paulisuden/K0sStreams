using K0sStreams.Contracts;
using Microsoft.Extensions.Configuration;
using OpenTelemetry.Exporter;
using OpenTelemetry.Instrumentation.AspNetCore;
using OpenTelemetry.Instrumentation.Http;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace K0sStreams.Broker;

/// <summary>Configura OpenTelemetry (trazas) para el broker K0sStreams. Dueño: D — Plataforma.</summary>
public static class OpenTelemetryExtensions
{
    /// <summary>Nombre de la fuente de actividad para trazas personalizadas del broker.</summary>
    public const string ActivitySourceName = "K0sStreams.Broker";

    /// <summary>Nombre de la sección de configuración para OpenTelemetry.</summary>
    public const string SectionName = "OpenTelemetry";

    /// <summary>
    /// Registra el proveedor de trazado de OpenTelemetry.
    /// Si no se configura <c>OpenTelemetry__OtlpEndpoint</c>, no se exporta nada: las trazas se
    /// generan pero no salen del proceso. En k8s el endpoint apunta a Jaeger (puerto 14250, OTLP gRPC).
    /// </summary>
    public static IServiceCollection AddK0sOpenTelemetry(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        string? otlpEndpoint = configuration[$"{SectionName}:OtlpEndpoint"];

        // El NodeId (nombre del pod) distingue las series de cada broker en Prometheus y en Jaeger.
        string nodeId = configuration["Broker:NodeId"] ?? "unknown";
        K0sMetrics.NodeId = nodeId;
        var resource = ResourceBuilder.CreateDefault()
            .AddService("k0sstreams-broker", serviceInstanceId: nodeId);

        services.AddOpenTelemetry()
            .WithTracing(tracing =>
            {
                tracing
                    .SetResourceBuilder(resource)
                    .AddAspNetCoreInstrumentation(options =>
                    {
                        // No registrar peticiones a los endpoints de salud ni al scrape: son ruido.
                        options.Filter = httpContext =>
                            !httpContext.Request.Path.StartsWithSegments("/health") &&
                            !httpContext.Request.Path.StartsWithSegments("/ready") &&
                            !httpContext.Request.Path.StartsWithSegments("/metrics");
                    })
                    .AddHttpClientInstrumentation()
                    .AddSource(ActivitySourceName);

                if (!string.IsNullOrEmpty(otlpEndpoint))
                {
                    tracing.AddOtlpExporter(o => o.Endpoint = new Uri(otlpEndpoint));
                }
            })
            .WithMetrics(metrics =>
            {
                metrics
                    .SetResourceBuilder(resource)
                    .AddMeter(K0sMetrics.MeterName)
                    .AddAspNetCoreInstrumentation()
                    .AddHttpClientInstrumentation()
                    .AddPrometheusExporter();
            });

        return services;
    }
}
