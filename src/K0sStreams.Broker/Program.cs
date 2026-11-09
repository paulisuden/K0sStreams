using K0sStreams.Broker;
using K0sStreams.Broker.Api;
using K0sStreams.Coordination;
using K0sStreams.Contracts;
using K0sStreams.Queue;
using K0sStreams.Replication;
using K0sStreams.Storage;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Diagnostics.HealthChecks;

var builder = WebApplication.CreateBuilder(args);

// Configuración: appsettings.json + appsettings.{Environment}.json (ej. appsettings.k8s.json si ASPNETCORE_ENVIRONMENT=k8s)
// + variables de entorno (prefijo Broker__, Retention__, Replication__, Coordination__)
// Cada bloque registra lo suyo en su proyecto (ServiceCollectionExtensions.cs); este archivo no debería cambiar.
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddK0sStorage(builder.Configuration);        // A
builder.Services.AddK0sQueue(builder.Configuration);          // B
builder.Services.AddK0sReplication(builder.Configuration);    // C
builder.Services.AddK0sCoordination(builder.Configuration);   // D
builder.Services.AddK0sMetrics();                             // /metrics, en formato Prometheus

builder.Services.AddProblemDetails();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddHealthChecks()
    // Liveness: el proceso responde. Si falla, Kubernetes reinicia el contenedor.
    .AddCheck("live", () => HealthCheckResult.Healthy("El broker está en ejecución."), tags: LiveTags)
    // Readiness: el broker puede atender tráfico. Un seguidor sin líder conocido no puede
    // recibir escrituras ni redirigirlas; un broker a mitad de una elección no sabe su rol.
    .AddCheck<ReadyHealthCheck>("ready", tags: ReadyTags);

var app = builder.Build();

app.UseExceptionHandler();
app.UseSwagger();
app.UseSwaggerUI();

// Cada endpoint corre solo su comprobación: /health el liveness, /ready el readiness. Si /health
// corriera también "ready", un broker que espera la elección fallaría el liveness, Kubernetes lo
// reiniciaría y nunca llegaría a liderar.
app.MapHealthChecks("/health", new HealthCheckOptions
{
    Predicate = check => check.Tags.Contains("live"),
});
app.MapHealthChecks("/ready", new HealthCheckOptions
{
    Predicate = check => check.Tags.Contains("ready"),
});
app.MapK0sMetrics();        // /metrics
app.MapK0sApi();            // B
app.MapK0sReplication();    // C

app.Run();

public partial class Program
{
    private static readonly string[] LiveTags = ["live"];
    private static readonly string[] ReadyTags = ["ready"];
}
