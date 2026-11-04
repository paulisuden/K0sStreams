using K0sStreams.Broker;
using K0sStreams.Broker.Api;
using K0sStreams.Coordination;
using K0sStreams.Contracts;
using K0sStreams.Queue;
using K0sStreams.Replication;
using K0sStreams.Storage;
using Microsoft.Extensions.Diagnostics.HealthChecks;

var builder = WebApplication.CreateBuilder(args);

// Cada bloque registra lo suyo en su proyecto (ServiceCollectionExtensions.cs); este archivo no debería cambiar.
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddK0sStorage(builder.Configuration);        // A
builder.Services.AddK0sQueue(builder.Configuration);          // B
builder.Services.AddK0sReplication(builder.Configuration);    // C
builder.Services.AddK0sCoordination(builder.Configuration);   // D

builder.Services.AddProblemDetails();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddHealthChecks()
    // Liveness: el proceso responde. Si falla, Kubernetes reinicia el contenedor.
    .AddCheck("live", () => HealthCheckResult.Healthy("El broker está en ejecución."))
    // Readiness: el broker puede atender tráfico. Un seguidor sin líder conocido no puede
    // recibir escrituras ni redirigirlas; un broker a mitad de una elección no sabe su rol.
    .AddCheck<ReadyHealthCheck>("ready", tags: ReadyTags);

var app = builder.Build();

app.UseExceptionHandler();
app.UseSwagger();
app.UseSwaggerUI();

// /health y /ready ejecutan ambas comprobaciones. "live" siempre pasa si el proceso responde;
// "ready" falla si no hay líder conocido, así que ambos endpoints devuelven unhealthy y
// Kubernetes detiene el tráfico y reinicia el contenedor.
app.MapHealthChecks("/health");
app.MapHealthChecks("/ready");
app.MapK0sApi();            // B
app.MapK0sReplication();    // C

app.Run();

public partial class Program
{
    private static readonly string[] ReadyTags = ["ready"];
}
