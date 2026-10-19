using K0sStreams.Broker.Api;
using K0sStreams.Contracts;
using K0sStreams.Contracts.Fakes;
using K0sStreams.Queue;

var builder = WebApplication.CreateBuilder(args);

// Punto único de integración: cada bloque reemplaza su fake por la implementación real acá.
string nodeId = builder.Configuration["Broker:NodeId"] ?? Environment.MachineName;
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<IClusterState>(new StaticClusterState(nodeId));   // D: Coordination
builder.Services.AddSingleton<ITopicCatalog, InMemoryTopicCatalog>();          // A: Storage
builder.Services.AddSingleton<ILog, InMemoryLog>();                            // A: Storage
builder.Services.AddSingleton<IReplicator, InstantReplicator>();               // C: Replication
builder.Services.AddK0sQueue(builder.Configuration);                           // B: Queue

builder.Services.AddProblemDetails();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddHealthChecks();

var app = builder.Build();

app.UseExceptionHandler();
app.UseSwagger();
app.UseSwaggerUI();

app.MapHealthChecks("/health");
app.MapHealthChecks("/ready");
app.MapK0sApi();            // B

app.Run();

public partial class Program;
