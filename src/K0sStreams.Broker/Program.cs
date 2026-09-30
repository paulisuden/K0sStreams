using K0sStreams.Broker.Api;
using K0sStreams.Coordination;
using K0sStreams.Queue;
using K0sStreams.Replication;
using K0sStreams.Storage;

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
builder.Services.AddHealthChecks();

var app = builder.Build();

app.UseExceptionHandler();
app.UseSwagger();
app.UseSwaggerUI();

app.MapHealthChecks("/health");
app.MapHealthChecks("/ready");
app.MapK0sApi();            // B
app.MapK0sReplication();    // C

app.Run();

public partial class Program;
