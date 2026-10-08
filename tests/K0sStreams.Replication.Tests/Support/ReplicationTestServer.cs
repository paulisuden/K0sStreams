using System.Net;
using Grpc.Net.Client;
using K0sStreams.Contracts;
using K0sStreams.Contracts.Fakes;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using GrpcReplication = K0sStreams.Contracts.Grpc.Replication;

namespace K0sStreams.Replication.Tests.Support;

/// <summary>
/// A broker that only runs the replication block, served by Kestrel over real HTTP/2 on a free loopback port,
/// registered and mapped exactly like in the real host. <see cref="Client"/> talks to it through gRPC.
/// </summary>
internal sealed class ReplicationTestServer : IAsyncDisposable
{
    private readonly WebApplication _app;
    private readonly GrpcChannel _channel;

    private ReplicationTestServer(WebApplication app, InMemoryLog log, ControllableClusterState cluster)
    {
        _app = app;
        Log = log;
        Cluster = cluster;
        _channel = GrpcReplicationPeer.CreateChannel(new Uri(app.Urls.Single()));
        Client = new GrpcReplicationPeer(new GrpcReplication.ReplicationClient(_channel), TimeSpan.FromSeconds(5), TimeProvider.System);
    }

    public InMemoryLog Log { get; }

    public ControllableClusterState Cluster { get; }

    public IReplicationPeer Client { get; }

    /// <summary>The broker's gRPC address, e.g. <c>http://127.0.0.1:41234</c>.</summary>
    public string Url => _app.Urls.Single();

    public int Port => new Uri(Url).Port;

    /// <summary>The broker's real <see cref="IReplicator"/>, as registered by <c>AddK0sReplication</c>.</summary>
    public IReplicator Replicator => _app.Services.GetRequiredService<IReplicator>();

    /// <param name="settings">Extra configuration, e.g. <c>Replication:Peers</c>.</param>
    /// <param name="port">0 for any free port; a fixed one to restart a broker at the same address.</param>
    public static async Task<ReplicationTestServer> StartAsync(
        string nodeId, long epoch = 1, bool isLeader = false, Dictionary<string, string?>? settings = null, int port = 0)
    {
        var log = new InMemoryLog();
        var cluster = new ControllableClusterState(nodeId, isLeader, epoch);

        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.Configuration.AddInMemoryCollection(settings ?? []);
        builder.WebHost.ConfigureKestrel(kestrel =>
            kestrel.Listen(IPAddress.Loopback, port, listen => listen.Protocols = HttpProtocols.Http2));
        builder.Services.AddSingleton<ILog>(log);
        builder.Services.AddSingleton<IClusterState>(cluster);
        builder.Services.AddK0sReplication(builder.Configuration);

        var app = builder.Build();
        app.MapK0sReplication();
        await app.StartAsync();
        return new ReplicationTestServer(app, log, cluster);
    }

    public async ValueTask DisposeAsync()
    {
        _channel.Dispose();
        await _app.DisposeAsync();
    }
}
