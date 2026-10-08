using System.Net;
using Grpc.Net.Client;
using K0sStreams.Contracts;
using K0sStreams.Contracts.Fakes;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
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

    public static async Task<ReplicationTestServer> StartAsync(string nodeId, long epoch = 1)
    {
        var log = new InMemoryLog();
        var cluster = new ControllableClusterState(nodeId, isLeader: false, epoch);

        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(kestrel =>
            kestrel.Listen(IPAddress.Loopback, 0, listen => listen.Protocols = HttpProtocols.Http2));
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
