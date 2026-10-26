using k8s;
using K0sStreams.Contracts;
using K0sStreams.Contracts.Fakes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace K0sStreams.Coordination;

public static class ServiceCollectionExtensions
{
    /// <summary>Registra el estado del cluster (Lease, épocas). Dueño: D — Plataforma y coordinación.</summary>
    public static IServiceCollection AddK0sCoordination(this IServiceCollection services, IConfiguration configuration)
    {
        string nodeId = configuration["Broker:NodeId"] ?? Environment.MachineName;
        string? leaderId = configuration["Coordination:LeaderId"];
        if (leaderId is null && KubernetesClientConfiguration.IsInCluster())
        {
            return services.AddLeaseElection(configuration, nodeId);
        }

        // Fuera de Kubernetes (docker compose, dotnet run): líder fijo por configuración, época 1.
        // Sin Coordination:LeaderId (un solo broker), este broker es el líder.
        bool isLeader = leaderId is null || leaderId == nodeId;
        string? leaderAddress = configuration["Coordination:LeaderAddress"];
        services.AddSingleton<IClusterState>(new StaticClusterState(nodeId, isLeader, leaderAddress: leaderAddress));
        return services;
    }

    /// <summary>Fase 4: el líder se elige con el Lease de Kubernetes. <see cref="ILeaderPromotion"/> lo registra la replicación.</summary>
    internal static IServiceCollection AddLeaseElection(this IServiceCollection services, IConfiguration configuration, string nodeId)
    {
        services.AddOptions<CoordinationOptions>()
            .Bind(configuration.GetSection(CoordinationOptions.SectionName))
            .Validate(
                static options => options.IsValid(),
                "Coordination: LeaseName no puede estar vacío y tiene que valer 0 < RetryPeriod < RenewDeadline < LeaseDuration.")
            .ValidateOnStart();

        services.TryAddSingleton(TimeProvider.System);

        // Dentro del pod, el token, el certificado y el namespace de la ServiceAccount los monta Kubernetes.
        services.TryAddSingleton(static _ => KubernetesClientConfiguration.InClusterConfig());
        services.TryAddSingleton<IKubernetes>(static sp => new Kubernetes(sp.GetRequiredService<KubernetesClientConfiguration>()));
        services.AddSingleton<ILeaseStore>(static sp => new KubernetesLeaseStore(
            sp.GetRequiredService<IKubernetes>(),
            PodNamespace(sp),
            sp.GetRequiredService<IOptions<CoordinationOptions>>(),
            sp.GetRequiredService<TimeProvider>()));

        // Una sola instancia, que se pide como IClusterState (el resto del broker) o como LeaseClusterState (el elector).
        services.AddSingleton(sp => new LeaseClusterState(
            nodeId,
            sp.GetRequiredService<IOptions<CoordinationOptions>>().Value.LeaderAddressFormat));
        services.AddSingleton<IClusterState>(static sp => sp.GetRequiredService<LeaseClusterState>());

        services.AddSingleton<LeaderElector>();
        services.AddHostedService(static sp => sp.GetRequiredService<LeaderElector>());

        // Fase 5: los tópicos declarados con la CRD Topic (deploy/topic-crd.yaml). El catálogo lo registra el almacenamiento.
        services.AddHostedService(static sp => new TopicReconciler(
            sp.GetRequiredService<IKubernetes>(),
            PodNamespace(sp),
            sp.GetRequiredService<ITopicCatalog>(),
            sp.GetRequiredService<IClusterState>(),
            sp.GetRequiredService<TimeProvider>(),
            sp.GetRequiredService<ILogger<TopicReconciler>>()));
        return services;
    }

    private static string PodNamespace(IServiceProvider sp) =>
        sp.GetRequiredService<KubernetesClientConfiguration>().Namespace
            ?? throw new InvalidOperationException("No se sabe en qué namespace corre el pod.");
}
