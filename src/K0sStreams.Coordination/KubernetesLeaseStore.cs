using System.Net;
using k8s;
using k8s.Autorest;
using k8s.Models;
using Microsoft.Extensions.Options;

namespace K0sStreams.Coordination;

/// <summary>
/// El Lease guardado en Kubernetes (<c>coordination.k8s.io/v1</c>). El compare-and-swap lo hace el API server: un
/// reemplazo lleva el <c>resourceVersion</c> leído y, si otro escribió desde entonces, responde 409 Conflict.
/// </summary>
internal sealed class KubernetesLeaseStore(
    IKubernetes client,
    string namespaceName,
    IOptions<CoordinationOptions> options,
    TimeProvider time) : ILeaseStore
{
    private string LeaseName => options.Value.LeaseName;

    public async Task<LeaseRecord?> GetAsync(CancellationToken ct = default)
    {
        try
        {
            V1Lease lease = await client.CoordinationV1
                .ReadNamespacedLeaseAsync(LeaseName, namespaceName, cancellationToken: ct)
                .ConfigureAwait(false);
            return ToRecord(lease);
        }
        catch (HttpOperationException ex) when (ex.Response.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public async Task<LeaseRecord?> TryCreateAsync(string holder, int transitions, CancellationToken ct = default)
    {
        try
        {
            V1Lease created = await client.CoordinationV1
                .CreateNamespacedLeaseAsync(NewLease(resourceVersion: null, holder, transitions), namespaceName, cancellationToken: ct)
                .ConfigureAwait(false);
            return ToRecord(created);
        }
        catch (HttpOperationException ex) when (ex.Response.StatusCode == HttpStatusCode.Conflict)
        {
            // Otro broker lo creó primero.
            return null;
        }
    }

    public async Task<LeaseRecord?> TryReplaceAsync(LeaseRecord expected, string? holder, int transitions, CancellationToken ct = default)
    {
        try
        {
            V1Lease replaced = await client.CoordinationV1
                .ReplaceNamespacedLeaseAsync(NewLease(expected.Version, holder, transitions), LeaseName, namespaceName, cancellationToken: ct)
                .ConfigureAwait(false);
            return ToRecord(replaced);
        }
        catch (HttpOperationException ex) when (ex.Response.StatusCode is HttpStatusCode.Conflict or HttpStatusCode.NotFound)
        {
            // 409: alguien escribió desde la versión leída. 404: alguien lo borró.
            return null;
        }
    }

    private V1Lease NewLease(string? resourceVersion, string? holder, int transitions) => new()
    {
        Metadata = new V1ObjectMeta { Name = LeaseName, ResourceVersion = resourceVersion },
        Spec = new V1LeaseSpec
        {
            HolderIdentity = holder,
            LeaseTransitions = transitions,
            LeaseDurationSeconds = (int)Math.Ceiling(options.Value.LeaseDuration.TotalSeconds),
            RenewTime = time.GetUtcNow().UtcDateTime,
        },
    };

    private static LeaseRecord ToRecord(V1Lease lease) =>
        new(lease.Spec?.HolderIdentity, lease.Spec?.LeaseTransitions ?? 0, lease.Metadata?.ResourceVersion ?? "");
}
