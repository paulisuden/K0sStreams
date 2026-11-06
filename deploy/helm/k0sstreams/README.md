# k0sstreams Helm Chart

A Helm chart for the [K0sStreams](https://github.com/paulisuden/K0sStreams) distributed message broker, designed to run on Kubernetes (k0s) as a Kafka replacement.

## Architecture

The chart deploys a 3-replica StatefulSet of K0sStreams brokers:

- **Headless Service** — stable DNS per pod (`k0sstreams-0`, `k0sstreams-1`, `k0sstreams-2`) used for inter-broker gRPC replication and leader address resolution
- **LoadBalancer Service** — single entry point for client HTTP traffic on port 9090
- **Topic CRD** — `topics.k0sstreams.io/v1`, installed from `crds/`
- **RBAC** — ServiceAccount + Roles for Lease-based leader election and for reading `Topic` objects
- **PVC** — one PersistentVolumeClaim per broker for the disk-backed log
- **Pod anti-affinity** — prefers brokers on different nodes

## Prerequisites

- Kubernetes 1.25+
- A default StorageClass for dynamic PVC provisioning
- MetalLB or equivalent for LoadBalancer (optional, for external client access)

## Quick start

```bash
# Install K0sStreams (this also installs the Topic CRD)
helm install k0sstreams ./deploy/helm/k0sstreams --namespace k0sstreams --create-namespace

# Check rollout
kubectl rollout status statefulset/k0sstreams -n k0sstreams

# Get the LoadBalancer URL
kubectl get service k0sstreams-lb -n k0sstreams

# Create a topic (the leader picks it up within ~5 s)
kubectl apply -n k0sstreams -f - <<'EOF'
apiVersion: k0sstreams.io/v1
kind: Topic
metadata:
  name: orders
spec:
  partitions: 3
EOF

kubectl get topics -n k0sstreams
```

In-cluster clients use `http://k0sstreams.k0sstreams.svc.cluster.local:9090`.
A follower answers writes with `307` to the leader's pod DNS name, so clients must follow
redirects; that name resolves only inside the cluster.

## Configuration

| Parameter | Description | Default |
|---|---|---|
| `replicaCount` | Number of broker pods | `3` |
| `image.repository` | Broker image repository | `ghcr.io/paulisuden/k0sstreams-broker` |
| `image.tag` | Broker image tag | `sha-<commit>` (updated by CI) |
| `broker.dataDir` | Data directory inside container | `/data` |
| `broker.httpPort` | HTTP API port | `9090` |
| `broker.grpcPort` | gRPC replication port | `9091` |
| `opentelemetry.otlpEndpoint` | OTLP endpoint for trace export (empty = no export) | `""` |
| `retention.maxAge` | Max message age (TTL) | `24:00:00` |
| `retention.maxBytes` | Max total size per topic | `1073741824` (1 GiB) |
| `retention.checkInterval` | Retention check interval | `00:05:00` |
| `coordination.leaseName` | Kubernetes Lease name for leader election | `k0sstreams-leader` |
| `coordination.leaseDuration` | Lease duration | `00:00:10` |
| `coordination.renewDeadline` | Lease renew deadline | `00:00:07` |
| `coordination.retryPeriod` | Lease retry period | `00:00:02` |
| `persistence.enabled` | Enable PVC for broker data | `true` |
| `persistence.size` | PVC size per broker | `1Gi` |
| `persistence.storageClass` | StorageClass name | `""` (default) |
| `imagePullSecrets` | Secrets to pull a private image | `[]` |

## Topics

Topics are `Topic` objects in the release namespace. The chart installs the CRD and the RBAC the
leader needs to list them; the leader reconciles every 5 seconds.

| Field | Rule | Default |
|---|---|---|
| `spec.partitions` | 1–64 | `1` |
| `spec.maxRetries` | ≥ 0 | `3` |
| `spec.retryBackoffSeconds` | ≥ 0 | `5` |

The name is the Kubernetes object name. `k0s.topics` is reserved. Editing or deleting a `Topic`
does not alter or delete an existing topic, and the partition count is permanent.

Clients can discover topics over HTTP:

```bash
curl 'http://k0sstreams.k0sstreams.svc.cluster.local:9090/v1/topics'
curl 'http://k0sstreams.k0sstreams.svc.cluster.local:9090/v1/topics/orders'
```

Helm installs CRDs but does not upgrade them. After changing the schema, apply it manually:

```bash
kubectl apply -f deploy/helm/k0sstreams/crds/topic-crd.yaml
```

The copy used by the raw manifests is `deploy/topic-crd.yaml`.

## Environment variables

All configuration is passed via environment variables using the `Broker__`, `Retention__`, `Replication__`, `Coordination__`, and `OpenTelemetry__` prefixes, matching the K0sStreams .NET configuration binding.

## OpenTelemetry

Set `opentelemetry.otlpEndpoint` to your OTLP collector (e.g. Jaeger) to export traces:

```yaml
opentelemetry:
  otlpEndpoint: "http://jaeger.observability.svc.cluster.local:4317"
```

When empty, traces are generated internally but not exported — useful for local development.

## CI/CD

Every push to `main` runs the tests, publishes
`ghcr.io/paulisuden/k0sstreams-broker:sha-<short>` (and `:latest`), and then commits the
new tag back into `values.yaml` and `Chart.yaml`. Pull and upgrade to take it:

```bash
git pull
helm upgrade k0sstreams ./deploy/helm/k0sstreams -n k0sstreams
```

The tag is immutable, so the pod template changes and the StatefulSet rolls. If the GHCR
package is private, create a pull secret and set `imagePullSecrets`; otherwise make the
package public once in GitHub → Packages.

## Observability

The chart does not include Jaeger, Prometheus, or Grafana. The stack (Prometheus,
Grafana dashboards, Jaeger) lives in the `cluster_k0s` step
`07_skywatch/observability`. Point `opentelemetry.otlpEndpoint` at Jaeger's OTLP gRPC
port (4317), e.g. `http://jaeger.observability.svc.cluster.local:4317`.
