# Observabilidad de la demo: Prometheus + Grafana

El cluster de la cátedra no trae Prometheus ni Grafana, así que acá van los dos, mínimos y
pensados para la defensa: Prometheus descubre los pods por anotaciones y Grafana arranca con el
datasource y el tablero ya cargados.

| Archivo | Qué levanta |
| --- | --- |
| `prometheus.yaml` | ServiceAccount, ClusterRole de solo lectura, config de scrape, Deployment y Service `ClusterIP`. |
| `grafana.yaml` | Provisioning del datasource y del proveedor de tableros, Deployment y Service `LoadBalancer`. |
| `../grafana/k0sstreams-dashboard.json` | El tablero. Se carga en el ConfigMap `grafana-dashboards`. |

## Cómo se descubren los brokers

No hay `ServiceMonitor` ni `PodMonitor` (este cluster no tiene prometheus-operator). El job
`kubernetes-pods` scrapea **cualquier pod** que tenga estas anotaciones, que el broker ya trae en
`deploy/broker.yaml` y en `values.yaml` del chart:

```yaml
prometheus.io/scrape: "true"
prometheus.io/port: "9090"
prometheus.io/path: "/metrics"
```

Para que otra aplicación aparezca en Prometheus (por ejemplo SkyWatch) basta agregarle esas tres
anotaciones a sus pods, siempre que exponga `/metrics` en formato Prometheus.

## Instalación

Todas las rutas son relativas a la raíz del repo: hay que correr los comandos desde ahí.

```bash
cd /ruta/a/K0sStreams
export KUBECONFIG=~/.kube/k0s-catedra.yaml

# 1. Prometheus.
kubectl apply -f deploy/monitoring/prometheus.yaml

# 2. El tablero, desde el único JSON que hay en el repo (no hay copia dentro del YAML).
#    El mismo comando sirve para actualizarlo: Grafana lo relee solo, sin reiniciar el pod.
kubectl -n k0sstreams create configmap grafana-dashboards \
  --from-file=deploy/grafana/k0sstreams-dashboard.json \
  --dry-run=client -o yaml | kubectl apply -f -

# 3. Grafana.
kubectl apply -f deploy/monitoring/grafana.yaml

# 4. La URL que se abre en el navegador (MetalLB asigna la IP).
kubectl -n k0sstreams get svc grafana -o jsonpath='{.status.loadBalancer.ingress[0].ip}'
```

Grafana entra sin usuario (rol `Viewer`) y abre el tablero del broker como página de inicio. Para
editar, `admin` / `admin`.

## Comprobar que el scrape funciona

```bash
kubectl -n k0sstreams port-forward svc/prometheus 9090:9090
# http://localhost:9090/targets  → tienen que estar los 3 pods del broker en "UP"
```

Si un target aparece en `DOWN` con `connection refused`, el pod todavía no tiene la imagen con
`/metrics`: hay que publicar la imagen nueva y actualizar el `tag` del chart.

## Límites conocidos

- Prometheus guarda las series en un `emptyDir` con 6 horas de retención: si se reinicia el pod,
  se pierde el histórico. Para la demo alcanza y evita depender del CSI de Cinder, que en este
  cluster viene fallando.
- Grafana también usa `emptyDir`: lo que se edite en la pantalla se pierde al reiniciarse. Lo que
  manda es el JSON del repo.
- El `ClusterRole` es de lectura y alcanza a todo el cluster, porque el descubrimiento de pods de
  Prometheus necesita listar pods de cualquier namespace.
