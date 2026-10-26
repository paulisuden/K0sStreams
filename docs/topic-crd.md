# CRD Topic: crear tópicos con kubectl (bloque D)

Corresponde a la fase 5 de [ARQUITECTURA.md](ARQUITECTURA.md): "Crear tópicos con `kubectl apply -f topic.yaml`; un watcher en el líder lo convierte en tópico".

## 1. La idea

Una CustomResourceDefinition le enseña a Kubernetes un tipo de objeto nuevo. Kubernetes ya sabe qué es un `Pod` o un `Lease`; con `deploy/topic-crd.yaml` también sabe qué es un `Topic`: lo valida, lo guarda en etcd y lo muestra con `kubectl get topics`. Pero **no hace nada con él**. El que lo convierte en un tópico del broker es un controlador: `TopicReconciler`, que corre en cada broker y solo actúa en el líder.

Es otra puerta de entrada al mismo catálogo. `POST /v1/topics` y un `Topic` terminan en `ITopicCatalog.CreateAsync`, que escribe el tópico en `k0s.topics` y espera el quórum ([DEC-003](decisions.md)).

## 2. Por qué listar y no hacer watch

Cada 5 s el líder pide **todos** los `Topic` del namespace y crea los que el catálogo no tiene. Compara el estado completo, no reacciona a eventos sueltos. Por eso:

- Un líder nuevo no necesita saber qué se perdió durante el failover: en su primera pasada lista todo.
- No hay una conexión de watch que se corte y haya que retomar (`resourceVersion`, `410 Gone`).
- Crear un tópico que ya existe no hace nada, así que repetir la pasada no tiene costo.

El precio es que un `Topic` nuevo tarda hasta 5 s en aparecer. Los controladores de Kubernetes funcionan igual por dentro: los eventos del watch solo adelantan una reconciliación que se hace igual cada tanto.

## 3. El objeto

```yaml
apiVersion: k0sstreams.io/v1
kind: Topic
metadata:
  name: pedidos            # el nombre del tópico
spec:
  partitions: 1            # 1–64; por defecto 1
  maxRetries: 3            # ≥ 0; por defecto 3
  retryBackoffSeconds: 5   # ≥ 0; por defecto 5
```

Los valores por defecto son los mismos que en `POST /v1/topics`. Kubernetes los completa al guardar, y un `Topic` sin `spec` también vale.

**Validación al hacer apply.** El esquema rechaza los rangos inválidos. Los nombres de Kubernetes ya son minúsculas, dígitos, `.` y `-`, y dos reglas CEL suman lo que pide `TopicConfig`: hasta 100 caracteres y que no sea `k0s.topics`. Si algo inválido llegara igual (un cluster sin CEL), el reconciliador lo saltea y lo deja en el log.

## 4. Lo que no hace

- **Editar un `Topic` no cambia el tópico**, y **borrarlo no borra el tópico.** El catálogo no tiene modificar ni borrar, y el plan no los pide.
- Si el tópico ya existía (creado por la API), el `Topic` no lo toca, aunque diga otra configuración.
- El `Topic` no tiene `status`: el resultado se ve leyendo el tópico (`GET /v1/topics/{nombre}/partitions/0/messages` responde 404 si no existe) o en los logs del líder (`Tópico ... creado desde la CRD Topic`). Escribir un `status` necesitaría permiso de escritura sobre `topics/status`.

## 5. Piezas

| Archivo | Qué es |
| --- | --- |
| `deploy/topic-crd.yaml` | La CRD: grupo `k0sstreams.io`, versión `v1`, esquema y columnas de `kubectl get topics` |
| `deploy/examples/topic.yaml` | Un `Topic` de ejemplo. Va en una subcarpeta porque `kubectl apply -f deploy/` no entra en subcarpetas: aplicado en la misma pasada que la CRD, fallaría con `no matches for kind "Topic"` |
| `deploy/rbac.yaml` | Role `broker-topic-reader`: solo `list` sobre `topics.k0sstreams.io` |
| `src/K0sStreams.Coordination/TopicReconciler.cs` | El controlador. Se registra junto con la elección por Lease, así que fuera de Kubernetes (compose, `dotnet run`) no corre |
| `tests/K0sStreams.Coordination.Tests/TopicReconcilerTests.cs` | Pasa por el cliente real de Kubernetes contra un API server falso (`FakeTopicApiServer`) |

## 6. Cómo probarlo

```bash
dotnet test tests/K0sStreams.Coordination.Tests

# Con el cluster de deploy/ andando (la CRD se aplica con el resto):
kubectl apply -f deploy/
kubectl apply -f deploy/examples/topic.yaml
kubectl get topics
kubectl logs <líder> | grep CRD
```

Para comprobar que sobrevive al failover: borrar al líder, aplicar otro `Topic` mientras se elige el nuevo y ver que aparece en cuanto el nuevo líder empieza a liderar.
