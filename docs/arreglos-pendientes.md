# Arreglos pendientes, por bloque

Lo que falta corregir en cada bloque, con el archivo y la línea exactos y el cambio concreto, para que cada uno pueda arreglar lo suyo sin tener que volver a investigar.

- **Revisado el:** 9 de noviembre de 2026, sobre `main` en `ceb7977`.
- **Cómo salió:** de arreglar la retención de segmentos (rama `fix/storage-retention`) y revisar de ahí para afuera. Todo lo que está acá se verificó leyendo el código; lo que no se pudo verificar está marcado como tal.
- **Quién arregla qué:** según la tabla de dueños de [CLAUDE.md](../CLAUDE.md#regla-1--cada-uno-toca-solo-lo-suyo). Nadie toca el bloque de otro: si algo de acá te parece mal, se discute en el grupo.

## Resumen

| Bloque | Qué | Gravedad | Esfuerzo |
| --- | --- | --- | --- |
| **D** | `/health` ejecuta la comprobación de `ready`, así que la sonda de liveness reinicia brokers durante cada elección | Alta | Ya existe el arreglo en otra rama |
| **B** | `POST .../offsets` responde 200 sin hacer nada: el cliente cree que confirmó y nunca avanza el cursor | Alta | ~15 min |
| **C** | La replicación no sabe saltar al inicio del log: un seguidor atrasado no vuelve nunca | Alta | Necesita acuerdo de equipo (`Contracts`) |
| **D** | `retention.maxAge` son 24 días y `maxBytes` es del tamaño del disco entero | Media | 3 valores |
| **C** | `HasRecordAsync` valida el epoch pero no el offset del registro que le devuelve el log | Media | 1 línea |
| **D** | La retención está apagada en `deploy/broker.yaml` y en `compose.yaml` | Media | 6 líneas |
| **D** | `appsettings.k8s.json` no se carga nunca | Baja | 1 línea |
| **B** | Los cuatro endpoints de compatibilidad no tienen ni un test | Baja | ~30 min |
| **D** | `compose.yaml` define dos claves de configuración que no existen | Baja | 2 líneas |
| **B** | Sufijo `Async` en cuatro métodos que no son asíncronos | Baja | 4 líneas |
| Compartido | `docs/contexto.md` describe el diseño anterior a DEC-001 y DEC-004 | Media | ~20 min |
| Compartido | Identificador en castellano en `Contracts` | Baja | Requiere acuerdo |
| Compartido | Qué se hace con la rama `archive/after-545a806` | — | Decisión de equipo |

---

## Bloque A — Almacenamiento

**Sin pendientes.** Queda el registro de lo que se corrigió, para que nadie lo vuelva a hacer.

| | Qué estaba mal | Cómo quedó |
| --- | --- | --- |
| A1 | La retención recorría solo los tópicos que alguien había pedido desde que arrancó el proceso, así que un tópico grande que nadie volvió a tocar después de un reinicio llenaba el disco igual | La pasada de fondo abre primero los tópicos que están en disco (`OpenTopicsOnDisk`). Un log dañado o una carpeta con nombre inválido se saltea con un aviso, sin cortar la pasada |
| A2 | `k0s.topics` se salvaba de la retención por accidente: entra en un solo segmento y el segmento del high watermark nunca se borra | Se excluye de forma explícita, porque es metadato y no caduca. Dos tests lo fijan: el catálogo nunca se poda, y un tópico de un solo segmento tampoco |
| A3 | `TopicLog.Partition`, resto del alcance con particiones, no lo usaba nadie | Borrada, junto con el parámetro del constructor |
| A4 | El tamaño de segmento era una constante de 64 MiB, así que la retención solo se podía ejercitar escribiendo más de 128 MiB: no era demostrable en una demo ni en `chaos/` | Nueva clave `Storage:SegmentBytes`, con 64 MiB por defecto. **Funciona sin configurarla**, así que D no está obligado a ponerla en `deploy/`, pero existe si se la quiere bajar para una demo |

Y antes, en el PR #22: el borrado por edad ahora mira el registro más nuevo de cada segmento, el borrado por tamaño respeta `MaxBytes`, ninguno toca el segmento del high watermark ni el activo ni los posteriores, el archivo `log-start` con `fsync` permite sobrevivir reinicios y cortes a mitad de un borrado, leer por debajo del inicio devuelve desde el inicio, una lectura que choca con un borrado se replanifica, y la tarea de fondo salió del constructor a un `RetentionService` con `TimeProvider`. Ver DEC-005.

## Bloque B — Cola y API

### B1 · `POST .../offsets` responde 200 sin hacer nada · alta

**Dónde:** `src/K0sStreams.Broker/Api/ApiEndpoints.cs:290` (ruta en la línea 27).

```csharp
private static IResult CommitOffsetsCompatAsync(
    string topic, string queue, CommitOffsetsRequest request) =>
    Results.Ok(new { topic, queue, consumerId = request.ConsumerId });
```

Recibe los offsets que el cliente quiere confirmar, los descarta y responde **200 OK**. Un cliente que confirme su progreso por esta vía en lugar de hacer `ack` recibe confirmación de algo que no ocurrió: el cursor nunca avanza y los mensajes se le vuelven a entregar indefinidamente. Un 200 falso es peor que un 404.

Lo mismo, con menos consecuencias, en `JoinGroupCompatAsync` (línea 246) y `HeartbeatCompatAsync` (línea 268): devuelven siempre `Partitions: [0]` y `Generation: 1` fijos, así que un cliente que use `Generation` para detectar un rebalanceo nunca ve uno.

**Arreglo, según lo que necesite SkyWatch:**
- Si SkyWatch **usa** `/offsets`: que haga el ack de verdad sobre los offsets recibidos.
- Si **no lo usa**: responder `501 Not Implemented`, o `204` con un encabezado de advertencia, en lugar de un 200 que miente.

**Antes de elegir hay que confirmar si SkyWatch llama a ese endpoint.** Si lo llama, hoy está perdiendo acks en silencio y es urgente.

### B2 · Los endpoints de compatibilidad no tienen ni un test · baja

**Dónde:** cuatro rutas públicas en `ApiEndpoints.cs:24-27`; `tests/K0sStreams.Broker.Tests/` no cambió cuando se agregaron.

**Arreglo:** un test por endpoint. Alcanza con el caso feliz, el `ConsumerId` vacío y el tópico inexistente.

### B3 · Sufijo `Async` en métodos que no son asíncronos · baja

**Dónde:** `ApiEndpoints.cs:246, 268, 284, 290`. Los cuatro devuelven `IResult`, no `Task`, y no tienen `await`.

**Arreglo:** quitarles el sufijo. CLAUDE.md lo reserva para métodos asíncronos, con `CancellationToken ct = default` al final.

> **Nada que hacer, pero conviene saberlo.** La cola dejaba de entregar en silencio cuando su cursor quedaba por debajo del inicio del log (`QueueEngine.cs:110`: el lote volvía vacío, `lastScanned < scanFrom`, `break`, y `ReceiveAsync` devolvía `null` para siempre). Lo arregla el cambio de lectura de A en `fix/storage-retention`: ahora pedir un offset ya borrado devuelve desde el inicio del log, así que la cola salta el hueco y sigue. No hace falta tocar nada en B.

---

## Bloque C — Replicación

### C1 · La replicación no sabe saltar al inicio del log · alta

**Dónde:** `src/K0sStreams.Replication/Leader/PeerReplicator.cs:193` y `src/K0sStreams.Replication/Follower/AppendHandler.cs`.

No existe el concepto de "el log empieza en N". Si la retención del líder borró lo que un seguidor todavía necesita:

- El líder valida `if (record.Offset != prev) throw` y lanza excepción; el `catch` del bucle hace backoff y reintenta **para siempre**, con un error por ronda.
- El seguidor rechaza con `LogMismatch`, el líder retrocede `_next` (línea 116) y vuelve a pedir lo mismo. Bucle cerrado.

No se pierden datos: el high watermark solo avanza con quórum, así que todo lo podado está en 2 de 3 brokers. Pero ese tercer broker queda inservible hasta que le borren el disco, y eso rompe justamente la demo de "matamos un pod y vuelve".

**Riesgo hoy:** con los valores actuales del chart un seguidor tendría que atrasarse más de 1 GiB en un tópico, así que en la demo no debería pasar. Si se corrigen los valores de D (ver D2) el margen baja a 128 MiB.

**Arreglo:** probablemente un código de error nuevo en el proto ("ese offset ya no está, arrancá en N") y que el seguidor sepa saltar ahí. Es cambio de `Contracts` y del `.proto`, así que va en un PR aparte con acuerdo del equipo, como pide CLAUDE.md. Está registrado como límite conocido en DEC-005.

### C2 · `HasRecordAsync` no valida el offset · media

**Dónde:** `src/K0sStreams.Replication/Follower/AppendHandler.cs:134`.

```csharp
await foreach (var record in log.ReadAsync(topic, offset, 1, ct).ConfigureAwait(false))
{
    return record.Epoch == epoch;   // falta comparar record.Offset == offset
}
```

Compara el epoch pero no el offset del registro que le devolvió el log. Con un log podado, `ReadAsync` devuelve el primer registro disponible en lugar de nada, así que puede dar `true` por un registro que no es el que se preguntó. No corrompe nada porque el `Write` de A rechaza un offset que no corresponde, pero falla de una forma incomprensible.

**Arreglo:** `return record.Offset == offset && record.Epoch == epoch;`

---

## Bloque D — Plataforma y coordinación

### D1 · La sonda de liveness reinicia brokers durante cada elección · alta

**Dónde:** `src/K0sStreams.Broker/Program.cs:40-41` y `src/K0sStreams.Broker/ReadyHealthCheck.cs`.

```csharp
app.MapHealthChecks("/health");
app.MapHealthChecks("/ready");
```

Ninguno lleva un `Predicate`, así que **los dos ejecutan todas las comprobaciones registradas**, incluida `ready` (línea 29), que falla cuando `!cluster.IsLeader && cluster.LeaderAddress is null`. Las etiquetas (`ReadyTags`) no filtran nada sin un predicado. El comentario de la línea 37 describe el efecto sin notar el problema.

`/health` es la sonda de **liveness** (`deploy/helm/k0sstreams/values.yaml:131-135`, `periodSeconds: 10`, sin `initialDelaySeconds`, `failureThreshold` por defecto 3). Un broker sin líder conocido se reinicia a los ~30 segundos. Eso pasa justo cuando menos conviene:

- **Sin quórum:** un broker que no puede tomar el Lease porque los otros dos están caídos no está enfermo, simplemente no puede atender escrituras. Hoy entra en ciclo de reinicios mientras dure la partición. Es el escenario `no_quorum` de `chaos/run.sh`.
- **Arranque en frío:** los 30 segundos se cuentan desde que arranca el contenedor e incluyen el arranque de .NET y la toma del Lease. Con `LeaseDuration` de 10 s suele alcanzar, pero sin margen declarado.
- Liveness y readiness quedaron idénticas, así que tener las dos sondas no aporta nada.

CLAUDE.md advierte justamente esto: *"El arranque tiene que ser rápido y no esperar a otros brokers (las sondas lo matan)"*.

**Arreglo: ya está escrito.** El commit `fca02d6` *fix(broker): keep /health independent from /ready* lo corrige, pero quedó solo en la rama `origin/archive/after-545a806` y nunca llegó a `main`. Conviene recuperarlo de ahí antes de reescribirlo:

```bash
git show fca02d6 --stat
git cherry-pick fca02d6     # o leerlo y aplicarlo a mano
```

Vale la pena además declarar `initialDelaySeconds` y `failureThreshold` en las sondas, en lugar de depender de los valores por defecto.

> `Program.cs` y `ReadyHealthCheck.cs` están en el proyecto Broker. `Program.cs` figura como archivo compartido en CLAUDE.md, así que este cambio se avisa en el grupo antes.

### D2 · Los valores de retención no pueden funcionar · media

**Dónde:** `deploy/helm/k0sstreams/values.yaml:53-55` y `deploy/helm/k0sstreams/README.md:62-64`.

| Clave | Valor hoy | Qué significa en realidad | Propuesta |
| --- | --- | --- | --- |
| `maxAge` | `"24:00:00"` | **24 días.** .NET lee ese formato como `d:hh:mm`, y 24 no es una hora válida | `"1.00:00:00"` = 1 día |
| `maxBytes` | `"1073741824"` (1 GiB por tópico) | El PVC es de **1Gi**, así que el límite recién actuaría cuando el disco ya se llenó | `"134217728"` (128 MiB) |
| `checkInterval` | `"00:05:00"` | Entre pasadas un tópico crece sin control | `"00:01:00"` |

Cuidado con el formato de `TimeSpan`, que es la trampa de la que salió todo esto. El **punto** separa días de horas; los **dos puntos** separan horas de minutos:

| Se escribe | .NET entiende |
| --- | --- |
| `"1:00:00"` | 1 hora |
| `"24:00:00"` | 24 días |
| `"1.00:00:00"` | 1 día ✅ |
| `"1:00:00:00"` | 1 día |

La cuenta del disco: 128 MiB de tope más hasta un segmento de 64 MiB entre pasadas da un pico de ~192 MiB por tópico, así que en 1Gi entran cuatro tópicos con margen. **No conviene agrandar el PVC:** `volumeClaimTemplates` es inmutable en un StatefulSet (lo advierte el propio chart en `templates/statefulset.yaml:111`) y agrandarlo obliga a borrar los PVC. Y `maxBytes` tiene que quedar bastante por encima de los 64 MiB de un segmento, o la retención no borra nunca.

El README del chart documenta `24:00:00` como "24 horas": hay que actualizarlo con el valor y el formato nuevos.

### D3 · La retención está apagada en dos de los tres caminos de despliegue · media

| Cómo se despliega | Retención |
| --- | --- |
| `helm install` | Encendida (con los valores de D2) |
| `kubectl apply -f deploy/` | **Apagada:** `deploy/broker.yaml:90` no define ninguna variable `Retention__` |
| `docker compose up` | **Apagada:** `compose.yaml` tampoco |

Los dos últimos son los comandos que documenta CLAUDE.md para probar en local y en minikube, así que ahí el disco crece sin tope hasta llenar el PVC.

**Arreglo:** agregar las tres variables `Retention__MaxAge`, `Retention__MaxBytes` y `Retention__CheckInterval` en los dos archivos, con los mismos valores del chart.

### D4 · `appsettings.k8s.json` no se carga nunca · baja

**Dónde:** `src/K0sStreams.Broker/appsettings.k8s.json` y `src/K0sStreams.Broker/Program.cs:12`.

Ese archivo solo aplica si `ASPNETCORE_ENVIRONMENT=k8s`, y eso no lo define el chart, ni `deploy/broker.yaml`, ni `compose.yaml`, ni el `Dockerfile`. Es configuración muerta: lo que tiene adentro (incluido un `MaxAge` de `"24:00:00"`) no tiene efecto en ningún despliegue.

**Arreglo:** o definir `ASPNETCORE_ENVIRONMENT=k8s` en el chart y en `deploy/broker.yaml` para que el archivo sirva, o borrarlo y dejar la configuración solo en variables de entorno. Lo segundo es menos cosas que mantener; lo primero evita repetir los valores en dos manifiestos.

### D5 · `compose.yaml` define dos claves que no existen · baja

**Dónde:** `compose.yaml:19-20`.

```yaml
Coordination__LeaderId: broker-0
Coordination__LeaderAddress: http://broker-0:9090
```

`CoordinationOptions` tiene `LeaseName`, `LeaseDuration`, `RenewDeadline`, `RetryPeriod` y `LeaderAddressFormat`. `LeaderId` y `LeaderAddress` no existen, así que se ignoran en silencio.

**Arreglo:** confirmar con qué `IClusterState` corre el compose y después corregirlas o borrarlas. Si el compose depende de que `broker-0` sea el líder fijo, hoy eso no lo está logrando por esta vía.

---

## Compartido — requiere acuerdo del equipo

### S1 · `docs/contexto.md` describe el diseño anterior · media

Es el archivo que CLAUDE.md manda leer **antes de tocar código**, y volvió a quedar desactualizado:

- Línea 84: *"El estado de la cola se guarda en el mismo log que los mensajes (event sourcing). Receive, ack, nack y timeout son registros con `Type` = 1..4"*. DEC-004 lo reemplazó: `QueueEngine` solo escribe `OffsetCommit` y marcas de DLQ.
- Línea 94: *"Ack y nack reciben la partición, porque con más de una partición el offset solo no identifica al mensaje"*, después de DEC-001.

Pasó porque `cc2f173` le había agregado un aviso de "documento histórico" y `8df79e3` (*restore documentation to the main baseline*) lo trajo de vuelta sin ese aviso.

**Arreglo:** actualizar esas dos secciones, o volver a poner el aviso de que es un documento de una etapa anterior y apuntar al estado actual. Es archivo compartido: se avisa en el grupo.

### S2 · Identificador en castellano en `Contracts` · baja

**Dónde:** `src/K0sStreams.Contracts/QueueEvent.cs:28`, usado en `src/K0sStreams.Queue/QueueEngine.cs:371`.

```csharp
public static QueueEvent ConfirmarOffset(string queue, int partition, long offset) => ...
```

CLAUDE.md: *"Identificadores en inglés, como en los contratos. Comentarios, documentación y tests en castellano"*. Sería `CommitOffset`.

**Arreglo:** renombrar. Es un método público de un archivo compartido, así que va en el próximo lote de cambios de `Contracts`, no suelto.

### S3 · Qué se hace con la rama `archive/after-545a806` · decisión

`main` y esa rama se separaron el 26 de octubre en `545a806` y vivieron en paralelo unos diez días. Esa rama tiene trabajo que no está en `main`:

| Commit | Qué |
| --- | --- |
| `fca02d6` | El arreglo de `/health` (ver D1) |
| `c0d4a2b` | `fix(chart): keep volumeClaimTemplates stable across chart bumps` |
| `d18c611` | Métricas de Prometheus para el broker |
| `fddd9f4` | La CRD de tópicos con su RBAC en el chart |
| `4e56a77` | DEC-005 sobre el `port_range 0-0` del secgroup de OpenStack |

**Hay que decidir si la rama se descarta o si se traen cosas de ahí.** Si se descarta, se pierde todo eso.

Y hay un choque de numeración: esa rama usa DEC-004 para "drop of chaos suite" y DEC-005 para el secgroup de OpenStack, mientras `main` usa DEC-004 para el estado efímero de entrega y la rama `fix/storage-retention` toma DEC-005 para la retención. Si alguna vez se mergea la rama archivada, hay dos DEC-004 y dos DEC-005.

---

## Cómo verificar antes de abrir el PR

```bash
dotnet build          # un warning rompe la compilación
dotnet test           # la solución completa, no solo tu bloque
git diff --stat main  # que solo aparezcan archivos de tu bloque
```

Y si tocaste algo de `deploy/`, el `Dockerfile` o el chart:

```bash
docker build -t k0sstreams/broker:check .
helm template deploy/helm/k0sstreams > /dev/null
```

## Un patrón que vale la pena mirar

Las fallas B1, B2, C2 y las de A que ya se corrigieron comparten la misma causa: **código que borra, escribe o expone algo sin un test que lo fije**. El CI (`.github/workflows/ci.yml`) corre `dotnet build`, `dotnet test` y la imagen Docker; no podía detectar ninguna de ellas, porque lo que faltaba eran los tests. La retención entró con el mensaje "All 368 tests pass", que era literalmente cierto y no significaba nada.

Lo más barato que podemos hacer no es revisar más, es pedir **un test por cada cosa que borre, escriba o exponga una ruta nueva**.

## Qué no se verificó

En esta revisión no se ejecutó el cluster: ni `docker compose up`, ni `kubectl apply`, ni `chaos/run.sh`, ni `bench/run.sh`. Todo lo de arriba sale de leer el código y de los tests escritos para la retención. **D1 en particular merece verse en el cluster**: medir cuánto tarda realmente una elección en frío y confirmar si los 30 segundos de la sonda alcanzan.
