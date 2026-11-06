# Contexto para implementar K0sStreams

Qué hay hoy en el repositorio, dónde trabaja cada integrante y qué tener en cuenta al programar cada bloque. Complementa a [ARQUITECTURA.md](ARQUITECTURA.md), que explica el diseño; este documento explica el código.

## 1. Qué está hecho (fase 0)

| Pieza | Dónde | Estado |
| --- | --- | --- |
| Solución y proyectos | `K0sStreams.slnx`, `src/`, `tests/` | 6 proyectos + 1 proyecto de tests por cada uno |
| Contratos | `src/K0sStreams.Contracts/` | Interfaces, formato de registro, eventos de cola, particionador, DTOs REST, `.proto` |
| Fakes en memoria | `src/K0sStreams.Contracts/Fakes/` | `InMemoryLog`, `InMemoryTopicCatalog`, `InstantReplicator`, `StaticClusterState` |
| Registro por bloque | `ServiceCollectionExtensions.cs` en cada proyecto | Hoy registran los fakes |
| Host del broker | `src/K0sStreams.Broker/Program.cs` | Solo `/health`, `/ready` y Swagger. Sin rutas REST ni gRPC todavía |
| Tests | `tests/` | 39 tests: formato de registro, eventos de cola, particionador, validación de tópicos, `InMemoryLog`, sondas del host y registro de servicios |
| Docker | `Dockerfile` | Multi-etapa, corre sin root, datos en `/data`. **Nunca se construyó localmente** |
| CI | `.github/workflows/ci.yml` | Build, tests e imagen Docker en cada push a `main` y en cada PR |

Lo que **no** está: el log en disco, el motor de cola, las rutas REST, el servidor gRPC, el Lease, los manifiestos de Kubernetes (`deploy/`), la suite de caos (`chaos/`) y el CLAUDE.md.

## 2. Cómo arrancar

```bash
dotnet build          # compila todo; un warning rompe la compilación
dotnet test           # corre los tests
dotnet run --project src/K0sStreams.Broker
# http://localhost:9090/swagger   http://localhost:9090/health
```

Requisitos: .NET SDK 10. `global.json` acepta cualquier 10.0.x a partir de 10.0.100.

## 3. Dónde trabaja cada uno

Cada bloque tiene su rama `feature/<bloque>` y sus propios archivos. `Program.cs` llama una vez al método de registro de cada bloque y **no debería editarse más**.

| Integrante | Rama | Proyecto | Registro que completa | Tests |
| --- | --- | --- | --- | --- |
| A — Almacenamiento | `feature/almacenamiento` | `K0sStreams.Storage` | `AddK0sStorage`: `ILog`, `ITopicCatalog` | `K0sStreams.Storage.Tests` |
| B — Cola y API | `feature/cola-api` | `K0sStreams.Queue`, `K0sStreams.Broker` | `AddK0sQueue`: `IQueueEngine`; `MapK0sApi` (en `Broker/Api/ApiEndpoints.cs`): rutas REST | `K0sStreams.Queue.Tests`, `K0sStreams.Broker.Tests` |
| C — Replicación | `feature/replicacion` | `K0sStreams.Replication` | `AddK0sReplication`: `IReplicator` + `AddGrpc()`; `MapK0sReplication`: servicio gRPC | `K0sStreams.Replication.Tests` |
| D — Plataforma | `feature/plataforma` | `K0sStreams.Coordination`, `deploy/`, `chaos/`, `.github/` | `AddK0sCoordination`: `IClusterState` | `K0sStreams.Coordination.Tests` |

Para reemplazar un fake, se cambia una línea en el `ServiceCollectionExtensions.cs` del propio bloque. Por ejemplo, A:

```csharp
services.AddSingleton<ILog, SegmentedLog>();   // antes: InMemoryLog
```

`tests/K0sStreams.Broker.Tests/HostTests.cs` verifica que cada contrato tenga una implementación registrada. Cuando B registre `IQueueEngine`, tiene que agregarlo a ese test.

### Qué fake usa cada uno mientras espera a los otros

- **A** no depende de nadie.
- **B** programa contra `InMemoryLog`, `InstantReplicator` y `StaticClusterState`. El motor de cola lo escribe B, así que no hay fake de `IQueueEngine`.
- **C** prueba la replicación contra dos o tres `InMemoryLog` dentro del mismo test.
- **D** usa `StaticClusterState` hasta tener el Lease.

## 4. Contratos: lo que cada implementación tiene que respetar

Los contratos están en `src/K0sStreams.Contracts/` y tienen comentarios XML. Estas son las reglas que no se ven a simple vista en las firmas.

### `ILog` (A lo implementa; B, C y D lo usan)

- Los offsets empiezan en **0** y son **contiguos** por tópico y partición. En un log vacío, `EndOffset` y `HighWatermark` valen **-1**.
- `AppendAsync` con `record.Offset == Record.Unassigned` (-1) asigna el offset: es el caso del líder. Con un offset explícito tiene que ser exactamente `EndOffset + 1`, que es el caso de la réplica; si no lo es, lanza `ArgumentException`.
- `AppendAsync` recién devuelve el offset cuando el registro está en disco (fsync). El group commit agrupa varios appends concurrentes en un solo fsync, pero cada llamada espera al suyo.
- `ReadAsync` lee hasta `EndOffset`, **no** corta en el high watermark. La API REST corta en el HW (los clientes nunca ven lo no confirmado); la replicación no corta, porque los seguidores necesitan lo no confirmado.
- `AdvanceHighWatermark` solo sube. Lanza excepción si se pasa de `EndOffset`. Lo llama la replicación, nunca la API.
- `TruncateAsync(toOffset)` borra los offsets **mayores** que `toOffset`. Lanza `InvalidOperationException` si con eso borraría algo por debajo del HW: lo confirmado nunca se borra.
- El log crea la partición la primera vez que se escribe en ella. Si nadie escribió, las lecturas devuelven vacío.
- Los mismos casos de `tests/K0sStreams.Contracts.Tests/Fakes/InMemoryLogTests.cs` deberían pasar contra el log real. Conviene copiarlos o parametrizarlos.

### Formato de registro (`RecordCodec`)

- Es el mismo formato **en disco y en gRPC**: la réplica manda los bytes tal cual, sin volver a serializar.
- Es little-endian, con CRC32 (`System.IO.Hashing`) calculado sobre todo lo que sigue al campo del CRC. El detalle está en el comentario de la clase.
- `TryDecode` distingue dos fallas:
  - `Incomplete`: faltan bytes. Es lo normal si el proceso murió a mitad de una escritura. En la recuperación, A trunca el segmento en ese punto y sigue.
  - `Corrupt`: el CRC o los largos no cierran. Es daño real: hay que registrarlo en el log y no seguir leyendo ese segmento.
- Un registro ocupa como máximo 16 MiB (`RecordCodec.MaxRecordSize`).
- **No cambiar** los valores numéricos de `RecordType` ni el orden de los campos: dejaría ilegibles los datos ya escritos.

### Eventos de cola (`QueueEvent`) — B

- El estado de la cola se guarda en el **mismo log** que los mensajes (event sourcing). Receive, ack, nack y timeout son registros con `Type` = 1..4.
- Formato: `Key` es el nombre de la cola; `Value` es `partition | targetOffset | untilMs | reason`. `QueueEvent.ToRecord` y `QueueEvent.FromRecord` lo codifican.
- Consecuencia: en el log hay mensajes y eventos mezclados. La lectura como historial tiene que **filtrar `Type == Message`**, y los offsets de mensajes no son consecutivos para el cliente.
- Cada evento se escribe con `AppendAsync` y se confirma con `WaitForQuorumAsync`, igual que un mensaje. Si no, un failover pierde acks.
- `Apply(topic, record)` reconstruye el estado leyendo los eventos. Lo usa el seguidor que pasa a ser líder. Tiene que dar el mismo estado que tenía el líder anterior.
- El tiempo va por `TimeProvider`, que está registrado en el host. En los tests se usa `FakeTimeProvider` (paquete `Microsoft.Extensions.TimeProvider.Testing`, que hay que agregar a `Directory.Packages.props`). No usar `DateTime.UtcNow` directamente.

### `IQueueEngine` — B

- `maxRetries` cuenta los reintentos **después** del primer intento, así que un mensaje se entrega como máximo `maxRetries + 1` veces antes de ir a la DLQ.
- Ack y nack reciben la **partición**, porque con más de una partición el offset solo no identifica al mensaje. En REST va como `?partition=` y vale 0 por defecto.
- Devuelven `false` si el mensaje no estaba en proceso en esa cola. La API responde 404 en ese caso.
- Cada cola es un grupo de consumidores independiente: dos colas del mismo tópico reciben cada una todos los mensajes.
- La cola solo entrega mensajes hasta el high watermark.

### `IReplicator` — C

- `WaitForQuorumAsync` espera 2 de 3 copias en disco (la del líder cuenta) y después llama a `ILog.AdvanceHighWatermark`.
- Si el broker deja de ser líder mientras espera, lanza `NotLeaderException`. La API la traduce a un 307 hacia el nuevo líder o a un 503.
- `InstantReplicator` es el fake de un solo nodo: sube el HW al instante.

### gRPC (`Protos/replication.proto`) — C

- El código C# se genera solo al compilar `Contracts`, en el namespace `K0sStreams.Contracts.Grpc`. Se genera tanto el cliente como la base del servidor.
- Respecto del documento original, `AppendRequest` tiene tres campos nuevos:
  - `prev_epoch`: el seguidor compara `prev_offset` y `prev_epoch` con su log. Si no coinciden, responde `APPEND_ERROR_LOG_MISMATCH`, y el líder retrocede o el seguidor trunca (como en Raft).
  - `leader_id`: identifica al líder que manda la réplica.
  - `code`: código de error (enum `AppendError`). No hace falta interpretar el texto de `error`.
- Fencing: si `request.epoch` es menor que la época local, se responde `APPEND_ERROR_STALE_EPOCH` junto con la época local. El líder que recibe una época mayor deja de ser líder.
- El servicio gRPC corre en el puerto **9091** y solo con HTTP/2 (configurado en `appsettings.json`).

### `IClusterState` — D

- `NodeId` sale de la configuración `Broker:NodeId` y, si no está, del nombre de la máquina. En el StatefulSet es el nombre del pod (`broker-0`).
- `LeaderAddress` es la URL HTTP base del líder, por ejemplo `http://broker-0.broker:9090`. La API la usa para el redirect 307.
- `EpochChanged` avisa a A, B y C cuando cambia la época: truncar el log, reconstruir la cola y rechazar réplicas viejas.

### `ITopicCatalog` y `Partitioner`

- Los nombres de tópico y de cola se validan con `TopicConfig.IsValidName`: minúsculas, dígitos, `.`, `-` y `_`, hasta 100 caracteres. El nombre se usa como carpeta en disco, así que esta validación además evita rutas como `../`.
- `Partitioner.ForKey`: CRC32 de la clave módulo la cantidad de particiones. Sin clave, va a la partición 0. Hay tests con valores fijos: si el algoritmo cambia, los mensajes de una misma clave terminan en otra partición.

## 5. Reglas del repositorio que afectan al código

- **Los warnings son errores** (`TreatWarningsAsErrors`) y los analizadores están en `latest-recommended`. Si algo no compila por un aviso CAxxxx, se corrige el código; no se desactiva la regla sin acordarlo.
- **Versiones de paquetes centralizadas**: para agregar un paquete se pone `<PackageVersion>` en `Directory.Packages.props` y `<PackageReference Include="..."/>` **sin versión** en el `.csproj`. Es un archivo compartido: avisar en el grupo para no chocar.
- **FluentAssertions queda en 7.x**. La 8 cambió a licencia comercial.
- **Nombres de tests**: `Que_pasa_en_que_caso`, en castellano, como los existentes.
- **Campos privados** con `_camelCase`; namespaces por archivo (`namespace X;`). Lo marca `.editorconfig`.
- **Todo en disco pasa por `ILog`.** Ningún otro bloque escribe archivos por su cuenta.
- Rutas y configuración de ejemplo: `Broker:DataDir` (en Docker es `/data`) y `Broker:NodeId`.

## 6. Flujo de trabajo con git

- Cada uno trabaja en su `feature/<bloque>`. Si una tarea es grande, conviene sacar una rama de tarea desde ahí.
- **Mergear a `main` seguido**, al menos al cerrar cada fase y con PR revisado por otro integrante. Una rama que vive 6 semanas sin integrarse termina en un merge doloroso.
- Antes de abrir un PR: `dotnet build` y `dotnet test` en verde, y la rama actualizada con `main`.
- **Cambios en `Contracts`**: PR aparte, discutido con todo el equipo y agrupado (idealmente una vez por semana). Rompe a los cuatro.
- Se recomienda configurar en GitHub la protección de `main` (PR obligatorio y CI en verde) y un `CODEOWNERS` para `src/K0sStreams.Contracts/`. Hoy no está configurado.

## 7. Temas abiertos que conviene resolver pronto

1. **¿Quién persiste los tópicos?** `ITopicCatalog` quedó registrado en Storage (A), pero el documento no lo asigna. Además, crear un tópico tiene que replicarse. Una opción coherente con "el log es la única fuente de verdad" es un tópico interno de metadatos.
2. **Compactación del log.** No está definido cuándo se borran segmentos viejos ni qué pasa con los eventos de cola ya confirmados. Sin compactación, `Apply` tarda cada vez más al reconstruir la cola.
3. **Lecturas en un líder aislado.** El fencing por época frena las escrituras de un líder viejo, pero no sus lecturas. Conviene que un líder deje de atender cuando pasó un tiempo sin poder renovar el Lease.
4. **Lecturas en seguidores.** Pueden estar un poco atrasadas respecto del líder: hay que decidir si se permiten y documentarlo.
5. **HW en los seguidores.** El seguidor recibe `leader_hw` en cada `Append` y tiene que guardarlo, para servir lecturas correctas y para cuando pase a ser líder.
6. **Pendientes de la fase 0**: escribir el CLAUDE.md (D), confirmar que el CI pasa en GitHub y construir la imagen Docker al menos una vez (`docker build -t k0sstreams/broker .`).
