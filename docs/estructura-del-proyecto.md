# Estructura del proyecto, explicada desde cero

Pantallazo de qué existe hoy en el repositorio (actualizado al 5 Nov 2026, fase 5), para alguien que no conoce .NET ni C#.
Complementa a [ARQUITECTURA.md](ARQUITECTURA.md) (el diseño) y a [contexto.md](contexto.md) (el estado del código).

---

## 1. Primero, el vocabulario mínimo de .NET

Sin esto nada del repo se entiende:

| Palabra | Qué es, en criollo |
|---|---|
| **Proyecto** (`.csproj`) | Una carpeta de código que se compila en **una** pieza reutilizable (una "librería") o en **un** programa ejecutable. El `.csproj` es su receta: qué librerías externas usa y de qué otros proyectos depende. Es el equivalente a un `package.json` por carpeta. |
| **Solución** (`.slnx`) | La lista de todos los proyectos que forman el sistema. [K0sStreams.slnx](../K0sStreams.slnx) no tiene código: solo dice "estos 12 proyectos son parte de esto". |
| **Interfaz** | Un **contrato**: la lista de operaciones que algo promete ofrecer, *sin* decir cómo las hace. Por convención su nombre empieza con `I`. Ej.: [ILog.cs](../src/K0sStreams.Contracts/ILog.cs) dice "el log sabe agregar un registro, leer registros y decirte su último offset" — pero no hay ni una línea que escriba en disco ahí. |
| **Implementación** | La clase que efectivamente cumple ese contrato (la que sí escribe en disco). |
| **Fake** | Una implementación de juguete que cumple el contrato pero hace trampa (guarda todo en RAM en vez de en disco). Sirve para que el resto del equipo pueda programar y probar **antes** de que exista la pieza real. |
| **Record** | Una forma corta de declarar "un paquetito de datos" (como un `struct`/objeto plano). `Record`, `TopicConfig`, `Delivery` son eso. Ojo con la confusión: en este proyecto la palabra `Record` también es el nombre de *un mensaje del log*. |
| **Namespace** | El "apellido" de cada clase, para que no choquen nombres. `namespace K0sStreams.Contracts;` arriba de cada archivo. |

## 2. Por qué existe `Contracts` (la clave de todo)

El proyecto está repartido entre 4 personas (A, B, C, D). Si cada una esperara a que la otra termine, nadie arranca.

La solución: **primero se escribieron solo los contratos**, y todos los firmaron. Entonces:

- La persona que hace el **log en disco** (A) sabe exactamente qué debe cumplir: `ILog`.
- La persona que hace la **API y la cola** (B) programa contra `ILog` *como si ya existiera*, usando el fake `InMemoryLog`.
- Cuando A termina su versión real, B **no cambia nada** de su código: solo se intercambia la pieza.

[src/K0sStreams.Contracts/](../src/K0sStreams.Contracts/) es, entonces, el "idioma común". Contiene:

- **6 contratos**: `ILog` (el log), `IQueueEngine` (la cola), `IReplicator` (copiar a otros brokers), `ILeaderPromotion` (puesta al día al asumir como líder), `IClusterState` (quién es el líder), `ITopicCatalog` (qué tópicos existen).
- **Los tipos de datos compartidos**: [Record.cs](../src/K0sStreams.Contracts/Record.cs) (un mensaje), [RecordType.cs](../src/K0sStreams.Contracts/RecordType.cs), [TopicConfig.cs](../src/K0sStreams.Contracts/TopicConfig.cs).
- **Dos piezas de lógica real ya terminada**:
  - [RecordCodec.cs](../src/K0sStreams.Contracts/RecordCodec.cs) — convierte un mensaje a bytes y al revés. Es el formato exacto de los bytes en disco (largo, CRC para detectar corrupción, offset, época, clave, valor). Es el archivo más grande del repo, 154 líneas, y **está funcionando**.
  - [QueueEvent.cs](../src/K0sStreams.Contracts/QueueEvent.cs) — codifica un "recibí / confirmé / rechacé este mensaje" como si fuera otro mensaje más del log.
- [Api/Dtos.cs](../src/K0sStreams.Contracts/Api/Dtos.cs) — la forma de los JSON que entran y salen por HTTP (`DTO` = paquetito de datos de transporte).
- [Protos/replication.proto](../src/K0sStreams.Contracts/Protos/replication.proto) — el contrato de la comunicación **broker ↔ broker** (gRPC). Está escrito en otro lenguaje (Protobuf) y el compilador genera el C# solo.
- [Fakes/](../src/K0sStreams.Contracts/Fakes/) — las 4 versiones de juguete.

## 3. El mapa completo

```
K0sStreams/
├─ K0sStreams.slnx              ← lista de proyectos
├─ Directory.Build.props        ← reglas que aplican a TODOS los proyectos
├─ Directory.Packages.props     ← versiones de librerías externas, centralizadas
├─ global.json                  ← exige .NET SDK 10
├─ Dockerfile                   ← cómo empaquetar el broker en una imagen
├─ .github/workflows/ci.yml     ← compila + testea + arma la imagen en cada push
├─ src/                         ← el código
│  ├─ K0sStreams.Contracts/     ← el idioma común        ✅ HECHO
│  ├─ K0sStreams.Storage/       ← log en disco           ✅ HECHO
│  ├─ K0sStreams.Queue/         ← motor de cola          ✅ HECHO
│  ├─ K0sStreams.Replication/   ← gRPC + quórum          ✅ HECHO
│  ├─ K0sStreams.Coordination/  ← líder y épocas         ✅ HECHO
│  └─ K0sStreams.Broker/        ← el EJECUTABLE          ✅ HECHO
├─ tests/                       ← un proyecto de tests por cada proyecto de src/
├─ deploy/                      ← manifiestos de Kubernetes (StatefulSet, Services, RBAC, CRD, observabilidad)
├─ chaos/                       ← suite de caos
└─ bench/                       ← benchmark con k6
```

Los 5 primeros de `src/` son **librerías** (no se ejecutan solas). El único programa que se ejecuta es `K0sStreams.Broker`, y su trabajo es *juntar las piezas*.

## 4. Cada bloque se enchufa con una línea

Cada proyecto tiene un `ServiceCollectionExtensions.cs` que registra sus implementaciones en el contenedor de dependencias. Ya no son fakes: [Storage](../src/K0sStreams.Storage/ServiceCollectionExtensions.cs) registra `SegmentedLog` como `ILog`, [Queue](../src/K0sStreams.Queue/ServiceCollectionExtensions.cs) registra `QueueEngine` como `IQueueEngine`, y así cada bloque.

```csharp
services.AddSingleton<ILog, SegmentedLog>();          // A
services.AddSingleton<IQueueEngine, QueueEngine>();   // B
```

Se lee así: *"cuando alguien pida un `ILog`, dale un `SegmentedLog`, y que sea siempre el mismo objeto (`Singleton`)"*.

Eso es **inyección de dependencias**: hay un registro central de "qué pieza concreta cumple cada contrato". El resto del código nunca dice `new SegmentedLog()`; solo pide "necesito un `ILog`" y recibe lo que esté registrado. `Program.cs` llama una vez a `AddK0sStorage`, `AddK0sQueue`, `AddK0sReplication` y `AddK0sCoordination`, y no necesita saber qué clase concreta hay detrás.

Los fakes de [Contracts/Fakes](../src/K0sStreams.Contracts/Fakes/) siguen existiendo para los tests y para correr sin Kubernetes (docker compose o `dotnet run`).

## 5. El ejecutable: `Broker`

[Program.cs](../src/K0sStreams.Broker/Program.cs) tiene 50 líneas y es el "main". Lo que hace:

1. Llama una vez al registro de cada bloque (`AddK0sStorage`, `AddK0sQueue`, `AddK0sReplication`, `AddK0sCoordination`).
2. Prende Swagger (la web que documenta y permite probar la API).
3. Publica `/health` y `/ready` (las dos URLs que Kubernetes consulta para saber si el pod está vivo).
4. Llama a `MapK0sApi()` y `MapK0sReplication()`.

El comentario del archivo dice que `Program.cs` no debería editarse más: cada bloque se registra solo.

[Api/ApiEndpoints.cs](../src/K0sStreams.Broker/Api/ApiEndpoints.cs) tiene las rutas REST: crear tópicos, publicar, leer historial por partición, y la cola con grupos de consumidores, ack/nack, offsets y DLQ. `MapK0sReplication()` expone el servicio gRPC del puerto 9091.

[appsettings.json](../src/K0sStreams.Broker/appsettings.json) es la configuración: abre el puerto **9090** para clientes (HTTP/1) y el **9091** para brokers entre sí (HTTP/2, que gRPC exige). [appsettings.k8s.json](../src/K0sStreams.Broker/appsettings.k8s.json) agrega la configuración del cluster (retención, réplicas, Lease y `/data`).

## 6. Los tests

Hay 6 proyectos de tests, uno por cada proyecto de `src/`. Todos tienen tests:

- `K0sStreams.Contracts.Tests`: formato de bytes, eventos de cola, particionador y validación de nombres de tópico.
- `K0sStreams.Storage.Tests`: segmentos, índice disperso, group commit, rotación, recuperación, corrupción, high watermark y catálogo de tópicos.
- `K0sStreams.Queue.Tests`: estados de la cola, timeouts, reintentos, DLQ y grupos de consumidores.
- `K0sStreams.Replication.Tests`: append/fetch/getState, quórum, fencing, failover y gRPC entre brokers.
- `K0sStreams.Coordination.Tests`: Lease, épocas, elección de líder y reconciliación de la CRD Topic.
- `K0sStreams.Broker.Tests`: arranque del host, rutas REST y redirección al líder.

La suite completa se corre con `dotnet test` y es la que valida el CI.

## 7. Las reglas del repo que te van a morder

Están en [Directory.Build.props](../Directory.Build.props) y son estrictas a propósito:

- **`TreatWarningsAsErrors`**: cualquier advertencia del compilador **rompe la compilación**. No hay "ya lo arreglo después".
- **Analizadores en `latest-recommended`**: además te marca cosas de estilo y de buenas prácticas, y eso también rompe el build.
- **`Nullable enable`**: tenés que declarar explícitamente qué variables pueden ser nulas (`string?` vs `string`). Si no lo hacés bien → warning → error.
- Las versiones de librerías van **solo** en `Directory.Packages.props`; en el `.csproj` se pone el nombre sin versión. Fijate en [K0sStreams.Storage.csproj](../src/K0sStreams.Storage/K0sStreams.Storage.csproj): tres líneas, ninguna con número de versión.

---

## Resumen en una frase

El broker está completo: log en disco con segmentos, índice, CRC, group commit y recuperación; cola con ack/nack, reintentos, DLQ y grupos de consumidores; API REST; replicación gRPC con quórum 2 de 3; elección de líder por Lease con épocas y fencing; manifiestos de Kubernetes, suite de caos y benchmark.

El README marca la fase 5 (caos, CRD Topic y benchmark) como implementada. Lo que queda es la demo: corregir la retención, adaptar el OpenTelemetry Astronomy Shop a la API de K0sStreams y desplegar la comparación con Kafka.
