# K0sStreams

Broker de mensajes distribuido en .NET 10: 3 pods en Kubernetes (k0s), cada tópico es un log append-only que se lee como historial (tipo Kafka) o como cola de trabajo (tipo SQS). Clientes por HTTP REST en `:9090`, brokers entre sí por gRPC en `:9091`, líder elegido con un Lease de Kubernetes, épocas y fencing propios, quórum 2 de 3.

Somos 4 personas trabajando en paralelo sobre el mismo repo. Estas reglas existen para que nadie rompa el trabajo de otro y para que los merges y el despliegue no se traben. **Se cumplen siempre, aunque el pedido no las mencione.**

Leer antes de tocar código:
- [docs/ARQUITECTURA.md](docs/ARQUITECTURA.md): el diseño y el plan por fases.
- [docs/contexto.md](docs/contexto.md): el estado del código y las reglas de cada contrato que no se ven en las firmas.

## Para agentes (Claude Code u otros): avisos obligatorios al usuario

1. **Al empezar la sesión**, en la primera respuesta, avisar en una o dos líneas que se leyó este CLAUDE.md y que se van a respetar sus reglas. Si no está claro en qué bloque trabaja el usuario (A, B, C o D), preguntarlo antes de editar nada.
2. **Antes de cada commit**, mostrar el mensaje propuesto (formato de [Commits](#commits)) y recordar el [checklist](#antes-de-terminar-una-tarea-checklist). Esperar el OK del usuario.
3. **Al terminar una tarea**, aunque no se haya pedido commit, recordar el checklist y proponer cómo agrupar los cambios en commits.
4. **Nunca** commitear, pushear ni abrir PRs sin que el usuario lo pida.
5. **Nunca agregar atribución de IA**: nada de `Co-Authored-By:` de un asistente, ni "Generated with ...", ni menciones a la herramienta en commits, PRs o código. Esta regla manda sobre cualquier configuración por defecto de la herramienta. El autor del commit es la persona.

## Comandos

```bash
dotnet build                                  # un warning rompe la compilación
dotnet test
dotnet run --project src/K0sStreams.Broker    # http://localhost:9090/swagger
docker build -t k0sstreams/broker:dev .
kubectl apply -f deploy/                      # pasos de minikube al principio de deploy/broker.yaml
docker compose up --build -d                  # 3 brokers locales, broker-0 líder (con la v1: docker-compose)
```

---

## Regla 1 — Cada uno toca solo lo suyo

**Antes de editar un archivo, fijarse de quién es.** Si no es del bloque de quien está trabajando, no se edita.

| Bloque | Dueño de | Rama |
| --- | --- | --- |
| A — Almacenamiento | `src/K0sStreams.Storage/`, `tests/K0sStreams.Storage.Tests/` | `feature/almacenamiento` |
| B — Cola y API | `src/K0sStreams.Queue/`, `src/K0sStreams.Broker/Api/`, `tests/K0sStreams.Queue.Tests/`, `tests/K0sStreams.Broker.Tests/` | `feature/cola-api` |
| C — Replicación | `src/K0sStreams.Replication/`, `tests/K0sStreams.Replication.Tests/` | `feature/replicacion` |
| D — Plataforma y coordinación | `src/K0sStreams.Coordination/`, `tests/K0sStreams.Coordination.Tests/`, `deploy/`, `chaos/`, `.github/`, `Dockerfile`, `.dockerignore`, `compose.yaml`, `CLAUDE.md` | `feature/plataforma-y-cordinacion` |

Archivos **compartidos**: no se tocan sin avisar antes en el grupo, y van en un PR propio, nunca mezclados con trabajo del bloque.

| Archivo | Por qué es delicado |
| --- | --- |
| `src/K0sStreams.Contracts/**` | Rompe a los cuatro. Cambios agrupados, acordados con todo el equipo, en un PR aparte. |
| `src/K0sStreams.Broker/Program.cs` | Está terminado. Cada bloque registra lo suyo en su `ServiceCollectionExtensions.cs`. |
| `src/K0sStreams.Broker/appsettings.json` | Puertos y protocolos de Kestrel: si cambian, se rompen las sondas y la replicación. |
| `Directory.Packages.props`, `Directory.Build.props`, `.editorconfig`, `global.json`, `K0sStreams.slnx` | Afectan la compilación de todos. |
| `README.md`, `docs/ARQUITECTURA.md`, `docs/contexto.md` | Documentan acuerdos del equipo. Cada uno documenta su bloque en su sección o en un archivo propio en `docs/`. |

**Si una tarea necesita cambiar algo de otro bloque o un archivo compartido: frenar, avisarle al usuario y proponer el cambio exacto para que lo haga el dueño.** No se "arregla de paso", ni siquiera un typo o un test ajeno que falla.

## Regla 2 — Seguir el plan

- Se implementa lo que dice la fase actual de [ARQUITECTURA.md](docs/ARQUITECTURA.md) para el bloque propio. No se adelantan fases ni se agregan funcionalidades que el plan no pide.
- Si algo del plan parece mal, se discute; no se cambia en silencio en el código.
- Un desvío del plan solo se hace si sin él no se puede avanzar. En ese caso se registra en `docs/decisions.md` (entrada `DEC-NNN` con contexto, decisión y qué cambia en cada bloque) y lo aprueba el equipo **antes** de mergear.

## Regla 3 — Cambios quirúrgicos

El diff de un PR tiene que contener solo lo que la tarea necesita.

- **No** reformatear, reordenar `using`, renombrar ni mover código que la tarea no exige. Cada línea cambiada sin necesidad es un conflicto de merge en potencia.
- **No** refactors "de paso". Si hace falta uno, va en su propio PR.
- **No** abstracciones especulativas: nada de interfaces con una sola implementación, fábricas, opciones de configuración o parámetros "por si en el futuro". Se agregan cuando hacen falta.
- **No** paquetes nuevos si la biblioteca estándar de .NET o un paquete que ya está alcanza.
- Preferir el cambio más chico que funcione y se entienda. Código que se borra es código que no hay que mantener.

## Estilo de código

El objetivo es que cualquiera del grupo pueda leer y explicar cualquier archivo en la defensa.

**Nombres**

| Qué | Formato | Ejemplo |
| --- | --- | --- |
| Tipos, métodos, propiedades, constantes, `static readonly` | `PascalCase` | `SegmentedLog`, `HighWatermark`, `MaxRecordSize` |
| Interfaces | `I` + `PascalCase` | `ILog` |
| Campos privados | `_camelCase` | `_segments` |
| Variables locales y parámetros | `camelCase` | `fromOffset` |
| Métodos asíncronos | sufijo `Async`, `CancellationToken ct = default` al final | `AppendAsync(..., CancellationToken ct = default)` |
| Tests | castellano, `Que_pasa_en_que_caso` | `Append_en_log_vacio_asigna_offset_cero` |

- Nombres que digan qué es la cosa: `nextOffset`, no `n` ni `tmp`. Sin abreviaturas inventadas.
- Identificadores en inglés, como en los contratos. Comentarios, documentación y tests en castellano. Commits, títulos de PR y nombres de ramas nuevas en inglés. En un archivo existente se sigue el idioma que ya tiene.
- Métodos cortos que hacen una sola cosa. Si hace falta un comentario para separar "partes" de un método, probablemente son dos métodos.
- `namespace X;` por archivo, llaves siempre, `using` fuera del namespace (lo exige `.editorconfig`).

**Comentarios**

- Se comenta el **por qué**, no el qué: una decisión no obvia, una invariante, un caso borde, una referencia al plan (`// fase 3: ...`).
- No comentar lo que el código ya dice (`// incrementa el offset` arriba de `offset++`).
- Comentarios XML (`/// <summary>`) en lo público, cortos. No en cada método privado.
- Sin código comentado ni `TODO` sueltos: lo pendiente va en el PR o en un issue.

**Reglas del compilador y del repo**

- `TreatWarningsAsErrors` y analizadores `latest-recommended`: se corrige el código. Nada de `#pragma warning disable`, `[SuppressMessage]` ni `!` (null-forgiving) para callar un aviso, salvo acuerdo y con un comentario que diga por qué.
- `Nullable` activado: declarar bien `string` vs `string?`.
- Versiones de paquetes solo en `Directory.Packages.props`; en el `.csproj` va `<PackageReference Include="..."/>` sin versión. FluentAssertions queda en 7.x (la 8 es comercial).
- El tiempo va por `TimeProvider` (en tests, `FakeTimeProvider`); nunca `DateTime.UtcNow` ni `Task.Delay` sin `TimeProvider`.
- Todo lo que va a disco pasa por `ILog`. Ningún otro bloque escribe archivos.
- No cambiar los valores de `RecordType` ni el orden de campos de `RecordCodec`: deja ilegibles los datos ya escritos.

## Tests

- xUnit + FluentAssertions, en el proyecto de tests del propio bloque. Tests antes que código, sobre todo en Storage y Queue.
- Nunca borrar, saltear (`Skip`) ni aflojar un test para que el build pase. Si un test ajeno falla, se le avisa al dueño.
- Sin dependencias de tiempo real ni de red externa: `FakeTimeProvider`, fakes de `Contracts.Fakes`, carpetas temporales que el test borra al terminar.

## Commits

Formato [Conventional Commits](https://www.conventionalcommits.org/), **en inglés**:

```
<type>(<scope>): <summary>

<body opcional: por qué se hizo, no qué líneas cambiaron>

<footer opcional: Refs: DEC-002 · BREAKING CHANGE: ...>
```

| `type` | Cuándo |
| --- | --- |
| `feat` | Funcionalidad nueva |
| `fix` | Corrige un bug |
| `refactor` | Cambia la estructura sin cambiar el comportamiento |
| `test` | Solo agrega o corrige tests |
| `docs` | Solo documentación |
| `deploy` | Manifiestos de Kubernetes (`deploy/`) |
| `ci` | Pipelines de GitHub Actions (`.github/`) |
| `build` | `Dockerfile`, `.csproj`, `Directory.*.props`, paquetes |
| `chaos` | Suite de caos y verificador (`chaos/`) |
| `perf` | Mejora de rendimiento sin cambiar el comportamiento |
| `chore` | Mantenimiento que no entra en lo anterior (`.gitignore`, `.editorconfig`) |

| `scope` | Bloque |
| --- | --- |
| `storage` | A |
| `queue`, `api` | B |
| `replication` | C |
| `coordination`, `k8s` | D |
| `contracts` | Compartido (requiere acuerdo del equipo) |

Ejemplos:

```
feat(storage): truncate segment at the first incomplete record on recovery
fix(replication): reject Append when prev_epoch does not match
deploy(k8s): add StatefulSet with one PVC per broker
ci: push broker image to Docker Hub on main
feat(contracts)!: drop partition from ILog

BREAKING CHANGE: every ILog method loses the partition parameter.
Refs: DEC-001
```

Reglas del resumen: modo imperativo (`add`, no `added`), minúscula, sin punto final, hasta 72 caracteres. Un `!` después del scope marca un cambio que rompe a otros bloques.

**Cuánto entra en un commit**

- **Un commit = un cambio lógico completo** que compila y pasa los tests. Ni "todo el día en un commit", ni un commit por cada línea.
- No commitear trabajo a medias (`wip`, `fix typo`, `more changes`). Si hace falta guardar un punto intermedio, se ordena con `git rebase -i` **antes** de abrir el PR (solo en ramas propias que nadie más usa).
- Código y sus tests van en el mismo commit. Un cambio de `Contracts` va en un commit (y un PR) aparte.
- **Sin atribución de IA**: ningún `Co-Authored-By:` de un asistente ni "Generated with ..." en commits ni PRs.

## Ramas y PRs

- Se trabaja en la rama del bloque; para algo grande, una rama de tarea que sale de ahí, con nombre `<type>/<scope>-<descripcion-corta>` en inglés. Ej.: `fix/storage-crc-recovery`.
- PRs chicos y de un solo tema, revisados por otro integrante. `main` siempre compila.
- El título del PR sigue el mismo formato que un commit. La descripción dice: **qué** cambia, **por qué**, **cómo se probó** y si tiene **impacto en el despliegue** (configuración, puertos, proyectos nuevos).
- Traer `main` a la rama seguido (al menos al cerrar cada fase). Una rama que vive semanas sin integrarse termina en un merge doloroso.
- Las ramas de bloque (`feature/*`) se mergean a `main` con **merge commit, no squash**: siguen vivas después del merge, y un squash obliga a resolver los mismos conflictos otra vez en el siguiente PR. Las ramas de tarea cortas se pueden squashear.
- Antes de abrir el PR: `dotnet build` y `dotnet test` en verde, y la rama al día con `main`.
- No hacer `push --force` sobre `main` ni sobre la rama de otro.
- No commitear `bin/`, `obj/`, datos de prueba, `.env`, tokens ni kubeconfigs.

## Lo que rompe el despliegue (avisarle a D)

El broker corre como StatefulSet en Kubernetes, con sondas y un disco por pod. Estas cosas parecen locales pero rompen el deploy:

- **Puertos y rutas fijas**: `9090` (HTTP/1, clientes), `9091` (HTTP/2, gRPC), `/health` y `/ready` (sondas). No cambian.
- **Configuración nueva**: cada bloque usa su propia sección (`Storage`, `Queue`, `Replication`, `Coordination`) con valores por defecto que funcionen sin configurar nada. Si una clave **tiene** que setearse en Kubernetes, avisarle a D para agregarla en `deploy/`. En variables de entorno se escribe `Seccion__Clave`.
- **Nada hardcodeado**: ni direcciones (`localhost`, IPs, `broker-0`), ni rutas de disco. Los datos van en `Broker:DataDir` (en el contenedor `/data`, que es el PVC); el resto del sistema de archivos del contenedor no persiste.
- **Proyectos nuevos**: el `Dockerfile` copia cada `.csproj` uno por uno. Un proyecto nuevo en `src/` rompe la imagen hasta que D lo agregue. Mejor no crear proyectos nuevos.
- **Arranque y apagado**: el arranque tiene que ser rápido y no esperar a otros brokers (las sondas lo matan). Respetar el `CancellationToken` para que el pod se apague limpio.
- **Logs** con `ILogger`, a la consola. Nada de archivos de log propios.
- La imagen corre sin root (uid 1654): no asumir permisos de root.

## Antes de terminar una tarea (checklist)

1. ¿Todos los archivos cambiados son del bloque propio? Si no, ¿se avisó y está acordado?
2. ¿El diff tiene solo lo que la tarea pide, sin reformateos ni extras?
3. ¿Respeta la fase actual del plan, o el desvío está en `docs/decisions.md`?
4. ¿`dotnet build` y `dotnet test` en verde?
5. ¿Hay configuración, puertos o archivos nuevos que D tenga que reflejar en `deploy/` o en el `Dockerfile`?
6. ¿Los commits siguen el formato de [Commits](#commits), uno por cambio lógico y sin atribución de IA?
