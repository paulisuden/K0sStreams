# Documentación de K0sStreams

Un solo punto de entrada. Los documentos se separan por **intención**, no por
fecha: qué quisimos construir, cómo está diseñado hoy, qué decidimos y qué quedó
en el camino.

## Orden de lectura

1. [Visión y alcance](vision.md) — problema, visión original, principios,
   evolución, límites y dirección.
2. [Arquitectura](ARQUITECTURA.md) — diseño vigente, componentes, flujos,
   garantías y estado de implementación.
3. [Decisiones](decisions.md) — registro de decisiones (ADR), con estado y
   consecuencias.
4. [Replicación](replication.md) y [Coordinación](coordination.md) — detalles de
   los dos mecanismos que sostienen la alta disponibilidad.
5. [Operación](topic-crd.md) — tópicos vía CRD y despliegue.
6. [Pruebas](chaos.md) y [benchmark](benchmark.md) — qué se verificó y qué se midió.
7. [Historia](history/): planes, iteraciones y documentos de etapas anteriores.

## Mapa de documentos

| Documento | Tipo | Estado | Dueño |
| --- | --- | --- | --- |
| [vision.md](vision.md) | Visión | Vigente | Equipo |
| [ARQUITECTURA.md](ARQUITECTURA.md) | Diseño | Vigente | Equipo |
| [decisions.md](decisions.md) | ADR | Vigente | Equipo |
| [replication.md](replication.md) | Diseño detallado | Vigente | C |
| [coordination.md](coordination.md) | Diseño detallado | Vigente | D |
| [topic-crd.md](topic-crd.md) | Operación | Vigente | D |
| [chaos.md](chaos.md) | Pruebas | Vigente | D |
| [benchmark.md](benchmark.md) | Pruebas | Vigente | D |
| [history/](history/) | Archivo | Histórico | Equipo |

## Convenciones

- **Vigente vs histórico.** Lo que describe el sistema actual vive en la raíz de
  `docs/`. Lo que describe una etapa anterior vive en `history/` y no debe usarse
  como especificación.
- **Una decisión, una entrada.** Cada cambio de arquitectura se registra en
  [decisions.md](decisions.md) con contexto, decisión y consecuencias. Los
  estados posibles son: propuesta, aceptada, reemplazada e histórica.
- **El código manda.** Si un documento y el código discrepan, el código es la
  fuente de verdad; el documento se corrige o se mueve a `history/`.
- **Los límites se escriben.** Toda capacidad no implementada se declara en
  “Estado de implementación” de [ARQUITECTURA.md](ARQUITECTURA.md) y en
  “Alcance y límites” del [README](../README.md).

## Mapa de propiedad

| Bloque | Dueño | Rutas |
| --- | --- | --- |
| A — Almacenamiento | A | `src/K0sStreams.Storage`, `tests/K0sStreams.Storage.Tests` |
| B — Cola y API | B | `src/K0sStreams.Queue`, `src/K0sStreams.Broker`, sus tests |
| C — Replicación | C | `src/K0sStreams.Replication`, sus tests |
| D — Plataforma y coordinación | D | `src/K0sStreams.Coordination`, `deploy/`, `chaos/`, `bench/`, `.github/` |
