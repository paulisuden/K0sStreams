# Visión y alcance

K0sStreams es un broker de mensajería distribuido escrito en .NET. Corre como tres
pods en Kubernetes y ofrece, sobre HTTP, dos formas de consumir el mismo log:
historial tipo Kafka y cola de trabajo tipo SQS.

Este documento define qué es el proyecto, qué garantiza y qué queda fuera. El
diseño vigente está en [ARQUITECTURA.md](ARQUITECTURA.md); las decisiones, en
[decisions.md](decisions.md).

## 1. Problema

El proyecto nace en la materia **Middlewares Distribuidos** (2026). El entregable
es un middleware propio, desplegado en un cluster k0s de tres nodos, con una
aplicación real consumiéndolo. La pregunta que guía el diseño es:

> ¿Se pueden ofrecer las dos formas de consumir un log que usan los sistemas
> reales —historial y cola de trabajo— sin depender de un broker existente?

## 2. Visión

Un solo log, dos contratos de lectura:

| Modo | Se parece a | Contrato |
| --- | --- | --- |
| Historial | Kafka | el consumidor lee desde el offset que elige y puede releer |
| Cola de trabajo | Amazon SQS | cada mensaje va a un consumidor, que lo confirma o lo devuelve; hay reintentos, programados y DLQ |

La infraestructura que sostiene ambos modos es parte del producto:

- log append-only con segmentos, índice, CRC32 y group commit;
- tres brokers con un líder electo por un Lease de Kubernetes;
- replicación por quórum (2 de 3) con high watermark, épocas y fencing;
- failover que no pierde mensajes confirmados;
- API HTTP para clientes y gRPC entre brokers;
- tópicos declarados con una CRD de Kubernetes.

### Prioridad de alcance

Cuando el tiempo aprieta, el orden de recorte es:

1. CRD Topic
2. Server-Sent Events
3. varias particiones por tópico
4. mensajes programados

Lo que **nunca** se recorta:

- replicación con quórum,
- failover con épocas,
- cola con ack y DLQ.

Esa prioridad define el sistema: la cola y la replicación son el producto; las
particiones son una capacidad opcional.

## 3. Principios de diseño

1. **El log es la fuente de verdad de los mensajes.** La API, la cola y la
   replicación son capas que leen o escriben ese log.
2. **Un solo escritor por término.** El líder ordena las escrituras; los
   seguidores replican. La corrección no depende de que varios brokers escriban.
3. **Lo durable es mínimo.** Mensajes, cursor confirmado de cada cola y marcas de
   DLQ. La entrega en proceso, la visibilidad y los reintentos son efímeros.
4. **At-least-once.** Si el líder cambia, lo no confirmado se reentrega. No hay
   exactly-once ni transacciones.
5. **La cola es un contrato de entrega, no un segundo log.** El log no guarda
   cada ack; el motor de cola materializa el cursor y las leases.
6. **La complejidad se paga solo si el alcance la pide.** Las particiones y los
   grupos de consumidores están en el contrato, pero no se implementan como
   mecanismo de escala.

## 4. Alcance actual

El sistema es un **broker de un solo escritor con alta disponibilidad y cola de
trabajo**:

- un log por tópico; `partitions` sigue en el contrato pero se usa la partición 0;
- un líder para todos los tópicos; la replicación da HA, no escala de escritura;
- cola sin grupos de consumidores: los consumidores compiten por el próximo
  mensaje;
- at-least-once; sin transacciones, compactación ni exactly-once;
- retención por tiempo o tamaño.

La tabla de implementado / no implementado está en
[ARQUITECTURA.md](ARQUITECTURA.md#estado-de-implementación).

## 5. Límites

- **Un solo escritor** para todos los tópicos: la replicación da HA, no throughput.
- **Un log por tópico**: las particiones no distribuyen escritura ni lectura entre
  brokers.
- **At-least-once**: un failover puede reentregar mensajes ya confirmados entre el
  ack y el volcado del cursor (~1 s).
- **Auto-commit perezoso**: si un consumidor confirma y deja de hacer `receive`,
  el último cursor puede no quedar persistido.
- **Rendimiento atado al disco**: en los volúmenes Cinder del cluster, cada
  escritura confirmada por quórum paga un fsync; en disco local el broker supera
  las 500 publicaciones por segundo.

## 6. Dirección

En orden de valor, si el proyecto continúa:

1. **Un líder por partición**, con una Lease por partición y asignación entre los
   brokers vivos. Es lo que convierte las particiones en escala de escritura.
2. **Grupos de consumidores** con asignación de particiones, generaciones y
   rebalance. Es lo que convierte la cola en consumidores paralelos.
3. **Retención consciente del cursor**: no borrar segmentos con mensajes no
   consumidos.
4. **Compactación** para tópicos usados como tabla de estado.
5. **Clientes** para otros lenguajes, generados desde OpenAPI.

## 7. Fuera de alcance

Transacciones y exactly-once, idempotencia de productor, compactación,
cooperación de rebalance, seguridad/ACL, Kafka Connect, Kafka Streams, schema
registry y tiered storage.

## 8. Lectura

- [README de docs](README.md): índice y orden de lectura.
- [ARQUITECTURA.md](ARQUITECTURA.md): diseño vigente.
- [decisions.md](decisions.md): decisiones y su estado.
- [history/](history/): planes y documentos de etapas anteriores. No son
  especificación.
