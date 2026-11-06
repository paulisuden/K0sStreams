# Visión y alcance

> Documento de intención. Describe qué quisimos construir, cómo evolucionó y hacia
> dónde puede crecer. El diseño vigente está en [ARQUITECTURA.md](ARQUITECTURA.md).

## 1. Problema

Materia **Middlewares Distribuidos** (2026). El entregable es un middleware de
mensajería propio, corriendo en un cluster k0s de tres nodos, con una aplicación
real consumiéndolo. No es un ejercicio de laboratorio: tiene que desplegarse,
fallar y recuperarse.

La pregunta de diseño fue:

> ¿Podemos ofrecer, sobre HTTP, las dos formas de consumir un log que usan los
> sistemas reales —historial tipo Kafka y cola de trabajo tipo SQS— sin depender
> de un broker existente?

## 2. Visión original

Desde la fase 0 el proyecto se definió como **dos modos sobre los mismos datos**:

| Modo | Se parece a | Contrato |
| --- | --- | --- |
| Historial | Kafka | el consumidor lee desde el offset que quiera y puede releer |
| Cola de trabajo | Amazon SQS | cada mensaje va a un consumidor, que lo confirma o lo devuelve; hay reintentos, programados y DLQ |

El resto era infraestructura para sostener esos dos modos:

- log append-only en disco, con segmentos, índice, CRC32 y group commit;
- tres brokers con un líder electo por Lease de Kubernetes;
- replicación por quórum (2 de 3) con high watermark, épocas y fencing;
- failover que no pierde mensajes confirmados;
- API HTTP para clientes y gRPC entre brokers;
- tópicos declarados con una CRD de Kubernetes.

### Prioridades explícitas

El README original fijaba qué recortar si faltaba tiempo, en este orden:

1. CRD Topic
2. Server-Sent Events
3. varias particiones por tópico
4. mensajes programados

Y qué **nunca** se recortaba:

- replicación con quórum,
- failover con épocas,
- cola con ack y DLQ.

Esa prioridad es la que explica el diseño: la cola y la replicación eran el
producto; las particiones eran una capacidad opcional.

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
   grupos de consumidores están en el contrato, pero no se implementaron como
   mecanismo de escala.

## 4. Evolución

| Fecha | Hito |
| --- | --- |
| Sep 30 | Fase 0: solución, contratos, fakes, CI, Dockerfile. |
| Oct 7 | DEC-001: se propone un log por tópico y eliminar particiones; se documenta que con un solo líder las particiones no escalan. |
| Oct 19 | Rama `no-partitions`: se implementa DEC-001, pero no se mergea. |
| Oct 26 | Demo con consola de cluster simulado (Farres). Main queda en ese estado. |
| Nov 3–4 | Rama de cola + asignación de particiones; se mergea a main. |
| Nov 5 | Se formaliza el alcance multi-partición en la documentación. |
| Nov 6 | `fix/original-scope`: se vuelve al alcance original, se corrige el motor de cola y se agregan los arreglos de retención, salud y quórum. |

La lección de esa evolución está en [decisions.md](decisions.md), DEC-001: las
particiones solo tienen sentido con un líder por partición. Sin eso agregan
complejidad y no escalan.

## 5. Estado actual y límites

El diseño vigente es un **broker de un solo escritor con alta disponibilidad y
cola de trabajo**:

- un log por tópico; `partitions` sigue en el contrato pero se usa la partición 0;
- un líder para todos los tópicos; la replicación da HA, no escala de escritura;
- cola sin grupos de consumidores: los consumidores compiten por el próximo
  mensaje;
- at-least-once; sin transacciones, compactación ni exactly-once;
- retención por tiempo o tamaño.

La tabla completa de implementado / no implementado está en
[ARQUITECTURA.md](ARQUITECTURA.md#estado-de-implementación).

## 6. Dirección (trabajo futuro)

En orden de valor, si el proyecto continuara:

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
registry, tiered storage.

## 8. Cómo leer esta documentación

- [README de docs](README.md): índice y orden de lectura.
- [ARQUITECTURA.md](ARQUITECTURA.md): diseño vigente.
- [decisions.md](decisions.md): decisiones y su estado.
- [history/](history/): planes y documentos de etapas anteriores.
