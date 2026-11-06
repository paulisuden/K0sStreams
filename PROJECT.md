# K0sStreams

**Un log distribuido y una cola de trabajo para Kubernetes.**

K0sStreams es un middleware de mensajería escrito en .NET que corre como tres
brokers en Kubernetes. Expone por HTTP un log append-only replicado y, sobre los
mismos datos, dos formas de consumir: historial y cola de trabajo.

## Definición

> Un solo log, dos contratos de lectura: historial tipo Kafka y cola de trabajo
> tipo SQS, con un líder electo por el Lease del cluster y replicación por
> quórum en tres brokers.

No es un clon de Kafka. Es un log distribuido con una cola encima, pensado para
clusters chicos donde Kafka es más infraestructura de la que hace falta.

## El problema

Kafka resuelve muy bien el log distribuido, pero operarlo requiere un ecosistema:
brokers, controladores, coordinación, clientes binarios, tuning. En un cluster
Kubernetes chico ya existen las piezas que un log necesita: un Lease para elegir
un líder, una CRD para declarar tópicos y un PVC para persistir.

K0sStreams usa esas piezas en lugar de reconstruirlas.

## Qué ofrece

| Modo | Se parece a | Contrato |
| --- | --- | --- |
| Historial | Kafka | el consumidor lee desde el offset que elige y puede releer |
| Cola de trabajo | Amazon SQS | cada mensaje va a un consumidor, que lo confirma o lo devuelve; hay reintentos, programados y DLQ |

Sobre el mismo log, sin duplicar datos.

## Cómo funciona

- **Un log por tópico**: append-only, con segmentos, índice y CRC32.
- **Tres brokers**: uno es líder y ordena las escrituras; los otros replican.
- **Quórum 2 de 3**: una escritura se confirma cuando está en el líder y en al
  menos un seguidor. El high watermark marca hasta dónde puede leer un cliente.
- **Elección de líder**: un Lease de Kubernetes; épocas y fencing rechazan a un
  líder viejo.
- **Failover**: el nuevo líder se pone al día antes de aceptar escrituras; los
  mensajes confirmados no se pierden.
- **API HTTP**: publicar, leer historial, consumir como cola. Los tópicos se
  declaran con una CRD.

## Garantías

- Sobrevive la caída de un broker sin perder mensajes confirmados.
- Entrega **at-least-once**: si el líder cambia, lo no confirmado se reentrega.
- Orden por tópico en el historial; en la cola, el orden es el del log.
- El cursor confirmado de cada cola y las marcas de DLQ son durables.

## Límites

- **Un solo escritor** para todos los tópicos: la replicación da alta
  disponibilidad, no escala de escritura.
- **Un log por tópico**: las particiones están en el contrato pero no se usan
  para distribuir carga.
- **At-least-once**, no exactly-once: un failover puede reentregar mensajes ya
  confirmados.
- **Sin grupos de consumidores**: los consumidores de una cola compiten por el
  próximo mensaje.
- **Sin transacciones, compactación ni idempotencia de productor.**
- **Rendimiento atado al disco**: cada escritura confirmada por quórum paga un
  fsync; en volúmenes de red lentos, eso domina la latencia.

## Lo que no es

- No es un clon de Kafka: no hay líderes por partición, ni protocolo Kafka, ni
  ecosistema de conectores.
- No es un sistema de logs de aplicación (Loki, Elastic): es un log de eventos.
- No promete escala horizontal de escritura.

## Demo en tres escenas

1. **Historial**: publicar mensajes y releerlos desde distintos offsets.
2. **Cola**: dos consumidores compiten; uno confirma, otro devuelve; el mensaje
   agotado termina en la DLQ.
3. **Failover**: matar el líder mientras se publica; otro broker toma el Lease y
   no se pierde ningún mensaje confirmado.

## Pitch de 30 segundos

> Kafka es potente pero pesado de operar. K0sStreams toma la parte útil —un log
> replicado y replayable— y la hace nativa de Kubernetes: tópicos como CRD,
> elección de líder con el Lease del cluster, tres brokers y HTTP. Encima agrega
> una cola de trabajo real, con acks, reintentos y dead-letter queue, sobre los
> mismos datos. La garantía es at-least-once y la disponibilidad es sobrevivir la
> caída de un broker. No intenta ser Kafka a escala; intenta ser el log y la cola
> que un cluster chico necesita.

## Una slide

| | |
| --- | --- |
| **Qué** | Log replicado + cola de trabajo, sobre HTTP, para Kubernetes. |
| **Por qué** | Kafka es pesado; el cluster ya tiene Lease, CRD y PVC. |
| **Cómo** | Un log por tópico, 3 réplicas, quórum 2 de 3, líder por Lease, épocas y fencing. |
| **Garantía** | Sobrevive un broker sin perder mensajes confirmados; at-least-once. |
| **Límite** | Un solo escritor: HA, no escala de escritura. |
