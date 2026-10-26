# Benchmark (bloque D)

Cuánto aguanta el broker con 3 réplicas y quórum 2 de 3. Corresponde al "benchmark simple" de la fase 5 de [ARQUITECTURA.md](ARQUITECTURA.md). Los scripts están en `bench/`.

## 1. Qué se mide

Cada corrida mide **una operación** con **N clientes en paralelo** durante 20 s:

| Modo | Qué hace cada cliente | Qué pone a prueba |
| --- | --- | --- |
| `publish` | `POST /v1/topics/{t}/messages` con 100 bytes | Escritura: append en el líder + réplica en un seguidor (quórum) |
| `read` | `GET .../partitions/0/messages?from=<al azar>&max=100` | Leer como historial: sin escrituras ni quórum |
| `queue` | `receive` + `ack` | Consumir como cola: cada uno escribe un evento de cola con quórum, así que son dos escrituras |

Los tres modos usan el mismo tópico, en ese orden: lo que publica `publish` lo leen `read` y `queue`.

Columnas:

| Columna | Qué es |
| --- | --- |
| `ops/s` | Operaciones completadas por segundo |
| `msgs/s` | Mensajes por segundo (en `read`, cada operación trae hasta 100) |
| `p50/p95/p99` | Latencia de una operación que no supera el 50/95/99 % de ellas, en ms |
| `fallas_%` | Respuestas que no fueron 200 |
| `vacías` | `receive` sin nada para entregar. Si es alto, la cola se vació y la fila mide de menos |

## 2. Las piezas

| Archivo | Qué hace |
| --- | --- |
| `bench/bench.js` | Script de [k6](https://k6.io). `setup()` corre una vez (crea el tópico y lee el high watermark), después cada cliente repite la operación, y `handleSummary` imprime una línea `RESULT` |
| `bench/run.sh` | Por cada modo y cantidad de clientes, lanza un pod `grafana/k6` con el script y junta las líneas en una tabla |

Como el caos, corre **adentro del cluster**: un seguidor redirige (307) a `broker-N.broker`, un nombre que solo resuelve el DNS del cluster. Por defecto le pega directo al líder (lo saca del Lease), para medir el broker y no el redirect.

```bash
bench/run.sh                                  # 20 s por corrida, con 1, 10 y 50 clientes (~5 min)
bench/run.sh 30 1 5 20                        # 30 s, con 1, 5 y 20 clientes
BROKER=http://broker-lb:9090 bench/run.sh     # por el LoadBalancer, como un cliente cualquiera
```

Las salidas quedan en `bench/results/<fecha>/` (no se commitean): un `.log` por corrida y `summary.txt`.

## 3. Resultados

26/10/2026, minikube con un solo nodo (Intel i5-13420H, 12 CPU, 16 GB): los 3 brokers, sus discos y k6 comparten la misma máquina.

**Directo al líder:**

| Modo | Clientes | ops/s | msgs/s | p50 ms | p95 ms | p99 ms | Fallas |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| publish | 1 | 506 | 506 | 1,8 | 2,4 | 2,9 | 0 % |
| publish | 10 | 984 | 984 | 9,7 | 11,5 | 24,6 | 0 % |
| publish | 50 | 1066 | 1066 | 45,0 | 57,1 | 91,3 | 0 % |
| read | 1 | 1648 | 164 829 | 0,3 | 0,6 | 1,5 | 0 % |
| read | 10 | 6374 | 637 400 | 0,8 | 2,7 | 6,7 | 0 % |
| read | 50 | 7596 | 759 601 | 4,7 | 14,0 | 28,1 | 0 % |
| queue | 1 | 192 | 192 | 4,8 | 6,3 | 7,2 | 0 % |
| queue | 10 | 381 | 381 | 24,0 | 40,8 | 52,3 | 0 % |
| queue | 50 | 209 | 209 | 232,2 | 314,6 | 367,7 | 0 % |

**Por el LoadBalancer (`broker-lb`):**

| Modo | Clientes | ops/s | msgs/s | p50 ms | p95 ms | p99 ms | Fallas |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| publish | 1 | 409 | 409 | 2,0 | 2,6 | 3,3 | 0 % |
| publish | 10 | 1001 | 1001 | 9,5 | 11,2 | 14,2 | 0 % |
| publish | 50 | 1053 | 1053 | 45,6 | 62,8 | 68,9 | 0 % |
| read | 1 | 1666 | 166 613 | 0,3 | 0,6 | 1,2 | 0 % |
| read | 10 | 4165 | 416 513 | 1,2 | 4,5 | 10,4 | 0 % |
| read | 50 | 4925 | 492 478 | 5,0 | 29,8 | 87,5 | 0 % |
| queue | 1 | 151 | 151 | 5,3 | 7,5 | 12,4 | 0 % |
| queue | 10 | 349 | 349 | 25,5 | 43,7 | 60,9 | 0 % |
| queue | 50 | 223 | 223 | 218,3 | 300,2 | 333,9 | 0 % |

Ninguna corrida tuvo fallas ni `receive` vacíos.

## 4. Cómo leerlos

**Escritura: ~1000 mensajes/s confirmados por quórum.** De 1 a 10 clientes el throughput se duplica. De 10 a 50 crece un 8 % y la latencia se multiplica por 5: el camino de escritura está saturado y los clientes de más solo hacen fila. Lo confirma la ley de Little (clientes = throughput × latencia): 50 / 1066 por segundo ≈ 47 ms, que es el p50 medido.

**Lectura: ~7600 pedidos/s, unos 760 mil mensajes/s.** Escala mucho más que la escritura porque no escribe en disco ni espera a otro broker: el log se lee de la caché de páginas del sistema operativo.

**Cola: cada ciclo cuesta dos escrituras con quórum.** Con 1 cliente tarda el doble que un publish (4,8 ms contra 1,8 ms). Con 50 clientes rinde **menos** que con 10 (209 contra 381 ciclos/s, p50 de 232 ms): los clientes compiten por algo dentro del motor de cola. No pierde ni duplica nada (0 fallas), pero es un dato para el bloque B.

**LoadBalancer.**
- Con 1 cliente, publicar rinde un 19 % menos (409 contra 506/s): 2 de cada 3 pedidos caen en un seguidor y pagan un 307 más el viaje al líder. Con 10 o más clientes no se nota, porque lo que limita es el líder.
- Leer por el LoadBalancer reparte entre los 3 pods, pero en una sola máquina comparten las mismas CPU y no rinde más.
- La latencia que se muestra es la del último pedido (k6 la mide por pedido), así que **no incluye el 307**. El costo del redirect se ve en `ops/s`.

## 5. Límites

- Es un solo nodo: brokers, discos y cliente comparten CPU y disco. Los números sirven para comparar modos y cantidades de clientes entre sí, no como capacidad de un cluster real.
- Una sola partición por tópico ([DEC-001](decisions.md)): todas las escrituras pasan por un único log en el líder.
- Mensajes de 100 bytes. Con mensajes más grandes pesa más el disco y la red, y menos el costo por pedido.
