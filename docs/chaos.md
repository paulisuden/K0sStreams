# Suite de caos (bloque D)

Cómo se comprueba que el broker no pierde mensajes confirmados cuando se caen brokers. Corresponde a la fase 5 de [ARQUITECTURA.md](ARQUITECTURA.md). Los scripts están en `chaos/`.

## 1. Qué se promete y qué no

Un mensaje que el broker **confirmó** (respondió 200 al publicar) nunca se pierde. Sobre uno **sin confirmar** (timeout, 503, conexión cortada) no se promete nada: el líder pudo morir antes de replicarlo (no queda) o después de replicarlo pero antes de responder (queda). Desde el cliente los dos casos se ven igual.

| Caso | ¿Es una falla? |
| --- | --- |
| Un confirmado que no está en alguna réplica | **Sí: pérdida.** Es lo que tiene que dar cero. |
| Un sin confirmar que está o no está | No. Se cuenta. |
| Un valor dos veces | **Sí.** El productor nunca reintenta, así que lo duplicó el broker. |
| Un valor que nunca se mandó | **Sí.** Mensaje inventado o corrupto. |
| Una réplica distinta de las otras | **Sí.** No convergieron. |

## 2. Las piezas

| Script | Dónde corre | Qué hace |
| --- | --- | --- |
| `producer.sh` | Pod `curlimages/curl` | Publica `<tópico>-1, -2, ...` durante N segundos e imprime `CONFIRMED` o `UNCONFIRMED <http>` por mensaje: los recibos. |
| `read-topic.sh` | Pod `curlimages/curl` | Lee el tópico de **cada** broker por su nombre (`broker-N.broker`) hasta su high watermark. |
| `verify.sh` | Tu máquina | Compara los recibos con cada réplica según la tabla de arriba. Si algo no cierra, vuelve a leer hasta 10 veces cada 3 s: un seguidor se entera del high watermark en el siguiente heartbeat, y uno que recién volvió se está poniendo al día. |
| `faults.sh` | Tu máquina | Las fallas (ver abajo) y `wait_healthy`, que espera a que los brokers estén Ready y haya titular del Lease. |
| `run.sh` | Tu máquina | Corre cada escenario N veces e imprime la tabla. |
| `lib.sh` | Tu máquina | Arrancar un pod con un script, juntar su salida, listar los brokers. |

El productor y el lector corren adentro del cluster porque un seguidor redirige (307) a `broker-N.broker:9090`, un nombre que solo resuelve el DNS del cluster.

El productor va por tiempo y no por cantidad: tiene que estar publicando cuando llega la falla. Si termina antes, el OK no prueba nada.

## 3. Escenarios

| Escenario | Falla | Qué pone a prueba |
| --- | --- | --- |
| `leader` | Borra al líder con aviso (SIGTERM) | El líder suelta el Lease y otro lo toma en un par de segundos |
| `leader_force` | Mata al líder sin aviso (`--grace-period=0 --force`) | Corte de luz: nadie suelta el Lease |
| `follower` | Mata un seguidor sin aviso | Con 2 de 3 se sigue confirmando |
| `no_quorum` | Mata al líder y a un seguidor sin aviso | Sin quórum no se confirma nada y lo confirmado antes sobrevive |
| `leader_repeated` | Tres muertes de líder seguidas | Épocas y fencing en cadena |

En minikube, un pod borrado a la fuerza vuelve en pocos segundos con el mismo nombre y retoma el Lease a su nombre sin esperar los 10 s (ver [coordination.md](coordination.md)). Por eso los cortes son más cortos que en un cluster donde el nodo entero se cae.

## 4. Cómo se corre

Con el cluster de [deploy/](../deploy/) andando:

```bash
chaos/run.sh 1                              # una pasada de cada escenario, ~5 min
chaos/run.sh 5                              # 25 ejecuciones, ~20 min
chaos/run.sh 3 no_quorum leader_repeated    # solo algunos escenarios
```

Cada ejecución usa un tópico nuevo (`chaos-<fecha>-<escenario>-<n>`). Las salidas quedan en `chaos/results/<fecha>/` (no se commitean): recibos (`.producer`), veredicto (`.verify`) y `summary.txt`.

Una ejecución a mano, paso por paso:

```bash
source chaos/faults.sh
T=chaos-manual-1
start_pod chaos-producer producer.sh $T 20
sleep 5
fault_leader_force
wait_healthy
mkdir -p chaos/results
pod_output chaos-producer 120 > chaos/results/$T.txt
chaos/verify.sh $T chaos/results/$T.txt
```

## 5. Resultados

minikube, 3 brokers, 23 oct 2026, `chaos/run.sh 5`:

| Escenario | Ejecuciones | OK | Confirmados | Sin confirmar |
| --- | --- | --- | --- | --- |
| `leader` | 5 | 5 | 19 778 | 8 |
| `leader_force` | 5 | 5 | 19 536 | 7 |
| `follower` | 5 | 5 | 20 986 | 2 |
| `no_quorum` | 5 | 5 | 19 549 | 8 |
| `leader_repeated` | 5 | 5 | 17 942 | 15 |

**0 mensajes confirmados perdidos en 25 ejecuciones.**

Los sin confirmar, por código:

| Código | Cuándo aparece |
| --- | --- |
| `307` | Solo cuando muere el líder. Un seguidor todavía apuntaba al líder muerto: curl siguió el 307, no pudo conectarse, y `%{http_code}` quedó en el último código recibido. |
| `503` | En medio del failover, cuando el broker sabe que no hay líder. |
| `000` | Sin ninguna respuesta: el pedido estaba en un broker que se murió, o pasaron los 15 s de `--max-time`. |
