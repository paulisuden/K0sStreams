# Consola de la demo (bloque D)

Una página que muestra en vivo lo que hace el cluster mientras se publica, se consume y se matan brokers. Es para la presentación de la fase 6: lo que en la terminal son líneas sueltas (`kubectl get lease -w`, logs, el productor) acá se ve junto y en orden. Está en `demo/`.

## 1. Qué muestra

| Panel | Qué cuenta | De dónde sale (en vivo) |
| --- | --- | --- |
| Cluster | Quién lidera, en qué época, qué broker está caído o arrancando, y puntos que viajan del líder a cada seguidor cuando avanza el high watermark | Lease y pods del API de Kubernetes; HW de cada broker leyéndolo uno por uno |
| Lease | Titular, época (`leaseTransitions`) y una barra que se vacía cuando el titular murió sin soltarlo | Lease `k0sstreams-leader` |
| Línea de tiempo | El failover paso a paso: la acción, la caída, el Lease que vence, quién lo toma, la copia antes de liderar, el truncado del que vuelve | Cambios entre un sondeo y el siguiente, más las líneas clave de los logs |
| Mensajes pasando por la cola | Cada mensaje de la cola `demo`: en el tópico → en vuelo → reintento → confirmado o DLQ | El productor y los consumidores de la propia página, y `GET /dlq` |
| Tópicos | Mensajes/s de los últimos 60 s y el HW en cada broker | HW de cada broker |
| Historial | El tópico leído como log, por offset | `GET .../messages?from=` |
| Logs | Los logs reales de los 3 brokers, con los eventos de failover resaltados | `pods/log` de Kubernetes |

Las acciones: productor (1–200 msgs/s), consumidores (1–16) con un porcentaje de mensajes que fallan (nack), crear tópico y cuatro fallas: apagar al líder (suelta el Lease), matarlo sin aviso (no lo suelta), matar un seguidor y romper el quórum (líder y un seguidor).

El panel de la cola no le pide nada nuevo a la API: lo arma con lo que ven el productor y los consumidores de la página, como lo vería cualquier cliente. Por eso solo muestra los mensajes de la cola `demo`.

## 2. Dos modos

| Modo | Cuándo | Cómo |
| --- | --- | --- |
| **Simulado** | Abriendo `demo/index.html` como archivo, o con `?mode=sim` | `demo/sim.js` imita a los 3 brokers en memoria: Lease de 10 s, renovación cada 2 s, promoción con puesta al día, quórum 2 de 3, truncado de la cola divergente, reintentos con backoff y DLQ. Escribe los mismos textos de log que los brokers reales |
| **En vivo** | Servida por nginx adentro del cluster | `demo/live.js` habla con el cluster real |

La página (`demo/app.js`) no sabe con cuál habla: los dos tienen la misma interfaz (`snapshot`, `logs`, `publish`, `read`, `receive`, `ack`, `nack`, `deadLetters`, `createTopic`, `kill`). El simulado sirve para diseñar y ensayar sin cluster, y de respaldo si minikube falla en plena presentación.

## 3. Modo en vivo: cómo llega la página al cluster

El navegador está afuera del cluster: no resuelve `broker-0.broker` ni tiene un token para el API server. Un pod con nginx (`deploy/demo/demo.yaml`) sirve la página y hace de intermediario:

| La página pide | nginx lo manda a | Para qué |
| --- | --- | --- |
| `/lb/...` | Service `broker-lb` | Publicar, leer y consumir como un cliente cualquiera |
| `/brokers/broker-N/...` | `broker-N.broker` (Service headless) | El high watermark de cada broker por separado |
| `/k8s/...` | API server, con el token de la ServiceAccount `demo` | Lease, pods, logs, Topic de la CRD y borrar un broker |
| `/config.json` | — | El namespace, para armar las rutas de `/k8s` |

Dos detalles:

- **El 307 de un seguidor.** Responde `Location: http://broker-2.broker:9090/...`, un nombre que el navegador no resuelve. nginx lo reescribe a `/brokers/broker-2/...` (`proxy_redirect`), así el navegador sigue la redirección real a través de nginx.
- **DNS en cada pedido.** nginx resuelve los nombres en cada pedido (`resolver ... valid=2s`) y no una vez al arrancar: un broker que se reinicia vuelve con otra IP.

El Role `demo` da solo lo que la página usa: leer el Lease, listar pods y Topic, leer los logs de los 3 brokers y borrar esos 3 pods (el botón de matar). El Service no tiene IP externa: se entra con `kubectl port-forward`.

Los nombres DNS asumen el dominio `cluster.local`, el de minikube y k0s por defecto.

## 4. Cómo se usa

**Simulado:**

```bash
xdg-open demo/index.html
```

**En vivo**, con el cluster de `deploy/` andando:

```bash
kubectl create configmap demo-web --from-file=demo/ --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f deploy/demo/
kubectl rollout status deployment/demo
kubectl port-forward svc/demo 8080:80        # dejarlo corriendo; abrir http://localhost:8080
```

Después de cambiar algo en `demo/`: repetir el `create configmap` y `kubectl rollout restart deployment/demo`.

La API no lista tópicos: la página conoce los de la CRD y los que crea ella. Para ver otros: `http://localhost:8080/?topics=chaos-1,chaos-2`.

Si algo no anda:

```bash
kubectl logs deployment/demo | head -30      # cómo arrancó nginx y qué completó el envsubst
curl -s localhost:8080/config.json
curl -s localhost:8080/k8s/apis/coordination.k8s.io/v1/namespaces/default/leases/k0sstreams-leader | head -c 300
curl -s "localhost:8080/brokers/broker-0/v1/topics/pedidos/partitions/0/messages?max=1"
```

## 5. Un guion para la presentación

1. `kubectl apply -f deploy/examples/topic.yaml`: el tópico aparece en la página (CRD).
2. Prender el productor y los consumidores con 30 % de errores: se ven los reintentos con backoff y, al rato, la DLQ.
3. **Matar líder sin aviso.** La barra del Lease se vacía durante 10 s, las escrituras fallan, otro broker toma el Lease con la época siguiente, se pone al día y lidera. Los acks que quedaron sin respuesta vuelven a entregarse (at-least-once).
4. **Apagar líder.** Ahora el Lease se suelta al apagarse y el cambio tarda un par de segundos.
5. **Romper quórum.** No se confirma nada hasta que vuelve un segundo broker; lo confirmado antes sigue ahí.
6. Cerrar con la suite de caos ([chaos.md](chaos.md)): lo que se vio a mano, repetido N veces con verificador.
