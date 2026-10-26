// Benchmark simple del broker (fase 5), con k6. Una corrida mide una operación (MODE) con VUS clientes en paralelo durante
// DURATION, e imprime una línea RESULT. La lanza bench/run.sh adentro del cluster.
//
//   publish  POST /messages de 100 bytes: incluye esperar el quórum (2 de 3)
//   read     GET /messages?from=<al azar>&max=100: leer como historial
//   queue    receive + ack: consumir como cola de trabajo
//
// Variables: BROKER (URL), TOPIC, MODE, VUS, DURATION (p. ej. 20s).
import http from 'k6/http';
import { Counter, Rate, Trend } from 'k6/metrics';

const BROKER = __ENV.BROKER;
const TOPIC = __ENV.TOPIC;
const MODE = __ENV.MODE;
const BATCH = 100;
const VALUE = 'x'.repeat(100);
const JSON_BODY = { headers: { 'Content-Type': 'application/json' } };

// Una operación es un pedido HTTP, salvo en queue, que son dos (receive y ack).
const opDuration = new Trend('op_duration', true);
const ops = new Counter('ops');
const failed = new Rate('failed');
const messages = new Counter('messages');
// receive sin nada para entregar: no es una operación ni una falla, pero si hay muchos la cola se vació.
const empty = new Counter('empty');

export const options = {
  scenarios: {
    bench: { executor: 'constant-vus', vus: Number(__ENV.VUS), duration: __ENV.DURATION },
  },
  summaryTrendStats: ['med', 'p(95)', 'p(99)'],
};

// Corre una vez antes que los clientes: crea el tópico si no existe y averigua hasta dónde se puede leer.
export function setup() {
  const created = http.post(`${BROKER}/v1/topics`, JSON.stringify({ name: TOPIC }), JSON_BODY);
  if (created.status !== 201 && created.status !== 409) {
    throw new Error(`No se pudo crear ${TOPIC}: HTTP ${created.status}`);
  }

  const read = http.get(`${BROKER}/v1/topics/${TOPIC}/partitions/0/messages?from=0&max=1`);
  return { highWatermark: read.json('highWatermark') };
}

export default function (data) {
  const result = MODES[MODE](data);
  if (result === null) {
    empty.add(1);
    return;
  }

  opDuration.add(result.ms);
  ops.add(1);
  failed.add(!result.ok);
}

const MODES = {
  publish() {
    const res = http.post(`${BROKER}/v1/topics/${TOPIC}/messages`, JSON.stringify({ value: VALUE }), JSON_BODY);
    if (res.status === 200) {
      messages.add(1);
    }
    return { ok: res.status === 200, ms: res.timings.duration };
  },

  read(data) {
    // Desde un offset al azar, para no leer siempre el mismo pedazo del log.
    const from = Math.floor(Math.random() * Math.max(1, data.highWatermark - BATCH + 2));
    const res = http.get(`${BROKER}/v1/topics/${TOPIC}/partitions/0/messages?from=${from}&max=${BATCH}`);
    if (res.status === 200) {
      messages.add(res.json('messages').length);
    }
    return { ok: res.status === 200, ms: res.timings.duration };
  },

  queue() {
    const received = http.post(`${BROKER}/v1/topics/${TOPIC}/queues/bench/receive`, '{}', JSON_BODY);
    if (received.status === 204) {
      return null;
    }
    if (received.status !== 200) {
      return { ok: false, ms: received.timings.duration };
    }

    const { partition, offset } = received.json();
    const acked = http.post(`${BROKER}/v1/topics/${TOPIC}/queues/bench/ack/${partition}/${offset}`);
    if (acked.status === 200) {
      messages.add(1);
    }
    return { ok: acked.status === 200, ms: received.timings.duration + acked.timings.duration };
  },
};

// En vez del resumen largo de k6, una línea que run.sh junta en una tabla.
export function handleSummary(data) {
  const value = (metric, stat) => (data.metrics[metric] ? data.metrics[metric].values[stat] : 0);
  const fields = [
    MODE,
    __ENV.VUS,
    value('ops', 'rate').toFixed(0),
    value('messages', 'rate').toFixed(0),
    value('op_duration', 'med').toFixed(1),
    value('op_duration', 'p(95)').toFixed(1),
    value('op_duration', 'p(99)').toFixed(1),
    (value('failed', 'rate') * 100).toFixed(2),
    value('empty', 'count'),
  ];
  return { stdout: `RESULT ${fields.join(' ')}\n` };
}
