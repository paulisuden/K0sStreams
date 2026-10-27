'use strict';

// El cluster real, con la misma interfaz que SimulatedCluster. La página la sirve nginx adentro del cluster
// (deploy/demo/demo.yaml), que hace de intermediario:
//   lb/...                → el Service broker-lb, como cualquier cliente (un seguidor responde 307 y nginx lo reescribe)
//   brokers/broker-N/...  → un broker en particular, para ver el high watermark de cada uno
//   k8s/...               → el API server de Kubernetes con el token de la ServiceAccount demo: Lease, pods, logs
// Los brokers no saben nada de la demo: todo sale de la API REST de siempre y de Kubernetes.

const LIVE = {
  brokers: ['broker-0', 'broker-1', 'broker-2'],
  leaseName: 'k0sstreams-leader',
  logsEveryMs: 1_000,
  // La primera vez se traen las últimas líneas de cada broker; después, solo lo de los últimos segundos.
  initialLogLines: 150,
  recentLogSeconds: 5,
  hwTimeoutMs: 1_500,
};

// Un HW sin leer mensajes: desde un offset que no existe, el broker responde vacío pero con su high watermark.
const BEYOND_ANY_OFFSET = 999_999_999_999;

class LiveCluster {
  static async create() {
    const config = await fetch('config.json').then((response) => response.json());
    const cluster = new LiveCluster(config.namespace);
    // Tópicos creados por la API fuera de la página (la API no los lista): ?topics=a,b
    for (const name of new URLSearchParams(location.search).get('topics')?.split(',') ?? []) {
      cluster.rememberTopic(name.trim());
    }
    return cluster;
  }

  constructor(namespace) {
    this.mode = 'live';
    this.namespace = namespace;
    this.topics = new Map();
    this.hw = new Map(LIVE.brokers.map((name) => [name, {}]));
    this.statuses = new Map();
    this.seenLogLines = new Set();
    this.logsLoaded = new Set();
    this.lastLogFetch = 0;
    // Por los logs: la última época que cada broker empezó a preparar y la última que lidera.
    this.preparing = new Map();
    this.leading = new Map();
  }

  // ---- Interfaz que usa la página ----

  async snapshot() {
    const [lease, pods, resources] = await Promise.all([
      this.k8s(`apis/coordination.k8s.io/v1/namespaces/${this.namespace}/leases/${LIVE.leaseName}`),
      this.k8s(`api/v1/namespaces/${this.namespace}/pods?labelSelector=app%3Dbroker`),
      this.k8s(`apis/k0sstreams.io/v1/namespaces/${this.namespace}/topics`),
    ]);
    this.rememberCrdTopics(resources?.items ?? []);
    for (const name of LIVE.brokers) {
      this.statuses.set(name, podStatus(pods?.items?.find((pod) => pod.metadata.name === name)));
    }
    await this.refreshHighWatermarks();

    const spec = lease?.spec ?? {};
    const holder = spec.holderIdentity || null;
    const epoch = spec.leaseTransitions ?? 0;
    const brokers = LIVE.brokers.map((name) => ({
      name,
      status: this.statuses.get(name),
      role: this.roleOf(name, holder, epoch),
      hw: this.hw.get(name),
    }));

    return {
      time: Date.now(),
      lease: { holder, transitions: epoch, renewedAt: parseKubernetesTime(spec.renewTime), durationSeconds: spec.leaseDurationSeconds ?? 10 },
      leader: brokers.find((b) => b.role === 'leader')?.name ?? null,
      epoch,
      brokers,
      topics: [...this.topics.values()],
    };
  }

  async logs() {
    if (Date.now() - this.lastLogFetch < LIVE.logsEveryMs) {
      return [];
    }
    this.lastLogFetch = Date.now();

    const alive = LIVE.brokers.filter((name) => this.statuses.get(name) !== 'down');
    const batches = await Promise.all(alive.map((name) => this.brokerLogs(name)));
    return batches.flat().sort((a, b) => a.time - b.time);
  }

  async createTopic(name) {
    const result = await this.request('POST', 'v1/topics', { name });
    if (result.status === 201 || result.status === 409) {
      this.rememberTopic(name);
    }
    return { status: result.status };
  }

  async publish(topic, value) {
    const result = await this.request('POST', `v1/topics/${topic}/messages`, { value });
    return { status: result.status, offset: result.data?.offset };
  }

  async read(topic, from, max) {
    const result = await this.request('GET', `v1/topics/${topic}/partitions/0/messages?from=${from}&max=${max}`);
    return result.status === 200 ? { status: 200, ...result.data } : { status: result.status };
  }

  async receive(topic, queue, visibilitySeconds) {
    const result = await this.request('POST', `v1/topics/${topic}/queues/${queue}/receive`, { visibilitySeconds });
    return { status: result.status, delivery: result.data };
  }

  async ack(topic, queue, partition, offset) {
    return this.request('POST', `v1/topics/${topic}/queues/${queue}/ack/${partition}/${offset}`);
  }

  async nack(topic, queue, partition, offset, reason) {
    return this.request('POST', `v1/topics/${topic}/queues/${queue}/nack/${partition}/${offset}`, { reason });
  }

  async deadLetters(topic, queue) {
    const result = await this.request('GET', `v1/topics/${topic}/queues/${queue}/dlq`);
    return { status: result.status, items: result.data ?? [] };
  }

  /** Como "kubectl delete pod": con force, --grace-period=0 (no hay apagado limpio, el Lease no se suelta). */
  async kill(name, force) {
    const query = force ? '?gracePeriodSeconds=0' : '';
    await fetch(`k8s/api/v1/namespaces/${this.namespace}/pods/${name}${query}`, { method: 'DELETE' }).catch(() => null);
  }

  // ---- Kubernetes ----

  async k8s(path) {
    try {
      const response = await fetch(`k8s/${path}`, { signal: AbortSignal.timeout(3000) });
      return response.ok ? await response.json() : null;
    } catch {
      return null;
    }
  }

  /** Lee los logs nuevos de un broker y los convierte en líneas como las del simulador. */
  async brokerLogs(name) {
    const first = !this.logsLoaded.has(name);
    const query = first ? `tailLines=${LIVE.initialLogLines}` : `sinceSeconds=${LIVE.recentLogSeconds}`;
    let text;
    try {
      const response = await fetch(`k8s/api/v1/namespaces/${this.namespace}/pods/${name}/log?timestamps=true&${query}`,
        { signal: AbortSignal.timeout(3000) });
      if (!response.ok) {
        return [];
      }
      text = await response.text();
    } catch {
      return [];
    }
    this.logsLoaded.add(name);
    return this.parseLogs(name, text);
  }

  /**
   * La consola de .NET escribe cada log en dos líneas: "info: Categoria[id]" y debajo, con sangría, el mensaje. Con
   * timestamps=true, Kubernetes antepone la hora a cada una. Como se pide una ventana de unos segundos en cada vuelta,
   * la misma línea llega varias veces: se reconoce por su hora y su texto.
   */
  parseLogs(broker, text) {
    const lines = [];
    let current = null;
    for (const raw of text.split('\n')) {
      const space = raw.indexOf(' ');
      if (space < 0) {
        continue;
      }
      const stamp = raw.slice(0, space);
      const content = raw.slice(space + 1);
      const header = /^(trce|dbug|info|warn|fail|crit): ([^[]+)\[/.exec(content);
      if (header) {
        const id = `${broker}|${stamp}|${content}`;
        current = this.seenLogLines.has(id)
          ? null
          : { broker, time: parseKubernetesTime(stamp), level: header[1], category: header[2].trim(), message: '' };
        this.seenLogLines.add(id);
        if (current) {
          lines.push(current);
        }
      } else if (current && current.message === '') {
        current.message = content.trim();
        this.followEpochs(broker, current.message);
      }
    }
    if (this.seenLogLines.size > 50_000) {
      this.seenLogLines.clear();
    }
    return lines.filter((line) => line.message !== '');
  }

  followEpochs(broker, message) {
    const preparing = /Lease tomado para la época (\d+)/.exec(message);
    if (preparing) {
      this.preparing.set(broker, Number(preparing[1]));
    }
    const leading = /Líder de la época (\d+)/.exec(message);
    if (leading) {
      this.leading.set(broker, Number(leading[1]));
    }
  }

  /**
   * Titular del Lease que todavía se está poniendo al día: según sus logs empezó a preparar esta época y todavía no
   * dijo que la lidera. Si los logs no dicen nada (la página abrió después), se lo toma como líder.
   */
  roleOf(name, holder, epoch) {
    const status = this.statuses.get(name);
    if (status !== 'up') {
      return 'none';
    }
    if (name !== holder) {
      return 'follower';
    }
    const promoting = this.preparing.get(name) === epoch && this.leading.get(name) !== epoch;
    return promoting ? 'promoting' : 'leader';
  }

  // ---- Brokers ----

  async refreshHighWatermarks() {
    const reads = [];
    for (const name of LIVE.brokers) {
      if (this.statuses.get(name) === 'down') {
        continue;
      }
      for (const topic of this.topics.keys()) {
        reads.push(this.highWatermark(name, topic).then((hw) => {
          if (hw !== null) {
            this.hw.get(name)[topic] = hw;
          }
        }));
      }
    }
    await Promise.all(reads);
  }

  async highWatermark(broker, topic) {
    try {
      const response = await fetch(`brokers/${broker}/v1/topics/${topic}/partitions/0/messages?from=${BEYOND_ANY_OFFSET}&max=1`,
        { signal: AbortSignal.timeout(LIVE.hwTimeoutMs) });
      return response.ok ? (await response.json()).highWatermark : null;
    } catch {
      return null;
    }
  }

  /** Un pedido como el de cualquier cliente, por el LoadBalancer. */
  async request(method, path, body = null) {
    try {
      const response = await fetch(`lb/${path}`, {
        method,
        headers: body ? { 'Content-Type': 'application/json' } : {},
        body: body ? JSON.stringify(body) : undefined,
        signal: AbortSignal.timeout(20_000),
      });
      const data = response.status === 200 || response.status === 201 ? await response.json().catch(() => null) : null;
      return { status: response.status, data };
    } catch {
      return { status: 0, data: null };
    }
  }

  // ---- Tópicos ----

  /** La API no lista tópicos: se conocen por los Topic de la CRD y por los que crea esta página. */
  rememberCrdTopics(items) {
    for (const item of items) {
      const spec = item.spec ?? {};
      this.topics.set(item.metadata.name, {
        name: item.metadata.name,
        partitions: spec.partitions ?? 1,
        maxRetries: spec.maxRetries ?? 3,
        retryBackoffSeconds: spec.retryBackoffSeconds ?? 5,
        origin: 'CRD',
      });
    }
  }

  rememberTopic(name) {
    if (!this.topics.has(name)) {
      // Creado con los valores por defecto de POST /v1/topics.
      this.topics.set(name, { name, partitions: 1, maxRetries: 3, retryBackoffSeconds: 5, origin: 'API' });
    }
  }
}

/** up = Running y Ready; starting = existe pero todavía no está Ready; down = no existe o se está borrando. */
function podStatus(pod) {
  if (!pod || pod.metadata.deletionTimestamp) {
    return 'down';
  }
  const ready = pod.status?.conditions?.some((c) => c.type === 'Ready' && c.status === 'True');
  return pod.status?.phase === 'Running' && ready ? 'up' : 'starting';
}

/** Kubernetes escribe hasta 9 decimales de segundo; Date.parse se queda con 3. */
function parseKubernetesTime(text) {
  return text ? Date.parse(text.replace(/(\.\d{3})\d+/, '$1')) : 0;
}
