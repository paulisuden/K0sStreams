'use strict';

// Cluster simulado: imita a los 3 brokers en memoria (Lease, épocas, réplicas con quórum 2 de 3, cola con reintentos y
// DLQ) para diseñar y ensayar la demo sin Kubernetes, y como respaldo si el cluster falla en vivo. Tiene la misma
// interfaz que el modo en vivo, así que la página no sabe con cuál habla.
//
// Los tiempos son los del broker real (Lease de 10 s, renovación cada 2 s) y los logs usan los mismos textos que
// LeaderElector, LeaderPromotion, QuorumReplicator y SegmentedLog: la línea de tiempo los reconoce igual en los dos modos.

const SIM = {
  tickMs: 100,
  leaseDurationMs: 10_000,
  retryPeriodMs: 2_000,
  promotionMs: 1_500,
  restartMs: 7_000,
  startingMs: 2_500,
  heartbeatMs: 500,
  quorum: 2,
  publishTimeoutMs: 15_000,
  // Registros que un seguidor copia por tick: así ponerse al día después de volver se ve, en vez de ser instantáneo.
  catchUpPerTick: 150,
};

const CATEGORY = {
  elector: 'K0sStreams.Coordination.LeaderElector',
  promotion: 'K0sStreams.Replication.Failover.LeaderPromotion',
  replicator: 'K0sStreams.Replication.Leader.QuorumReplicator',
  peer: 'K0sStreams.Replication.Leader.PeerReplicator',
  append: 'K0sStreams.Replication.Follower.AppendHandler',
  storage: 'K0sStreams.Storage.SegmentedLog',
  topics: 'K0sStreams.Coordination.TopicReconciler',
};

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

class SimulatedCluster {
  constructor() {
    const now = Date.now();
    this.mode = 'sim';
    this.brokers = ['broker-0', 'broker-1', 'broker-2'].map((name) => ({
      name,
      status: 'up',
      changedAt: now,
      restartAt: 0,
      logs: new Map(),
      hw: new Map(),
    }));
    this.lease = { holder: 'broker-0', transitions: 7, renewedAt: now };
    this.leaderName = 'broker-0';
    this.promotion = null;
    this.topics = new Map();
    this.queues = new Map();
    this.pending = [];
    this.logBuffer = [];
    this.lastHeartbeat = 0;
    this.lastLeaseAttempt = 0;
    this.unreachable = new Set();

    this.addTopic('pedidos', 3, 2, 'CRD');
    this.addTopic('facturas', 2, 3, 'API');
    this.seed('pedidos', 180);
    this.seed('facturas', 60);
    this.log('broker-0', 'info', CATEGORY.elector, 'Líder de la época 7.');
    this.log('broker-0', 'info', CATEGORY.replicator, 'Leading at epoch 7, replicating to 2 broker(s).');

    setInterval(() => this.tick(), SIM.tickMs);
  }

  // ---- Interfaz que usa la página (igual que el modo en vivo) ----

  async snapshot() {
    return {
      time: Date.now(),
      lease: { ...this.lease, durationSeconds: SIM.leaseDurationMs / 1000 },
      leader: this.leaderName,
      epoch: this.lease.transitions,
      brokers: this.brokers.map((b) => ({
        name: b.name,
        status: b.status,
        since: b.changedAt,
        role: this.roleOf(b),
        hw: Object.fromEntries(b.hw),
      })),
      topics: [...this.topics.values()],
    };
  }

  async logs() {
    const lines = this.logBuffer;
    this.logBuffer = [];
    return lines;
  }

  async createTopic(name) {
    await delay(5);
    if (!this.writableLeader()) {
      return { status: 503 };
    }
    if (this.topics.has(name)) {
      return { status: 409 };
    }
    this.addTopic(name, 3, 5, 'API');
    return { status: 201 };
  }

  async publish(topic, value) {
    await delay(2);
    if (!this.topics.has(topic)) {
      return { status: 404 };
    }

    const leader = this.leader();
    if (!leader) {
      return { status: 503 };
    }

    const records = leader.logs.get(topic);
    const record = { offset: records.length, epoch: this.lease.transitions, value, timestamp: Date.now() };
    records.push(record);
    return new Promise((resolve) => this.pending.push({
      topic,
      offset: record.offset,
      leader: leader.name,
      deadline: Date.now() + SIM.publishTimeoutMs,
      resolve,
    }));
  }

  async read(topic, from, max) {
    await delay(2);
    const broker = this.leader() ?? this.brokers.find((b) => b.status === 'up');
    if (!broker) {
      return { status: 0 };
    }
    if (!this.topics.has(topic)) {
      return { status: 404 };
    }

    const hw = broker.hw.get(topic);
    const records = broker.logs.get(topic);
    const messages = [];
    for (let offset = Math.max(0, from); offset <= hw && messages.length < max; offset++) {
      const record = records[offset];
      messages.push({ offset, value: record.value, timestamp: new Date(record.timestamp).toISOString() });
    }
    return { status: 200, messages, highWatermark: hw, nextOffset: Math.max(0, from) + messages.length };
  }

  async receive(topic, queue, visibilitySeconds) {
    await delay(3);
    const leader = this.writableLeader();
    if (!leader) {
      return { status: 503 };
    }

    const state = this.queueState(topic, queue);
    const now = Date.now();
    let chosen = null;
    for (const [offset, message] of state.messages) {
      // Venció el visibility timeout sin ack: vuelve a estar disponible.
      if (message.state === 'inflight' && message.visibleUntil <= now) {
        message.state = 'waiting';
        message.availableAt = now;
      }
      if (message.state === 'waiting' && message.availableAt <= now && (chosen === null || offset < chosen)) {
        chosen = offset;
      }
    }
    if (chosen === null && state.cursor <= leader.hw.get(topic)) {
      chosen = state.cursor++;
      state.messages.set(chosen, { attempts: 0, state: 'waiting', availableAt: now });
    }
    if (chosen === null) {
      return { status: 204 };
    }

    const message = state.messages.get(chosen);
    message.attempts++;
    message.state = 'inflight';
    message.visibleUntil = now + visibilitySeconds * 1000;
    const record = leader.logs.get(topic)[chosen];
    return {
      status: 200,
      delivery: {
        partition: 0,
        offset: chosen,
        key: null,
        value: record.value,
        attempt: message.attempts,
        visibleUntil: new Date(message.visibleUntil).toISOString(),
      },
    };
  }

  async ack(topic, queue, partition, offset) {
    await delay(3);
    if (!this.writableLeader()) {
      return { status: 503 };
    }

    const state = this.queueState(topic, queue);
    const message = state.messages.get(offset);
    if (!message || message.state !== 'inflight') {
      return { status: 404 };
    }
    state.messages.delete(offset);
    return { status: 200 };
  }

  async nack(topic, queue, partition, offset, reason) {
    await delay(3);
    const leader = this.writableLeader();
    if (!leader) {
      return { status: 503 };
    }

    const state = this.queueState(topic, queue);
    const message = state.messages.get(offset);
    if (!message || message.state !== 'inflight') {
      return { status: 404 };
    }

    const config = this.topics.get(topic);
    if (message.attempts > config.maxRetries) {
      state.messages.delete(offset);
      state.dead.push({
        partition: 0,
        offset,
        value: leader.logs.get(topic)[offset].value,
        attempts: message.attempts,
        lastReason: reason,
      });
    } else {
      // Backoff exponencial: base, 2×base, 4×base...
      message.state = 'waiting';
      message.availableAt = Date.now() + config.retryBackoffSeconds * 1000 * 2 ** (message.attempts - 1);
    }
    return { status: 200 };
  }

  async deadLetters(topic, queue) {
    await delay(2);
    if (!this.leader()) {
      return { status: 503 };
    }
    return { status: 200, items: this.queueState(topic, queue).dead };
  }

  /** Como "kubectl delete pod": con force, sin aviso (no suelta el Lease); sin force, apagado limpio. */
  async kill(name, force) {
    const broker = this.byName(name);
    if (!broker || broker.status === 'down') {
      return;
    }

    const now = Date.now();
    broker.status = 'down';
    broker.changedAt = now;
    broker.restartAt = now + SIM.restartMs;
    if (this.leaderName === name) {
      this.leaderName = null;
    }
    if (this.promotion?.broker === name) {
      this.promotion = null;
    }
    if (this.lease.holder === name && !force) {
      this.lease = { ...this.lease, holder: null };
    }
  }

  // ---- El "cluster" ----

  tick() {
    const now = Date.now();
    this.restartPods(now);
    this.runLease(now);
    this.finishPromotion(now);
    const leader = this.leader();
    if (leader) {
      this.replicate(leader, now);
    }
    this.settlePending(now);
  }

  /** El StatefulSet recrea el pod: arranca, relee su disco y queda como seguidor. */
  restartPods(now) {
    for (const broker of this.brokers) {
      if (broker.status === 'down' && now >= broker.restartAt) {
        broker.status = 'starting';
        broker.changedAt = now;
        for (const [topic, records] of broker.logs) {
          const segments = Math.max(1, Math.ceil(records.length / 5000));
          this.log(broker.name, 'info', CATEGORY.storage,
            `Recovered ${topic} from disk: ${records.length} records in ${segments} segments, end offset ${records.length - 1}, high watermark ${broker.hw.get(topic)}.`);
        }
      } else if (broker.status === 'starting' && now - broker.changedAt >= SIM.startingMs) {
        broker.status = 'up';
        broker.changedAt = now;
      }
    }
  }

  /** El titular renueva cada 2 s; si nadie renueva durante 10 s (o lo soltó), lo toma otro con la época siguiente. */
  runLease(now) {
    const holder = this.byName(this.lease.holder);
    const leading = holder?.status === 'up' && (this.leaderName === holder.name || this.promotion?.broker === holder.name);
    if (leading) {
      if (now - this.lease.renewedAt >= SIM.retryPeriodMs) {
        this.lease.renewedAt = now;
      }
      return;
    }

    const next = holder?.status === 'up' ? holder : this.pickCandidate(now);
    if (!next) {
      return;
    }

    const epoch = this.lease.transitions + 1;
    this.lease = { holder: next.name, transitions: epoch, renewedAt: now };
    this.leaderName = null;
    this.promotion = { broker: next.name, epoch, readyAt: now + SIM.promotionMs };
    this.log(next.name, 'info', CATEGORY.elector, `Lease tomado para la época ${epoch}; poniéndose al día antes de liderar.`);
    this.log(next.name, 'info', CATEGORY.promotion, `Getting ready to lead epoch ${epoch}.`);
  }

  /**
   * Quién toma un Lease libre o vencido. En el cluster gana el primer compare-and-swap que llega al API server: acá,
   * uno al azar. (Un titular que volvió con el Lease a su nombre lo retoma sin esperar: eso lo resuelve runLease.)
   */
  pickCandidate(now) {
    // Cada seguidor mira el Lease una vez por ciclo (RetryPeriod), no en cada tick.
    if (now - this.lastLeaseAttempt < SIM.retryPeriodMs) {
      return null;
    }
    const expired = this.lease.holder === null || now - this.lease.renewedAt >= SIM.leaseDurationMs;
    const candidates = this.brokers.filter((b) => b.status === 'up');
    if (!expired || candidates.length === 0) {
      return null;
    }
    this.lastLeaseAttempt = now;
    return candidates[Math.floor(Math.random() * candidates.length)];
  }

  /** Se pone al día con el seguidor más completo (DEC-002) y recién entonces lidera. Sin quórum, suelta el Lease. */
  finishPromotion(now) {
    const promotion = this.promotion;
    if (!promotion || now < promotion.readyAt) {
      return;
    }

    this.promotion = null;
    const broker = this.byName(promotion.broker);
    if (broker.status !== 'up' || this.lease.holder !== broker.name) {
      return;
    }

    const peers = this.brokers.filter((b) => b !== broker && b.status === 'up');
    if (peers.length + 1 < SIM.quorum) {
      this.log(broker.name, 'fail', CATEGORY.elector, `No se pudo liderar la época ${promotion.epoch}; se libera el Lease.`);
      this.lease = { ...this.lease, holder: null };
      return;
    }

    for (const topic of this.topics.keys()) {
      const own = broker.logs.get(topic);
      const richest = peers.reduce((best, peer) => (peer.logs.get(topic).length > best.logs.get(topic).length ? peer : best), broker);
      if (richest !== broker) {
        const records = richest.logs.get(topic);
        this.log(broker.name, 'info', CATEGORY.promotion,
          `Copying ${topic} up to offset ${records.length - 1} from ${richest.name}, whose log is more complete.`);
        own.length = 0;
        own.push(...records);
      }
    }

    this.leaderName = broker.name;
    this.unreachable = new Set();
    this.log(broker.name, 'info', CATEGORY.promotion, `Ready to lead epoch ${promotion.epoch}.`);
    this.log(broker.name, 'info', CATEGORY.elector, `Líder de la época ${promotion.epoch}.`);
    this.log(broker.name, 'info', CATEGORY.replicator, `Leading at epoch ${promotion.epoch}, replicating to ${peers.length} broker(s).`);
  }

  /** Copia lo nuevo del líder a cada seguidor vivo y avanza el high watermark cuando 2 de 3 lo tienen. */
  replicate(leader, now) {
    const followers = this.brokers.filter((b) => b !== leader && b.status === 'up');
    this.reportPeers(leader, followers);

    for (const topic of this.topics.keys()) {
      const source = leader.logs.get(topic);
      for (const follower of followers) {
        const target = follower.logs.get(topic);
        const keep = SimulatedCluster.commonPrefix(source, target);
        if (keep < target.length) {
          this.log(follower.name, 'warn', CATEGORY.append,
            `Truncating the divergent tail of ${topic}: keeping up to offset ${keep - 1}, the local end offset was ${target.length - 1}.`);
          target.length = keep;
          follower.hw.set(topic, Math.min(follower.hw.get(topic), keep - 1));
        }
        const end = Math.min(source.length, target.length + SIM.catchUpPerTick);
        for (let i = target.length; i < end; i++) {
          target.push(source[i]);
        }
      }

      const ends = [source.length, ...followers.map((f) => f.logs.get(topic).length)].sort((a, b) => b - a);
      if (ends.length >= SIM.quorum) {
        leader.hw.set(topic, Math.max(leader.hw.get(topic), ends[SIM.quorum - 1] - 1));
      }
    }

    // Un seguidor se entera del high watermark en el siguiente heartbeat.
    if (now - this.lastHeartbeat >= SIM.heartbeatMs) {
      this.lastHeartbeat = now;
      for (const follower of followers) {
        for (const topic of this.topics.keys()) {
          follower.hw.set(topic, Math.min(leader.hw.get(topic), follower.logs.get(topic).length - 1));
        }
      }
    }
  }

  /** Lo que el líder loguea cuando un seguidor deja de contestar y cuando vuelve. */
  reportPeers(leader, followers) {
    for (const broker of this.brokers) {
      if (broker === leader) {
        continue;
      }
      const reachable = followers.includes(broker);
      if (!reachable && !this.unreachable.has(broker.name)) {
        this.unreachable.add(broker.name);
        for (const topic of this.topics.keys()) {
          this.log(leader.name, 'warn', CATEGORY.peer, `Replicating ${topic} to ${broker.name} failed; retrying with backoff.`);
        }
      } else if (reachable && this.unreachable.delete(broker.name)) {
        for (const topic of this.topics.keys()) {
          this.log(leader.name, 'info', CATEGORY.peer, `${broker.name} answers again for ${topic}.`);
        }
      }
    }
  }

  /** Responde a cada publicación cuando su offset queda confirmado, o con error si el líder cayó o pasaron 15 s. */
  settlePending(now) {
    const leader = this.leader();
    this.pending = this.pending.filter((p) => {
      if (leader?.name === p.leader && leader.hw.get(p.topic) >= p.offset) {
        p.resolve({ status: 200, offset: p.offset });
      } else if (leader?.name !== p.leader && this.lease.holder !== p.leader) {
        p.resolve({ status: 0 });
      } else if (now >= p.deadline) {
        p.resolve({ status: 0 });
      } else {
        return true;
      }
      return false;
    });
  }

  // ---- Ayudas ----

  addTopic(name, maxRetries, retryBackoffSeconds, origin) {
    this.topics.set(name, { name, partitions: 1, maxRetries, retryBackoffSeconds, origin });
    for (const broker of this.brokers) {
      broker.logs.set(name, []);
      broker.hw.set(name, -1);
    }
  }

  /** Historia previa repartida en varias épocas, para que el tópico no arranque vacío. */
  seed(topic, count) {
    const start = Date.now() - count * 1000;
    const records = [];
    for (let offset = 0; offset < count; offset++) {
      records.push({
        offset,
        epoch: 1 + Math.floor((offset / count) * 7),
        value: sampleValue(topic, offset),
        timestamp: start + offset * 1000,
      });
    }
    for (const broker of this.brokers) {
      broker.logs.get(topic).push(...records);
      broker.hw.set(topic, count - 1);
    }
  }

  /** Cuántos registros tienen en común desde el principio. Comparten los objetos, así que alcanza con la identidad. */
  static commonPrefix(source, target) {
    let length = Math.min(source.length, target.length);
    while (length > 0 && source[length - 1] !== target[length - 1]) {
      length--;
    }
    return length;
  }

  queueState(topic, queue) {
    const key = `${topic}/${queue}`;
    if (!this.queues.has(key)) {
      this.queues.set(key, { cursor: 0, messages: new Map(), dead: [] });
    }
    return this.queues.get(key);
  }

  roleOf(broker) {
    if (broker.name === this.leaderName) {
      return 'leader';
    }
    if (broker.name === this.promotion?.broker) {
      return 'promoting';
    }
    return broker.status === 'up' ? 'follower' : 'none';
  }

  byName(name) {
    return this.brokers.find((b) => b.name === name) ?? null;
  }

  leader() {
    const broker = this.byName(this.leaderName);
    return broker?.status === 'up' ? broker : null;
  }

  /** Los eventos de cola también se escriben con quórum: sin 2 de 3, la cola no avanza. */
  writableLeader() {
    const leader = this.leader();
    const alive = this.brokers.filter((b) => b.status === 'up').length;
    return leader && alive >= SIM.quorum ? leader : null;
  }

  log(broker, level, category, message) {
    this.logBuffer.push({ broker, time: Date.now(), level, category, message });
  }
}

/** Un mensaje con cara de pedido o de factura. */
function sampleValue(topic, n) {
  const clients = ['ana', 'bruno', 'carla', 'diego', 'eva', 'facu', 'gabi', 'hugo'];
  const client = clients[n % clients.length];
  const amount = 500 + ((n * 7919) % 9500);
  return topic === 'facturas'
    ? JSON.stringify({ factura: 30000 + n, cliente: client, monto: amount })
    : JSON.stringify({ pedido: 1000 + n, cliente: client, total: amount });
}
