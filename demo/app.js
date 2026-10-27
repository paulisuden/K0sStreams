'use strict';

// Consola de la demo: muestra en vivo lo que hace el cluster (Lease, épocas, réplicas, cola, logs) y permite generar
// carga y provocar fallas. Habla con una "fuente": el cluster simulado (sim.js) o el real (live.js), con la misma
// interfaz. Abierta como archivo usa el simulado; servida por nginx en el cluster, el real. ?mode=sim fuerza el simulado.
//
// El panel de la cola se arma con lo que hacen el productor y los consumidores de esta página: es lo que vería
// cualquier cliente. Sirve igual con el cluster simulado y con el real, sin pedirle nada nuevo a la API.

const QUEUE = 'demo';
const POLL_MS = 500;
const VISIBILITY_SECONDS = 15;
const HISTORY_MS = 60_000;
const RATE_WINDOW_MS = 5_000;
const MAX_LOG_LINES = 400;
const MAX_MESSAGES = 80;
const MAX_EVENTS = 60;
const CHIPS_PER_COLUMN = 12;
const NODE_POSITIONS = { 'broker-0': [260, 100], 'broker-1': [100, 290], 'broker-2': [420, 290] };
const FAILURE_REASONS = ['timeout del servicio de pagos', 'stock insuficiente', 'dirección inválida', 'error 500 del ERP'];

// Líneas de log que cuentan algo del failover: se resaltan y entran en la línea de tiempo.
const KEY_LOGS = [
  {
    pattern: /Truncating the divergent tail of (\S+): keeping up to offset (-?\d+), the local end offset was (-?\d+)/,
    event: (m, broker) => ['warn', '✂', `${broker} descarta ${Number(m[3]) - Number(m[2])} registro(s) sin confirmar de ${m[1]}`,
      'tenía una cola que el nuevo líder no tiene'],
  },
  {
    pattern: /Copying (\S+) up to offset (\d+) from (\S+), whose log is more complete/,
    event: (m, broker) => ['ok', '⇄', `${broker} copia ${m[1]} de ${m[3]} antes de liderar`, `hasta el offset ${m[2]}`],
  },
  {
    pattern: /No se pudo liderar la época (\d+)/,
    event: (m, broker) => ['danger', '✖', `${broker} no puede liderar la época ${m[1]}`, 'sin quórum suelta el Lease'],
  },
  {
    pattern: /Tópico (\S+) creado desde la CRD Topic/,
    event: (m) => ['ok', '＋', `Tópico ${m[1]} creado desde la CRD`, 'kubectl apply -f topic.yaml'],
  },
  { pattern: /Lease tomado para la época/ },
  { pattern: /Líder de la época/ },
  { pattern: /Leading at epoch/ },
  { pattern: /Getting ready to lead/ },
  { pattern: /Recovered \S+ from disk/ },
  { pattern: /failed; retrying with backoff/ },
  { pattern: /answers again/ },
];

const TOPIC_NAME = /^[a-z0-9][a-z0-9._-]{0,99}$/;

let source = null;

const state = {
  topic: null,
  snapshot: null,
  hwHistory: new Map(),
  producer: { running: false, rate: 8, credit: 0, sequence: 5000 },
  consumers: { running: false, count: 4, errorRate: 0, generation: 0 },
  flow: emptyFlow(),
  history: { next: null },
  events: [],
  logs: [],
  logFilter: 'all',
  expiredLeases: new Set(),
  polling: false,
};

function emptyFlow() {
  return {
    published: [],
    inflight: new Map(),
    retrying: new Map(),
    acked: [],
    dead: [],
    publishedTimes: [],
    ackedTimes: [],
    counts: { published: 0, failed: 0, acked: 0, nacked: 0, redelivered: 0 },
    lastFailure: null,
    maxDelivered: -1,
  };
}

// ---- Sondeo ----

async function poll() {
  if (state.polling) {
    return;
  }
  state.polling = true;
  try {
    const previous = state.snapshot;
    state.snapshot = await source.snapshot();
    // Primero los logs: lo que cuentan pasó antes de este snapshot, y en la línea de tiempo tiene que quedar debajo.
    handleLogs(await source.logs());
    detectEvents(previous, state.snapshot);
    recordHistory(state.snapshot);
    renderTopicOptions(state.snapshot);
    await refreshHistory();
    await refreshDeadLetters();
    render(previous);
  } catch (error) {
    console.error(error);
  } finally {
    state.polling = false;
  }
}

/** Compara con el sondeo anterior: lo que cambió entre los dos es un evento de la línea de tiempo. */
function detectEvents(previous, next) {
  if (!previous) {
    addEvent('ok', '●', `Conectado: ${next.leader ?? 'sin líder'} lidera la época ${next.epoch}`,
      `${next.brokers.filter((b) => b.status === 'up').length} de 3 brokers vivos`);
    return;
  }

  for (const broker of next.brokers) {
    const before = previous.brokers.find((b) => b.name === broker.name);
    if (before.status === broker.status) {
      continue;
    }
    if (broker.status === 'down') {
      addEvent('danger', '✖', `${broker.name} cayó${before.role === 'leader' ? ' (era el líder)' : ''}`,
        before.role === 'leader' ? 'las escrituras quedan sin líder' : 'con 2 de 3 se sigue confirmando');
    } else if (broker.status === 'starting') {
      addEvent('warn', '↻', `${broker.name} arranca`, 'el StatefulSet lo recrea y relee su log del disco');
    } else if (broker.status === 'up' && before.status === 'starting') {
      addEvent('ok', '✔', `${broker.name} volvió`, 'se pone al día copiando del líder');
    }
  }

  const lease = next.lease;
  if (previous.lease.holder !== lease.holder) {
    if (lease.holder === null) {
      addEvent('warn', '🔓', `${previous.lease.holder} soltó el Lease`, 'apagado limpio: otro lo toma en el próximo ciclo');
    } else {
      addEvent('leader', '🔑', `${lease.holder} tomó el Lease`, `época ${lease.transitions} (leaseTransitions)`);
    }
  } else if (previous.lease.transitions !== lease.transitions) {
    addEvent('leader', '🔑', `${lease.holder} retomó su Lease`, `época ${lease.transitions}: volvió antes de que venciera`);
  }

  const holder = next.brokers.find((b) => b.name === lease.holder);
  if (holder && holder.status !== 'up' && leaseRemainingMs(next) <= 0 && !state.expiredLeases.has(lease.transitions)) {
    state.expiredLeases.add(lease.transitions);
    addEvent('warn', '⏱', `Venció el Lease de ${lease.holder}`, `${lease.durationSeconds} s sin renovar: cualquiera lo puede tomar`);
  }

  if (previous.leader !== next.leader) {
    if (next.leader) {
      addEvent('leader', '♛', `${next.leader} lidera la época ${next.epoch}`, 'las escrituras vuelven a confirmarse');
    } else {
      addEvent('danger', '⚠', 'Sin líder', 'las escrituras responden 503 o no responden');
    }
  }
}

function handleLogs(lines) {
  for (const line of lines) {
    const rule = KEY_LOGS.find((r) => r.pattern.test(line.message));
    line.key = Boolean(rule);
    if (rule?.event) {
      const [kind, icon, text, detail] = rule.event(line.message.match(rule.pattern), line.broker);
      addEvent(kind, icon, text, detail, line.time);
    }
    state.logs.push(line);
    appendLog(line);
  }
  if (state.logs.length > MAX_LOG_LINES) {
    state.logs.splice(0, state.logs.length - MAX_LOG_LINES);
    trimList(document.getElementById('logs'), MAX_LOG_LINES, 'start');
  }
}

/** HW del líder (o del más avanzado) por tópico, para calcular mensajes por segundo. */
function recordHistory(snapshot) {
  for (const topic of snapshot.topics) {
    const hw = Math.max(...snapshot.brokers.map((b) => b.hw[topic.name] ?? -1));
    const samples = state.hwHistory.get(topic.name) ?? [];
    samples.push({ time: snapshot.time, hw });
    while (samples.length && samples[0].time < snapshot.time - HISTORY_MS) {
      samples.shift();
    }
    state.hwHistory.set(topic.name, samples);
  }
}

async function refreshHistory() {
  const topic = state.topic;
  if (!topic) {
    return;
  }
  const leaderHw = topicHw(topic);
  if (state.history.next === null) {
    state.history.next = Math.max(0, leaderHw - 30);
  }

  const result = await source.read(topic, state.history.next, 60);
  if (result.status !== 200 || topic !== state.topic) {
    return;
  }
  state.history.next = result.nextOffset;
  appendMessages(result.messages);

  // Lo publicado por esta página aparece en "En el tópico" recién cuando el broker lo confirmó (200).
}

async function refreshDeadLetters() {
  const flow = state.flow;
  if (!state.topic) {
    return;
  }
  const result = await source.deadLetters(state.topic, QUEUE);
  if (result.status !== 200 || flow !== state.flow) {
    return;
  }
  flow.dead = [...result.items].reverse();
  for (const item of result.items) {
    flow.retrying.delete(item.offset);
  }
}

// ---- Productor y consumidores ----

function producerTick() {
  const producer = state.producer;
  if (!producer.running || !state.topic) {
    return;
  }
  producer.credit += producer.rate / 10;
  while (producer.credit >= 1) {
    producer.credit -= 1;
    publishOne();
  }
}

async function publishOne() {
  const flow = state.flow;
  const topic = state.topic;
  const value = sampleValue(topic, state.producer.sequence++);
  const result = await source.publish(topic, value).catch(() => ({ status: 0 }));
  if (flow !== state.flow) {
    return;
  }
  if (result.status === 200) {
    flow.counts.published++;
    flow.publishedTimes.push(Date.now());
    flow.published.unshift({ offset: result.offset, value });
    flow.published.length = Math.min(flow.published.length, CHIPS_PER_COLUMN);
  } else {
    flow.counts.failed++;
    flow.lastFailure = { status: result.status, time: Date.now() };
  }
}

function startConsumers() {
  const consumers = state.consumers;
  consumers.generation++;
  for (let id = 0; id < consumers.count; id++) {
    consumeLoop(consumers.generation);
  }
}

async function consumeLoop(generation) {
  while (state.consumers.running && state.consumers.generation === generation) {
    const topic = state.topic;
    if (!topic) {
      await sleep(1000);
      continue;
    }
    const result = await source.receive(topic, QUEUE, VISIBILITY_SECONDS).catch(() => ({ status: 0 }));
    if (result.status === 200) {
      await handleDelivery(topic, result.delivery);
    } else {
      // 204: no hay nada que entregar. Otro código: sin líder o sin quórum, se reintenta más tarde.
      await sleep(result.status === 204 ? 400 : 1000);
    }
  }
}

/** "Procesa" el mensaje un rato y lo confirma, o lo devuelve con nack según el porcentaje de errores elegido. */
async function handleDelivery(topic, delivery) {
  const flow = state.flow;
  const item = {
    offset: delivery.offset,
    value: delivery.value,
    attempt: delivery.attempt,
    processMs: 250 + Math.random() * 900,
  };
  if (delivery.attempt > 1) {
    flow.counts.redelivered++;
  }
  flow.maxDelivered = Math.max(flow.maxDelivered, delivery.offset);
  flow.retrying.delete(item.offset);
  flow.inflight.set(item.offset, item);

  await sleep(item.processMs);
  if (flow !== state.flow) {
    return;
  }

  const fails = Math.random() * 100 < state.consumers.errorRate;
  const reason = fails ? pick(FAILURE_REASONS) : null;
  const result = fails
    ? await source.nack(topic, QUEUE, delivery.partition, delivery.offset, reason).catch(() => ({ status: 0 }))
    : await source.ack(topic, QUEUE, delivery.partition, delivery.offset).catch(() => ({ status: 0 }));
  flow.inflight.delete(item.offset);

  if (result.status !== 200) {
    // Sin respuesta del líder, el mensaje sigue "en vuelo" en el broker y vuelve cuando vence la visibilidad.
    flow.retrying.set(item.offset, { ...item, reason: `${fails ? 'nack' : 'ack'} sin respuesta (${describeStatus(result.status)})`,
      retryAt: Date.now() + VISIBILITY_SECONDS * 1000 });
  } else if (fails) {
    flow.counts.nacked++;
    const backoff = topicConfig(topic)?.retryBackoffSeconds ?? 5;
    flow.retrying.set(item.offset, { ...item, reason, retryAt: Date.now() + backoff * 1000 * 2 ** (item.attempt - 1) });
  } else {
    flow.counts.acked++;
    flow.ackedTimes.push(Date.now());
    flow.acked.unshift(item);
    flow.acked.length = Math.min(flow.acked.length, CHIPS_PER_COLUMN);
  }
}

// ---- Dibujo ----

function render(previous) {
  const snapshot = state.snapshot;
  renderStats(snapshot);
  renderCluster(snapshot);
  renderPulses(previous, snapshot);
  renderLease(snapshot);
  renderFlow();
  renderTopics(snapshot);
  renderFaultButtons(snapshot);
}

function renderStats(snapshot) {
  const alive = snapshot.brokers.filter((b) => b.status === 'up').length;
  setText('stat-epoch', snapshot.epoch);
  setText('stat-leader', snapshot.leader ?? '—');
  setText('stat-alive', `${alive} / ${snapshot.brokers.length}`);
  setText('stat-published', perSecond(state.flow.publishedTimes).toFixed(0));
  setText('stat-acked', perSecond(state.flow.ackedTimes).toFixed(0));
}

function renderCluster(snapshot) {
  const leader = snapshot.brokers.find((b) => b.role === 'leader');
  const leaderHw = leader?.hw[state.topic] ?? null;

  const edges = [];
  const names = Object.keys(NODE_POSITIONS);
  for (let i = 0; i < names.length; i++) {
    for (let j = i + 1; j < names.length; j++) {
      const [a, b] = [names[i], names[j]];
      const ends = [a, b].map((name) => snapshot.brokers.find((x) => x.name === name));
      const replicating = leader && ends.includes(leader) && ends.every((x) => x.status === 'up');
      const broken = ends.some((x) => x.status === 'down');
      const [x1, y1] = NODE_POSITIONS[a];
      const [x2, y2] = NODE_POSITIONS[b];
      const kind = replicating ? 'edge--replicating' : broken ? 'edge--broken' : '';
      edges.push(`<line class="edge ${kind}" x1="${x1}" y1="${y1}" x2="${x2}" y2="${y2}"/>`);
    }
  }
  document.getElementById('edges').innerHTML = edges.join('');

  document.getElementById('nodes').innerHTML = snapshot.brokers.map((broker) => {
    const [x, y] = NODE_POSITIONS[broker.name];
    const hw = broker.hw[state.topic];
    const lag = leaderHw !== null && hw !== undefined && broker.role === 'follower' ? leaderHw - hw : 0;
    const status = broker.status === 'up' ? broker.role : broker.status;
    const label = {
      leader: `líder · época ${snapshot.epoch}`,
      follower: 'seguidor',
      promoting: 'tomando el mando',
      starting: 'arrancando',
      down: 'caído',
      none: '',
    }[status];
    return `
      <g class="node node--${status}" transform="translate(${x} ${y})">
        <circle class="node__halo" r="60"/>
        <circle class="node__body" r="52"/>
        ${broker.role === 'leader' ? '<text class="node__crown" y="-62">♛</text>' : ''}
        <text class="node__name" y="-10">${broker.name}</text>
        <text class="node__role" y="9">${label}</text>
        <text class="node__hw" y="29">HW ${hw ?? '–'}</text>
        ${lag > 0 ? `<g class="lag" transform="translate(44 -44)"><rect x="-22" y="-11" width="44" height="22" rx="11"/><text y="4">−${lag}</text></g>` : ''}
      </g>`;
  }).join('');
}

/** Un punto que viaja del líder a cada seguidor cuando el high watermark del tópico avanzó: una réplica confirmada. */
function renderPulses(previous, snapshot) {
  const leader = snapshot.brokers.find((b) => b.role === 'leader');
  const before = previous?.brokers.find((b) => b.name === leader?.name);
  if (!leader || !before || leader.hw[state.topic] <= before.hw[state.topic]) {
    return;
  }

  const layer = document.getElementById('pulses');
  const [x1, y1] = NODE_POSITIONS[leader.name];
  for (const follower of snapshot.brokers.filter((b) => b.role === 'follower')) {
    const [x2, y2] = NODE_POSITIONS[follower.name];
    const pulse = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
    pulse.setAttribute('r', '5');
    pulse.setAttribute('class', 'pulse');
    pulse.innerHTML = `<animateMotion dur="0.6s" fill="freeze" path="M${x1},${y1} L${x2},${y2}"/>`;
    layer.appendChild(pulse);
    setTimeout(() => pulse.remove(), 650);
  }
}

function renderLease(snapshot) {
  const lease = snapshot.lease;
  const remaining = leaseRemainingMs(snapshot);
  const holder = snapshot.brokers.find((b) => b.name === lease.holder);
  const renewing = holder?.status === 'up';
  const fraction = lease.holder ? Math.max(0, remaining) / (lease.durationSeconds * 1000) : 0;
  const expiring = lease.holder && !renewing;
  document.getElementById('lease').innerHTML = `
    <div class="lease__title"><span>Lease <code>k0sstreams-leader</code></span><span>coordination.k8s.io/v1</span></div>
    <dt>titular</dt><dd>${lease.holder ?? 'libre'}</dd>
    <dt>época</dt><dd>${lease.transitions} <span class="mono" style="color:var(--muted)">(leaseTransitions)</span></dd>
    <dt>${expiring ? 'vence en' : 'renovado hace'}</dt>
    <dd>${lease.holder ? (expiring ? `${(Math.max(0, remaining) / 1000).toFixed(1)} s · nadie lo renueva` : `${((snapshot.time - lease.renewedAt) / 1000).toFixed(1)} s`) : '—'}</dd>
    <div class="lease__bar ${expiring ? 'lease__bar--expiring' : ''}"><span style="width:${(fraction * 100).toFixed(1)}%"></span></div>`;
}

function renderFlow() {
  const flow = state.flow;
  const now = Date.now();
  const hw = topicHw(state.topic);
  const pending = flow.maxDelivered >= 0 ? Math.max(0, hw - flow.maxDelivered) : null;

  setText('count-published', flow.counts.published);
  setText('count-inflight', flow.inflight.size);
  setText('count-retrying', flow.retrying.size);
  setText('count-acked', flow.counts.acked);
  setText('count-dead', flow.dead.length);
  setText('hint-published', pending === null ? 'confirmados por quórum' : `≈ ${pending} sin entregar todavía`);

  const failure = flow.lastFailure && now - flow.lastFailure.time < 5000
    ? ` · ⚠ ${flow.counts.failed} publicaciones fallidas (${describeStatus(flow.lastFailure.status)})`
    : '';
  setText('flow-summary', `${flow.counts.published} publicados · ${flow.counts.acked} ack · ${flow.counts.nacked} nack · `
    + `${flow.counts.redelivered} reentregas${failure}`);

  syncChips('col-published', flow.published, (m) => chip('published', m.offset, m.value));
  syncChips('col-inflight', [...flow.inflight.values()].reverse(), (m) => chip('inflight', m.offset, m.value,
    `intento ${m.attempt}`, null, m.processMs));
  syncChips('col-retrying', [...flow.retrying.values()].reverse(), (m) => chip('retrying', m.offset, m.value,
    `intento ${m.attempt}`, m.reason, Math.max(0, m.retryAt - now)));
  syncChips('col-acked', flow.acked, (m) => chip('acked', m.offset, m.value, m.attempt > 1 ? `intento ${m.attempt}` : null));
  syncChips('col-dead', flow.dead, (m) => chip('dead', m.offset, m.value, `${m.attempts} intentos`, m.lastReason));
}

function renderTopics(snapshot) {
  const leader = snapshot.brokers.find((b) => b.role === 'leader');
  document.getElementById('topics').innerHTML = snapshot.topics.map((topic) => {
    const samples = state.hwHistory.get(topic.name) ?? [];
    const leaderHw = leader?.hw[topic.name];
    const replicas = snapshot.brokers.map((broker) => {
      const hw = broker.hw[topic.name];
      const behind = leaderHw !== undefined && hw !== undefined && broker.role === 'follower' && hw < leaderHw;
      const kind = broker.status !== 'up' ? 'down' : broker.role === 'leader' ? 'leader' : behind ? 'behind' : '';
      return `<div class="replica replica--${kind}"><b>${broker.name}${broker.role === 'leader' ? ' ♛' : ''}</b>${hw ?? '–'}${behind ? ` <em>−${leaderHw - hw}</em>` : ''}</div>`;
    }).join('');
    return `
      <article class="topic ${topic.name === state.topic ? 'is-selected' : ''}">
        <div class="topic__head">
          <span class="topic__name">${topic.name}</span>
          <span class="topic__rate">${rateOf(samples).toFixed(0)} msgs/s</span>
        </div>
        <div class="topic__config"><span class="origin">${topic.origin ?? 'API'}</span>
          ${topic.partitions} partición · ${topic.maxRetries} reintentos · backoff ${topic.retryBackoffSeconds} s</div>
        ${sparkline(samples)}
        <div class="replicas">${replicas}</div>
      </article>`;
  }).join('');
}

/** La lista cambia cuando aparece un Topic nuevo o se crea uno desde la página. Sin tópico elegido, toma el primero. */
function renderTopicOptions(snapshot) {
  const select = document.getElementById('topic');
  const names = snapshot.topics.map((t) => t.name);
  if (names.join() !== [...select.options].map((o) => o.value).join()) {
    select.innerHTML = names.map((name) => `<option value="${name}">${name}</option>`).join('');
  }
  if (!state.topic && names.length > 0) {
    selectTopic(names[0]);
  }
  select.value = state.topic ?? '';
}

function renderFaultButtons(snapshot) {
  const leader = snapshot.leader;
  const followers = snapshot.brokers.filter((b) => b.role === 'follower');
  document.getElementById('kill-leader').disabled = !leader;
  document.getElementById('kill-leader-force').disabled = !leader;
  document.getElementById('kill-follower').disabled = followers.length === 0;
  document.getElementById('kill-quorum').disabled = !leader || followers.length === 0;
}

function addEvent(kind, icon, text, detail, time = Date.now()) {
  state.events.unshift({ kind, icon, text, detail, time });
  const list = document.getElementById('timeline');
  list.insertAdjacentHTML('afterbegin', `
    <li class="event event--${kind}">
      <time>${clock(time, false)}</time>
      <span class="event__icon">${icon}</span>
      <span class="event__text">${escapeHtml(text)}${detail ? `<small>${escapeHtml(detail)}</small>` : ''}</span>
    </li>`);
  trimList(list, MAX_EVENTS, 'end');
}

function appendLog(line) {
  if (state.logFilter !== 'all' && state.logFilter !== line.broker) {
    return;
  }
  const list = document.getElementById('logs');
  const atBottom = list.scrollTop + list.clientHeight >= list.scrollHeight - 30;
  list.insertAdjacentHTML('beforeend', logHtml(line));
  if (atBottom) {
    list.scrollTop = list.scrollHeight;
  }
}

function renderAllLogs() {
  const list = document.getElementById('logs');
  list.innerHTML = state.logs.filter((l) => state.logFilter === 'all' || l.broker === state.logFilter).map(logHtml).join('');
  list.scrollTop = list.scrollHeight;
}

function logHtml(line) {
  const category = line.category.split('.').pop();
  return `
    <li class="log log--${line.level} ${line.key ? 'log--key' : ''}">
      <time>${clock(line.time, true)}</time>
      <span class="log__broker">${line.broker}</span>
      <span class="log__level">${line.level}</span>
      <span class="log__message"><small>${category}</small> ${escapeHtml(line.message)}</span>
    </li>`;
}

function appendMessages(messages) {
  if (messages.length === 0) {
    return;
  }
  const list = document.getElementById('messages');
  const html = [...messages].reverse().map((m) => `
    <li class="message">
      <span class="message__offset">#${m.offset}</span>
      <span class="message__value" title="${escapeHtml(m.value)}">${escapeHtml(m.value)}</span>
    </li>`).join('');
  list.insertAdjacentHTML('afterbegin', html);
  trimList(list, MAX_MESSAGES, 'end');
}

/**
 * Actualiza una columna de fichas sin rehacerla entera: las que siguen quedan donde están (no se reinicia su animación),
 * las nuevas entran arriba y las que ya no están se van.
 */
function syncChips(id, items, render) {
  const list = document.getElementById(id);
  const wanted = items.slice(0, CHIPS_PER_COLUMN);
  // La clave lleva el intento: un mensaje que vuelve a reintento es una ficha nueva, con su intento y su espera.
  const keyOf = (item) => `${item.offset}-${item.attempt ?? item.attempts ?? ''}`;
  const existing = new Map([...list.children].map((element) => [element.dataset.key, element]));
  const keys = new Set(wanted.map(keyOf));
  for (const [key, element] of existing) {
    if (!keys.has(key)) {
      element.remove();
    }
  }

  let anchor = null;
  for (const item of wanted) {
    const key = keyOf(item);
    let element = existing.get(key);
    if (!element) {
      const template = document.createElement('template');
      template.innerHTML = render(item).trim();
      element = template.content.firstElementChild;
      element.dataset.key = key;
    }
    const inPlace = anchor ? anchor.nextElementSibling === element : list.firstElementChild === element;
    if (!inPlace) {
      if (anchor) {
        anchor.after(element);
      } else {
        list.prepend(element);
      }
    }
    anchor = element;
  }
}

function chip(kind, offset, value, badge = null, note = null, progressMs = null) {
  return `
    <li class="chip chip--${kind}">
      <span class="chip__top"><span class="chip__offset">#${offset}</span>${badge ? `<span class="chip__badge">${badge}</span>` : ''}</span>
      <span class="chip__value">${escapeHtml(value)}</span>
      ${note ? `<span class="chip__note">${escapeHtml(note)}</span>` : ''}
      ${progressMs ? `<span class="chip__progress" style="animation-duration:${progressMs.toFixed(0)}ms"></span>` : ''}
    </li>`;
}

function sparkline(samples) {
  const width = 300;
  const height = 38;
  const rates = [];
  for (let i = 2; i < samples.length; i += 2) {
    const a = samples[i - 2];
    const b = samples[i];
    rates.push(Math.max(0, ((b.hw - a.hw) * 1000) / Math.max(1, b.time - a.time)));
  }
  if (rates.length < 2) {
    return `<svg class="spark" viewBox="0 0 ${width} ${height}"></svg>`;
  }
  const max = Math.max(5, ...rates);
  const points = rates.map((r, i) => [(i / (rates.length - 1)) * width, height - 2 - (r / max) * (height - 4)]);
  const line = points.map(([x, y], i) => `${i ? 'L' : 'M'}${x.toFixed(1)},${y.toFixed(1)}`).join(' ');
  return `<svg class="spark" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">
    <path class="area" d="${line} L${width},${height} L0,${height} Z"/><path class="line" d="${line}"/></svg>`;
}

// ---- Acciones ----

function bindControls() {
  const topicSelect = document.getElementById('topic');
  topicSelect.addEventListener('change', () => selectTopic(topicSelect.value));

  const createForm = document.getElementById('create-topic');
  createForm.addEventListener('submit', async (event) => {
    event.preventDefault();
    const input = document.getElementById('new-topic');
    const name = input.value.trim();
    if (!TOPIC_NAME.test(name)) {
      return;
    }
    const result = await source.createTopic(name);
    if (result.status === 201 || result.status === 409) {
      addEvent('action', '＋', `Tópico ${name} ${result.status === 201 ? 'creado' : 'ya existía'}`, 'POST /v1/topics');
      input.value = '';
      await poll();
      selectTopic(name);
    } else {
      addEvent('danger', '✖', `No se pudo crear ${name}`, describeStatus(result.status));
    }
  });

  const producerToggle = document.getElementById('producer-toggle');
  producerToggle.addEventListener('click', () => {
    state.producer.running = !state.producer.running;
    producerToggle.classList.toggle('is-on', state.producer.running);
    producerToggle.textContent = state.producer.running ? '■ Detener' : '▶ Publicar';
  });
  bindRange('producer-rate', (value) => { state.producer.rate = value; });

  const consumerToggle = document.getElementById('consumer-toggle');
  consumerToggle.addEventListener('click', () => {
    state.consumers.running = !state.consumers.running;
    consumerToggle.classList.toggle('is-on', state.consumers.running);
    consumerToggle.textContent = state.consumers.running ? '■ Detener' : '▶ Consumir';
    if (state.consumers.running) {
      startConsumers();
    }
  });
  bindRange('consumer-count', (value) => {
    state.consumers.count = value;
    if (state.consumers.running) {
      startConsumers();
    }
  });
  bindRange('error-rate', (value) => { state.consumers.errorRate = value; });

  bindFault('kill-leader', () => [[state.snapshot.leader, false]], 'Apagado limpio del líder');
  bindFault('kill-leader-force', () => [[state.snapshot.leader, true]], 'Corte de luz en el líder');
  bindFault('kill-follower', () => [[pick(followerNames()), true]], 'Corte de luz en un seguidor');
  bindFault('kill-quorum', () => [[state.snapshot.leader, true], [pick(followerNames()), true]], 'Sin quórum: líder y un seguidor');

  for (const tab of document.querySelectorAll('#log-tabs .tab')) {
    tab.addEventListener('click', () => {
      state.logFilter = tab.dataset.broker;
      document.querySelectorAll('#log-tabs .tab').forEach((t) => t.classList.toggle('is-active', t === tab));
      renderAllLogs();
    });
  }
}

function bindRange(id, apply) {
  const input = document.getElementById(id);
  const output = document.getElementById(`${id}-value`);
  input.addEventListener('input', () => {
    apply(Number(input.value));
    if (output) {
      output.textContent = input.value;
    }
  });
}

function bindFault(id, targets, description) {
  document.getElementById(id).addEventListener('click', async () => {
    const victims = targets().filter(([name]) => name);
    if (victims.length === 0) {
      return;
    }
    const names = victims.map(([name, force]) => `${name}${force ? ' (sin aviso)' : ''}`).join(' y ');
    addEvent('action', '⚡', `Acción: ${description}`, `kubectl delete pod ${names}`);
    await Promise.all(victims.map(([name, force]) => source.kill(name, force)));
    poll();
  });
}

function selectTopic(topic) {
  state.topic = topic;
  state.flow = emptyFlow();
  state.history = { next: null };
  for (const id of ['col-published', 'col-inflight', 'col-retrying', 'col-acked', 'col-dead', 'messages']) {
    document.getElementById(id).innerHTML = '';
  }
  setText('messages-topic', topic);
  if (state.consumers.running) {
    startConsumers();
  }
}

async function createSource() {
  const requested = new URLSearchParams(location.search).get('mode');
  const live = requested ? requested === 'live' : location.protocol !== 'file:';
  return live ? LiveCluster.create() : new SimulatedCluster();
}

async function init() {
  source = await createSource();
  const mode = document.getElementById('mode');
  mode.textContent = source.mode === 'live' ? 'en vivo' : 'simulado';
  mode.classList.toggle('mode--live', source.mode === 'live');
  setText('flow-queue', QUEUE);
  bindControls();

  setInterval(poll, POLL_MS);
  setInterval(producerTick, 100);
  setInterval(() => { document.getElementById('clock').textContent = clock(Date.now(), false); }, 250);
  poll();
}

// ---- Ayudas ----

function topicHw(topic) {
  const snapshot = state.snapshot;
  if (!snapshot) {
    return -1;
  }
  return Math.max(...snapshot.brokers.map((b) => b.hw[topic] ?? -1));
}

function topicConfig(topic) {
  return state.snapshot?.topics.find((t) => t.name === topic) ?? null;
}

function followerNames() {
  return state.snapshot.brokers.filter((b) => b.role === 'follower').map((b) => b.name);
}

function leaseRemainingMs(snapshot) {
  return snapshot.lease.durationSeconds * 1000 - (snapshot.time - snapshot.lease.renewedAt);
}

function perSecond(times) {
  const since = Date.now() - RATE_WINDOW_MS;
  while (times.length && times[0] < since) {
    times.shift();
  }
  return times.length / (RATE_WINDOW_MS / 1000);
}

function rateOf(samples) {
  const recent = samples.filter((s) => s.time >= Date.now() - RATE_WINDOW_MS);
  if (recent.length < 2) {
    return 0;
  }
  const first = recent[0];
  const last = recent[recent.length - 1];
  return ((last.hw - first.hw) * 1000) / Math.max(1, last.time - first.time);
}

function describeStatus(status) {
  return { 0: 'sin respuesta', 404: '404', 503: '503 sin líder' }[status] ?? `HTTP ${status}`;
}

function trimList(list, max, from) {
  while (list.children.length > max) {
    (from === 'end' ? list.lastElementChild : list.firstElementChild).remove();
  }
}

function clock(time, millis) {
  const date = new Date(time);
  const text = date.toLocaleTimeString('es-AR', { hour12: false });
  return millis ? `${text}.${String(date.getMilliseconds()).padStart(3, '0')}` : text;
}

function setText(id, text) {
  document.getElementById(id).textContent = text;
}

function escapeHtml(text) {
  return String(text).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]);
}

function pick(items) {
  return items[Math.floor(Math.random() * items.length)];
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

init();
