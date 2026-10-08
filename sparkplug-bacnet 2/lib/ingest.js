/**
 * Plain-MQTT ingest: subscribes to a customer broker and turns every topic into a point on the
 * app's own layer, writing each new value as point data. The BACnet sync then treats these
 * points exactly like Sparkplug points.
 */
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { MqttClient } = require('./mqtt');
const E = require('./engine');

const FILES = {
  state: path.join(path.dirname(E.FILES.mapping), 'mqtt.json'),
  status: path.join(path.dirname(E.FILES.mapping), 'mqtt-status.json'),
  lock: path.join(path.dirname(E.FILES.mapping), 'mqtt.lock'),
};
const NS = '7d3f4c1e-6b1a-4c9e-9a51-2f0c8b6d5e11';

function uuidv5(name, ns = NS) {
  const nsb = Buffer.from(ns.replace(/-/g, ''), 'hex');
  const h = crypto.createHash('sha1').update(Buffer.concat([nsb, Buffer.from(name, 'utf8')])).digest();
  h[6] = (h[6] & 0x0f) | 0x50; h[8] = (h[8] & 0x3f) | 0x80;
  const x = h.subarray(0, 16).toString('hex');
  return `${x.slice(0, 8)}-${x.slice(8, 12)}-${x.slice(12, 16)}-${x.slice(16, 20)}-${x.slice(20)}`;
}

const VALUE_KEYS = ['value', 'Value', 'v', 'val', 'presentValue', 'pv', 'data'];
const TS_KEYS = ['ts', 'timestamp', 'Timestamp', 'time', 't'];

function scalarText(s) {
  const t = String(s).trim();
  if (/^(true|on|active)$/i.test(t)) return true;
  if (/^(false|off|inactive)$/i.test(t)) return false;
  if (t !== '' && !isNaN(Number(t))) return Number(t);
  return t;
}
function parseTs(v) {
  if (v === undefined || v === null || v === '') return null;
  if (typeof v === 'number') return new Date(v < 1e12 ? v * 1000 : v).toISOString();
  const d = new Date(v); return isNaN(d) ? null : d.toISOString();
}
function getPath(o, p) { return p.split('.').reduce((a, k) => (a && typeof a === 'object' ? a[k] : undefined), o); }

/** Payload → { value, ts }. Handles bare values, JSON scalars and JSON objects. */
function parsePayload(buf, jsonPath) {
  const text = buf.toString('utf8').trim();
  if (!text) return { value: null, ts: null };
  let j;
  try { j = JSON.parse(text); } catch (e) { return { value: scalarText(text), ts: null }; }
  if (j === null) return { value: null, ts: null };
  if (typeof j !== 'object') return { value: typeof j === 'string' ? scalarText(j) : j, ts: null };
  let v = jsonPath ? getPath(j, jsonPath) : undefined;
  if (v === undefined) for (const k of VALUE_KEYS) if (k in j) { v = j[k]; break; }
  let ts = null;
  for (const k of TS_KEYS) if (k in j) { ts = parseTs(j[k]); break; }
  if (v && typeof v === 'object') v = undefined;
  return { value: v === undefined ? null : (typeof v === 'string' ? scalarText(v) : v), ts };
}

function dataType(v) { return typeof v === 'boolean' ? 'Boolean' : typeof v === 'number' ? 'Float' : typeof v === 'string' ? 'String' : ''; }
function valueBody(v) {
  if (typeof v === 'boolean') return { boolean: v };
  if (typeof v === 'number') return { real: v };
  return { characterString: String(v) };
}

function pointFor(topic, value, cfg) {
  const strip = cfg.stripSuffix || '';
  const shown = strip && topic.endsWith(strip) ? topic.slice(0, -strip.length) : topic;
  const segs = shown.split('/');
  return {
    uuid: uuidv5(`${cfg.layer}|${topic}`),
    name: shown,
    layer: cfg.layer,
    protocol_id: topic,
    parent_name: segs.slice(0, -1).join('/'),
    attrs: {
      mqtt_topic: topic, connection_name: cfg.name || 'mqtt', device_id: segs.slice(0, -1).join('/'),
      metric_name: segs[segs.length - 1], datatype: dataType(value),
    },
  };
}

async function ensureLayer(api, cfg) {
  try {
    const res = await api.get(`/api/v1/point/layers/${encodeURIComponent(cfg.layer)}`);
    if ((res.layers || []).length) return;
  } catch (e) { if (e.status && e.status !== 404 && e.status !== 400) throw e; }
  const layer = {
    name: cfg.layer, description: 'MQTT topics ingested by the sparkplug-bacnet app', indexed: true,
    structuredComponents: ['mqtt_topic', 'connection_name', 'device_id', 'metric_name', 'datatype']
      .map(n => ({ name: n, type: 'TAG' })),
  };
  try { await api.post('/api/v1/point/layers', { layer: { kind: 'LAYER_BASE', ...layer } }); }
  catch (e) { await api.post('/api/v1/point/layers', { layer }); }
}

async function pool(items, n, fn) {
  let i = 0; const errs = [];
  await Promise.all(Array.from({ length: Math.min(n, items.length) }, async () => {
    while (i < items.length) { const it = items[i++]; try { await fn(it); } catch (e) { errs.push(e); } }
  }));
  return errs;
}


/** "10.94.2.181, 10.94.2.182:1883, mqtts://host" → full URLs (bare host = mqtt://host:1883). */
function brokerUrls(url) {
  return String(url || '').split(/[,\s;]+/).map(u => u.trim()).filter(Boolean)
    .map(u => (/^[a-z]+:\/\//i.test(u) ? u : 'mqtt://' + u));
}

/** Try each broker in turn, starting with the last one that worked. Returns { mq, url }. */
async function connectAny(cfg, clientId, preferred) {
  let urls = brokerUrls(cfg.url);
  if (preferred && urls.includes(preferred)) urls = [preferred, ...urls.filter(u => u !== preferred)];
  if (!urls.length) throw new Error('no broker URL configured');
  const errs = [];
  for (const url of urls) {
    const mq = new MqttClient({ url, username: cfg.username || undefined, password: cfg.password || undefined,
      ca: cfg.ca || undefined, insecure: !!cfg.insecure, clientId });
    try { await mq.connect(10000); return { mq, url, errs }; }
    catch (e) { errs.push(`${url}: ${e.message}`); try { mq.end(); } catch (x) { /* ignore */ } }
  }
  throw new Error('could not connect to any broker - ' + errs.join('; '));
}

/** Connect, subscribe, and keep writing values until maxSeconds. */
async function runIngest({ maxSeconds } = {}) {
  const mapping = E.loadMapping();
  const cfg = { ...E.DEFAULT_SETTINGS.mqtt, ...(mapping.settings.mqtt || {}) };
  const st = E.readJson(FILES.state, { known: {}, layerReady: false });
  const status = E.readJson(FILES.status, { log: [] });
  const say = (level, msg) => { E.log(status, level, 'MQTT: ' + msg); };
  if (!cfg.enabled || !cfg.url) { status.enabled = false; E.writeJson(FILES.status, status); return { skipped: 'MQTT ingest disabled' }; }

  const lock = E.readJson(FILES.lock, null);
  if (lock && Date.now() - lock.at < 90000) return { skipped: 'another MQTT run is active' };
  E.writeJson(FILES.lock, { at: Date.now(), pid: process.pid });

  const api = E.client(mapping.settings);
  const end = Date.now() + 1000 * (maxSeconds ?? mapping.settings.runSeconds);
  const buffer = new Map();
  let msgs = 0, lastMsgAt = status.lastMessageAt || null;
  let mq = null;
  const onMessage = (topic, payload) => {
    msgs++; lastMsgAt = new Date().toISOString();
    const p = parsePayload(payload, cfg.jsonPath);
    if (p.value === null || p.value === undefined) return;
    buffer.set(topic, { value: p.value, ts: p.ts || lastMsgAt });
  };

  async function flush() {
    if (!buffer.size) return;
    const batch = [...buffer.entries()]; buffer.clear();
    const fresh = batch.filter(([t]) => !st.known[t]);
    for (let i = 0; i < fresh.length; i += 250) {
      const chunk = fresh.slice(i, i + 250);
      await api.post('/api/v1/point/points', { points: chunk.map(([t, m]) => pointFor(t, m.value, cfg)) });
      for (const [t, m] of chunk) st.known[t] = { uuid: pointFor(t, m.value, cfg).uuid, type: dataType(m.value) };
      say('info', `added ${chunk.length} new topic(s) as points`);
    }
    const repost = Number(cfg.repostMinutes || 0) * 60000;
    const now = Date.now();
    const writes = batch.filter(([t, m]) => {
      const k = st.known[t];
      return k && (k.last !== m.value || (repost && now - (k.at || 0) > repost));
    });
    const errs = await pool(writes, 8, async ([t, m]) => {
      await api.post('/api/v1/point/data', { uuid: st.known[t].uuid, values: [{ ts: m.ts, ...valueBody(m.value) }] });
      st.known[t].last = m.value; st.known[t].at = now;
    });
    if (errs.length) say('error', `${errs.length} value write(s) failed: ${errs[0].message}`);
    status.lastFlush = { at: new Date().toISOString(), received: batch.length, written: writes.length - errs.length, errors: errs.length };
  }

  try {
    if (!st.layerReady) { await ensureLayer(api, cfg); st.layerReady = true; say('info', `layer ${cfg.layer} ready`); }
    const c = await connectAny(cfg, cfg.clientId || 'abound-normalizer-mqtt', status.broker);
    mq = c.mq; mq.on('message', onMessage);
    if (status.broker !== c.url) say(c.errs.length ? 'warn' : 'info', `connected to ${c.url}` + (c.errs.length ? ` (failed over: ${c.errs.join('; ')})` : ''));
    status.broker = c.url;
    const filters = String(cfg.topics || '#').split(',').map(s => s.trim()).filter(Boolean);
    await mq.subscribe(filters, Number(cfg.qos ?? 1));
    status.connected = true; status.error = ''; status.since = status.since || new Date().toISOString();
    while (Date.now() < end && mq.connected) {
      await new Promise(r => setTimeout(r, Math.max(1, Number(cfg.flushSeconds || 5)) * 1000));
      await flush();
      status.messages = (status.messages || 0); status.lastMessageAt = lastMsgAt; status.topics = Object.keys(st.known).length;
      E.writeJson(FILES.status, status); E.writeJson(FILES.state, st);
      E.writeJson(FILES.lock, { at: Date.now(), pid: process.pid });
    }
    await flush();
    if (!mq.connected) throw new Error('broker closed the connection (check that no other client uses the same client ID)');
  } catch (e) {
    status.connected = false; status.error = e.message; say('error', e.message);
  } finally {
    if (mq) mq.end();
    status.enabled = true; status.messagesLastRun = msgs; status.lastMessageAt = lastMsgAt;
    status.topics = Object.keys(st.known).length; status.lastRun = new Date().toISOString();
    if (!status.error) status.connected = true;
    E.writeJson(FILES.status, status); E.writeJson(FILES.state, st);
    try { fs.unlinkSync(FILES.lock); } catch (e) { /* gone */ }
  }
  return { messages: msgs, topics: Object.keys(st.known).length, error: status.error || '' };
}

/** Connection test for the Settings page: connect, subscribe, collect a sample for a few seconds. */
async function testConnection(cfgIn, seconds = 6) {
  const mapping = E.loadMapping();
  const cfg = { ...E.DEFAULT_SETTINGS.mqtt, ...(mapping.settings.mqtt || {}), ...(cfgIn || {}) };
  const sample = new Map(); let count = 0; let mq = null;
  const brokers = [];
  const onMsg = (t, p) => { count++; if (sample.size < 25 && !sample.has(t)) sample.set(t, { raw: p.toString('utf8').slice(0, 120), parsed: parsePayload(p, cfg.jsonPath).value }); };
  try {
    // check every listed broker so the user sees which ones answer, then sample from the first good one
    const clientId = (cfg.clientId || 'abound-normalizer-mqtt') + '-test';
    for (const url of brokerUrls(cfg.url)) {
      const t = new MqttClient({ url, username: cfg.username || undefined, password: cfg.password || undefined,
        ca: cfg.ca || undefined, insecure: !!cfg.insecure, clientId });
      try { await t.connect(5000); brokers.push({ url, ok: true }); } catch (e) { brokers.push({ url, ok: false, error: e.message }); }
      finally { try { t.end(); } catch (x) { /* ignore */ } }
    }
    const c = await connectAny(cfg, clientId);
    mq = c.mq; mq.on('message', onMsg);
    await mq.subscribe(String(cfg.topics || '#').split(',').map(s => s.trim()).filter(Boolean), Number(cfg.qos ?? 1));
    await new Promise(r => setTimeout(r, seconds * 1000));
    return { ok: true, broker: c.url, brokers, messages: count, sample: [...sample.entries()].map(([topic, s]) => ({ topic, ...s })) };
  } catch (e) {
    return { ok: false, error: e.message, brokers };
  } finally { if (mq) mq.end(); }
}

module.exports = { brokerUrls, connectAny, runIngest, testConnection, parsePayload, uuidv5, pointFor, FILES };
