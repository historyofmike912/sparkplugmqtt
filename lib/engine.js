/**
 * Sparkplug → BACnet mapping engine for the Abound Normalizer.
 *
 * Reads the Sparkplug Consumer's points (layer hpl:sparkplug:1), keeps one BACnet
 * local object per approved point, copies each new value to Present_Value, and sets
 * Status_Flags FAULT when a source stops reporting. Read-only toward the PLC.
 */
const fs = require('fs');
const path = require('path');
const { client } = require('./nfapi');

const APP_DIR = process.env.SPB_APP_DIR || path.join(__dirname, '..');
const DATA = path.join(APP_DIR, 'data');
const FILES = {
  mapping: path.join(DATA, 'mapping.json'),
  status: path.join(DATA, 'status.json'),
  runtime: path.join(DATA, 'runtime.json'),
  lock: path.join(DATA, 'sync.lock'),
};

const DEFAULT_SETTINGS = {
  layer: 'hpl:sparkplug:1',
  interval: 5,          // seconds between cycles
  runSeconds: 50,       // each scheduled run loops this long (schedule fires every minute)
  staleS: 60,           // seconds without an update before FAULT
  staleAction: 'flags', // 'flags' | 'none'
  faultBit: 1,          // Status_Flags bit 1 = fault
  deviceOffset: 0,      // local BACnet device (0 = the platform's own device)
  startInstance: 1,
  nameStyle: 'device',  // 'device' = Device.Metric, 'full' = Node.Device.Metric
  apiBase: '',          // blank = http://localhost:<PORT or 8080>
  apiToken: '',
  maxObjectsPerDevice: 250, // new tags move to the next device offset when a device is full
  mqtt: {
    enabled: false, name: 'mqtt', url: '', username: '', password: '', ca: '', insecure: false,
    topics: '#', clientId: 'abound-normalizer-mqtt', qos: 1, jsonPath: '', stripSuffix: '/Value',
    layer: 'mqtt-ingest', flushSeconds: 5, repostMinutes: 0,
  },
};

/** Layers the sync reads: the Sparkplug layer(s) plus the MQTT ingest layer when enabled. */
function sourceLayers(s) {
  const out = String(s.layer || '').split(',').map(x => x.trim()).filter(Boolean);
  if (s.mqtt && s.mqtt.enabled && s.mqtt.layer && !out.includes(s.mqtt.layer)) out.push(s.mqtt.layer);
  return out;
}
async function sourcePoints(api, s) {
  const all = [];
  for (const l of sourceLayers(s)) all.push(...await api.sparkplugPoints(l));
  return all;
}

const OBJ = { av: 'OBJECT_ANALOG_VALUE', bv: 'OBJECT_BINARY_VALUE', msv: 'OBJECT_MULTI_STATE_VALUE' };

const UNIT_MAP = {
  degf: 64, '°f': 64, f: 64, degc: 62, '°c': 62, c: 62, k: 63, '%': 98, pct: 98, percent: 98, '%rh': 29,
  lpm: 88, 'l/min': 88, 'liters/min': 88, lph: 136, psi: 56, inh2o: 58, 'in wc': 58, pa: 53, kpa: 54, bar: 55, a: 3, amps: 3, ma: 2, v: 5, volts: 5,
  kw: 48, w: 47, kwh: 19, hz: 27, rpm: 104, cfm: 84, gpm: 89, 'l/s': 87, fpm: 77, 'ft/min': 77,
  s: 73, sec: 73, min: 72, h: 71, hr: 71, ppm: 96,
};
const NO_UNITS = 95;

// ------------------------------------------------------------------ storage
function readJson(file, fallback) {
  try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch (e) { return fallback; }
}
function writeJson(file, obj) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const tmp = file + '.' + process.pid + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(obj, null, 1));
  fs.renameSync(tmp, file);
}
function loadMapping() {
  const m = readJson(FILES.mapping, {});
  const settings = { ...DEFAULT_SETTINGS, ...(m.settings || {}) };
  settings.mqtt = { ...DEFAULT_SETTINGS.mqtt, ...((m.settings || {}).mqtt || {}) };
  return { version: 1, rows: [], ...m, settings };
}
function saveMapping(m) { m.updatedAt = new Date().toISOString(); writeJson(FILES.mapping, m); }
function loadStatus() { return readJson(FILES.status, { log: [] }); }
function loadRuntime() { return readJson(FILES.runtime, { last: {}, stale: {}, pvEnc: {}, created: {} }); }

function log(status, level, msg) {
  status.log = status.log || [];
  status.log.unshift({ t: new Date().toISOString(), level, msg });
  status.log.length = Math.min(status.log.length, 200);
}

// ------------------------------------------------------------------ point helpers
const attr = (p, k) => { const v = (p.attrs || {})[k]; return v === undefined || v === null ? '' : String(v); };

function sourcePath(p) {
  if (attr(p, 'mqtt_topic')) return attr(p, 'mqtt_topic');
  const parts = [attr(p, 'edge_node_id'), attr(p, 'device_id'), attr(p, 'metric_name')].filter(Boolean);
  return parts.length ? parts.join('/') : (p.name || p.uuid);
}

function scalar(v) {
  if (v && typeof v === 'object') {
    for (const [k, x] of Object.entries(v)) if (!['ts', '@type', 'version', 'layer', 'array'].includes(k)) return scalar(x);
    return null;
  }
  if (typeof v === 'string') {
    const s = v.trim().toLowerCase();
    if (s === 'true' || s === 'false') return s === 'true';
    if (s !== '' && !isNaN(Number(v))) return Number(v);
  }
  return v === undefined ? null : v;
}

function latest(p) {
  const lv = p.latestValue || p.latest_value;
  if (!lv) return { value: null, ts: null };
  const ts = lv.ts ? Date.parse(typeof lv.ts === 'object' ? Number(lv.ts.seconds) * 1000 : lv.ts) : NaN;
  return { value: scalar(lv), ts: isNaN(ts) ? null : ts };
}

const HOUSEKEEPING = /^(bdSeq|Node Control\/|Properties\/|_types_\/)/;

function skipReason(p, value) {
  if (attr(p, 'mqtt_topic')) {
    if (typeof value === 'string' || attr(p, 'datatype') === 'String') return 'String: no BACnet object type';
    if (value && typeof value === 'object') return 'Structured value: not a single number or state';
    return '';
  }
  const dt = attr(p, 'sparkplug_datatype').toLowerCase();
  if (!attr(p, 'device_id')) return 'Node-level Sparkplug metric';
  if (HOUSEKEEPING.test(attr(p, 'metric_name'))) return 'Sparkplug control metric';
  if (dt.includes('template') || (value && typeof value === 'object')) return 'Template (UDT): publish members as individual metrics';
  if (dt.includes('dataset')) return 'Dataset metric';
  if (['string', 'text', 'uuid'].includes(dt) || typeof value === 'string') return 'String: no BACnet object type';
  return '';
}

function guessType(p, value) {
  const dt = (attr(p, 'sparkplug_datatype') || attr(p, 'datatype')).toLowerCase();
  if (dt.includes('bool') || typeof value === 'boolean') return 'bv';
  return 'av';
}

function unitsFor(p) { return UNIT_MAP[attr(p, 'engUnit').trim().toLowerCase()] || NO_UNITS; }

function cleanName(s) { return s.replace(/\//g, '.').replace(/[^A-Za-z0-9_.\-]/g, '_'); }

function round(v) { return typeof v === 'number' && isFinite(v) ? Number(v.toPrecision(7)) : v; }

function transform(row, value) {
  if (value === null || value === undefined) return null;
  if (row.bit !== '' && row.bit !== undefined && row.bit !== null) return ((Number(value) >> Number(row.bit)) & 1) === 1;
  if (row.objectType === 'bv') return Boolean(Number(value)) || value === true;
  if (row.objectType === 'msv') {
    // BACnet multi-state values start at 1; set offset to 1 for sources that count from 0
    const off = row.offset !== '' && row.offset !== undefined && row.offset !== null ? Number(row.offset) : 0;
    return Math.max(1, Math.round(Number(value) + off));
  }
  let v = Number(value);
  if (!isFinite(v)) return null;
  if (row.scale !== '' && row.scale !== undefined && row.scale !== null) v *= Number(row.scale);
  if (row.offset !== '' && row.offset !== undefined && row.offset !== null) v += Number(row.offset);
  return round(v);
}

const same = (a, b) => (typeof a === 'number' && typeof b === 'number') ? Math.abs(a - b) < 1e-9 : a === b;

// ------------------------------------------------------------------ BACnet object calls
function objBody(row, props) {
  return {
    local_device_instance_offset: Number(row.deviceOffset || 0),
    object_id: { object_type: OBJ[row.objectType], instance: Number(row.instance) },
    props,
  };
}

async function createObject(api, row) {
  const props = [
    { property: 'PROP_OBJECT_NAME', value: { character_string: row.name } },
    { property: 'PROP_DESCRIPTION', value: { character_string: String(row.path).slice(0, 120) } },
  ];
  if (row.objectType === 'av') props.push({ property: 'PROP_UNITS', value: { enumerated: Number(row.units || NO_UNITS) } });
  if (row.objectType === 'msv') props.push({ property: 'PROP_NUMBER_OF_STATES', value: { unsigned: Number(row.states || 4) } });
  await api.post('/api/v1/bacnet/local', objBody(row, props));
}

async function deleteObject(api, row) {
  const off = Number(row.deviceOffset || 0);
  await api.del(`/api/v1/bacnet/local/${OBJ[row.objectType]}/${Number(row.instance)}`,
    off ? { local_device_instance_offset: off } : undefined);
}

// Present_Value encodings per object type; the first the API accepts is remembered.
const PV_ENC = {
  av: [v => ({ real: Number(v) })],
  bv: [v => ({ enumerated: v ? 1 : 0 }), v => ({ boolean: Boolean(v) })],
  msv: [v => ({ unsigned: Number(v) }), v => ({ enumerated: Number(v) })],
};

async function writePV(api, row, value, rt) {
  const encs = PV_ENC[row.objectType];
  let lastErr;
  for (let i = rt.pvEnc[row.objectType] || 0; i < encs.length; i++) {
    try {
      await api.patch('/api/v1/bacnet/local', objBody(row, [{ property: 'PROP_PRESENT_VALUE', value: encs[i](value) }]));
      rt.pvEnc[row.objectType] = i;
      return;
    } catch (e) { lastErr = e; }
  }
  throw lastErr;
}

async function writeFlags(api, row, fault, bit) {
  await api.patch('/api/v1/bacnet/local', objBody(row, [{
    property: 'PROP_STATUS_FLAGS',
    value: { bit_string: { length: 4, set_bits: fault ? [Number(bit)] : [] } },
  }]));
}

// ------------------------------------------------------------------ planning
/** Add a draft row for every Sparkplug point not yet in the mapping. Never renumbers. */
function plan(mapping, points) {
  const s = mapping.settings;
  const known = new Set(mapping.rows.map(r => r.point));
  const next = {};
  for (const r of mapping.rows) {
    if (!r.objectType || !r.instance) continue;
    const k = `${r.deviceOffset || 0}:${r.objectType}`;
    next[k] = Math.max(next[k] || s.startInstance - 1, Number(r.instance));
  }
  const perDevice = {};
  for (const r of mapping.rows) if (r.enabled && r.objectType) perDevice[r.deviceOffset || 0] = (perDevice[r.deviceOffset || 0] || 0) + 1;
  let offsetNow = Number(s.deviceOffset || 0);
  const maxPer = Number(s.maxObjectsPerDevice || 0);
  let added = 0;
  for (const p of [...points].sort((a, b) => sourcePath(a).localeCompare(sourcePath(b)))) {
    if (known.has(p.uuid)) continue;
    const { value } = latest(p);
    const pathStr = sourcePath(p);
    const why = skipReason(p, value);
    const row = {
      key: p.uuid, point: p.uuid, path: pathStr, objectType: '', instance: '', deviceOffset: Number(s.deviceOffset || 0),
      name: cleanName(s.nameStyle === 'full' ? pathStr : pathStr.split('/').slice(1).join('/') || pathStr),
      units: '', states: '', bit: '', scale: '', offset: '', enabled: false, approved: false, note: why,
    };
    if (!why) {
      while (maxPer && (perDevice[offsetNow] || 0) >= maxPer) offsetNow++;
      row.deviceOffset = offsetNow;
      perDevice[offsetNow] = (perDevice[offsetNow] || 0) + 1;
      const t = guessType(p, value);
      const k = `${row.deviceOffset}:${t}`;
      next[k] = (next[k] || s.startInstance - 1) + 1;
      Object.assign(row, { objectType: t, instance: next[k], enabled: true, units: t === 'av' ? unitsFor(p) : '' });
    }
    mapping.rows.push(row);
    added++;
  }
  return added;
}

function nextInstance(mapping, offset, type) {
  let n = mapping.settings.startInstance - 1;
  for (const r of mapping.rows) if (r.objectType === type && Number(r.deviceOffset || 0) === Number(offset)) n = Math.max(n, Number(r.instance || 0));
  return n + 1;
}

/** Validate rows: unique instance per device/type, unique names, required fields. */
function validate(mapping) {
  const problems = [];
  const seenInst = new Map();
  const seenName = new Map();
  for (const r of mapping.rows) {
    if (!r.enabled) continue;
    if (!OBJ[r.objectType]) { problems.push(`${r.path}: choose av, bv or msv`); continue; }
    if (!(Number(r.instance) >= 0 && Number(r.instance) <= 4194302)) problems.push(`${r.path}: instance must be 0–4194302`);
    const ik = `${r.deviceOffset || 0}:${r.objectType}:${r.instance}`;
    if (seenInst.has(ik)) problems.push(`${r.objectType}.${r.instance} used by both ${seenInst.get(ik)} and ${r.path}`);
    seenInst.set(ik, r.path);
    const nk = `${r.deviceOffset || 0}:${r.name}`;
    if (!r.name) problems.push(`${r.path}: name is empty`);
    else if (seenName.has(nk)) problems.push(`Name "${r.name}" used twice`);
    seenName.set(nk, r.path);
  }
  const counts = {};
  for (const r of mapping.rows) if (r.enabled && OBJ[r.objectType]) counts[r.deviceOffset || 0] = (counts[r.deviceOffset || 0] || 0) + 1;
  const maxPer = Number(mapping.settings.maxObjectsPerDevice || 0);
  for (const [off, n] of Object.entries(counts)) if (maxPer && n > maxPer)
    problems.push(`Device offset ${off} has ${n} objects; the limit is ${maxPer}. Move some to another offset.`);
  return problems;
}

// ------------------------------------------------------------------ apply / remove
async function apply(mapping, api, rt, status, keys) {
  const rows = mapping.rows.filter(r => r.enabled && r.approved && OBJ[r.objectType] && (!keys || keys.includes(r.key)));
  const byOffset = {};
  let created = 0, failed = 0;
  for (const r of rows) {
    const off = Number(r.deviceOffset || 0);
    if (!byOffset[off]) byOffset[off] = await api.localObjects(off);
    const id = `${OBJ[r.objectType]}:${Number(r.instance)}`;
    if (byOffset[off].has(id)) { rt.created[r.key] = id; continue; }
    try {
      await createObject(api, r);
      rt.created[r.key] = id;
      delete rt.last[r.key];
      created++;
    } catch (e) {
      failed++;
      log(status, 'error', `Create ${r.objectType}.${r.instance} (${r.name}) failed: ${e.message}`);
    }
  }
  if (created || failed) log(status, failed ? 'warn' : 'info', `Created ${created} BACnet object(s)${failed ? `, ${failed} failed` : ''}`);
  return { created, failed };
}

async function remove(mapping, api, rt, status, keys) {
  let removed = 0;
  for (const r of mapping.rows.filter(x => keys.includes(x.key))) {
    if (OBJ[r.objectType] && r.instance !== '') {
      try { await deleteObject(api, r); removed++; } catch (e) {
        if (e.status !== 404) log(status, 'warn', `Delete ${r.objectType}.${r.instance} failed: ${e.message}`);
      }
    }
    r.enabled = false; r.approved = false;
    delete rt.created[r.key]; delete rt.last[r.key]; delete rt.stale[r.key];
  }
  log(status, 'info', `Removed ${removed} BACnet object(s)`);
  return removed;
}

// ------------------------------------------------------------------ one sync cycle
async function cycle(mapping, api, rt, status) {
  const s = mapping.settings;
  const t0 = Date.now();
  const points = await sourcePoints(api, s);
  const byUuid = new Map(points.map(p => [p.uuid, p]));
  const active = mapping.rows.filter(r => r.enabled && r.approved && OBJ[r.objectType]);
  if (active.some(r => !rt.created[r.key])) await apply(mapping, api, rt, status);

  const now = Date.now();
  let updated = 0, stale = 0, missing = 0, errors = 0;
  const staleNames = [];
  for (const r of active) {
    if (!rt.created[r.key]) continue;
    const p = byUuid.get(r.point);
    if (!p) { missing++; continue; }
    const { value, ts } = latest(p);
    const isStale = s.staleAction !== 'none' && ts !== null && (now - ts) / 1000 > Number(s.staleS);
    if (isStale) { stale++; if (staleNames.length < 50) staleNames.push(r.name); }
    try {
      if (s.staleAction !== 'none' && rt.stale[r.key] !== isStale) {
        await writeFlags(api, r, isStale, s.faultBit);
        if (rt.stale[r.key] !== undefined) log(status, isStale ? 'warn' : 'info', `${isStale ? 'Stale' : 'Recovered'}: ${r.name}`);
        rt.stale[r.key] = isStale;
      }
      const v = transform(r, value);
      if (v === null || (r.key in rt.last && same(v, rt.last[r.key]))) continue;
      await writePV(api, r, v, rt);
      rt.last[r.key] = v;
      updated++;
    } catch (e) {
      errors++;
      if (errors <= 3) log(status, 'error', `${r.name}: ${e.message}`);
      if (e.status === 404) delete rt.created[r.key]; // object deleted outside the app: recreate next cycle
    }
  }
  status.lastCycle = {
    at: new Date().toISOString(), ms: Date.now() - t0, points: points.length, active: active.length,
    updated, stale, missing, errors,
  };
  status.staleNames = staleNames;
  status.counts = summarize(mapping, points.length);
  return status.lastCycle;
}

function summarize(mapping, pointCount) {
  const rows = mapping.rows;
  return {
    points: pointCount ?? null,
    rows: rows.length,
    enabled: rows.filter(r => r.enabled).length,
    approved: rows.filter(r => r.enabled && r.approved).length,
    draft: rows.filter(r => r.enabled && !r.approved).length,
    skipped: rows.filter(r => !r.enabled).length,
  };
}

// ------------------------------------------------------------------ scheduled run
async function runScheduled({ maxSeconds } = {}) {
  fs.mkdirSync(DATA, { recursive: true });
  const lock = readJson(FILES.lock, null);
  if (lock && Date.now() - lock.at < 90000) return { skipped: 'another sync run is active' };
  writeJson(FILES.lock, { at: Date.now(), pid: process.pid });
  const status = loadStatus();
  const rt = loadRuntime();
  let mapping = loadMapping();
  const end = Date.now() + 1000 * (maxSeconds ?? mapping.settings.runSeconds);
  let cycles = 0, last;
  try {
    do {
      const started = Date.now();
      mapping = loadMapping(); // pick up edits made on the mapping page
      const api = client(mapping.settings);
      try {
        last = await cycle(mapping, api, rt, status);
        status.ok = true; status.error = '';
      } catch (e) {
        status.ok = false; status.error = e.message;
        log(status, 'error', 'Sync failed: ' + e.message);
      }
      cycles++;
      writeJson(FILES.status, status);
      writeJson(FILES.runtime, rt);
      writeJson(FILES.lock, { at: Date.now(), pid: process.pid });
      const wait = mapping.settings.interval * 1000 - (Date.now() - started);
      if (Date.now() + wait >= end) break;
      if (wait > 0) await new Promise(r => setTimeout(r, wait));
    } while (Date.now() < end);
  } finally {
    try { fs.unlinkSync(FILES.lock); } catch (e) { /* gone */ }
  }
  return { cycles, last };
}

// ------------------------------------------------------------------ CSV export / import
const CSV_COLS = ['enabled', 'approved', 'path', 'objectType', 'instance', 'deviceOffset', 'name', 'units', 'states',
  'bit', 'scale', 'offset', 'note'];
const csvCell = v => { const s = v === undefined || v === null ? '' : String(v); return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s; };
function toCsv(mapping) {
  return [CSV_COLS.join(','), ...mapping.rows.map(r => CSV_COLS.map(c => csvCell(r[c])).join(','))].join('\n') + '\n';
}
function parseCsv(text) {
  const rows = []; let row = [], cell = '', q = false;
  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (q) { if (ch === '"' && text[i + 1] === '"') { cell += '"'; i++; } else if (ch === '"') q = false; else cell += ch; }
    else if (ch === '"') q = true;
    else if (ch === ',') { row.push(cell); cell = ''; }
    else if (ch === '\n' || ch === '\r') { if (ch === '\r' && text[i + 1] === '\n') i++; row.push(cell); rows.push(row); row = []; cell = ''; }
    else cell += ch;
  }
  if (cell || row.length) { row.push(cell); rows.push(row); }
  const head = (rows.shift() || []).map(h => h.trim());
  return rows.filter(r => r.some(c => c.trim())).map(r => Object.fromEntries(head.map((h, i) => [h, (r[i] || '').trim()])));
}
/**
 * Compare an edited CSV (or an existing gateway's point map) with the mapping.
 * Returns only real differences, matched by path (+ bit), so the caller can apply
 * them with the same rules as edits made on the page.
 */
function importCsv(mapping, text) {
  const recs = parseCsv(text);
  const changes = [], addRows = [];
  let matched = 0, unmatched = 0;
  const num = v => (v === undefined || v === '' ? undefined : Number(v));
  for (const rec of recs) {
    const p = rec.path || rec.source_path;
    const bit = rec.bit || '';
    let row = mapping.rows.find(r => r.path === p && String(r.bit ?? '') === bit);
    if (!row && bit) {
      const base = mapping.rows.find(r => r.path === p && (r.bit === '' || r.bit === undefined || r.bit === null));
      if (base) {
        addRows.push({ ...base, key: `${base.point}#${bit}`, bit: Number(bit), objectType: rec.objectType || 'bv',
          instance: num(rec.instance) ?? '', name: rec.name || `${base.name}.Bit${bit}`, units: '', approved: false,
          enabled: true, note: `Bit ${bit} of ${base.path}` });
        matched++;
        continue;
      }
    }
    if (!row) { unmatched++; continue; }
    matched++;
    const want = {
      objectType: rec.objectType || rec.object_type, instance: num(rec.instance), deviceOffset: num(rec.deviceOffset),
      name: rec.name || rec.object_name, units: num(rec.units), states: num(rec.states),
      scale: rec.scale, offset: rec.offset,
      enabled: rec.enabled === undefined || rec.enabled === '' ? undefined : /^(1|true|yes|y)$/i.test(rec.enabled),
    };
    const ch = { key: row.key };
    for (const [k, v] of Object.entries(want)) {
      if (v === undefined) continue;
      if (String(row[k] ?? '') !== String(v)) ch[k] = v;
    }
    if (Object.keys(ch).length > 1) changes.push(ch);
  }
  return { changes, addRows, matched, unmatched };
}

module.exports = {
  FILES, DEFAULT_SETTINGS, OBJ, UNIT_MAP,
  loadMapping, saveMapping, loadStatus, loadRuntime, readJson, writeJson, log,
  sourceLayers, sourcePoints, sourcePath, latest, skipReason, guessType, transform, plan, validate, apply, remove, cycle, summarize,
  runScheduled, toCsv, importCsv, nextInstance, client,
};
