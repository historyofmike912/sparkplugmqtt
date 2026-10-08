/**
 * api — On Request hook behind the mapping page.
 * Actions: status, points, plan, save, approve, remove, syncNow, exportCsv, importCsv, settings.
 */
const E = require('../lib/engine');
const I = require('../lib/ingest');
const { readRequest, writeAnswer } = require('../lib/rpc');

const EDITABLE = ['objectType', 'instance', 'deviceOffset', 'name', 'units', 'states', 'bit', 'scale', 'offset', 'enabled'];

async function handle(req) {
  const mapping = E.loadMapping();
  const status = E.loadStatus();
  const rt = E.loadRuntime();
  const api = E.client(mapping.settings);
  const saveAll = () => { E.saveMapping(mapping); E.writeJson(E.FILES.status, status); E.writeJson(E.FILES.runtime, rt); };

  // Apply field edits with one rule: if a published (approved) row changes, its BACnet
  // object is removed and the row returns to draft until it is approved again.
  async function applyChanges(changes, addBits, addRows = []) {
    let changed = 0, reset = 0;
    for (const ch of changes) {
      const row = mapping.rows.find(r => r.key === ch.key);
      if (!row) continue;
      const before = { ...row };
      for (const k of EDITABLE) if (k in ch) row[k] = ch[k];
      for (const k of ['instance', 'deviceOffset', 'units', 'states'])
        if (row[k] !== '' && row[k] !== null && row[k] !== undefined) row[k] = Number(row[k]);
      const differs = EDITABLE.some(k => String(before[k] ?? '') !== String(row[k] ?? ''));
      if (!differs) continue;
      if (before.approved) {
        if (rt.created[row.key] && E.OBJ[before.objectType]) {
          try { await E.remove({ rows: [before] }, api, rt, status, [before.key]); } catch (e) { /* logged */ }
        }
        row.approved = false; reset++;
      }
      changed++;
    }
    for (const b of addBits) {
      const base = mapping.rows.find(r => r.key === b.key);
      if (!base) continue;
      const key = `${base.point}#${b.bit}`;
      if (mapping.rows.some(r => r.key === key)) continue;
      mapping.rows.push({ ...base, key, bit: Number(b.bit), objectType: 'bv', units: '', approved: false,
        enabled: true, instance: E.nextInstance(mapping, base.deviceOffset || 0, 'bv'),
        name: `${base.name}.Bit${b.bit}`, note: `Bit ${b.bit} of ${base.path}` });
      changed++;
    }
    for (const r of addRows) {
      if (mapping.rows.some(x => x.key === r.key)) continue;
      if (r.instance === '' || r.instance === undefined) r.instance = E.nextInstance(mapping, r.deviceOffset || 0, r.objectType);
      mapping.rows.push(r); changed++;
    }
    if (reset) E.log(status, 'info', `${reset} published row(s) changed and returned to draft; approve them again to publish`);
    return { changed, reset };
  }

  switch (req.action) {
    case 'status': {
      let platform = null, error = '';
      try { platform = await api.get('/api/v1/platform/info'); } catch (e) { error = e.message; }
      return {
        ok: true, apiBase: api.base, apiError: error, platform,
        settings: masked(mapping.settings),
        mqtt: E.readJson(I.FILES.status, null),
        counts: E.summarize(mapping, status.lastCycle ? status.lastCycle.points : null),
        lastCycle: status.lastCycle || null, syncOk: status.ok !== false, syncError: status.error || '',
        staleNames: status.staleNames || [],
        log: [...(status.log || []), ...((E.readJson(I.FILES.status, {}) || {}).log || [])]
          .sort((x, y) => (x.t < y.t ? 1 : -1)).slice(0, 100),
        problems: E.validate(mapping),
      };
    }

    case 'points': {
      const pts = await E.sourcePoints(api, mapping.settings);
      const mapped = new Set(mapping.rows.map(r => r.point));
      return {
        ok: true,
        points: pts.map(p => {
          const { value, ts } = E.latest(p);
          return {
            uuid: p.uuid, path: E.sourcePath(p), datatype: (p.attrs || {}).sparkplug_datatype || '',
            units: (p.attrs || {}).engUnit || '', value: typeof value === 'object' ? null : value,
            ts: ts ? new Date(ts).toISOString() : null, mapped: mapped.has(p.uuid), skip: E.skipReason(p, value),
          };
        }),
      };
    }

    case 'rows':
      return { ok: true, rows: mapping.rows, settings: mapping.settings, problems: E.validate(mapping), runtime: { last: rt.last, stale: rt.stale } };

    case 'plan': {
      const pts = await E.sourcePoints(api, mapping.settings);
      const added = E.plan(mapping, pts);
      E.log(status, 'info', `Found ${added} new Sparkplug point(s); added as drafts`);
      saveAll();
      return { ok: true, added, rows: mapping.rows };
    }

    case 'save': {
      const res = await applyChanges(req.changes || [], req.addBits || []);
      saveAll();
      return { ok: true, ...res, rows: mapping.rows, problems: E.validate(mapping) };
    }

    case 'approve': {
      const problems = E.validate(mapping);
      if (problems.length) return { ok: false, error: 'Fix these first: ' + problems.slice(0, 5).join('; '), problems };
      const keys = req.keys && req.keys.length ? req.keys
        : mapping.rows.filter(r => r.enabled && !r.approved && E.OBJ[r.objectType]).map(r => r.key);
      for (const r of mapping.rows) if (keys.includes(r.key) && r.enabled && E.OBJ[r.objectType]) {
        r.approved = true; r.approvedAt = new Date().toISOString(); r.approvedBy = req.by || '';
      }
      E.saveMapping(mapping);
      const res = await E.apply(mapping, api, rt, status, keys);
      E.log(status, 'info', `Approved ${keys.length} row(s)${req.by ? ' by ' + req.by : ''}`);
      saveAll();
      return { ok: true, approved: keys.length, ...res, rows: mapping.rows };
    }

    case 'remove': {
      const n = await E.remove(mapping, api, rt, status, req.keys || []);
      saveAll();
      return { ok: true, removed: n, rows: mapping.rows };
    }

    case 'syncNow': {
      const res = await E.runScheduled({ maxSeconds: 0 });
      return { ok: true, ...res };
    }

    case 'settings': {
      const allowed = ['interval', 'runSeconds', 'staleS', 'staleAction', 'faultBit', 'deviceOffset', 'startInstance',
        'nameStyle', 'apiBase', 'apiToken', 'layer', 'maxObjectsPerDevice'];
      if (req.settings && req.settings.mqtt) {
        const m = { ...mapping.settings.mqtt };
        for (const [k, v] of Object.entries(req.settings.mqtt)) {
          if (k === 'password' && v === '********') continue;
          if (k in E.DEFAULT_SETTINGS.mqtt) m[k] = v;
        }
        for (const k of ['qos', 'flushSeconds', 'repostMinutes']) m[k] = Number(m[k]);
        m.enabled = m.enabled === true || m.enabled === 'true';
        m.insecure = m.insecure === true || m.insecure === 'true';
        mapping.settings.mqtt = m;
      }
      for (const k of allowed) if (req.settings && k in req.settings) {
        if (k === 'apiToken' && req.settings[k] === '********') continue;
        mapping.settings[k] = req.settings[k];
      }
      for (const k of ['interval', 'runSeconds', 'staleS', 'faultBit', 'deviceOffset', 'startInstance', 'maxObjectsPerDevice'])
        mapping.settings[k] = Number(mapping.settings[k]);
      mapping.settings.interval = Math.max(2, mapping.settings.interval);
      mapping.settings.runSeconds = Math.min(55, Math.max(0, mapping.settings.runSeconds));
      E.log(status, 'info', 'Settings saved');
      saveAll();
      return { ok: true, settings: masked(mapping.settings) };
    }

    case 'mqttTest': {
      const cfg = { ...(req.mqtt || {}) };
      if (cfg.password === '********') delete cfg.password;
      return { ok: true, test: await I.testConnection(cfg, Math.min(15, Number(req.seconds || 6))) };
    }

    case 'mqttRunNow': {
      return { ok: true, ...(await I.runIngest({ maxSeconds: 10 })) };
    }

    case 'exportCsv':
      return { ok: true, csv: E.toCsv(mapping), filename: 'sparkplug-bacnet-mapping.csv' };

    case 'importCsv': {
      const imp = E.importCsv(mapping, String(req.csv || ''));
      const res = await applyChanges(imp.changes, [], imp.addRows);
      E.log(status, 'info', `Imported CSV: ${imp.matched} row(s) matched, ${res.changed} changed, ${imp.unmatched} not matched`);
      saveAll();
      return { ok: true, matched: imp.matched, unmatched: imp.unmatched, ...res, rows: mapping.rows, problems: E.validate(mapping) };
    }

    default:
      return { ok: false, error: 'Unknown action ' + req.action };
  }
}

function masked(s) {
  return { ...s, apiToken: s.apiToken ? '********' : '',
    mqtt: { ...(s.mqtt || {}), password: s.mqtt && s.mqtt.password ? '********' : '' } };
}

module.exports = async ({ args }) => {
  let req = {}, out;
  try { req = readRequest(args); } catch (e) { out = { ok: false, error: 'Bad request: ' + e.message }; }
  if (!out) {
    try { out = await handle(req); } catch (e) { out = { ok: false, error: e.message }; }
  }
  writeAnswer(req && req.rid, out);
  return JSON.stringify(out);
};
module.exports.handle = handle;
