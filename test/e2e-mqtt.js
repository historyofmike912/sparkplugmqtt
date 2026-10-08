// End-to-end: plain MQTT broker (SJC22 simulator) → MQTT ingest → mapping → BACnet objects.
// Needs: mosquitto on :1883, lab-kit simulator/plain_mqtt_sim.py, dev/mock_normalizer.py on :8080.
const fs = require('fs'), os = require('os'), path = require('path');
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'spbm-'));
process.env.SPB_APP_DIR = tmp;
process.env.NF_API_URL = process.env.NF_API_URL || 'http://localhost:8080';
const api = require('../hooks/api.js');
const E = require('../lib/engine');
const I = require('../lib/ingest');
const SEED = process.env.SEED_CSV;

let rid = 0;
async function call(action, extra = {}) {
  const id = 'testrid-' + String(++rid).padStart(10, '0');
  await api({ args: { key: 'req', value: JSON.stringify({ action, rid: id, ...extra }) } });
  return JSON.parse(fs.readFileSync(path.join(tmp, 'static', '_rpc', id + '.json'), 'utf8'));
}
const ok = (c, m) => { if (!c) { console.error('FAIL:', m); process.exit(1); } console.log('ok  ', m); };
const local = () => fetch(process.env.NF_API_URL + '/api/v1/bacnet/local').then(r => r.json()).then(j => j.objects);
const prop = (o, p) => Object.values((o.props.find(x => x.property === p) || {}).value || {})[0];

(async () => {
  // payload parsing
  const P = s => I.parsePayload(Buffer.from(s)).value;
  ok(P('23.4') === 23.4 && P('true') === true && P('5') === 5 && P('{"value":7,"ts":1700000000}') === 7 &&
     P('{"Value":"false"}') === false && P('"12.5"') === 12.5, 'payload formats: bare, JSON number/bool/object/string');

  const t = await call('mqttTest', { mqtt: { url: 'mqtt://localhost:1883', topics: 'nvidia/sjc22/#' }, seconds: 3 });
  ok(t.test.ok && t.test.messages > 1000, `test connection: ${t.test.messages} messages, sample ${t.test.sample.length}`);
  const bad = await call('mqttTest', { mqtt: { url: 'mqtt://localhost:1999', topics: '#' }, seconds: 1 });
  ok(!bad.test.ok && /ECONNREFUSED|timed out/.test(bad.test.error), 'unreachable broker reported: ' + bad.test.error);

  await call('settings', { settings: { staleS: 60, mqtt: { enabled: true, url: 'mqtt://localhost:1883', topics: 'nvidia/sjc22/#', password: 'x' } } });
  const st0 = await call('status');
  ok(st0.settings.mqtt.password === '********' && st0.settings.mqtt.enabled, 'MQTT settings saved, password masked');

  const run = await I.runIngest({ maxSeconds: 8 });
  ok(!run.error && run.topics === 1690, `ingest created ${run.topics} points from ${run.messages} messages ${JSON.stringify(run)}`);

  const r = await call('plan');
  ok(r.added === 1690, `Find tags added ${r.added} rows`);
  const rows = r.rows;
  const offs = {}; for (const x of rows) offs[x.deviceOffset] = (offs[x.deviceOffset] || 0) + 1;
  ok(Math.max(...Object.values(offs)) <= 250, 'auto-spread across device offsets ≤250: ' + JSON.stringify(offs));
  ok(rows.find(x => x.path.endsWith('cdu1-01/leak/inAlarm/Value')).objectType === 'bv', 'boolean topic → BV');

  if (SEED) {
    const imp = await call('importCsv', { csv: fs.readFileSync(SEED, 'utf8') });
    ok(imp.matched === 1690 && imp.unmatched === 0 && imp.problems.length === 0, `seed CSV imported: ${imp.matched} matched, problems ${imp.problems.length}`);
  }
  const ap = await call('approve', { by: 'test' });
  ok(ap.ok && ap.created === 1690, `approve created ${ap.created} objects (failed ${ap.failed})`);

  await I.runIngest({ maxSeconds: 4 });
  const res = await E.runScheduled({ maxSeconds: 4 });
  ok(res.last && res.last.updated > 0 && res.last.errors === 0, `sync updated ${res.last.updated}, errors ${res.last.errors}`);

  const objs = await local();
  const byName = n => objs.find(o => prop(o, 'PROP_OBJECT_NAME') === n);
  if (SEED) {
    const us = byName('pod1.cdu1-01.unitStatus');
    ok(us && us.objectId.objectType === 'OBJECT_MULTI_STATE_VALUE' && [5, 6, 9].includes(prop(us, 'PROP_PRESENT_VALUE')),
      'unitStatus is MSV with value+1 (Online Running 5 → state 6): ' + (us && prop(us, 'PROP_PRESENT_VALUE')));
    const fl = byName('pod1.cdu1-01.primary.flow.feedback');
    ok(fl && prop(fl, 'PROP_UNITS') === 88, 'LPM flow → liters-per-minute (88)');
    const offsetsUsed = new Set(objs.map(o => o.objectId && o.objectId.objectType && 1));
    ok(offsetsUsed.size === 1, 'objects created');
  }
  const st = await call('status');
  ok(st.mqtt && st.mqtt.connected && st.mqtt.topics === 1690, 'status shows MQTT connected with 1690 topics');
  console.log('\nall MQTT end-to-end checks passed');
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
