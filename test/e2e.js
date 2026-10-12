// End-to-end test: needs a broker, the PLC simulator and the mock platform on :8080
// (see the lab kit). Runs the real hooks against them in a scratch app folder.
const fs = require('fs'), os = require('os'), path = require('path');
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'spb-'));
process.env.SPB_APP_DIR = tmp;
process.env.NF_API_URL = process.env.NF_API_URL || 'http://localhost:8080';
const api = require('../hooks/api.js');
const E = require('../lib/engine');

let rid = 0;
async function call(action, extra = {}) {
  const id = 'testrid-' + String(++rid).padStart(10, '0');
  await api({ args: { key: 'req', value: JSON.stringify({ action, rid: id, ...extra }) } });
  return JSON.parse(fs.readFileSync(path.join(tmp, 'static', '_rpc', id + '.json'), 'utf8'));
}
const assert = (c, m) => { if (!c) { console.error('FAIL:', m); process.exit(1); } console.log('ok  ', m); };
const local = () => fetch('http://localhost:8080/api/v1/bacnet/local').then(r => r.json()).then(j => j.objects);
const pv = o => Object.values((o.props.find(p => p.property === 'PROP_PRESENT_VALUE') || {}).value || {})[0];

(async () => {
  for (const o of await local()) await fetch(`http://localhost:8080/api/v1/bacnet/local/${o.objectId.objectType}/${o.objectId.instance}`, { method: 'DELETE' });

  let r = await call('status');
  assert(r.ok && r.apiError === '' || r.apiError.includes('404'), 'status answers (mock has no platform/info)');

  r = await call('plan');
  assert(r.added === 21, `plan added 21 rows (got ${r.added})`);
  const rows = r.rows;
  const skipped = rows.filter(x => !x.enabled);
  assert(skipped.length === 5, `5 skipped: node metrics, template, string (got ${skipped.map(x => x.path + ' ' + x.note).join(' | ')})`);
  assert(rows.every(x => !x.approved), 'all rows start as drafts');
  assert(rows.find(x => x.path.endsWith('Tank1/Temp')).units === 64, 'degF → 64');
  assert(rows.find(x => x.path.endsWith('Pump1/Fault')).objectType === 'bv', 'Boolean → bv');

  // nothing is published before approval
  await E.runScheduled({ maxSeconds: 0 });
  assert((await local()).length === 0, 'no objects before approval');

  // edits: state tag → msv, scaling, bit rows
  const key = s => rows.find(x => x.path.endsWith(s)).key;
  r = await call('save', {
    changes: [{ key: key('Line1/State'), objectType: 'msv', instance: 1, states: 4, units: '' },
              { key: key('Raw_Pressure'), scale: '0.0036169', units: 56 }],
    addBits: [{ key: key('Alarm_Word'), bit: 0 }, { key: key('Alarm_Word'), bit: 1 }],
  });
  assert(r.changed === 4 && r.problems.length === 0, 'save edits + 2 bit rows, no validation problems');

  r = await call('approve', { by: 'test' });
  assert(r.ok && r.created === 18, `approve created 18 objects (got ${r.created}, failed ${r.failed})`);

  const res = await E.runScheduled({ maxSeconds: 6 });
  assert(res.cycles >= 2, `sync ran ${res.cycles} cycles`);
  const objs = await local();
  assert(objs.length === 18, '18 local objects');
  const byName = n => objs.find(o => o.props.some(p => p.property === 'PROP_OBJECT_NAME' && Object.values(p.value)[0] === n));
  const pres = byName('Line1_PLC.Line1.Raw_Pressure');
  assert(pres && pv(pres) <= 100.1 && pv(pres) >= 0, `scaled pressure in psi range (${pres && pv(pres)})`);
  const temp = byName('Line1_PLC.Tank1.Temp');
  assert(temp && String(pv(temp)).length <= 9, `rounded value ${temp && pv(temp)}`);
  const bv = objs.filter(o => o.objectId.objectType === 'OBJECT_BINARY_VALUE');
  assert(bv.length === 5 && bv.every(o => 'enumerated' in o.props.find(p => p.property === 'PROP_PRESENT_VALUE').value),
    'binary values written as enumerated');

  // changing an approved row removes its object and returns it to draft
  r = await call('save', { changes: [{ key: key('Tank1/Temp'), name: 'Tank1_Temperature' }] });
  assert(r.reset === 1 && (await local()).length === 17, 'renaming an approved row removes the object, back to draft');
  r = await call('approve', { keys: [key('Tank1/Temp')] });
  assert(r.created === 1 && (await local()).length === 18, 're-approval recreates it');

  // duplicate instance is caught
  r = await call('save', { changes: [{ key: key('Tank1/Level'), instance: rows.find(x => x.path.endsWith('Tank1/Temp')).instance }] });
  assert(r.problems.length === 1, 'duplicate instance flagged: ' + r.problems[0]);
  r = await call('approve');
  assert(!r.ok, 'approve refused while problems exist');
  await call('save', { changes: [{ key: key('Tank1/Level'), instance: 200 }] });
  await call('approve');

  // stale handling: settings with a short timeout, stop data by pointing at a frozen copy
  await call('settings', { settings: { staleS: 2, interval: 2 } });
  const status = await call('status');
  assert(status.settings.staleS === 2, 'settings saved');

  // export / import round trip
  r = await call('exportCsv');
  assert(r.csv.split('\n').length > 20, 'CSV export');
  const csv = r.csv.replace('Line1_PLC.Motor1.Speed', 'L1_Motor1_Speed');
  r = await call('importCsv', { csv });
  assert(r.matched === 23 && r.unmatched === 0 && r.changed === 1 && r.reset === 1,
    `CSV import matched ${r.matched}, changed ${r.changed}, reset ${r.reset}`);
  r = await call('approve');
  assert(r.ok && r.created === 1, 'only the renamed row needed re-approval');

  // remove
  r = await call('remove', { keys: [key('Boiler/Enable')] });
  assert(r.removed === 1, 'remove deletes the object');

  console.log('\nall end-to-end checks passed');
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
