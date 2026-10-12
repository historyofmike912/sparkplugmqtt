// Offline unit tests (no platform needed): npm test
const os = require('os'), fs = require('fs'), path = require('path');
process.env.SPB_APP_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'spb-unit-'));
const E = require('../lib/engine');
let fails = 0;
const ok = (c, m) => { console.log((c ? 'ok   ' : 'FAIL ') + m); if (!c) fails++; };

const pt = (metric, dt, value, extra = {}) => ({
  uuid: 'u-' + metric, attrs: { edge_node_id: 'Edge01', device_id: extra.device ?? 'PLC1', metric_name: metric,
    sparkplug_datatype: dt, ...(extra.unit ? { engUnit: extra.unit } : {}) },
  latestValue: value === undefined ? null : { ts: new Date().toISOString(), double: value },
});
const pts = [pt('Temp', 'Float', 72.1, { unit: 'degF' }), pt('Run', 'Boolean', 1), pt('Count', 'UInt32', 10),
  pt('Recipe', 'String', undefined), pt('Motor', 'Template', undefined), pt('bdSeq', 'UInt64', 0, { device: '' }),
  pt('Pres', 'Float', 6.5, { unit: 'inH2O' })];

const m = E.loadMapping();
ok(E.plan(m, pts) === 7, 'plan adds every point');
ok(E.plan(m, pts) === 0, 'plan is idempotent');
const r = n => m.rows.find(x => x.path.endsWith('/' + n));
ok(r('Temp').objectType === 'av' && r('Temp').units === 64 && r('Temp').name === 'PLC1.Temp', 'float → AV, degF, Device.Tag name');
ok(r('Run').objectType === 'bv', 'boolean → BV');
ok(!r('Recipe').enabled && /String/.test(r('Recipe').note), 'string skipped');
ok(!r('Motor').enabled && /Template/.test(r('Motor').note), 'template skipped');
ok(!r('bdSeq').enabled, 'node-level skipped');
ok(r('Pres').units === 58, 'inH2O → 58');
ok(m.rows.every(x => !x.approved), 'drafts only');
const avs = m.rows.filter(x => x.objectType === 'av').map(x => x.instance);
ok(new Set(avs).size === avs.length, 'unique AV instances ' + avs);

// stable numbering: a new point never renumbers existing rows
const before = JSON.stringify(m.rows.map(x => [x.key, x.instance]));
E.plan(m, [...pts, pt('Flow', 'Float', 3)]);
ok(JSON.stringify(m.rows.slice(0, 7).map(x => [x.key, x.instance])) === before && r('Flow').instance === Math.max(...avs) + 1,
  'new tag appended with next instance, existing unchanged');

// transforms
ok(E.transform({ objectType: 'av', scale: '0.5', offset: '1' }, 10) === 6, 'scale/offset');
ok(E.transform({ objectType: 'bv', bit: 3 }, 8) === true && E.transform({ objectType: 'bv', bit: 2 }, 8) === false, 'bit extraction');
ok(E.transform({ objectType: 'av' }, 39.72200012207031) === 39.722, 'float noise rounded');
ok(E.transform({ objectType: 'msv' }, 0) === 1, 'MSV never below 1');
ok(E.transform({ objectType: 'bv' }, 0) === false && E.transform({ objectType: 'bv' }, 1) === true, 'BV from 0/1');

// validation
r('Count').instance = r('Temp').instance;
ok(E.validate(m).some(p => /used by both/.test(p)), 'duplicate instance detected');
r('Count').instance = 999;
r('Count').name = r('Temp').name;
ok(E.validate(m).some(p => /used twice/.test(p)), 'duplicate name detected');
r('Count').name = 'PLC1.Count';
ok(E.validate(m).length === 0, 'valid after fixes');

// CSV round trip is a no-op; an edit produces exactly one change
const csv = E.toCsv(m);
ok(E.importCsv(m, csv).changes.length === 0, 'unchanged CSV produces no changes');
const imp = E.importCsv(m, csv.replace('PLC1.Temp', 'AHU1_SAT'));
ok(imp.changes.length === 1 && imp.changes[0].name === 'AHU1_SAT', 'edited CSV produces one change');
const gw = 'path,object_type,instance,object_name\nEdge01/PLC1/Temp,av,301,Existing_Gateway_Name\n';
const g = E.importCsv(m, gw);
ok(g.changes.length === 1 && g.changes[0].instance === 301 && g.changes[0].name === 'Existing_Gateway_Name',
  'existing gateway point map (source_path/object_name columns) imports');

// stale timestamps
ok(E.latest({ latestValue: { ts: '2026-10-01T11:50:54.668Z', double: 5 } }).ts === Date.parse('2026-10-01T11:50:54.668Z'), 'timestamp parsed');

console.log(fails ? `\n${fails} failed` : '\nall unit tests passed');
process.exit(fails ? 1 : 0);
