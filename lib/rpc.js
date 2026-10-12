/**
 * Hook request/answer helpers, same pattern as the Carrier Connect app:
 * the platform delivers args as {"key":"req","value":"<JSON>"} and does not record
 * return values, so each answer is also written to static/_rpc/<rid>.json for the page.
 */
const fs = require('fs');
const path = require('path');

const APP_DIR = process.env.SPB_APP_DIR || path.join(__dirname, '..');
const RPC_DIR = path.join(APP_DIR, 'static', '_rpc');
const TTL_MS = 5 * 60 * 1000;

function readRequest(args) {
  if (!args) return {};
  if (typeof args === 'string') return JSON.parse(args);
  const raw = args.req !== undefined ? args.req : (args.value !== undefined ? args.value : null);
  if (raw == null) return typeof args.action === 'string' ? args : {};
  return typeof raw === 'string' ? JSON.parse(raw) : raw;
}

function writeAnswer(rid, out) {
  if (!rid || !/^[A-Za-z0-9-]{16,64}$/.test(rid)) return;
  try {
    fs.mkdirSync(RPC_DIR, { recursive: true });
    const tmp = path.join(RPC_DIR, rid + '.tmp');
    fs.writeFileSync(tmp, JSON.stringify(out));
    fs.renameSync(tmp, path.join(RPC_DIR, rid + '.json'));
    const now = Date.now();
    for (const f of fs.readdirSync(RPC_DIR)) {
      try { const p = path.join(RPC_DIR, f); if (now - fs.statSync(p).mtimeMs > TTL_MS) fs.unlinkSync(p); } catch (e) { /* ignore */ }
    }
  } catch (e) { console.log('[sparkplug-bacnet] could not write answer: ' + e.message); }
}

module.exports = { readRequest, writeAnswer };
