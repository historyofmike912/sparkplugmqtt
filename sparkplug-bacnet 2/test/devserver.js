// Development server that hosts the app the way the platform does:
//   GET  /api/v1/apps/static/sparkplug-bacnet/...   static files
//   POST /api/v1/apps/sparkplug-bacnet/hooks/api    runs hooks/api.js (answer goes to static/_rpc)
// Hooks talk to the platform API at NF_API_URL (the lab kit's mock_normalizer.py, or a real instance).
//   SPB_APP_DIR=/tmp/spb-dev NF_API_URL=http://localhost:8080 node test/devserver.js 8090
const fs = require('fs'), path = require('path'), http = require('http');
const SRC = path.join(__dirname, '..');
const APP_DIR = process.env.SPB_APP_DIR || fs.mkdtempSync('/tmp/spb-dev-');
process.env.SPB_APP_DIR = APP_DIR;
fs.mkdirSync(path.join(APP_DIR, 'static'), { recursive: true });
fs.copyFileSync(path.join(SRC, 'static', 'index.html'), path.join(APP_DIR, 'static', 'index.html'));
const apiHook = require('../hooks/api.js');
const E = require('../lib/engine');
const port = Number(process.argv[2] || 8090);
const PREFIX = '/api/v1/apps/static/sparkplug-bacnet/';

const HOOK_ID = 'c0ffee00-1111-2222-3333-444455556666'; // the platform assigns its own hook ids
http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  const authed = (req.headers.authorization || '') === 'Bearer devtoken' || url.searchParams.get('token') === 'devtoken';
  if (url.pathname.startsWith('/api/') && !authed) { res.writeHead(401); return res.end('unauthorized'); }
  if (req.method === 'GET' && url.pathname === '/api/v1/apps/sparkplug-bacnet') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ applications: [{ id: 'sparkplug-bacnet', hooks: [{ id: HOOK_ID, name: 'api' }] }] }));
  }
  if (req.method === 'POST' && url.pathname === '/api/v1/apps/sparkplug-bacnet/hooks/' + HOOK_ID) {
    let body = ''; for await (const c of req) body += c;
    const pid = 'p' + Date.now();
    res.writeHead(200, { 'Content-Type': 'application/json' }); res.end(JSON.stringify({ pid }));
    setTimeout(() => apiHook({ args: JSON.parse(body) }), 30); // runs asynchronously, like the platform
    return;
  }
  if (req.method === 'GET' && url.pathname.startsWith(PREFIX)) {
    const rel = url.pathname.slice(PREFIX.length) || 'index.html';
    const file = path.join(APP_DIR, 'static', path.normalize(rel).replace(/^(\.\.[/\\])+/, ''));
    if (fs.existsSync(file) && fs.statSync(file).isFile()) {
      res.writeHead(200, { 'Content-Type': file.endsWith('.json') ? 'application/json' : 'text/html' });
      return fs.createReadStream(file).pipe(res);
    }
  }
  res.writeHead(404); res.end('not found');
}).listen(port, () => console.log(`dev app at http://localhost:${port}${PREFIX}?applicationId=sparkplug-bacnet&token=devtoken  (data in ${APP_DIR})`));

if (process.env.SPB_DEV_SYNC !== '0') {
  (async function loop() { await E.runScheduled({ maxSeconds: 20 }); setTimeout(loop, 1000); })();
}
