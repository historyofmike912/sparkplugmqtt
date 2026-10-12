/**
 * Minimal client for the Abound Normalizer REST API, used from inside hooks.
 * Hooks run inside the platform container, so the API is on localhost.
 * If the console port is not 8080, set "API address" on the app's Settings tab.
 */
const http = require('http');
const https = require('https');
const { URL } = require('url');

function client(settings = {}) {
  const base = (settings.apiBase || process.env.NF_API_URL ||
    `http://localhost:${process.env.PORT || 8080}`).replace(/\/+$/, '');
  const token = settings.apiToken || process.env.NF_TOKEN || '';

  function request(method, path, { query, body } = {}) {
    const u = new URL(base + path);
    if (query) for (const [k, v] of Object.entries(query)) if (v !== undefined) u.searchParams.set(k, String(v));
    const data = body === undefined ? null : Buffer.from(JSON.stringify(body));
    const headers = { Accept: 'application/json' };
    if (data) { headers['Content-Type'] = 'application/json'; headers['Content-Length'] = data.length; }
    if (token) headers.Authorization = 'Bearer ' + token;
    const mod = u.protocol === 'https:' ? https : http;
    return new Promise((resolve, reject) => {
      const req = mod.request(u, { method, headers, timeout: 20000 }, res => {
        const chunks = [];
        res.on('data', c => chunks.push(c));
        res.on('end', () => {
          const text = Buffer.concat(chunks).toString('utf8');
          if (res.statusCode >= 400) {
            let msg = res.headers['grpc-message'] || text.slice(0, 300) || ('HTTP ' + res.statusCode);
            try { msg = decodeURIComponent(String(msg)); } catch (e) { /* keep raw */ }
            const err = new Error(`${method} ${path} -> HTTP ${res.statusCode}: ${msg}`);
            err.status = res.statusCode;
            return reject(err);
          }
          try { resolve(text ? JSON.parse(text) : {}); } catch (e) { resolve({}); }
        });
      });
      req.on('timeout', () => req.destroy(new Error(`${method} ${path} timed out`)));
      req.on('error', reject);
      if (data) req.write(data);
      req.end();
    });
  }

  return {
    base,
    get: (p, query) => request('GET', p, { query }),
    post: (p, body) => request('POST', p, { body }),
    patch: (p, body) => request('PATCH', p, { body }),
    del: (p, query) => request('DELETE', p, { query, body: {} }),

    /** All points on the Sparkplug layer, with latest values. */
    async sparkplugPoints(layer, page = 250) {
      const out = [];
      for (let offset = 0; ; offset += page) {
        const res = await request('GET', '/api/v1/point/points',
          { query: { query: '*', layer, page_size: page, page_offset: offset } });
        const pts = res.points || [];
        out.push(...pts);
        if (pts.length < page) break;
      }
      if (out.length && !out.some(p => p.latestValue || p.latest_value)) {
        // The listing had no values on this version: fall back to the query API.
        const values = {};
        for (let offset = 0; ;) {
          const res = await request('POST', '/api/v1/point/query', {
            body: { layer, pageSize: page, pageOffset: offset, responseFormat: 'LAYERS_COLLAPSED' } });
          for (const p of res.points || []) values[p.uuid] = p.latestValue || p.latest_value;
          const next = Number(res.nextPageOffset || 0);
          if (!(res.points || []).length || next <= offset) break;
          offset = next;
        }
        for (const p of out) if (values[p.uuid]) p.latestValue = values[p.uuid];
      }
      return out;
    },

    /** Local BACnet objects on one local device, keyed "OBJECT_TYPE:instance". */
    async localObjects(offset = 0) {
      const res = await request('GET', '/api/v1/bacnet/local', { query: { local_device_instance_offset: offset } });
      const map = new Map();
      for (const o of res.objects || []) {
        const id = o.objectId || o.object_id || {};
        map.set(`${id.objectType || id.object_type}:${Number(id.instance || 0)}`, o);
      }
      return map;
    },
  };
}

module.exports = { client };
