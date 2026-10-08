/**
 * Minimal MQTT 3.1.1 client for subscribing (no npm dependencies, so the app installs on sites
 * without internet access). Supports mqtt:// and mqtts:// (TLS, optional custom CA), username /
 * password, QoS 0 and 1 subscriptions, keep-alive, and a clean disconnect.
 */
const net = require('net');
const tls = require('tls');
const { EventEmitter } = require('events');
const { URL } = require('url');

function encLen(n) {
  const out = [];
  do { let b = n % 128; n = Math.floor(n / 128); if (n > 0) b |= 0x80; out.push(b); } while (n > 0);
  return Buffer.from(out);
}
const str = s => { const b = Buffer.from(String(s), 'utf8'); const l = Buffer.alloc(2); l.writeUInt16BE(b.length); return Buffer.concat([l, b]); };
const packet = (type, flags, body) => Buffer.concat([Buffer.from([(type << 4) | flags]), encLen(body.length), body]);

class MqttClient extends EventEmitter {
  constructor(opts) {
    super();
    this.o = { keepalive: 30, clientId: 'abound-mqtt-' + process.pid, ...opts };
    this.buf = Buffer.alloc(0);
    this.pid = 1;
    this.pending = new Map();
    this.connected = false;
  }

  connect(timeoutMs = 15000) {
    const u = new URL(this.o.url);
    const secure = /^(mqtts|ssl|tls):$/.test(u.protocol);
    const port = Number(u.port || (secure ? 8883 : 1883));
    const host = u.hostname;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { this.destroy(); reject(new Error(`MQTT connect to ${host}:${port} timed out`)); }, timeoutMs);
      const onConnect = () => {
        const pw = this.o.username && this.o.password; // MQTT 3.1.1: a password needs a username
        const flags = 0x02 | (this.o.username ? 0x80 : 0) | (pw ? 0x40 : 0); // clean session
        const ka = Buffer.alloc(2); ka.writeUInt16BE(this.o.keepalive);
        const body = Buffer.concat([str('MQTT'), Buffer.from([4, flags]), ka, str(this.o.clientId),
          this.o.username ? str(this.o.username) : Buffer.alloc(0), pw ? str(this.o.password) : Buffer.alloc(0)]);
        this.sock.write(packet(1, 0, body));
      };
      if (secure) {
        const topts = { host, port, rejectUnauthorized: !this.o.insecure };
        if (!require('net').isIP(host)) topts.servername = host;
        if (this.o.ca) topts.ca = this.o.ca;
        this.sock = tls.connect(topts, onConnect);
      } else {
        this.sock = net.connect({ host, port }, onConnect);
      }
      this.sock.on('data', d => this._data(d));
      this.sock.on('error', e => { clearTimeout(timer); if (!this.connected) reject(e); else this.emit('error', e); });
      this.sock.on('close', () => {
        const was = this.connected; this.connected = false; clearInterval(this.ping); this.emit('close');
        if (!was) { clearTimeout(timer); reject(new Error(`broker ${host}:${port} closed the connection before accepting it (check credentials and TLS settings)`)); }
      });
      this.once('connack', rc => {
        clearTimeout(timer);
        if (rc !== 0) {
          const why = { 1: 'bad protocol version', 2: 'client ID rejected', 3: 'server unavailable', 4: 'bad username or password', 5: 'not authorized' }[rc] || `code ${rc}`;
          this.destroy();
          return reject(new Error('MQTT connection refused: ' + why));
        }
        this.connected = true;
        this.ping = setInterval(() => { try { this.sock.write(Buffer.from([0xc0, 0])); } catch (e) { /* closing */ } },
          Math.max(5, this.o.keepalive - 5) * 1000);
        resolve();
      });
    });
  }

  subscribe(filters, qos = 1) {
    const id = this.pid++ & 0xffff || 1;
    const parts = [Buffer.from([id >> 8, id & 0xff])];
    for (const f of filters) parts.push(str(f), Buffer.from([qos]));
    this.sock.write(packet(8, 2, Buffer.concat(parts)));
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error('MQTT subscribe timed out')), 10000);
      this.pending.set(id, codes => { clearTimeout(t); codes.some(c => c === 0x80) ? reject(new Error('Subscription refused by broker (check the account’s topic permissions)')) : resolve(codes); });
    });
  }

  end() {
    clearInterval(this.ping);
    try { this.sock.write(Buffer.from([0xe0, 0])); } catch (e) { /* already closed */ }
    try { this.sock.end(); } catch (e) { /* ignore */ }
  }

  destroy() { clearInterval(this.ping); try { this.sock.destroy(); } catch (e) { /* ignore */ } }

  _data(d) {
    this.buf = Buffer.concat([this.buf, d]);
    for (;;) {
      if (this.buf.length < 2) return;
      let mult = 1, len = 0, i = 1, b;
      do { if (i >= this.buf.length) return; b = this.buf[i++]; len += (b & 127) * mult; mult *= 128; } while (b & 128);
      if (this.buf.length < i + len) return;
      const head = this.buf[0], body = this.buf.subarray(i, i + len);
      this.buf = this.buf.subarray(i + len);
      this._packet(head >> 4, head & 0x0f, body);
    }
  }

  _packet(type, flags, body) {
    if (type === 2) return this.emit('connack', body[1]);
    if (type === 9) { const id = body.readUInt16BE(0); const cb = this.pending.get(id); this.pending.delete(id); if (cb) cb([...body.subarray(2)]); return; }
    if (type === 3) {
      const qos = (flags >> 1) & 3, retain = !!(flags & 1);
      const tl = body.readUInt16BE(0);
      const topic = body.subarray(2, 2 + tl).toString('utf8');
      let off = 2 + tl;
      if (qos > 0) {
        const id = body.readUInt16BE(off); off += 2;
        if (qos === 1) this.sock.write(Buffer.from([0x40, 2, id >> 8, id & 0xff]));            // PUBACK
        if (qos === 2) this.sock.write(Buffer.from([0x50, 2, id >> 8, id & 0xff]));            // PUBREC
      }
      this.emit('message', topic, body.subarray(off), { qos, retain });
      return;
    }
    if (type === 6) { this.sock.write(Buffer.from([0x70, 2, body[0], body[1]])); }            // PUBREL -> PUBCOMP
  }
}

module.exports = { MqttClient };
