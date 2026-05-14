import WebSocket from 'ws';
import os from 'os';
import { execSync } from 'child_process';
import { readFileSync, existsSync } from 'fs';
import { fileURLToPath } from 'url';
import { join, dirname } from 'path';

const RECONNECT_INITIAL = 1000;
const RECONNECT_MAX = 30000;
const KNOWN_RUNTIMES = ['claude', 'codex', 'kimi'];

function readDaemonVersion() {
  try {
    const pkgPath = join(dirname(fileURLToPath(import.meta.url)), '..', 'package.json');
    return JSON.parse(readFileSync(pkgPath, 'utf8')).version ?? '0.0.0';
  } catch {
    return '0.0.0';
  }
}

const DAEMON_VERSION = readDaemonVersion();

function detectRuntimes() {
  const cmd = process.platform === 'win32' ? 'where' : 'which';
  const runtimes = [];
  for (const runtime of KNOWN_RUNTIMES) {
    try {
      execSync(`${cmd} ${runtime}`, { stdio: 'pipe' });
      runtimes.push(runtime);
    } catch {}
  }
  return runtimes;
}

// Probe installed models per runtime by reading each CLI's config file.
// Returns an empty array for runtimes whose config format we can't parse;
// downstream code treats empty = "no whitelist, accept any value".
function probeModelsByRuntime(runtimes) {
  const home = os.homedir();
  const out = {};
  for (const runtime of runtimes) {
    out[runtime] = [];
    try {
      if (runtime === 'kimi') {
        const p = join(home, '.kimi', 'config.toml');
        if (existsSync(p)) {
          const toml = readFileSync(p, 'utf8');
          // Match [models."foo/bar"] or [models.foo]
          const re = /^\[models\.(?:"([^"]+)"|([^\]\s]+))\]/gm;
          const seen = new Set();
          let m;
          while ((m = re.exec(toml)) !== null) {
            const name = m[1] ?? m[2];
            if (name && !seen.has(name)) { seen.add(name); out[runtime].push(name); }
          }
        }
      }
      // claude / codex: no standard parseable config — leave empty so the
      // server accepts any user-typed value. Wire up later if needed.
    } catch {}
  }
  return out;
}

export class DaemonConnection {
  /**
   * @param {object} opts
   * @param {string} opts.serverUrl
   * @param {string} opts.machineApiKey
   * @param {(msg:any)=>void} opts.onMessage
   * @param {()=>void|Promise<void>} [opts.onReady]
   *   Called once per ws-open (initial connect + every reconnect). Used by
   *   index.js to (re-)register + (re-)pull devices. Errors are caught and
   *   logged so a failing onReady never tears down the WS.
   * @param {(url:string)=>any} [opts.wsFactory]
   *   Test seam for swapping the underlying ws instance. Defaults to
   *   `(url) => new WebSocket(url)`.
   * @param {number} [opts.reconnectInitialMs]
   * @param {number} [opts.reconnectMaxMs]
   */
  constructor({
    serverUrl,
    machineApiKey,
    onMessage,
    onReady = null,
    wsFactory = null,
    reconnectInitialMs = RECONNECT_INITIAL,
    reconnectMaxMs = RECONNECT_MAX,
  }) {
    this.serverUrl = serverUrl.replace(/^http/, 'ws');
    this.machineApiKey = machineApiKey;
    this.onMessage = onMessage;
    this.onReady = typeof onReady === 'function' ? onReady : null;
    this._wsFactory = typeof wsFactory === 'function' ? wsFactory : (url) => new WebSocket(url);
    this._reconnectInitialMs = reconnectInitialMs;
    this._reconnectMaxMs = reconnectMaxMs;
    this.ws = null;
    this.reconnectDelay = this._reconnectInitialMs;
    this.stopped = false;
    this.pendingRequests = new Map();
  }

  connect() {
    const url = `${this.serverUrl}/daemon/connect?key=${this.machineApiKey}`;
    console.error(`[Connection] Connecting to ${url}`);
    this.ws = this._wsFactory(url);

    this.ws.on('open', () => {
      console.error(`[Connection] Connected (daemon v${DAEMON_VERSION})`);
      this.reconnectDelay = this._reconnectInitialMs;
      this._sendReady();
      if (this.onReady) {
        try {
          Promise.resolve(this.onReady({ daemonVersion: DAEMON_VERSION }))
            .catch((err) => {
              console.error(`[Connection] onReady handler failed: ${err?.message ?? err}`);
            });
        } catch (err) {
          console.error(`[Connection] onReady handler threw synchronously: ${err?.message ?? err}`);
        }
      }
      // WS ping 心跳：远程代理 idle timeout 60-100 秒，30 秒发 ping 防止断
      this.pingTimer = setInterval(() => {
        if (this.ws?.readyState === WebSocket.OPEN) {
          try { this.ws.ping(); } catch {}
        }
      }, 30_000);
    });

    this.ws.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw.toString()); }
      catch { return; }
      if (this._resolvePending(msg)) return;
      if (msg.type !== 'pong') {
        console.error(`[Connection] ← ${msg.type}${msg.agentId ? ` agent=${msg.agentId.slice(0,8)}` : ''}${msg.teamId ? ` team=${msg.teamId.slice(0,8)}` : ''}${msg.seq != null ? ` seq=${msg.seq}` : ''}`);
      }
      this.onMessage(msg);
    });

    this.ws.on('close', (code) => {
      console.error(`[Connection] Disconnected (code=${code})`);
      if (this.pingTimer) {
        clearInterval(this.pingTimer);
        this.pingTimer = null;
      }
      if (!this.stopped) this._scheduleReconnect();
    });

    this.ws.on('error', (err) => {
      console.error('[Connection] Error:', err.message);
    });
  }

  send(msg) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(msg));
    }
  }

  async request({ message, expect, timeoutMs = 10000 }) {
    if (!expect?.type || !expect?.requestId) {
      throw new Error('request expect.type and expect.requestId are required');
    }

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pendingRequests.delete(expect.requestId);
        reject(new Error(`daemon request timeout: ${expect.type}:${expect.requestId}`));
      }, timeoutMs);

      this.pendingRequests.set(expect.requestId, {
        type: expect.type,
        resolve: (payload) => {
          clearTimeout(timer);
          resolve(payload);
        },
      });

      this.send(message);
    });
  }

  stop() {
    this.stopped = true;
    this.ws?.close();
  }

  _sendReady() {
    const runtimes = detectRuntimes();
    const modelsByRuntime = probeModelsByRuntime(runtimes);
    const summary = runtimes.map(r => `${r}[${modelsByRuntime[r]?.length ?? 0}]`).join(',');
    console.error(`[Connection] Ready — host=${os.hostname()} runtimes=${summary} v${DAEMON_VERSION}`);
    this.send({
      type: 'ready',
      hostname: os.hostname(),
      os: `${os.platform()} ${os.arch()}`,
      runtimes,
      modelsByRuntime,
      daemonVersion: DAEMON_VERSION,
    });
  }

  _resolvePending(msg) {
    const requestId = msg?.requestId;
    if (!requestId) return false;

    const pending = this.pendingRequests.get(requestId);
    if (!pending) return false;
    if (pending.type !== msg.type) return false;

    this.pendingRequests.delete(requestId);
    pending.resolve(msg);
    return true;
  }

  _scheduleReconnect() {
    console.error(`[Connection] Reconnecting in ${this.reconnectDelay}ms...`);
    setTimeout(() => this.connect(), this.reconnectDelay);
    this.reconnectDelay = Math.min(this.reconnectDelay * 2, this._reconnectMaxMs);
  }
}
