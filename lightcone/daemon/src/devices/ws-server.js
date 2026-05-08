// DeviceWsServer — daemon-side device WebSocket endpoint (M1.1-T3 §7.1.2).
//
// 复用 RpcServer 的 http server，通过 'upgrade' 事件拦截 path /device/{deviceId}。
// 鉴权：URL 查询参数 `key`（device api key），通过 verifyKey({deviceId, key}) callback。
// 维护 deviceId → ws map；提供 pushCommand(deviceId, payload)。每 30s ping 心跳。
//
// 入站 frame（extension → daemon，仅日志/事件转发）：
//   - {type:'ack', correlation_id}
//   - 其它任意 frame：通过 onMessage callback 透传到上层（如 channel-manager
//     未来可消费）。本模块自身不消费。
//
// 出站 frame（daemon → extension）由 pushCommand 负责，调用方决定 payload 形态。
//
// 设计取舍：使用 ws.Server({ noServer: true }) + httpServer.upgrade 单端口；
//          离线推送返回 {ok:false, reason:'device_offline'}（不抛），由调用方决定是否
//          emit dispatch.failed。

import WebSocket, { WebSocketServer } from 'ws';

const PING_INTERVAL_MS = 30_000;
const PING_TIMEOUT_MS = 90_000;

function parseDeviceIdFromPath(pathname) {
  // /device/{deviceId} — 单段 deviceId，禁止再带子路径
  const m = /^\/device\/([^/]+)\/?$/.exec(pathname ?? '');
  if (!m) return null;
  try {
    return decodeURIComponent(m[1]);
  } catch {
    return null;
  }
}

export function parseDeviceKeysEnv(value) {
  const raw = String(value ?? '').trim();
  if (!raw) return new Map();
  // JSON object form: {"deviceId":"key", ...}
  if (raw.startsWith('{')) {
    try {
      const obj = JSON.parse(raw);
      if (obj && typeof obj === 'object' && !Array.isArray(obj)) {
        const map = new Map();
        for (const [k, v] of Object.entries(obj)) {
          if (typeof v === 'string' && v.length > 0 && k) {
            map.set(String(k), v);
          }
        }
        return map;
      }
    } catch {
      // fall through to csv parsing
    }
  }
  // CSV form: id1:key1,id2:key2
  const map = new Map();
  for (const entry of raw.split(',')) {
    const trimmed = entry.trim();
    if (!trimmed) continue;
    const idx = trimmed.indexOf(':');
    if (idx <= 0) continue;
    const id = trimmed.slice(0, idx).trim();
    const key = trimmed.slice(idx + 1).trim();
    if (id && key) map.set(id, key);
  }
  return map;
}

/** Build a verifier closure from a device-keys map (or single fallback key). */
export function makeKeyVerifier({ deviceKeys = new Map(), fallbackKey = '' } = {}) {
  return ({ deviceId, key }) => {
    if (!deviceId || !key) return false;
    const expected = deviceKeys.get(deviceId);
    if (expected) return expected === key;
    if (fallbackKey) return fallbackKey === key;
    return false;
  };
}

export class DeviceWsServer {
  /**
   * @param {object} opts
   * @param {(args:{deviceId:string,key:string}) => boolean} opts.verifyKey
   * @param {(args:{deviceId:string,frame:any}) => void} [opts.onMessage]
   *   Optional inbound frame callback; ack frames are still emitted.
   * @param {(args:{deviceId:string,event:'connect'|'disconnect'}) => void} [opts.onPresence]
   * @param {boolean} [opts.logEnabled=true]
   */
  constructor({ verifyKey, onMessage = () => {}, onPresence = () => {}, logEnabled = true } = {}) {
    if (typeof verifyKey !== 'function') {
      throw new Error('DeviceWsServer requires verifyKey(args) callback');
    }
    this.verifyKey = verifyKey;
    this.onMessage = onMessage;
    this.onPresence = onPresence;
    this.logEnabled = logEnabled;
    this.wss = new WebSocketServer({ noServer: true });
    /** @type {Map<string, import('ws').WebSocket>} */
    this.connections = new Map();
    this._upgradeHandler = null;
    this._heartbeatTimer = null;
    this._attachedServer = null;
  }

  _log(level, ...args) {
    if (!this.logEnabled) return;
    const line = `[DeviceWS] ${args.map((a) => (typeof a === 'string' ? a : JSON.stringify(a))).join(' ')}`;
    if (level === 'error') console.error(line);
    else if (level === 'warn') console.error(line);
    else console.error(line);
  }

  /** Attach to an existing http.Server. Multiple device endpoints share a single port. */
  attach(httpServer) {
    if (this._attachedServer) {
      throw new Error('DeviceWsServer already attached');
    }
    this._attachedServer = httpServer;
    this._upgradeHandler = (req, socket, head) => {
      let url;
      try {
        url = new URL(req.url ?? '/', 'http://localhost');
      } catch {
        socket.destroy();
        return;
      }
      const deviceId = parseDeviceIdFromPath(url.pathname);
      if (!deviceId) {
        // Not a device endpoint — let other listeners handle it. If none, close.
        // We only handle our prefix; bail without consuming the socket so other
        // libs (if any) can claim it. Node will keep it alive for them; if no
        // one handles it within Node default, the connection just hangs and
        // gets timed out.
        if (!url.pathname?.startsWith('/device/')) return;
        socket.write('HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n');
        socket.destroy();
        return;
      }
      const key = url.searchParams.get('key') ?? '';
      if (!this.verifyKey({ deviceId, key })) {
        this._log('warn', `unauthorized device ws ${deviceId}`);
        socket.write('HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n');
        socket.destroy();
        return;
      }
      this.wss.handleUpgrade(req, socket, head, (ws) => {
        this._registerConnection(deviceId, ws);
      });
    };
    httpServer.on('upgrade', this._upgradeHandler);

    this._heartbeatTimer = setInterval(() => this._heartbeatTick(), PING_INTERVAL_MS);
    this._heartbeatTimer.unref?.();
  }

  /** Detach from http server and close all device connections. */
  async close() {
    if (this._heartbeatTimer) {
      clearInterval(this._heartbeatTimer);
      this._heartbeatTimer = null;
    }
    if (this._attachedServer && this._upgradeHandler) {
      this._attachedServer.off('upgrade', this._upgradeHandler);
      this._attachedServer = null;
      this._upgradeHandler = null;
    }
    for (const [, ws] of this.connections) {
      try { ws.close(1001, 'server shutdown'); } catch {}
    }
    await new Promise((resolve) => {
      this.wss.close(() => resolve());
    });
    this.connections.clear();
  }

  _registerConnection(deviceId, ws) {
    // Replace previous connection if any (last-writer-wins; common with reconnects).
    const existing = this.connections.get(deviceId);
    if (existing && existing !== ws) {
      try { existing.close(1000, 'replaced by new connection'); } catch {}
    }
    ws.isAlive = true;
    ws.deviceId = deviceId;
    ws._lastPongAt = Date.now();
    this.connections.set(deviceId, ws);
    this._log('info', `device connected ${deviceId}`);
    this.onPresence({ deviceId, event: 'connect' });

    ws.on('pong', () => {
      ws.isAlive = true;
      ws._lastPongAt = Date.now();
    });

    ws.on('message', (raw) => {
      let frame;
      try { frame = JSON.parse(raw.toString()); } catch {
        this._log('warn', `device ${deviceId} sent non-json frame`);
        return;
      }
      try {
        this.onMessage({ deviceId, frame });
      } catch (err) {
        this._log('error', `onMessage handler failed for ${deviceId}: ${err?.message ?? err}`);
      }
    });

    ws.on('close', () => {
      // Only delete from map if the current entry is this socket (avoid race
      // with replacement above).
      if (this.connections.get(deviceId) === ws) {
        this.connections.delete(deviceId);
      }
      this._log('info', `device disconnected ${deviceId}`);
      this.onPresence({ deviceId, event: 'disconnect' });
    });

    ws.on('error', (err) => {
      this._log('error', `device ${deviceId} ws error: ${err?.message ?? err}`);
    });
  }

  _heartbeatTick() {
    const now = Date.now();
    for (const [deviceId, ws] of this.connections) {
      if (!ws || ws.readyState !== WebSocket.OPEN) {
        this.connections.delete(deviceId);
        continue;
      }
      if (ws._lastPongAt && now - ws._lastPongAt > PING_TIMEOUT_MS) {
        this._log('warn', `device ${deviceId} heartbeat timeout, closing`);
        try { ws.terminate(); } catch {}
        this.connections.delete(deviceId);
        continue;
      }
      try { ws.ping(); } catch {}
    }
  }

  /** Push a JSON frame to a connected device. Returns sync result, never throws. */
  pushCommand(deviceId, payload) {
    const ws = this.connections.get(deviceId);
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      return { ok: false, reason: 'device_offline' };
    }
    let json;
    try {
      json = JSON.stringify(payload);
    } catch (err) {
      return { ok: false, reason: 'payload_serialization_failed', message: String(err?.message ?? err) };
    }
    try {
      ws.send(json);
      return { ok: true };
    } catch (err) {
      return { ok: false, reason: 'send_failed', message: String(err?.message ?? err) };
    }
  }

  isOnline(deviceId) {
    const ws = this.connections.get(deviceId);
    return !!(ws && ws.readyState === WebSocket.OPEN);
  }

  /**
   * Force-disconnect a device's existing ws (T74 §2 — server `device.revoked`
   * must drop already-connected extensions). Idempotent: returns `false` if no
   * connection is tracked for `deviceId`. The actual map entry is removed by
   * the `close` handler registered in `_registerConnection`.
   */
  disconnect(deviceId, reason = 'revoked') {
    const id = String(deviceId ?? '').trim();
    if (!id) return false;
    const ws = this.connections.get(id);
    if (!ws) return false;
    try {
      ws.close(1008, String(reason ?? 'revoked'));
    } catch {
      try { ws.terminate(); } catch { /* ignore */ }
    }
    return true;
  }

  listOnline() {
    const out = [];
    for (const [deviceId, ws] of this.connections) {
      if (ws.readyState === WebSocket.OPEN) out.push(deviceId);
    }
    return out;
  }
}
