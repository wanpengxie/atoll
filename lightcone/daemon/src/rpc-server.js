import http from 'http';
import path from 'path';
import { chmodSync, existsSync, mkdirSync, unlinkSync } from 'fs';

function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let raw = '';
    req.setEncoding('utf8');
    req.on('data', (chunk) => {
      raw += chunk;
      if (raw.length > 1_000_000) {
        reject(new Error('request body too large'));
        req.destroy();
      }
    });
    req.on('end', () => {
      if (!raw.trim()) return resolve({});
      try {
        resolve(JSON.parse(raw));
      } catch (err) {
        reject(new Error(`invalid json body: ${err.message}`));
      }
    });
    req.on('error', reject);
  });
}

function writeJson(res, statusCode, payload) {
  res.statusCode = statusCode;
  res.setHeader('Content-Type', 'application/json');
  res.end(JSON.stringify(payload));
}

function headerValue(req, name) {
  const value = req.headers[name.toLowerCase()];
  if (Array.isArray(value)) return String(value[0] ?? '').trim();
  return String(value ?? '').trim();
}

export class RpcServer {
  constructor({
    channelManager,
    socketPath,
    httpPort = null,
    httpHost = '127.0.0.1',
    authToken = '',
    authTokens = [],
    deviceWsServer = null,
    verifyDeviceKey = null,
  }) {
    this.channelManager = channelManager;
    this.socketPath = socketPath;
    this.httpPort = httpPort;
    this.httpHost = httpHost;
    this.authToken = authToken;
    this.authTokens = [...new Set([authToken, ...authTokens].filter(Boolean))];
    this.socketServer = null;
    this.httpServer = null;
    this.deviceWsServer = deviceWsServer;
    this.verifyDeviceKey = typeof verifyDeviceKey === 'function' ? verifyDeviceKey : null;
  }

  async start() {
    this.socketServer = http.createServer((req, res) => {
      this._handleRequest(req, res, { transport: 'socket' }).catch((err) => {
        writeJson(res, 500, { ok: false, error: { code: 'internal_error', message: err.message } });
      });
    });

    if (existsSync(this.socketPath)) {
      unlinkSync(this.socketPath);
    }
    mkdirSync(path.dirname(this.socketPath), { recursive: true });

    await new Promise((resolve, reject) => {
      this.socketServer.once('error', reject);
      this.socketServer.listen(this.socketPath, () => resolve());
    });
    chmodSync(this.socketPath, 0o600);

    if (this.httpPort != null) {
      this.httpServer = http.createServer((req, res) => {
        this._handleRequest(req, res, { transport: 'http' }).catch((err) => {
          writeJson(res, 500, { ok: false, error: { code: 'internal_error', message: err.message } });
        });
      });

      await new Promise((resolve, reject) => {
        this.httpServer.once('error', reject);
        this.httpServer.listen(this.httpPort, this.httpHost, () => resolve());
      });

      // Attach device WS server (if configured) to the same http port —
      // shares the listener via 'upgrade' event interception (see DeviceWsServer.attach).
      if (this.deviceWsServer && typeof this.deviceWsServer.attach === 'function') {
        this.deviceWsServer.attach(this.httpServer);
      }
    }
  }

  async stop() {
    if (this.deviceWsServer && typeof this.deviceWsServer.close === 'function') {
      try { await this.deviceWsServer.close(); } catch {}
    }
    await Promise.all([
      this._closeServer(this.socketServer),
      this._closeServer(this.httpServer),
    ]);
  }

  async _closeServer(server) {
    if (!server) return;
    await new Promise((resolve) => server.close(() => resolve()));
  }

  async _handleRequest(req, res, context) {
    const url = new URL(req.url ?? '/', 'http://localhost');

    if (url.pathname.startsWith('/admin/')) {
      await this._handleAdminRequest(req, res, url);
      return;
    }

    if (url.pathname.startsWith('/api/device/')) {
      await this._handleDeviceRequest(req, res, url);
      return;
    }

    if (req.method !== 'POST' || url.pathname !== '/rpc') {
      writeJson(res, 404, { ok: false, error: { code: 'not_found', message: 'POST /rpc required' } });
      return;
    }

    if (this.authTokens.length > 0 && !this._isAuthorized(req, context)) {
      writeJson(res, 401, { ok: false, error: { code: 'unauthorized', message: 'invalid bearer token' } });
      return;
    }

    let body;
    try {
      body = await readJsonBody(req);
    } catch (err) {
      writeJson(res, 400, { ok: false, error: { code: 'bad_request', message: err.message } });
      return;
    }

    const method = String(body.method ?? '').trim();
    const params = body.params ?? {};
    if (!method) {
      writeJson(res, 400, { ok: false, error: { code: 'bad_request', message: 'method is required' } });
      return;
    }

    try {
      const result = await this.channelManager.rpcCall(method, params, {
        ...context,
        agentName: headerValue(req, 'x-coagent-agent-name'),
        channelId: headerValue(req, 'x-coagent-channel-id'),
        sessionId: headerValue(req, 'x-coagent-session-id'),
      });
      writeJson(res, 200, { ok: true, result });
    } catch (err) {
      writeJson(res, err.statusCode ?? 400, {
        ok: false,
        error: {
          code: err.code ?? 'rpc_error',
          message: err.message,
        },
      });
    }
  }

  _isAuthorized(req, _context = {}) {
    if (this.authTokens.length === 0) return true;
    const auth = req.headers.authorization ?? '';
    if (!auth.startsWith('Bearer ')) return false;
    return this.authTokens.includes(auth.slice(7));
  }

  _bearerKey(req) {
    const auth = req.headers.authorization ?? '';
    if (typeof auth !== 'string' || !auth.startsWith('Bearer ')) return '';
    return auth.slice(7).trim();
  }

  async _handleDeviceRequest(req, res, url) {
    // Routes:
    //   POST /api/device/{deviceId}/callback
    //   POST /api/device/{deviceId}/session
    const m = /^\/api\/device\/([^/]+)\/(callback|session)\/?$/.exec(url.pathname);
    if (!m) {
      writeJson(res, 404, { ok: false, error: { code: 'not_found', message: 'unknown device endpoint' } });
      return;
    }
    if (req.method !== 'POST') {
      writeJson(res, 405, { ok: false, error: { code: 'method_not_allowed', message: 'POST required' } });
      return;
    }
    let deviceId;
    try {
      deviceId = decodeURIComponent(m[1]);
    } catch {
      writeJson(res, 400, { ok: false, error: { code: 'bad_request', message: 'invalid device id' } });
      return;
    }
    const action = m[2];

    const key = this._bearerKey(req);
    if (!key) {
      writeJson(res, 401, { ok: false, error: { code: 'unauthorized', message: 'Bearer token required' } });
      return;
    }
    if (!this.verifyDeviceKey || !this.verifyDeviceKey({ deviceId, key })) {
      writeJson(res, 401, { ok: false, error: { code: 'unauthorized', message: 'invalid device api key' } });
      return;
    }

    let body;
    try {
      body = await readJsonBody(req);
    } catch (err) {
      writeJson(res, 400, { ok: false, error: { code: 'bad_request', message: err.message } });
      return;
    }

    try {
      if (action === 'callback') {
        const correlationId = String(body.correlation_id ?? body.correlationId ?? '').trim();
        if (!correlationId) {
          writeJson(res, 400, { ok: false, error: { code: 'bad_request', message: 'correlation_id is required' } });
          return;
        }
        const status = String(body.status ?? '').trim().toLowerCase();
        if (!['ok', 'error'].includes(status)) {
          writeJson(res, 400, { ok: false, error: { code: 'bad_request', message: 'status must be ok|error' } });
          return;
        }
        const result = await this.channelManager.deviceCallback({
          deviceId,
          correlationId,
          status,
          result: body.result ?? null,
          error: body.error ?? null,
        });
        writeJson(res, 200, { ok: true, result });
        return;
      }

      if (action === 'session') {
        // body must contain user_id (caller provides which xhs login this device represents).
        const userId = String(body.user_id ?? body.userId ?? '').trim();
        if (!userId) {
          writeJson(res, 400, { ok: false, error: { code: 'bad_request', message: 'user_id is required' } });
          return;
        }
        const patch = { ...body };
        delete patch.user_id;
        delete patch.userId;
        const result = await this.channelManager.deviceSessionUpdate({
          deviceId,
          userId,
          patch,
        });
        writeJson(res, 200, { ok: true, result });
        return;
      }
    } catch (err) {
      writeJson(res, err.statusCode ?? 400, {
        ok: false,
        error: {
          code: err.code ?? 'device_error',
          message: err.message,
        },
      });
    }
  }

  async _handleAdminRequest(req, res, url) {
    if (req.method !== 'GET') {
      writeJson(res, 405, { ok: false, error: { code: 'method_not_allowed', message: 'GET required' } });
      return;
    }
    if (!this._isAuthorized(req)) {
      writeJson(res, 401, { ok: false, error: { code: 'unauthorized', message: 'invalid bearer token' } });
      return;
    }

    try {
      if (url.pathname === '/admin/status') {
        writeJson(res, 200, await this.channelManager.getAdminStatus());
        return;
      }
      if (url.pathname === '/admin/channels') {
        writeJson(res, 200, await this.channelManager.listChannels());
        return;
      }
      if (url.pathname.startsWith('/admin/channels/')) {
        const channelId = decodeURIComponent(url.pathname.slice('/admin/channels/'.length));
        writeJson(res, 200, await this.channelManager.getChannelInfo(channelId));
        return;
      }
      if (url.pathname === '/admin/machines') {
        writeJson(res, 200, await this.channelManager.listMachines());
        return;
      }
      writeJson(res, 404, { ok: false, error: { code: 'not_found', message: 'unknown admin endpoint' } });
    } catch (err) {
      writeJson(res, 400, {
        ok: false,
        error: {
          code: err.code ?? 'admin_error',
          message: err.message,
        },
      });
    }
  }
}
