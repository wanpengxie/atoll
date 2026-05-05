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

export class RpcServer {
  constructor({ channelManager, socketPath, httpPort = null, httpHost = '127.0.0.1', authToken = '', authTokens = [] }) {
    this.channelManager = channelManager;
    this.socketPath = socketPath;
    this.httpPort = httpPort;
    this.httpHost = httpHost;
    this.authToken = authToken;
    this.authTokens = [...new Set([authToken, ...authTokens].filter(Boolean))];
    this.socketServer = null;
    this.httpServer = null;
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
    }
  }

  async stop() {
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
      const result = await this.channelManager.rpcCall(method, params, context);
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
