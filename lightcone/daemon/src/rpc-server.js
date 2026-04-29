import http from 'http';
import { chmodSync, existsSync, unlinkSync } from 'fs';

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
  constructor({ channelManager, socketPath, httpPort = null, httpHost = '127.0.0.1', authToken = '' }) {
    this.channelManager = channelManager;
    this.socketPath = socketPath;
    this.httpPort = httpPort;
    this.httpHost = httpHost;
    this.authToken = authToken;
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
    if (req.method !== 'POST' || req.url !== '/rpc') {
      writeJson(res, 404, { ok: false, error: { code: 'not_found', message: 'POST /rpc required' } });
      return;
    }

    if (context.transport === 'http' && this.authToken) {
      const auth = req.headers.authorization ?? '';
      if (auth !== `Bearer ${this.authToken}`) {
        writeJson(res, 401, { ok: false, error: { code: 'unauthorized', message: 'invalid bearer token' } });
        return;
      }
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
      writeJson(res, 400, {
        ok: false,
        error: {
          code: err.code ?? 'rpc_error',
          message: err.message,
        },
      });
    }
  }
}
