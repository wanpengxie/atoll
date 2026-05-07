// HTTP-side tests for /api/device/{deviceId}/callback and /session endpoints.
// channelManager is replaced with a fake that records calls.

import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { RpcServer } from '../src/rpc-server.js';

function makeFakeChannelManager() {
  const calls = { callback: [], session: [] };
  return {
    calls,
    async deviceCallback(args) {
      calls.callback.push(args);
      return { recorded: true };
    },
    async deviceSessionUpdate(args) {
      calls.session.push(args);
      return { stored: true, user_id: args.userId };
    },
    // Below stubs avoid existing rpc-server paths from breaking when not used.
    async rpcCall() { return {}; },
    async getAdminStatus() { return {}; },
    async listChannels() { return []; },
    async getChannelInfo() { return {}; },
    async listMachines() { return []; },
  };
}

async function startServer({ verifyDeviceKey, authToken = 'token-rpc', defaultUserId = '' } = {}) {
  const tmp = mkdtempSync(path.join(os.tmpdir(), 'rpc-device-'));
  const socketPath = path.join(tmp, 'd.sock');
  const channelManager = makeFakeChannelManager();
  const server = new RpcServer({
    channelManager,
    socketPath,
    httpPort: 0,
    httpHost: '127.0.0.1',
    authToken,
    verifyDeviceKey,
    defaultUserId,
  });
  await server.start();
  const port = server.httpServer.address().port;
  return { server, channelManager, tmp, port };
}

async function stopServer(ctx) {
  await ctx.server.stop();
  rmSync(ctx.tmp, { recursive: true, force: true });
}

function httpJson(port, { method = 'POST', path: urlPath, headers = {}, body } = {}) {
  return new Promise((resolve, reject) => {
    const data = body ? JSON.stringify(body) : '';
    const req = http.request({
      host: '127.0.0.1',
      port,
      method,
      path: urlPath,
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(data),
        ...headers,
      },
    }, (res) => {
      let raw = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => { raw += chunk; });
      res.on('end', () => {
        let parsed = null;
        if (raw) {
          try { parsed = JSON.parse(raw); } catch { /* leave null */ }
        }
        resolve({ statusCode: res.statusCode, body: parsed, raw });
      });
    });
    req.on('error', reject);
    if (data) req.write(data);
    req.end();
  });
}

test('callback: rejects without Bearer (401)', async () => {
  const ctx = await startServer({ verifyDeviceKey: () => true });
  try {
    const res = await httpJson(ctx.port, {
      path: '/api/device/dev-1/callback',
      body: { correlation_id: 'c', status: 'ok' },
    });
    assert.equal(res.statusCode, 401);
    assert.equal(res.body.ok, false);
    assert.equal(res.body.error.code, 'unauthorized');
    assert.equal(ctx.channelManager.calls.callback.length, 0);
  } finally { await stopServer(ctx); }
});

test('callback: rejects bad device api key (401)', async () => {
  const ctx = await startServer({
    verifyDeviceKey: ({ deviceId, key }) => deviceId === 'dev-1' && key === 'good',
  });
  try {
    const res = await httpJson(ctx.port, {
      path: '/api/device/dev-1/callback',
      headers: { Authorization: 'Bearer bad' },
      body: { correlation_id: 'c', status: 'ok' },
    });
    assert.equal(res.statusCode, 401);
    assert.equal(res.body.error.code, 'unauthorized');
  } finally { await stopServer(ctx); }
});

test('callback: rejects missing correlation_id / bad status (400)', async () => {
  const ctx = await startServer({ verifyDeviceKey: () => true });
  try {
    const a = await httpJson(ctx.port, {
      path: '/api/device/dev-1/callback',
      headers: { Authorization: 'Bearer x' },
      body: { status: 'ok' },
    });
    assert.equal(a.statusCode, 400);
    assert.match(a.body.error.message, /correlation_id/);

    const b = await httpJson(ctx.port, {
      path: '/api/device/dev-1/callback',
      headers: { Authorization: 'Bearer x' },
      body: { correlation_id: 'c1', status: 'maybe' },
    });
    assert.equal(b.statusCode, 400);
    assert.match(b.body.error.message, /status must be/);
  } finally { await stopServer(ctx); }
});

test('callback: forwards normalized payload to channelManager.deviceCallback', async () => {
  const ctx = await startServer({ verifyDeviceKey: () => true });
  try {
    const res = await httpJson(ctx.port, {
      path: '/api/device/dev%2F1/callback',
      headers: { Authorization: 'Bearer x' },
      body: {
        correlation_id: 'corr-42',
        status: 'OK', // case-insensitive normalize
        result: { url: 'https://xhs.com/note/123', note_id: '123' },
      },
    });
    assert.equal(res.statusCode, 200);
    assert.equal(res.body.ok, true);
    assert.deepEqual(res.body.result, { recorded: true });
    assert.equal(ctx.channelManager.calls.callback.length, 1);
    const call = ctx.channelManager.calls.callback[0];
    assert.equal(call.deviceId, 'dev/1');
    assert.equal(call.correlationId, 'corr-42');
    assert.equal(call.status, 'ok');
    assert.deepEqual(call.result, { url: 'https://xhs.com/note/123', note_id: '123' });
    assert.equal(call.error, null);
  } finally { await stopServer(ctx); }
});

test('callback: propagates channelManager error code+statusCode', async () => {
  const ctx = await startServer({ verifyDeviceKey: () => true });
  ctx.channelManager.deviceCallback = async () => {
    const err = new Error('correlation not found');
    err.code = 'correlation_unknown';
    err.statusCode = 404;
    throw err;
  };
  try {
    const res = await httpJson(ctx.port, {
      path: '/api/device/dev-1/callback',
      headers: { Authorization: 'Bearer x' },
      body: { correlation_id: 'c1', status: 'ok' },
    });
    assert.equal(res.statusCode, 404);
    assert.equal(res.body.error.code, 'correlation_unknown');
  } finally { await stopServer(ctx); }
});

test('session: rejects missing user_id (400)', async () => {
  const ctx = await startServer({ verifyDeviceKey: () => true });
  try {
    const res = await httpJson(ctx.port, {
      path: '/api/device/dev-1/session',
      headers: { Authorization: 'Bearer x' },
      body: { cookies: [] },
    });
    assert.equal(res.statusCode, 400);
    assert.match(res.body.error.message, /user_id/);
  } finally { await stopServer(ctx); }
});

test('session: forwards user_id + patch to channelManager.deviceSessionUpdate', async () => {
  const ctx = await startServer({ verifyDeviceKey: () => true });
  try {
    const res = await httpJson(ctx.port, {
      path: '/api/device/dev-1/session',
      headers: { Authorization: 'Bearer x' },
      body: {
        user_id: 'user-001',
        cookies: [{ name: 'sid', value: 'v1' }],
        login_state: 'logged_in',
      },
    });
    assert.equal(res.statusCode, 200);
    assert.equal(res.body.ok, true);
    assert.deepEqual(res.body.result, { stored: true, user_id: 'user-001' });
    assert.equal(ctx.channelManager.calls.session.length, 1);
    const call = ctx.channelManager.calls.session[0];
    assert.equal(call.deviceId, 'dev-1');
    assert.equal(call.userId, 'user-001');
    assert.deepEqual(call.patch.cookies, [{ name: 'sid', value: 'v1' }]);
    assert.equal(call.patch.login_state, 'logged_in');
    assert.ok(!('user_id' in call.patch), 'user_id stripped from patch');
  } finally { await stopServer(ctx); }
});

test('unknown /api/device/.. action returns 404', async () => {
  const ctx = await startServer({ verifyDeviceKey: () => true });
  try {
    const res = await httpJson(ctx.port, {
      path: '/api/device/dev-1/other',
      headers: { Authorization: 'Bearer x' },
      body: {},
    });
    assert.equal(res.statusCode, 404);
  } finally { await stopServer(ctx); }
});

test('non-POST device endpoint returns 405', async () => {
  const ctx = await startServer({ verifyDeviceKey: () => true });
  try {
    const res = await httpJson(ctx.port, {
      method: 'GET',
      path: '/api/device/dev-1/callback',
    });
    assert.equal(res.statusCode, 405);
  } finally { await stopServer(ctx); }
});

// ── Fix-T2 §1 regression: device key MUST NOT authorize /rpc or /admin/* ────

test('valid device key cannot call /rpc (only daemon/machine tokens are admitted)', async () => {
  // verifyDeviceKey returns true for ('dev-1','device-key'); RPC authToken is
  // 'token-rpc'. A request to /rpc bearing the device key must 401.
  const ctx = await startServer({
    verifyDeviceKey: ({ deviceId, key }) => deviceId === 'dev-1' && key === 'device-key',
    authToken: 'token-rpc',
  });
  try {
    const res = await httpJson(ctx.port, {
      path: '/rpc',
      headers: { Authorization: 'Bearer device-key' },
      body: { method: 'channel.list', params: {} },
    });
    assert.equal(res.statusCode, 401);
    assert.equal(res.body.error.code, 'unauthorized');
  } finally { await stopServer(ctx); }
});

test('valid device key cannot call /admin/status', async () => {
  const ctx = await startServer({
    verifyDeviceKey: ({ deviceId, key }) => deviceId === 'dev-1' && key === 'device-key',
    authToken: 'token-rpc',
  });
  try {
    const res = await httpJson(ctx.port, {
      method: 'GET',
      path: '/admin/status',
      headers: { Authorization: 'Bearer device-key' },
    });
    assert.equal(res.statusCode, 401);
  } finally { await stopServer(ctx); }
});

test('callback: rejects empty status (400)', async () => {
  const ctx = await startServer({ verifyDeviceKey: () => true });
  try {
    const res = await httpJson(ctx.port, {
      path: '/api/device/dev-1/callback',
      headers: { Authorization: 'Bearer x' },
      body: { correlation_id: 'c', status: '' },
    });
    assert.equal(res.statusCode, 400);
    assert.match(res.body.error.message, /status must be/);
  } finally { await stopServer(ctx); }
});

// ── Fix-T2 §5: /api/device/{id}/session user_id optional via defaultUserId ──

test('session: falls back to defaultUserId when body omits user_id', async () => {
  const ctx = await startServer({
    verifyDeviceKey: () => true,
    defaultUserId: 'user-default',
  });
  try {
    const res = await httpJson(ctx.port, {
      path: '/api/device/dev-1/session',
      headers: { Authorization: 'Bearer x' },
      body: { cookies: [{ name: 'sid', value: 'v1' }] },
    });
    assert.equal(res.statusCode, 200);
    assert.equal(res.body.ok, true);
    assert.equal(ctx.channelManager.calls.session.length, 1);
    const call = ctx.channelManager.calls.session[0];
    assert.equal(call.userId, 'user-default');
    assert.deepEqual(call.patch.cookies, [{ name: 'sid', value: 'v1' }]);
  } finally { await stopServer(ctx); }
});

test('session: rejects when neither body.user_id nor defaultUserId provided', async () => {
  const ctx = await startServer({ verifyDeviceKey: () => true, defaultUserId: '' });
  try {
    const res = await httpJson(ctx.port, {
      path: '/api/device/dev-1/session',
      headers: { Authorization: 'Bearer x' },
      body: { cookies: [] },
    });
    assert.equal(res.statusCode, 400);
    assert.match(res.body.error.message, /user_id is required/);
  } finally { await stopServer(ctx); }
});

test('session: explicit body.user_id wins over defaultUserId', async () => {
  const ctx = await startServer({
    verifyDeviceKey: () => true,
    defaultUserId: 'user-default',
  });
  try {
    const res = await httpJson(ctx.port, {
      path: '/api/device/dev-1/session',
      headers: { Authorization: 'Bearer x' },
      body: { user_id: 'user-override', cookies: [] },
    });
    assert.equal(res.statusCode, 200);
    assert.equal(ctx.channelManager.calls.session[0].userId, 'user-override');
  } finally { await stopServer(ctx); }
});

test('callback: rejects when Authorization is not a Bearer (401)', async () => {
  const ctx = await startServer({ verifyDeviceKey: () => true });
  try {
    const res = await httpJson(ctx.port, {
      path: '/api/device/dev-1/callback',
      headers: { Authorization: 'Basic dXNlcjpwYXNz' },
      body: { correlation_id: 'c', status: 'ok' },
    });
    assert.equal(res.statusCode, 401);
  } finally { await stopServer(ctx); }
});
