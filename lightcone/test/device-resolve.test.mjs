import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createServer } from 'node:http';
import test from 'node:test';

import express from 'express';

import { createDeviceRouter } from '../src/routes/device.js';

function createApp(router) {
  const app = express();
  app.use(express.json());
  app.use('/api/device', router);
  app.use((err, _req, res, _next) => {
    res.status(500).json({ error: err.message });
  });
  return app;
}

async function withServer(app, run) {
  const server = createServer(app);
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const { port } = server.address();
  try {
    await run(`http://127.0.0.1:${port}`);
  } finally {
    await new Promise((resolve, reject) => {
      server.close((err) => (err ? reject(err) : resolve()));
    });
  }
}

async function callJson(baseUrl, path, { method = 'POST', body, headers = {} } = {}) {
  const res = await fetch(`${baseUrl}${path}`, {
    method,
    headers: {
      'content-type': 'application/json',
      ...headers,
    },
    body: body == null ? undefined : JSON.stringify(body),
  });
  let json = null;
  try { json = await res.json(); } catch { /* empty */ }
  return { status: res.status, json };
}

const ACTIVE_DEVICE = {
  id: 'd-1',
  device_id: 'xhs-001',
  api_key: 'sk_dev_active',
  user_id: 'user-001',
  channel_id: 'ch-001',
  daemon_id: 'daemon-001',
  device_type: 'xhs',
  status: 'active',
  created_at: '2026-05-08',
  revoked_at: null,
};

const REVOKED_DEVICE = { ...ACTIVE_DEVICE, id: 'd-2', api_key: 'sk_dev_revoked', status: 'revoked', revoked_at: '2026-05-08' };

test('POST /api/device/resolve returns ws_url, http_url and device info for active key', async () => {
  const router = createDeviceRouter({
    getDbImpl: () => ({}),
    getDeviceByApiKeyImpl: async (_db, key) => (key === ACTIVE_DEVICE.api_key ? ACTIVE_DEVICE : null),
    getMachineByIdImpl: async (_db, id) => (id === 'daemon-001'
      ? { id: 'daemon-001', daemon_host: '192.168.0.10', daemon_port: 9501 }
      : null),
    resolveRateLimitImpl: () => true,
  });

  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/device/resolve', {
      body: { api_key: ACTIVE_DEVICE.api_key },
    });
    assert.equal(res.status, 200);
    assert.equal(res.json.ws_url, 'ws://192.168.0.10:9501');
    assert.equal(res.json.http_url, 'http://192.168.0.10:9501');
    assert.equal(res.json.device_id, 'xhs-001');
    assert.equal(res.json.user_id, 'user-001');
    assert.equal(res.json.channel_id, 'ch-001');
    assert.equal(res.json.daemon_id, 'daemon-001');
    // never echo back the secret
    assert.equal(Object.hasOwn(res.json, 'api_key'), false);
  });
});

test('POST /api/device/resolve returns 404 for revoked device', async () => {
  const router = createDeviceRouter({
    getDbImpl: () => ({}),
    getDeviceByApiKeyImpl: async () => REVOKED_DEVICE,
    getMachineByIdImpl: async () => ({ id: 'daemon-001', daemon_host: 'h', daemon_port: 9501 }),
    resolveRateLimitImpl: () => true,
  });

  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/device/resolve', {
      body: { api_key: REVOKED_DEVICE.api_key },
    });
    assert.equal(res.status, 404);
  });
});

test('POST /api/device/resolve returns 404 for unknown api_key', async () => {
  const router = createDeviceRouter({
    getDbImpl: () => ({}),
    getDeviceByApiKeyImpl: async () => null,
    getMachineByIdImpl: async () => null,
    resolveRateLimitImpl: () => true,
  });

  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/device/resolve', { body: { api_key: 'sk_dev_nope' } });
    assert.equal(res.status, 404);
  });
});

test('POST /api/device/resolve returns 503 when daemon has no host/port info yet', async () => {
  const router = createDeviceRouter({
    getDbImpl: () => ({}),
    getDeviceByApiKeyImpl: async () => ACTIVE_DEVICE,
    getMachineByIdImpl: async () => ({ id: 'daemon-001', daemon_host: null, daemon_port: null }),
    resolveRateLimitImpl: () => true,
  });

  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/device/resolve', { body: { api_key: ACTIVE_DEVICE.api_key } });
    assert.equal(res.status, 503);
  });
});

test('POST /api/device/resolve returns 400 when api_key is missing', async () => {
  const router = createDeviceRouter({
    getDbImpl: () => ({}),
    getDeviceByApiKeyImpl: async () => null,
    getMachineByIdImpl: async () => null,
    resolveRateLimitImpl: () => true,
  });

  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/device/resolve', { body: {} });
    assert.equal(res.status, 400);
  });
});

test('POST /api/device/resolve enforces rate limit (429)', async () => {
  let allowed = 2;
  const router = createDeviceRouter({
    getDbImpl: () => ({}),
    getDeviceByApiKeyImpl: async () => ACTIVE_DEVICE,
    getMachineByIdImpl: async () => ({ id: 'daemon-001', daemon_host: 'h', daemon_port: 9501 }),
    resolveRateLimitImpl: () => {
      if (allowed > 0) { allowed -= 1; return true; }
      return false;
    },
  });

  await withServer(createApp(router), async (baseUrl) => {
    const ok1 = await callJson(baseUrl, '/api/device/resolve', { body: { api_key: ACTIVE_DEVICE.api_key } });
    const ok2 = await callJson(baseUrl, '/api/device/resolve', { body: { api_key: ACTIVE_DEVICE.api_key } });
    const blocked = await callJson(baseUrl, '/api/device/resolve', { body: { api_key: ACTIVE_DEVICE.api_key } });
    assert.equal(ok1.status, 200);
    assert.equal(ok2.status, 200);
    assert.equal(blocked.status, 429);
  });
});
