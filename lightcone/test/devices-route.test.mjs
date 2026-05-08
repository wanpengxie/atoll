import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createServer } from 'node:http';
import test from 'node:test';

import express from 'express';

import { createDevicesRouter } from '../src/routes/devices.js';

function createApp(router, { user = { id: 'user-001', name: 'Owner' } } = {}) {
  const app = express();
  app.use(express.json());
  app.use((req, _res, next) => {
    if (user) req.user = user;
    next();
  });
  app.use('/api/devices', router);
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

async function callJson(baseUrl, path, { method = 'GET', body, headers = {} } = {}) {
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

function makeFakeDb() {
  const rows = [];
  return {
    rows,
    async insertDevice(_db, device) {
      rows.push({ ...device });
      return { ...device };
    },
    async getDeviceById(_db, id) {
      return rows.find(r => r.id === id) ?? null;
    },
    async getDevices(_db, filters = {}) {
      return rows.filter(r => {
        if (filters.user_id && r.user_id !== filters.user_id) return false;
        if (filters.daemon_id && r.daemon_id !== filters.daemon_id) return false;
        if (filters.channel_id && r.channel_id !== filters.channel_id) return false;
        if (filters.status && r.status !== filters.status) return false;
        return true;
      });
    },
    async revokeDevice(_db, id) {
      const target = rows.find(r => r.id === id);
      if (!target) return null;
      target.status = 'revoked';
      target.revoked_at = '2026-05-08T17:00:00.000Z';
      return { ...target };
    },
  };
}

test('POST /api/devices creates device with sk_dev key, persists row, and pushes device.created over WS', async () => {
  const fake = makeFakeDb();
  const sentToDaemons = [];
  const ids = ['dev-uuid-1'];
  const apiKeys = ['sk_dev_test_1'];

  const router = createDevicesRouter({
    getDbImpl: () => ({}),
    insertDeviceImpl: fake.insertDevice,
    getDevicesImpl: fake.getDevices,
    getDeviceByIdImpl: fake.getDeviceById,
    revokeDeviceImpl: fake.revokeDevice,
    sendToDaemonImpl: (daemonId, frame) => { sentToDaemons.push({ daemonId, frame }); return true; },
    isMachineOnlineImpl: () => true,
    uuidv4Impl: () => ids.shift(),
    generateApiKeyImpl: () => apiKeys.shift(),
  });

  await withServer(createApp(router), async (baseUrl) => {
    const created = await callJson(baseUrl, '/api/devices', {
      method: 'POST',
      body: {
        user_id: 'user-001',
        channel_id: 'ch-001',
        daemon_id: 'daemon-001',
        device_type: 'xhs',
        device_id: 'xhs-001',
      },
    });

    assert.equal(created.status, 201);
    assert.equal(created.json.id, 'dev-uuid-1');
    assert.equal(created.json.api_key, 'sk_dev_test_1');
    assert.equal(created.json.device_id, 'xhs-001');
    assert.equal(created.json.user_id, 'user-001');
    assert.equal(created.json.channel_id, 'ch-001');
    assert.equal(created.json.daemon_id, 'daemon-001');
    assert.equal(created.json.device_type, 'xhs');
    assert.equal(created.json.status, 'active');

    assert.equal(fake.rows.length, 1);
    assert.equal(fake.rows[0].api_key, 'sk_dev_test_1');

    assert.equal(sentToDaemons.length, 1);
    assert.equal(sentToDaemons[0].daemonId, 'daemon-001');
    assert.equal(sentToDaemons[0].frame.type, 'device.created');
    assert.equal(sentToDaemons[0].frame.payload.id, 'dev-uuid-1');
    assert.equal(sentToDaemons[0].frame.payload.api_key, 'sk_dev_test_1');
  });
});

test('POST /api/devices generates default device_id when omitted', async () => {
  const fake = makeFakeDb();
  const router = createDevicesRouter({
    getDbImpl: () => ({}),
    insertDeviceImpl: fake.insertDevice,
    getDevicesImpl: fake.getDevices,
    getDeviceByIdImpl: fake.getDeviceById,
    revokeDeviceImpl: fake.revokeDevice,
    sendToDaemonImpl: () => true,
    isMachineOnlineImpl: () => true,
    uuidv4Impl: () => 'dev-uuid-2',
    generateApiKeyImpl: () => 'sk_dev_test_2',
  });

  await withServer(createApp(router), async (baseUrl) => {
    const created = await callJson(baseUrl, '/api/devices', {
      method: 'POST',
      body: {
        channel_id: 'ch-002',
        daemon_id: 'daemon-001',
        device_type: 'xhs',
      },
    });
    assert.equal(created.status, 201);
    // device_id falls back to a non-empty string when omitted
    assert.equal(typeof created.json.device_id, 'string');
    assert.ok(created.json.device_id.length > 0);
    // user_id falls back to req.user.id
    assert.equal(created.json.user_id, 'user-001');
  });
});

test('POST /api/devices rejects without auth', async () => {
  const router = createDevicesRouter({
    getDbImpl: () => ({}),
    insertDeviceImpl: async () => { throw new Error('should not call'); },
    getDevicesImpl: async () => [],
    getDeviceByIdImpl: async () => null,
    revokeDeviceImpl: async () => null,
    sendToDaemonImpl: () => true,
    isMachineOnlineImpl: () => true,
  });

  await withServer(createApp(router, { user: null }), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/devices', {
      method: 'POST',
      body: { channel_id: 'ch-001', device_type: 'xhs' },
    });
    assert.equal(res.status, 401);
  });
});

test('POST /api/devices validates required fields (device_type)', async () => {
  const router = createDevicesRouter({
    getDbImpl: () => ({}),
    insertDeviceImpl: async () => { throw new Error('should not call'); },
    getDevicesImpl: async () => [],
    getDeviceByIdImpl: async () => null,
    revokeDeviceImpl: async () => null,
    sendToDaemonImpl: () => true,
    isMachineOnlineImpl: () => true,
  });

  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/devices', {
      method: 'POST',
      body: { channel_id: 'ch-001' },
    });
    assert.equal(res.status, 400);
    assert.match(res.json.error, /device_type/);
  });
});

test('GET /api/devices returns devices and accepts filters', async () => {
  const fake = makeFakeDb();
  fake.rows.push(
    { id: 'd1', device_id: 'xhs-001', api_key: 'sk_dev_a', user_id: 'user-001', channel_id: 'ch-001', daemon_id: 'd-1', device_type: 'xhs', status: 'active', created_at: '2026-05-08', revoked_at: null },
    { id: 'd2', device_id: 'xhs-002', api_key: 'sk_dev_b', user_id: 'user-001', channel_id: 'ch-002', daemon_id: 'd-2', device_type: 'xhs', status: 'revoked', created_at: '2026-05-08', revoked_at: '2026-05-08' },
    { id: 'd3', device_id: 'dy-001', api_key: 'sk_dev_c', user_id: 'user-002', channel_id: 'ch-003', daemon_id: 'd-1', device_type: 'douyin', status: 'active', created_at: '2026-05-08', revoked_at: null },
  );

  const router = createDevicesRouter({
    getDbImpl: () => ({}),
    insertDeviceImpl: fake.insertDevice,
    getDevicesImpl: fake.getDevices,
    getDeviceByIdImpl: fake.getDeviceById,
    revokeDeviceImpl: fake.revokeDevice,
    sendToDaemonImpl: () => true,
    isMachineOnlineImpl: () => true,
  });

  await withServer(createApp(router), async (baseUrl) => {
    const all = await callJson(baseUrl, '/api/devices');
    assert.equal(all.status, 200);
    assert.equal(all.json.devices.length, 3);

    const onlyActive = await callJson(baseUrl, '/api/devices?status=active');
    assert.equal(onlyActive.json.devices.length, 2);

    const byUser = await callJson(baseUrl, '/api/devices?user_id=user-001');
    assert.equal(byUser.json.devices.length, 2);

    const byDaemon = await callJson(baseUrl, '/api/devices?daemon_id=d-1');
    assert.equal(byDaemon.json.devices.length, 2);
  });
});

test('DELETE /api/devices/:id revokes device and pushes device.revoked over WS', async () => {
  const fake = makeFakeDb();
  fake.rows.push({
    id: 'd-revoke', device_id: 'xhs-r', api_key: 'sk_dev_x', user_id: 'user-001',
    channel_id: 'ch-001', daemon_id: 'daemon-001', device_type: 'xhs',
    status: 'active', created_at: '2026-05-08', revoked_at: null,
  });
  const sent = [];
  const router = createDevicesRouter({
    getDbImpl: () => ({}),
    insertDeviceImpl: fake.insertDevice,
    getDevicesImpl: fake.getDevices,
    getDeviceByIdImpl: fake.getDeviceById,
    revokeDeviceImpl: fake.revokeDevice,
    sendToDaemonImpl: (daemonId, frame) => { sent.push({ daemonId, frame }); return true; },
    isMachineOnlineImpl: () => true,
  });

  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/devices/d-revoke', { method: 'DELETE' });
    assert.equal(res.status, 200);
    assert.equal(res.json.ok, true);
    assert.equal(fake.rows[0].status, 'revoked');

    assert.equal(sent.length, 1);
    assert.equal(sent[0].daemonId, 'daemon-001');
    assert.equal(sent[0].frame.type, 'device.revoked');
    assert.equal(sent[0].frame.payload.id, 'd-revoke');
    assert.equal(sent[0].frame.payload.status, 'revoked');
  });
});

test('DELETE /api/devices/:id returns 404 when device does not exist', async () => {
  const fake = makeFakeDb();
  const router = createDevicesRouter({
    getDbImpl: () => ({}),
    insertDeviceImpl: fake.insertDevice,
    getDevicesImpl: fake.getDevices,
    getDeviceByIdImpl: fake.getDeviceById,
    revokeDeviceImpl: fake.revokeDevice,
    sendToDaemonImpl: () => true,
    isMachineOnlineImpl: () => true,
  });

  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/devices/missing', { method: 'DELETE' });
    assert.equal(res.status, 404);
  });
});
