import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createServer } from 'node:http';
import test from 'node:test';

import express from 'express';

import {
  createDevicesRouter,
  getDevicePushFailureCount,
  resetDevicePushFailureCount,
} from '../src/routes/devices.js';

function createApp(router, { user = { id: 'user-001', name: 'Owner' }, isService = false } = {}) {
  const app = express();
  app.use(express.json());
  app.use((req, _res, next) => {
    if (user) {
      req.user = user;
      if (isService) req.isService = true;
    }
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

// ── ownership stubs ─────────────────────────────────────────────────────────
// Default: a single channel `ch-001` in workspace `ws-001` and a single
// machine `daemon-001` owned by `user-001` — happy path for tests that don't
// care about ownership rejection. Override per-test as needed.
function makeOwnershipStubs({
  channels = { 'ch-001': { id: 'ch-001', workspace_id: 'ws-001', is_del: 0, deleted_at: null } },
  machines = { 'daemon-001': { id: 'daemon-001', owner_id: 'user-001', is_platform: 0 } },
  workspaceMembers = new Set(['ws-001:user-001']),
} = {}) {
  return {
    getChannelByIdImpl: async (_db, id) => channels[id] ?? null,
    getMachineByIdImpl: async (_db, id) => machines[id] ?? null,
    isWorkspaceMemberImpl: async (_db, wsId, userId) => workspaceMembers.has(`${wsId}:${userId}`),
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
    ...makeOwnershipStubs(),
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
    ...makeOwnershipStubs(),
    sendToDaemonImpl: () => true,
    isMachineOnlineImpl: () => true,
    uuidv4Impl: () => 'dev-uuid-2',
    generateApiKeyImpl: () => 'sk_dev_test_2',
  });

  await withServer(createApp(router), async (baseUrl) => {
    const created = await callJson(baseUrl, '/api/devices', {
      method: 'POST',
      body: {
        channel_id: 'ch-001',
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
    ...makeOwnershipStubs(),
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
    ...makeOwnershipStubs(),
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

// ── Negative ownership tests ────────────────────────────────────────────────

test('POST /api/devices rejects cross-user user_id (non-service)', async () => {
  const fake = makeFakeDb();
  const router = createDevicesRouter({
    getDbImpl: () => ({}),
    insertDeviceImpl: fake.insertDevice,
    getDevicesImpl: fake.getDevices,
    getDeviceByIdImpl: fake.getDeviceById,
    revokeDeviceImpl: fake.revokeDevice,
    ...makeOwnershipStubs(),
    sendToDaemonImpl: () => true,
    isMachineOnlineImpl: () => true,
  });

  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/devices', {
      method: 'POST',
      body: { user_id: 'user-evil', device_type: 'xhs' },
    });
    assert.equal(res.status, 403);
    assert.match(res.json.error, /another user/i);
    assert.equal(fake.rows.length, 0);
  });
});

test('POST /api/devices rejects channel_id when caller is not a workspace member', async () => {
  const fake = makeFakeDb();
  const router = createDevicesRouter({
    getDbImpl: () => ({}),
    insertDeviceImpl: fake.insertDevice,
    getDevicesImpl: fake.getDevices,
    getDeviceByIdImpl: fake.getDeviceById,
    revokeDeviceImpl: fake.revokeDevice,
    // ch-other lives in ws-other; user-001 is NOT a member of ws-other.
    ...makeOwnershipStubs({
      channels: {
        'ch-001': { id: 'ch-001', workspace_id: 'ws-001', is_del: 0, deleted_at: null },
        'ch-other': { id: 'ch-other', workspace_id: 'ws-other', is_del: 0, deleted_at: null },
      },
      workspaceMembers: new Set(['ws-001:user-001']),
    }),
    sendToDaemonImpl: () => true,
    isMachineOnlineImpl: () => true,
  });

  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/devices', {
      method: 'POST',
      body: { channel_id: 'ch-other', daemon_id: 'daemon-001', device_type: 'xhs' },
    });
    assert.equal(res.status, 403);
    assert.match(res.json.error, /workspace membership/i);
    assert.equal(fake.rows.length, 0);
  });
});

test('POST /api/devices returns 404 when channel_id does not exist', async () => {
  const fake = makeFakeDb();
  const router = createDevicesRouter({
    getDbImpl: () => ({}),
    insertDeviceImpl: fake.insertDevice,
    getDevicesImpl: fake.getDevices,
    getDeviceByIdImpl: fake.getDeviceById,
    revokeDeviceImpl: fake.revokeDevice,
    ...makeOwnershipStubs({ channels: {} }),
    sendToDaemonImpl: () => true,
    isMachineOnlineImpl: () => true,
  });

  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/devices', {
      method: 'POST',
      body: { channel_id: 'ch-missing', device_type: 'xhs' },
    });
    assert.equal(res.status, 404);
    assert.match(res.json.error, /Channel not found/);
    assert.equal(fake.rows.length, 0);
  });
});

test('POST /api/devices rejects daemon_id when caller does not own the machine', async () => {
  const fake = makeFakeDb();
  const router = createDevicesRouter({
    getDbImpl: () => ({}),
    insertDeviceImpl: fake.insertDevice,
    getDevicesImpl: fake.getDevices,
    getDeviceByIdImpl: fake.getDeviceById,
    revokeDeviceImpl: fake.revokeDevice,
    ...makeOwnershipStubs({
      machines: {
        'daemon-001': { id: 'daemon-001', owner_id: 'user-001', is_platform: 0 },
        'daemon-evil': { id: 'daemon-evil', owner_id: 'user-evil', is_platform: 0 },
      },
    }),
    sendToDaemonImpl: () => true,
    isMachineOnlineImpl: () => true,
  });

  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/devices', {
      method: 'POST',
      body: { daemon_id: 'daemon-evil', device_type: 'xhs' },
    });
    assert.equal(res.status, 403);
    assert.match(res.json.error, /daemon/i);
    assert.equal(fake.rows.length, 0);
  });
});

test('POST /api/devices accepts platform-owned daemon for any user', async () => {
  const fake = makeFakeDb();
  const router = createDevicesRouter({
    getDbImpl: () => ({}),
    insertDeviceImpl: fake.insertDevice,
    getDevicesImpl: fake.getDevices,
    getDeviceByIdImpl: fake.getDeviceById,
    revokeDeviceImpl: fake.revokeDevice,
    ...makeOwnershipStubs({
      machines: {
        'platform-1': { id: 'platform-1', owner_id: null, is_platform: 1 },
      },
    }),
    sendToDaemonImpl: () => true,
    isMachineOnlineImpl: () => true,
    uuidv4Impl: () => 'dev-platform',
    generateApiKeyImpl: () => 'sk_dev_platform',
  });

  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/devices', {
      method: 'POST',
      body: { daemon_id: 'platform-1', device_type: 'xhs' },
    });
    assert.equal(res.status, 201);
    assert.equal(res.json.user_id, 'user-001');
    assert.equal(res.json.daemon_id, 'platform-1');
  });
});

test('POST /api/devices service caller may set arbitrary user_id', async () => {
  const fake = makeFakeDb();
  const router = createDevicesRouter({
    getDbImpl: () => ({}),
    insertDeviceImpl: fake.insertDevice,
    getDevicesImpl: fake.getDevices,
    getDeviceByIdImpl: fake.getDeviceById,
    revokeDeviceImpl: fake.revokeDevice,
    ...makeOwnershipStubs(),
    sendToDaemonImpl: () => true,
    isMachineOnlineImpl: () => true,
    uuidv4Impl: () => 'dev-svc',
    generateApiKeyImpl: () => 'sk_dev_svc',
  });

  await withServer(createApp(router, { user: { id: 'service', name: 'Service' }, isService: true }), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/devices', {
      method: 'POST',
      body: { user_id: 'user-target', device_type: 'xhs' },
    });
    assert.equal(res.status, 201);
    assert.equal(res.json.user_id, 'user-target');
  });
});

// ── GET tests ───────────────────────────────────────────────────────────────

test('GET /api/devices defaults to caller-scoped listing and accepts filters', async () => {
  const fake = makeFakeDb();
  fake.rows.push(
    { id: 'd1', device_id: 'xhs-001', api_key: 'sk_dev_a', user_id: 'user-001', channel_id: 'ch-001', daemon_id: 'd-1', device_type: 'xhs', status: 'active', created_at: '2026-05-08', revoked_at: null },
    { id: 'd2', device_id: 'xhs-002', api_key: 'sk_dev_b', user_id: 'user-001', channel_id: 'ch-002', daemon_id: 'd-2', device_type: 'xhs', status: 'revoked', created_at: '2026-05-08', revoked_at: '2026-05-08' },
    { id: 'd3', device_id: 'dy-001',  api_key: 'sk_dev_c', user_id: 'user-002', channel_id: 'ch-003', daemon_id: 'd-1', device_type: 'douyin', status: 'active', created_at: '2026-05-08', revoked_at: null },
  );

  const router = createDevicesRouter({
    getDbImpl: () => ({}),
    insertDeviceImpl: fake.insertDevice,
    getDevicesImpl: fake.getDevices,
    getDeviceByIdImpl: fake.getDeviceById,
    revokeDeviceImpl: fake.revokeDevice,
    ...makeOwnershipStubs(),
    sendToDaemonImpl: () => true,
    isMachineOnlineImpl: () => true,
  });

  await withServer(createApp(router), async (baseUrl) => {
    const all = await callJson(baseUrl, '/api/devices');
    assert.equal(all.status, 200);
    // user-001 only (was previously 3 — IDOR)
    assert.equal(all.json.devices.length, 2);
    assert.ok(all.json.devices.every(d => d.user_id === 'user-001'));

    const onlyActive = await callJson(baseUrl, '/api/devices?status=active');
    assert.equal(onlyActive.json.devices.length, 1);
    assert.equal(onlyActive.json.devices[0].id, 'd1');

    const byUser = await callJson(baseUrl, '/api/devices?user_id=user-001');
    assert.equal(byUser.json.devices.length, 2);

    const byDaemon = await callJson(baseUrl, '/api/devices?daemon_id=d-1');
    // d-1 has user-001's d1 + user-002's d3, but caller scope drops d3.
    assert.equal(byDaemon.json.devices.length, 1);
    assert.equal(byDaemon.json.devices[0].id, 'd1');
  });
});

test('GET /api/devices?user_id=other-user returns 403 for non-service caller', async () => {
  const fake = makeFakeDb();
  const router = createDevicesRouter({
    getDbImpl: () => ({}),
    insertDeviceImpl: fake.insertDevice,
    getDevicesImpl: fake.getDevices,
    getDeviceByIdImpl: fake.getDeviceById,
    revokeDeviceImpl: fake.revokeDevice,
    ...makeOwnershipStubs(),
    sendToDaemonImpl: () => true,
    isMachineOnlineImpl: () => true,
  });

  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/devices?user_id=user-evil');
    assert.equal(res.status, 403);
    assert.match(res.json.error, /another user/i);
  });
});

test('GET /api/devices?all=true returns 403 for non-service caller', async () => {
  const fake = makeFakeDb();
  const router = createDevicesRouter({
    getDbImpl: () => ({}),
    insertDeviceImpl: fake.insertDevice,
    getDevicesImpl: fake.getDevices,
    getDeviceByIdImpl: fake.getDeviceById,
    revokeDeviceImpl: fake.revokeDevice,
    ...makeOwnershipStubs(),
    sendToDaemonImpl: () => true,
    isMachineOnlineImpl: () => true,
  });

  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/devices?all=true');
    assert.equal(res.status, 403);
  });
});

test('GET /api/devices service caller sees all without scope', async () => {
  const fake = makeFakeDb();
  fake.rows.push(
    { id: 'd1', device_id: 'xhs-001', api_key: 'sk_dev_a', user_id: 'user-001', channel_id: 'ch-001', daemon_id: 'd-1', device_type: 'xhs', status: 'active', created_at: '2026-05-08', revoked_at: null },
    { id: 'd3', device_id: 'dy-001',  api_key: 'sk_dev_c', user_id: 'user-002', channel_id: 'ch-003', daemon_id: 'd-1', device_type: 'douyin', status: 'active', created_at: '2026-05-08', revoked_at: null },
  );
  const router = createDevicesRouter({
    getDbImpl: () => ({}),
    insertDeviceImpl: fake.insertDevice,
    getDevicesImpl: fake.getDevices,
    getDeviceByIdImpl: fake.getDeviceById,
    revokeDeviceImpl: fake.revokeDevice,
    ...makeOwnershipStubs(),
    sendToDaemonImpl: () => true,
    isMachineOnlineImpl: () => true,
  });

  await withServer(createApp(router, { user: { id: 'service', name: 'Service' }, isService: true }), async (baseUrl) => {
    const all = await callJson(baseUrl, '/api/devices');
    assert.equal(all.status, 200);
    assert.equal(all.json.devices.length, 2);
  });
});

// ── DELETE tests ────────────────────────────────────────────────────────────

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
    ...makeOwnershipStubs(),
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
    ...makeOwnershipStubs(),
    sendToDaemonImpl: () => true,
    isMachineOnlineImpl: () => true,
  });

  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/devices/missing', { method: 'DELETE' });
    assert.equal(res.status, 404);
  });
});

test('DELETE /api/devices/:id returns 403 when caller is not the device owner', async () => {
  const fake = makeFakeDb();
  fake.rows.push({
    id: 'd-other', device_id: 'xhs-x', api_key: 'sk_dev_other', user_id: 'user-evil',
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
    ...makeOwnershipStubs(),
    sendToDaemonImpl: (daemonId, frame) => { sent.push({ daemonId, frame }); return true; },
    isMachineOnlineImpl: () => true,
  });

  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/devices/d-other', { method: 'DELETE' });
    assert.equal(res.status, 403);
    assert.match(res.json.error, /another user/i);
    // device was not revoked
    assert.equal(fake.rows[0].status, 'active');
    assert.equal(sent.length, 0);
  });
});

test('DELETE /api/devices/:id allows service caller to revoke any device', async () => {
  const fake = makeFakeDb();
  fake.rows.push({
    id: 'd-svc', device_id: 'xhs-svc', api_key: 'sk_dev_svc', user_id: 'user-evil',
    channel_id: 'ch-001', daemon_id: 'daemon-001', device_type: 'xhs',
    status: 'active', created_at: '2026-05-08', revoked_at: null,
  });
  const router = createDevicesRouter({
    getDbImpl: () => ({}),
    insertDeviceImpl: fake.insertDevice,
    getDevicesImpl: fake.getDevices,
    getDeviceByIdImpl: fake.getDeviceById,
    revokeDeviceImpl: fake.revokeDevice,
    ...makeOwnershipStubs(),
    sendToDaemonImpl: () => true,
    isMachineOnlineImpl: () => true,
  });

  await withServer(createApp(router, { user: { id: 'service', name: 'Service' }, isService: true }), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/devices/d-svc', { method: 'DELETE' });
    assert.equal(res.status, 200);
    assert.equal(fake.rows[0].status, 'revoked');
  });
});

// ── T78 (M1.2-FIX-C, P2 #7): device.* push failure observability ────────────
// Recovery for an offline-daemon push is the daemon-side reconnect re-pull
// (bootstrapDeviceSync). These tests pin the metric/warn signal that ops
// uses to detect lost pushes — the API call still succeeds (the DB write
// already landed), but a counter increments and a structured warn is emitted.

test('T78: device.created push records failure metric when daemon is offline', async () => {
  const fake = makeFakeDb();
  const sent = [];
  const warns = [];
  const router = createDevicesRouter({
    getDbImpl: () => ({}),
    insertDeviceImpl: fake.insertDevice,
    getDevicesImpl: fake.getDevices,
    getDeviceByIdImpl: fake.getDeviceById,
    revokeDeviceImpl: fake.revokeDevice,
    ...makeOwnershipStubs(),
    sendToDaemonImpl: (daemonId, frame) => { sent.push({ daemonId, frame }); return true; },
    isMachineOnlineImpl: () => false, // daemon offline
    uuidv4Impl: () => 'd-offline',
    generateApiKeyImpl: () => 'sk_dev_offline',
  });

  resetDevicePushFailureCount();
  const before = getDevicePushFailureCount();
  const origWarn = console.warn;
  console.warn = (...args) => { warns.push(args.join(' ')); };
  try {
    await withServer(createApp(router), async (baseUrl) => {
      const res = await callJson(baseUrl, '/api/devices', {
        method: 'POST',
        body: { device_type: 'xhs', channel_id: 'ch-001', daemon_id: 'daemon-001' },
      });
      assert.equal(res.status, 201, 'create still succeeds — DB write already landed');
    });
  } finally {
    console.warn = origWarn;
  }
  assert.equal(sent.length, 0, 'no WS frame should be sent when daemon offline');
  assert.equal(getDevicePushFailureCount() - before, 1, 'failure counter must increment by exactly 1');
  assert.ok(warns.some(w => w.includes('[devicePush]') && w.includes('device.created')), `expected structured warn, got: ${warns.join(' | ')}`);
});

test('T78: device.revoked push records failure metric when daemon is offline', async () => {
  const fake = makeFakeDb();
  fake.rows.push({
    id: 'd-revoke-offline', device_id: 'xhs-r', api_key: 'sk_dev_x', user_id: 'user-001',
    channel_id: 'ch-001', daemon_id: 'daemon-001', device_type: 'xhs',
    status: 'active', created_at: '2026-05-08', revoked_at: null,
  });
  const sent = [];
  const warns = [];
  const router = createDevicesRouter({
    getDbImpl: () => ({}),
    insertDeviceImpl: fake.insertDevice,
    getDevicesImpl: fake.getDevices,
    getDeviceByIdImpl: fake.getDeviceById,
    revokeDeviceImpl: fake.revokeDevice,
    ...makeOwnershipStubs(),
    sendToDaemonImpl: (daemonId, frame) => { sent.push({ daemonId, frame }); return true; },
    isMachineOnlineImpl: () => false,
  });

  resetDevicePushFailureCount();
  const before = getDevicePushFailureCount();
  const origWarn = console.warn;
  console.warn = (...args) => { warns.push(args.join(' ')); };
  try {
    await withServer(createApp(router), async (baseUrl) => {
      const res = await callJson(baseUrl, '/api/devices/d-revoke-offline', { method: 'DELETE' });
      assert.equal(res.status, 200);
      assert.equal(fake.rows[0].status, 'revoked');
    });
  } finally {
    console.warn = origWarn;
  }
  assert.equal(sent.length, 0);
  assert.equal(getDevicePushFailureCount() - before, 1);
  assert.ok(warns.some(w => w.includes('[devicePush]') && w.includes('device.revoked')), `expected structured warn, got: ${warns.join(' | ')}`);
});

test('T78: sendToDaemon=false (race) also bumps failure counter', async () => {
  // isMachineOnline returns true but sendToDaemon returns false (e.g. ws state
  // changed between online check and send). We still want to bump the counter.
  const fake = makeFakeDb();
  const router = createDevicesRouter({
    getDbImpl: () => ({}),
    insertDeviceImpl: fake.insertDevice,
    getDevicesImpl: fake.getDevices,
    getDeviceByIdImpl: fake.getDeviceById,
    revokeDeviceImpl: fake.revokeDevice,
    ...makeOwnershipStubs(),
    sendToDaemonImpl: () => false,
    isMachineOnlineImpl: () => true,
    uuidv4Impl: () => 'd-race',
    generateApiKeyImpl: () => 'sk_dev_race',
  });

  resetDevicePushFailureCount();
  const before = getDevicePushFailureCount();
  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/devices', {
      method: 'POST',
      body: { device_type: 'xhs', channel_id: 'ch-001', daemon_id: 'daemon-001' },
    });
    assert.equal(res.status, 201);
  });
  assert.equal(getDevicePushFailureCount() - before, 1);
});

test('T78: successful push does NOT bump failure counter', async () => {
  const fake = makeFakeDb();
  const router = createDevicesRouter({
    getDbImpl: () => ({}),
    insertDeviceImpl: fake.insertDevice,
    getDevicesImpl: fake.getDevices,
    getDeviceByIdImpl: fake.getDeviceById,
    revokeDeviceImpl: fake.revokeDevice,
    ...makeOwnershipStubs(),
    sendToDaemonImpl: () => true,
    isMachineOnlineImpl: () => true,
    uuidv4Impl: () => 'd-ok',
    generateApiKeyImpl: () => 'sk_dev_ok',
  });

  resetDevicePushFailureCount();
  const before = getDevicePushFailureCount();
  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/devices', {
      method: 'POST',
      body: { device_type: 'xhs', channel_id: 'ch-001', daemon_id: 'daemon-001' },
    });
    assert.equal(res.status, 201);
  });
  assert.equal(getDevicePushFailureCount(), before, 'happy path must not bump failure counter');
});
