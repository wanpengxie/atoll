import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createServer } from 'node:http';
import test from 'node:test';

import express from 'express';

import { createDaemonRouter } from '../src/routes/daemon.js';

function createApp(router) {
  const app = express();
  app.use(express.json());
  app.use('/api/daemon', router);
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

test('POST /api/daemon/register validates machine_api_key and updates daemon-info on machine', async () => {
  const updates = [];
  const router = createDaemonRouter({
    getDbImpl: () => ({}),
    getMachineByApiKeyImpl: async (_db, key) => (
      key === 'sk_machine_good' ? { id: 'daemon-001', server_id: 'server-001' } : null
    ),
    updateMachineDaemonInfoImpl: async (_db, id, fields) => {
      updates.push({ id, fields });
      return { id, ...fields };
    },
    getDevicesByDaemonIdImpl: async () => [],
  });

  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/daemon/register', {
      method: 'POST',
      body: {
        machine_api_key: 'sk_machine_good',
        daemon_id: 'daemon-001',
        host: '192.168.0.10',
        port: 9501,
        capabilities: ['xhs-creator'],
      },
    });
    assert.equal(res.status, 200);
    assert.equal(res.json.ok, true);
    assert.equal(res.json.daemon_id, 'daemon-001');

    assert.equal(updates.length, 1);
    assert.equal(updates[0].id, 'daemon-001');
    assert.equal(updates[0].fields.daemon_host, '192.168.0.10');
    assert.equal(updates[0].fields.daemon_port, 9501);
    assert.deepEqual(updates[0].fields.capabilities, ['xhs-creator']);
    assert.equal(updates[0].fields.status, 'online');
  });
});

test('POST /api/daemon/register rejects invalid machine_api_key with 401', async () => {
  const router = createDaemonRouter({
    getDbImpl: () => ({}),
    getMachineByApiKeyImpl: async () => null,
    updateMachineDaemonInfoImpl: async () => { throw new Error('should not call'); },
    getDevicesByDaemonIdImpl: async () => [],
  });

  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/daemon/register', {
      method: 'POST',
      body: { machine_api_key: 'bad', daemon_id: 'x', host: 'h', capabilities: [] },
    });
    assert.equal(res.status, 401);
  });
});

test('POST /api/daemon/register persists daemon_scheme when provided (T77)', async () => {
  const updates = [];
  const router = createDaemonRouter({
    getDbImpl: () => ({}),
    getMachineByApiKeyImpl: async () => ({ id: 'daemon-001', server_id: 'server-001' }),
    updateMachineDaemonInfoImpl: async (_db, id, fields) => {
      updates.push({ id, fields });
      return { id, ...fields };
    },
    getDevicesByDaemonIdImpl: async () => [],
  });

  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/daemon/register', {
      method: 'POST',
      body: {
        machine_api_key: 'k',
        daemon_id: 'daemon-001',
        host: 'daemon.example.com',
        port: 443,
        scheme: 'https',
        capabilities: ['xhs-creator'],
      },
    });
    assert.equal(res.status, 200);
    assert.equal(updates[0].fields.daemon_scheme, 'https');
    assert.equal(updates[0].fields.daemon_port, 443);
  });
});

test('POST /api/daemon/register defaults daemon_scheme to null when omitted (T77)', async () => {
  const updates = [];
  const router = createDaemonRouter({
    getDbImpl: () => ({}),
    getMachineByApiKeyImpl: async () => ({ id: 'daemon-001', server_id: 'server-001' }),
    updateMachineDaemonInfoImpl: async (_db, id, fields) => {
      updates.push({ id, fields });
      return { id, ...fields };
    },
    getDevicesByDaemonIdImpl: async () => [],
  });

  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/daemon/register', {
      method: 'POST',
      body: {
        machine_api_key: 'k',
        daemon_id: 'daemon-001',
        host: '127.0.0.1',
        port: 9501,
        capabilities: [],
      },
    });
    assert.equal(res.status, 200);
    assert.equal(updates[0].fields.daemon_scheme, null);
  });
});

test('POST /api/daemon/register rejects unknown scheme with 400 (T77)', async () => {
  const router = createDaemonRouter({
    getDbImpl: () => ({}),
    getMachineByApiKeyImpl: async () => ({ id: 'daemon-001', server_id: 'server-001' }),
    updateMachineDaemonInfoImpl: async () => { throw new Error('should not call'); },
    getDevicesByDaemonIdImpl: async () => [],
  });

  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/daemon/register', {
      method: 'POST',
      body: {
        machine_api_key: 'k',
        daemon_id: 'daemon-001',
        host: 'h',
        port: 1,
        scheme: 'gopher',
        capabilities: [],
      },
    });
    assert.equal(res.status, 400);
  });
});

test('POST /api/daemon/register rejects missing port with 400 (T81 M1.2-FIX-F)', async () => {
  // T81 contract: port is now a hard precondition. Previously the route
  // accepted null port and silently wrote daemon_port=null, which made
  // /api/device/resolve return 503 long after the daemon believed register
  // succeeded. Reject upfront with a precise error message.
  const router = createDaemonRouter({
    getDbImpl: () => ({}),
    getMachineByApiKeyImpl: async () => ({ id: 'daemon-001', server_id: 'server-001' }),
    updateMachineDaemonInfoImpl: async () => { throw new Error('should not call'); },
    getDevicesByDaemonIdImpl: async () => [],
  });

  await withServer(createApp(router), async (baseUrl) => {
    const resMissing = await callJson(baseUrl, '/api/daemon/register', {
      method: 'POST',
      body: {
        machine_api_key: 'k',
        daemon_id: 'daemon-001',
        host: 'h',
        capabilities: [],
      },
    });
    assert.equal(resMissing.status, 400);
    assert.equal(resMissing.json.error, 'port is required');

    const resEmpty = await callJson(baseUrl, '/api/daemon/register', {
      method: 'POST',
      body: {
        machine_api_key: 'k',
        daemon_id: 'daemon-001',
        host: 'h',
        port: '',
        capabilities: [],
      },
    });
    assert.equal(resEmpty.status, 400);
    assert.equal(resEmpty.json.error, 'port is required');
  });
});

test('POST /api/daemon/register rejects non-integer port with 400 (T77 regression guard)', async () => {
  const router = createDaemonRouter({
    getDbImpl: () => ({}),
    getMachineByApiKeyImpl: async () => ({ id: 'daemon-001', server_id: 'server-001' }),
    updateMachineDaemonInfoImpl: async () => { throw new Error('should not call'); },
    getDevicesByDaemonIdImpl: async () => [],
  });

  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/daemon/register', {
      method: 'POST',
      body: {
        machine_api_key: 'k',
        daemon_id: 'daemon-001',
        host: 'h',
        port: 'not-a-number',
        capabilities: [],
      },
    });
    assert.equal(res.status, 400);
  });
});

test('POST /api/daemon/register rejects daemon_id mismatch with 403', async () => {
  const router = createDaemonRouter({
    getDbImpl: () => ({}),
    getMachineByApiKeyImpl: async () => ({ id: 'daemon-A', server_id: 'server-001' }),
    updateMachineDaemonInfoImpl: async () => { throw new Error('should not call'); },
    getDevicesByDaemonIdImpl: async () => [],
  });

  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/daemon/register', {
      method: 'POST',
      body: {
        machine_api_key: 'good',
        daemon_id: 'daemon-B',
        host: 'h',
        capabilities: [],
      },
    });
    assert.equal(res.status, 403);
  });
});

test('GET /api/daemon/:id/devices returns active devices for the bearer machine', async () => {
  const router = createDaemonRouter({
    getDbImpl: () => ({}),
    getMachineByApiKeyImpl: async (_db, key) => (
      key === 'sk_machine_good' ? { id: 'daemon-001', server_id: 'server-001' } : null
    ),
    updateMachineDaemonInfoImpl: async () => ({}),
    getDevicesByDaemonIdImpl: async (_db, daemonId) => {
      assert.equal(daemonId, 'daemon-001');
      return [
        { id: 'd-1', device_id: 'xhs-001', api_key: 'sk_dev_aa', user_id: 'u1', channel_id: 'ch1', daemon_id: 'daemon-001', device_type: 'xhs', status: 'active', created_at: 't', revoked_at: null },
        { id: 'd-2', device_id: 'xhs-002', api_key: 'sk_dev_bb', user_id: 'u2', channel_id: 'ch2', daemon_id: 'daemon-001', device_type: 'xhs', status: 'active', created_at: 't', revoked_at: null },
      ];
    },
    getRevokedDeviceIdsByDaemonIdImpl: async () => [],
  });

  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/daemon/daemon-001/devices', {
      headers: { authorization: 'Bearer sk_machine_good' },
    });
    assert.equal(res.status, 200);
    assert.equal(res.json.devices.length, 2);
    assert.equal(res.json.devices[0].device_id, 'xhs-001');
    assert.equal(res.json.devices[0].api_key, 'sk_dev_aa');
    // T82 (M1.2-FIX-G): response always carries revoked_device_ids — empty
    // here because no rows in the revoked window.
    assert.deepEqual(res.json.revoked_device_ids, []);
  });
});

test('GET /api/daemon/:id/devices returns revoked_device_ids alongside active devices (T82 M1.2-FIX-G)', async () => {
  // Spec: response { devices: [...active], revoked_device_ids: [...] } so a
  // freshly-booted daemon can seed tombstones deterministically without
  // relying on push-event delivery across the restart. Old daemons ignore
  // the new field — this test pins the additive contract.
  const calls = [];
  const router = createDaemonRouter({
    getDbImpl: () => ({}),
    getMachineByApiKeyImpl: async () => ({ id: 'daemon-001', server_id: 'server-001' }),
    updateMachineDaemonInfoImpl: async () => ({}),
    getDevicesByDaemonIdImpl: async (_db, daemonId) => {
      calls.push(['active', daemonId]);
      return [
        { id: 'd-1', device_id: 'xhs-001', api_key: 'sk_dev_aa', user_id: 'u1', channel_id: 'ch1', daemon_id: 'daemon-001', device_type: 'xhs', status: 'active', created_at: 't', revoked_at: null },
      ];
    },
    getRevokedDeviceIdsByDaemonIdImpl: async (_db, daemonId) => {
      calls.push(['revoked', daemonId]);
      return ['xhs-002', 'xhs-003'];
    },
  });

  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/daemon/daemon-001/devices', {
      headers: { authorization: 'Bearer good' },
    });
    assert.equal(res.status, 200);
    assert.equal(res.json.devices.length, 1);
    assert.equal(res.json.devices[0].device_id, 'xhs-001');
    assert.deepEqual(res.json.revoked_device_ids, ['xhs-002', 'xhs-003']);
    // Both queries scoped to the bearer machine's daemon id.
    assert.deepEqual(calls.sort(), [['active', 'daemon-001'], ['revoked', 'daemon-001']].sort());
  });
});

test('GET /api/daemon/:id/devices defaults revoked_device_ids to [] when impl returns non-array (T82)', async () => {
  // Defense-in-depth: route normalizes a missing/garbage return into [] so
  // daemons never see `undefined` in JSON.
  const router = createDaemonRouter({
    getDbImpl: () => ({}),
    getMachineByApiKeyImpl: async () => ({ id: 'daemon-001', server_id: 'server-001' }),
    updateMachineDaemonInfoImpl: async () => ({}),
    getDevicesByDaemonIdImpl: async () => [],
    getRevokedDeviceIdsByDaemonIdImpl: async () => undefined,
  });

  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/daemon/daemon-001/devices', {
      headers: { authorization: 'Bearer good' },
    });
    assert.equal(res.status, 200);
    assert.deepEqual(res.json.devices, []);
    assert.deepEqual(res.json.revoked_device_ids, []);
  });
});

test('GET /api/daemon/:id/devices rejects when bearer machine != daemon_id (403)', async () => {
  const router = createDaemonRouter({
    getDbImpl: () => ({}),
    getMachineByApiKeyImpl: async () => ({ id: 'daemon-A', server_id: 'server-001' }),
    updateMachineDaemonInfoImpl: async () => ({}),
    getDevicesByDaemonIdImpl: async () => [],
    getRevokedDeviceIdsByDaemonIdImpl: async () => [],
  });

  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/daemon/daemon-B/devices', {
      headers: { authorization: 'Bearer good' },
    });
    assert.equal(res.status, 403);
  });
});

test('GET /api/daemon/:id/devices rejects missing bearer with 401', async () => {
  const router = createDaemonRouter({
    getDbImpl: () => ({}),
    getMachineByApiKeyImpl: async () => null,
    updateMachineDaemonInfoImpl: async () => ({}),
    getDevicesByDaemonIdImpl: async () => [],
    getRevokedDeviceIdsByDaemonIdImpl: async () => [],
  });

  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/daemon/daemon-A/devices');
    assert.equal(res.status, 401);
  });
});
