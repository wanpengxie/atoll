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
  });

  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/daemon/daemon-001/devices', {
      headers: { authorization: 'Bearer sk_machine_good' },
    });
    assert.equal(res.status, 200);
    assert.equal(res.json.devices.length, 2);
    assert.equal(res.json.devices[0].device_id, 'xhs-001');
    assert.equal(res.json.devices[0].api_key, 'sk_dev_aa');
  });
});

test('GET /api/daemon/:id/devices rejects when bearer machine != daemon_id (403)', async () => {
  const router = createDaemonRouter({
    getDbImpl: () => ({}),
    getMachineByApiKeyImpl: async () => ({ id: 'daemon-A', server_id: 'server-001' }),
    updateMachineDaemonInfoImpl: async () => ({}),
    getDevicesByDaemonIdImpl: async () => [],
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
  });

  await withServer(createApp(router), async (baseUrl) => {
    const res = await callJson(baseUrl, '/api/daemon/daemon-A/devices');
    assert.equal(res.status, 401);
  });
});
