// T77 (M1.2-FIX-B): mock smoke of the 1-key main flow —
//   daemon → POST /api/daemon/register (host+port+scheme)
//   extension → POST /api/device/resolve → ws_url+http_url
//
// Both routes share an in-memory `machines` row so the register write
// shows up in the resolve read, mirroring how the production MySQL
// row is updated and re-read.
//
// We also exercise the daemon-side wrapper (`registerDaemon` from
// daemon/src/devices/registrar.js) against the real express server so
// the full request/response shape is verified, not just the SQL.

import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createServer } from 'node:http';
import test from 'node:test';

import express from 'express';

import { createDaemonRouter } from '../src/routes/daemon.js';
import { createDeviceRouter } from '../src/routes/device.js';
import { registerDaemon } from '../daemon/src/devices/registrar.js';

function buildApp({ machines, devices, online }) {
  const app = express();
  app.use(express.json());

  const getDbImpl = () => ({});
  const getMachineByApiKeyImpl = async (_db, key) => {
    for (const m of machines.values()) if (m.api_key === key) return m;
    return null;
  };
  const updateMachineDaemonInfoImpl = async (_db, id, fields) => {
    const m = machines.get(id);
    if (!m) return null;
    Object.assign(m, fields);
    return m;
  };
  const getMachineByIdImpl = async (_db, id) => machines.get(id) ?? null;
  const getDeviceByApiKeyImpl = async (_db, key) => devices.get(key) ?? null;
  const getDevicesByDaemonIdImpl = async (_db, daemonId) => (
    [...devices.values()].filter((d) => d.daemon_id === daemonId && d.status === 'active')
  );
  const isMachineOnlineImpl = (id) => online.has(id);

  app.use('/api/daemon', createDaemonRouter({
    getDbImpl,
    getMachineByApiKeyImpl,
    updateMachineDaemonInfoImpl,
    getDevicesByDaemonIdImpl,
    nowDatetimeImpl: () => '2026-05-08 20:00:00',
  }));
  app.use('/api/device', createDeviceRouter({
    getDbImpl,
    getMachineByIdImpl,
    getDeviceByApiKeyImpl,
    isMachineOnlineImpl,
    resolveRateLimitImpl: () => true,
  }));
  app.use((err, _req, res, _next) => res.status(500).json({ error: err.message }));
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
    await new Promise((resolve, reject) => server.close((err) => (err ? reject(err) : resolve())));
  }
}

async function callJson(baseUrl, path, { method = 'POST', body, headers = {} } = {}) {
  const res = await fetch(`${baseUrl}${path}`, {
    method,
    headers: { 'content-type': 'application/json', ...headers },
    body: body == null ? undefined : JSON.stringify(body),
  });
  let json = null;
  try { json = await res.json(); } catch { /* empty */ }
  return { status: res.status, json };
}

test('register → resolve smoke: daemon publishes port+scheme, extension gets wss/https URLs (T77 acceptance)', async () => {
  const machines = new Map([['daemon-001', {
    id: 'daemon-001',
    server_id: 'server-001',
    api_key: 'sk_machine_T77',
    daemon_host: null,
    daemon_port: null,
    daemon_scheme: null,
  }]]);
  const devices = new Map([['sk_dev_T77', {
    id: 'd-1',
    device_id: 'xhs-001',
    api_key: 'sk_dev_T77',
    user_id: 'user-001',
    channel_id: 'ch-001',
    daemon_id: 'daemon-001',
    status: 'active',
  }]]);
  const online = new Set(['daemon-001']);

  await withServer(buildApp({ machines, devices, online }), async (baseUrl) => {
    // Step 1: daemon → register (uses the real registrar wrapper)
    const reg = await registerDaemon({
      serverUrl: baseUrl,
      machineApiKey: 'sk_machine_T77',
      daemonId: 'daemon-001',
      host: 'daemon.example.com',
      port: 443,
      scheme: 'https',
      capabilities: ['xhs-creator'],
    });
    assert.equal(reg.ok, true);
    assert.equal(reg.daemon_id, 'daemon-001');
    // Server-side row reflects the registered public endpoint.
    assert.equal(machines.get('daemon-001').daemon_host, 'daemon.example.com');
    assert.equal(machines.get('daemon-001').daemon_port, 443);
    assert.equal(machines.get('daemon-001').daemon_scheme, 'https');

    // Step 2: extension → resolve
    const res = await callJson(baseUrl, '/api/device/resolve', { body: { api_key: 'sk_dev_T77' } });
    assert.equal(res.status, 200);
    assert.equal(res.json.ws_url, 'wss://daemon.example.com:443');
    assert.equal(res.json.http_url, 'https://daemon.example.com:443');
    assert.equal(res.json.daemon_id, 'daemon-001');
    assert.equal(res.json.device_id, 'xhs-001');
    assert.equal(res.json.user_id, 'user-001');
    assert.equal(res.json.channel_id, 'ch-001');
  });
});

test('register without port → server rejects with 400 + DB row stays null (T81 M1.2-FIX-F)', async () => {
  // T81 contract reversal: t77 left port optional and silently persisted
  // null, which made `/api/device/resolve` return 503 long after the daemon
  // believed register succeeded. The server route now fails fast with 400
  // and the daemon-side registrar throws before the request fires.
  const machines = new Map([['daemon-001', {
    id: 'daemon-001',
    server_id: 'server-001',
    api_key: 'sk_machine_T77',
    daemon_host: null,
    daemon_port: null,
    daemon_scheme: null,
  }]]);
  const devices = new Map([['sk_dev_T77', {
    id: 'd-1',
    device_id: 'xhs-001',
    api_key: 'sk_dev_T77',
    user_id: 'user-001',
    channel_id: 'ch-001',
    daemon_id: 'daemon-001',
    status: 'active',
  }]]);
  const online = new Set(['daemon-001']);

  await withServer(buildApp({ machines, devices, online }), async (baseUrl) => {
    // daemon-side registrar throws synchronously (defense-in-depth) before
    // the request reaches the network — `port is required`.
    await assert.rejects(
      () => registerDaemon({
        serverUrl: baseUrl,
        machineApiKey: 'sk_machine_T77',
        daemonId: 'daemon-001',
        host: 'daemon.example.com',
        capabilities: ['xhs-creator'],
      }),
      /port is required/,
    );
    // Server-side: explicit POST without port returns 400 from the route.
    const reg = await callJson(baseUrl, '/api/daemon/register', {
      body: {
        machine_api_key: 'sk_machine_T77',
        daemon_id: 'daemon-001',
        host: 'daemon.example.com',
        capabilities: ['xhs-creator'],
      },
    });
    assert.equal(reg.status, 400);
    assert.equal(reg.json.error, 'port is required');
    // DB row never mutated — daemon_port stays null (no silent regression).
    assert.equal(machines.get('daemon-001').daemon_port, null);
  });
});

test('register → daemon WS drops → resolve returns 503 "Daemon offline" (T77 P1#5)', async () => {
  const machines = new Map([['daemon-001', {
    id: 'daemon-001',
    server_id: 'server-001',
    api_key: 'sk_machine_T77',
    daemon_host: null,
    daemon_port: null,
    daemon_scheme: null,
  }]]);
  const devices = new Map([['sk_dev_T77', {
    id: 'd-1',
    device_id: 'xhs-001',
    api_key: 'sk_dev_T77',
    user_id: 'user-001',
    channel_id: 'ch-001',
    daemon_id: 'daemon-001',
    status: 'active',
  }]]);
  const online = new Set(); // daemon WS dropped

  await withServer(buildApp({ machines, devices, online }), async (baseUrl) => {
    await registerDaemon({
      serverUrl: baseUrl,
      machineApiKey: 'sk_machine_T77',
      daemonId: 'daemon-001',
      host: '192.168.0.10',
      port: 9501,
      capabilities: ['xhs-creator'],
    });

    const res = await callJson(baseUrl, '/api/device/resolve', { body: { api_key: 'sk_dev_T77' } });
    assert.equal(res.status, 503);
    assert.equal(res.json.error, 'Daemon offline');
  });
});
