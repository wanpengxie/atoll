// Daemon registrar — POST /api/daemon/register + GET /api/daemon/{id}/devices
// (T74 §1, contract).
//
// Coverage:
//   - registerDaemon POSTs JSON body with required fields
//   - registerDaemon throws on non-2xx with status surfaced
//   - fetchDevices GETs with Authorization: Bearer header, returns devices array
//   - fetchDevices throws on non-2xx with status surfaced
//   - URL composition handles both `serverUrl` with and without trailing slash

import test from 'node:test';
import assert from 'node:assert/strict';

import { registerDaemon, fetchDevices } from '../src/devices/registrar.js';

function fakeFetchOk(payload) {
  return async (...args) => {
    fakeFetchOk.calls.push(args);
    return {
      ok: true,
      status: 200,
      json: async () => payload,
      text: async () => JSON.stringify(payload),
    };
  };
}

test('registerDaemon POSTs to /api/daemon/register with expected body', async () => {
  const calls = [];
  const fetchImpl = async (url, options) => {
    calls.push({ url, options });
    return {
      ok: true,
      status: 200,
      json: async () => ({ ok: true, daemon_id: 'd1' }),
    };
  };
  const result = await registerDaemon({
    serverUrl: 'http://srv',
    machineApiKey: 'mk',
    daemonId: 'd1',
    host: 'host-x',
    capabilities: ['xhs-creator'],
    fetchImpl,
  });
  assert.deepEqual(result, { ok: true, daemon_id: 'd1' });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, 'http://srv/api/daemon/register');
  assert.equal(calls[0].options.method, 'POST');
  const body = JSON.parse(calls[0].options.body);
  assert.equal(body.machine_api_key, 'mk');
  assert.equal(body.daemon_id, 'd1');
  assert.equal(body.host, 'host-x');
  assert.deepEqual(body.capabilities, ['xhs-creator']);
});

test('registerDaemon includes port + scheme in body when provided (T77 M1.2-FIX-B)', async () => {
  const calls = [];
  const fetchImpl = async (url, options) => {
    calls.push({ url, options });
    return {
      ok: true,
      status: 200,
      json: async () => ({ ok: true, daemon_id: 'd1' }),
    };
  };
  await registerDaemon({
    serverUrl: 'http://srv',
    machineApiKey: 'mk',
    daemonId: 'd1',
    host: 'daemon.example.com',
    port: 9501,
    scheme: 'https',
    capabilities: ['xhs-creator'],
    fetchImpl,
  });
  const body = JSON.parse(calls[0].options.body);
  assert.equal(body.port, 9501);
  assert.equal(body.scheme, 'https');
  assert.equal(body.host, 'daemon.example.com');
});

test('registerDaemon omits scheme when null/empty (T77 backward compat)', async () => {
  const calls = [];
  const fetchImpl = async (url, options) => {
    calls.push({ url, options });
    return { ok: true, status: 200, json: async () => ({ ok: true, daemon_id: 'd' }) };
  };
  await registerDaemon({
    serverUrl: 'http://srv',
    machineApiKey: 'mk',
    daemonId: 'd',
    host: 'h',
    port: 9501,
    fetchImpl,
  });
  const body = JSON.parse(calls[0].options.body);
  assert.equal(Object.hasOwn(body, 'scheme'), false);
  assert.equal(body.port, 9501);
});

test('registerDaemon omits port when null (T77 fallback when daemon HTTP is disabled)', async () => {
  const calls = [];
  const fetchImpl = async (url, options) => {
    calls.push({ url, options });
    return { ok: true, status: 200, json: async () => ({ ok: true, daemon_id: 'd' }) };
  };
  await registerDaemon({
    serverUrl: 'http://srv',
    machineApiKey: 'mk',
    daemonId: 'd',
    host: 'h',
    fetchImpl,
  });
  const body = JSON.parse(calls[0].options.body);
  assert.equal(Object.hasOwn(body, 'port'), false);
});

test('registerDaemon strips trailing slash from serverUrl', async () => {
  const calls = [];
  const fetchImpl = async (url, options) => {
    calls.push(url);
    return { ok: true, status: 200, json: async () => ({ ok: true, daemon_id: 'd' }) };
  };
  await registerDaemon({
    serverUrl: 'http://srv/',
    machineApiKey: 'mk',
    daemonId: 'd',
    fetchImpl,
  });
  assert.equal(calls[0], 'http://srv/api/daemon/register');
});

test('registerDaemon throws on non-2xx with status code in error', async () => {
  const fetchImpl = async () => ({
    ok: false,
    status: 401,
    text: async () => 'unauthorized',
  });
  await assert.rejects(
    () => registerDaemon({ serverUrl: 'http://x', machineApiKey: 'k', daemonId: 'd', fetchImpl }),
    /401/,
  );
});

test('fetchDevices GETs with Authorization Bearer header and returns devices array', async () => {
  const calls = [];
  const sample = { devices: [{ device_id: 'a', api_key: 'ka' }, { device_id: 'b', api_key: 'kb' }] };
  const fetchImpl = async (url, options) => {
    calls.push({ url, options });
    return { ok: true, status: 200, json: async () => sample };
  };
  const devices = await fetchDevices({
    serverUrl: 'http://srv',
    machineApiKey: 'mk',
    daemonId: 'd1',
    fetchImpl,
  });
  assert.equal(devices.length, 2);
  assert.equal(devices[0].device_id, 'a');
  assert.equal(calls[0].url, 'http://srv/api/daemon/d1/devices');
  assert.equal(calls[0].options.method, 'GET');
  assert.equal(calls[0].options.headers.Authorization, 'Bearer mk');
});

test('fetchDevices returns [] when payload omits devices', async () => {
  const fetchImpl = async () => ({ ok: true, status: 200, json: async () => ({}) });
  const devices = await fetchDevices({
    serverUrl: 'http://srv',
    machineApiKey: 'mk',
    daemonId: 'd1',
    fetchImpl,
  });
  assert.deepEqual(devices, []);
});

test('fetchDevices throws on non-2xx with status code in error', async () => {
  const fetchImpl = async () => ({
    ok: false,
    status: 403,
    text: async () => 'forbidden',
  });
  await assert.rejects(
    () => fetchDevices({ serverUrl: 'http://srv', machineApiKey: 'mk', daemonId: 'd1', fetchImpl }),
    /403/,
  );
});

test('fetchDevices encodes daemonId in URL path', async () => {
  const calls = [];
  const fetchImpl = async (url) => {
    calls.push(url);
    return { ok: true, status: 200, json: async () => ({ devices: [] }) };
  };
  await fetchDevices({
    serverUrl: 'http://srv',
    machineApiKey: 'mk',
    daemonId: 'with space',
    fetchImpl,
  });
  assert.equal(calls[0], 'http://srv/api/daemon/with%20space/devices');
});
