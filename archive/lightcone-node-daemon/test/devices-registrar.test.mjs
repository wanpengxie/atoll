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
    port: 9501,
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
  assert.equal(body.port, 9501);
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

test('registerDaemon throws when port is missing (T81 M1.2-FIX-F port required)', async () => {
  // T81 reversal: t77 silently omitted port from the body when null, which
  // produced a server row with daemon_port=null and made /resolve return
  // 503. The new contract is "port required"; registrar throws at the call
  // site (defense-in-depth, before the network round trip).
  const fetchImpl = async () => {
    throw new Error('fetchImpl should not be invoked when port is missing');
  };
  await assert.rejects(
    () => registerDaemon({
      serverUrl: 'http://srv',
      machineApiKey: 'mk',
      daemonId: 'd',
      host: 'h',
      fetchImpl,
    }),
    /port is required/,
  );
});

test('registerDaemon throws when port is empty string (T81 M1.2-FIX-F)', async () => {
  const fetchImpl = async () => {
    throw new Error('fetchImpl should not be invoked when port is empty');
  };
  await assert.rejects(
    () => registerDaemon({
      serverUrl: 'http://srv',
      machineApiKey: 'mk',
      daemonId: 'd',
      host: 'h',
      port: '',
      fetchImpl,
    }),
    /port is required/,
  );
});

test('registerDaemon throws when port is whitespace only (T83 M1.2-FIX-H)', async () => {
  // Pre-T83 Number("   ") === 0 would have slipped past — call-site check
  // now trims first so whitespace is rejected as "port is required",
  // matching the server-side route.
  const fetchImpl = async () => {
    throw new Error('fetchImpl should not be invoked when port is whitespace');
  };
  await assert.rejects(
    () => registerDaemon({
      serverUrl: 'http://srv',
      machineApiKey: 'mk',
      daemonId: 'd',
      host: 'h',
      port: '   ',
      fetchImpl,
    }),
    /port is required/,
  );
});

test('registerDaemon throws when port is non-integer (T83 M1.2-FIX-H mirrors server)', async () => {
  // Defense-in-depth: the daemon-side wrapper now matches the server
  // contract — non-integer fails at the call site instead of round-tripping
  // a 400 over the network.
  const fetchImpl = async () => {
    throw new Error('fetchImpl should not be invoked for non-integer port');
  };
  await assert.rejects(
    () => registerDaemon({
      serverUrl: 'http://srv',
      machineApiKey: 'mk',
      daemonId: 'd',
      host: 'h',
      port: 'not-a-number',
      fetchImpl,
    }),
    /port must be an integer/,
  );
});

test('registerDaemon throws on out-of-range port (T83 M1.2-FIX-H)', async () => {
  // 0 / -1 / 65536 / 99999 each pass Number.isInteger — only the new
  // 1..65535 range check rejects them. Without this the daemon would push
  // garbage values into the server's persistence path (fail-fast lost) or
  // burn a network round trip on the server's 400.
  const fetchImpl = async () => {
    throw new Error('fetchImpl should not be invoked for out-of-range port');
  };
  for (const port of [0, -1, 65536, 99999]) {
    await assert.rejects(
      () => registerDaemon({
        serverUrl: 'http://srv',
        machineApiKey: 'mk',
        daemonId: 'd',
        host: 'h',
        port,
        fetchImpl,
      }),
      /port must be a valid TCP port \(1-65535\)/,
      `port=${port}`,
    );
  }
});

test('registerDaemon accepts boundary ports 1 and 65535 (T83 happy-path guard)', async () => {
  const calls = [];
  const fetchImpl = async (url, options) => {
    calls.push({ url, options });
    return { ok: true, status: 200, json: async () => ({ ok: true, daemon_id: 'd' }) };
  };
  for (const port of [1, 65535]) {
    await registerDaemon({
      serverUrl: 'http://srv',
      machineApiKey: 'mk',
      daemonId: 'd',
      host: 'h',
      port,
      fetchImpl,
    });
  }
  assert.equal(calls.length, 2);
  assert.equal(JSON.parse(calls[0].options.body).port, 1);
  assert.equal(JSON.parse(calls[1].options.body).port, 65535);
});

test('registerDaemon coerces string port to integer in body (T83 M1.2-FIX-H)', async () => {
  // The wrapper now sends the canonical numeric form so the server doesn't
  // have to re-parse "9501". This also guarantees the round-trip: caller
  // passes "9501" → server stores 9501.
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
    port: '9501',
    fetchImpl,
  });
  const body = JSON.parse(calls[0].options.body);
  assert.equal(body.port, 9501);
  assert.equal(typeof body.port, 'number');
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
    port: 9501,
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
    () => registerDaemon({ serverUrl: 'http://x', machineApiKey: 'k', daemonId: 'd', port: 9501, fetchImpl }),
    /401/,
  );
});

test('fetchDevices GETs with Authorization Bearer header and returns { devices, revokedDeviceIds }', async () => {
  // T82 (M1.2-FIX-G): fetchDevices now returns an object so the caller can
  // pass revoked_device_ids to DeviceStore.replaceServer's 2nd arg.
  const calls = [];
  const sample = { devices: [{ device_id: 'a', api_key: 'ka' }, { device_id: 'b', api_key: 'kb' }] };
  const fetchImpl = async (url, options) => {
    calls.push({ url, options });
    return { ok: true, status: 200, json: async () => sample };
  };
  const result = await fetchDevices({
    serverUrl: 'http://srv',
    machineApiKey: 'mk',
    daemonId: 'd1',
    fetchImpl,
  });
  assert.equal(result.devices.length, 2);
  assert.equal(result.devices[0].device_id, 'a');
  assert.deepEqual(result.revokedDeviceIds, []);
  assert.equal(calls[0].url, 'http://srv/api/daemon/d1/devices');
  assert.equal(calls[0].options.method, 'GET');
  assert.equal(calls[0].options.headers.Authorization, 'Bearer mk');
});

test('fetchDevices returns { devices: [], revokedDeviceIds: [] } when payload omits both fields', async () => {
  const fetchImpl = async () => ({ ok: true, status: 200, json: async () => ({}) });
  const result = await fetchDevices({
    serverUrl: 'http://srv',
    machineApiKey: 'mk',
    daemonId: 'd1',
    fetchImpl,
  });
  assert.deepEqual(result, { devices: [], revokedDeviceIds: [] });
});

test('fetchDevices parses revoked_device_ids from payload (T82 M1.2-FIX-G)', async () => {
  // Spec: server responds { devices, revoked_device_ids } so the daemon can
  // seed tombstones on fresh boot. Daemon-side normalizes the snake_case
  // wire field into camelCase + dedupes / strips whitespace.
  const sample = {
    devices: [{ device_id: 'a', api_key: 'ka' }],
    revoked_device_ids: ['device-X', '', '  ', 'device-X', 'device-Y'],
  };
  const fetchImpl = async () => ({ ok: true, status: 200, json: async () => sample });
  const result = await fetchDevices({
    serverUrl: 'http://srv',
    machineApiKey: 'mk',
    daemonId: 'd1',
    fetchImpl,
  });
  assert.equal(result.devices.length, 1);
  assert.deepEqual(result.revokedDeviceIds, ['device-X', 'device-Y']);
});

test('fetchDevices defaults revokedDeviceIds to [] for old servers omitting the field (T82 backward compat)', async () => {
  const sample = { devices: [{ device_id: 'a', api_key: 'ka' }] };
  const fetchImpl = async () => ({ ok: true, status: 200, json: async () => sample });
  const result = await fetchDevices({
    serverUrl: 'http://srv',
    machineApiKey: 'mk',
    daemonId: 'd1',
    fetchImpl,
  });
  assert.equal(result.devices.length, 1);
  assert.deepEqual(result.revokedDeviceIds, []);
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
