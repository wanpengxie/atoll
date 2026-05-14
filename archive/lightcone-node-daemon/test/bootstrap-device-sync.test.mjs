// T78 phase-3: integration smoke for daemon reconnect → re-pull → store sync.
//
// Wires the same lifecycle index.js uses in production: DaemonConnection
// onReady fires registerDaemon + fetchDevices and feeds the result into
// DeviceStore.replaceServer. We swap the WS layer with a controllable mock
// (same pattern as connection-onready.test.mjs) and stub fetchImpl so each
// reconnect can return a different device list.
//
// What this proves:
//   1. After the first ws-open, the store reflects the initial pull.
//   2. After a forced reconnect with a different server-side device list,
//      the store reflects the NEW set — devices that disappeared from the
//      pull are tombstoned (cannot re-auth via env), and a freshly added
//      device verifies with its key.
//
// Covers ticket validation point: "WS push failure → daemon onReady re-pull
// behavior verified" — even when device.* push frames are dropped (server
// outbox is "log + reconnect re-pull"), the next ws-open is the recovery.

import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';

import { DaemonConnection } from '../src/connection.js';
import { DeviceStore } from '../src/devices/device-store.js';
import { registerDaemon, fetchDevices } from '../src/devices/registrar.js';
import { createBootstrapDeviceSync } from '../src/devices/bootstrap.js';

class MockWebSocket extends EventEmitter {
  constructor(url) {
    super();
    this.url = url;
    this.readyState = 0; // CONNECTING
    this.sent = [];
  }
  send(payload) { this.sent.push(payload); }
  ping() {}
  close(code = 1000) { this.emit('close', code); }
}

function makeFetchImpl(scripts) {
  // scripts: array of `(url, init) => Response-like` functions, consumed in
  // order. Each ws-open fires register + fetchDevices, so two HTTP calls
  // per cycle.
  const queue = [...scripts];
  return async (url, init) => {
    const handler = queue.shift();
    if (!handler) throw new Error(`fetchImpl: no scripted response for ${url}`);
    return handler(url, init);
  };
}

function jsonResponse(status, body) {
  return {
    ok: status >= 200 && status < 300,
    status,
    json: async () => body,
    text: async () => JSON.stringify(body),
  };
}

test('T78: reconnect → re-pull → DeviceStore reflects new server set, drops stale ids', async () => {
  // Two ws-open cycles. Each cycle: register call + fetchDevices call.
  // Cycle 1: server has device-A:key-Y1
  // Cycle 2: server has device-A:key-Y2 (rotated) and device-B:key-K (new);
  //          device-C (which only existed in env) was never server-managed.
  const scripts = [
    // Cycle 1
    (url) => {
      assert.match(url, /\/api\/daemon\/register$/);
      return jsonResponse(200, { ok: true, daemon_id: 'daemon-T78' });
    },
    (url) => {
      assert.match(url, /\/api\/daemon\/daemon-T78\/devices$/);
      return jsonResponse(200, { devices: [{ device_id: 'device-A', api_key: 'key-Y1' }] });
    },
    // Cycle 2 (reconnect)
    (url) => {
      assert.match(url, /\/api\/daemon\/register$/);
      return jsonResponse(200, { ok: true, daemon_id: 'daemon-T78' });
    },
    (url) => {
      assert.match(url, /\/api\/daemon\/daemon-T78\/devices$/);
      return jsonResponse(200, { devices: [
        { device_id: 'device-A', api_key: 'key-Y2' },
        { device_id: 'device-B', api_key: 'key-K' },
      ] });
    },
  ];
  const fetchImpl = makeFetchImpl(scripts);

  // env seeds device-A:key-X (operator left a stale env value); pure env
  // device-C:key-C (server never touched it).
  const store = new DeviceStore({
    envEntries: new Map([['device-A', 'key-X'], ['device-C', 'key-C']]),
  });

  let resolvedDaemonId = '';
  const bootstrap = async () => {
    const reg = await registerDaemon({
      serverUrl: 'http://srv',
      machineApiKey: 'mk',
      daemonId: resolvedDaemonId,
      host: 'host',
      port: 1234,
      capabilities: [],
      fetchImpl,
    });
    resolvedDaemonId = reg.daemon_id;
    const { devices, revokedDeviceIds } = await fetchDevices({
      serverUrl: 'http://srv',
      machineApiKey: 'mk',
      daemonId: resolvedDaemonId,
      fetchImpl,
    });
    store.replaceServer(devices, revokedDeviceIds);
  };

  const mocks = [];
  const wsFactory = (url) => {
    const ws = new MockWebSocket(url);
    mocks.push(ws);
    return ws;
  };
  const conn = new DaemonConnection({
    serverUrl: 'http://srv',
    machineApiKey: 'mk',
    onMessage: () => {},
    onReady: () => bootstrap(),
    wsFactory,
    reconnectInitialMs: 1,
    reconnectMaxMs: 1,
  });

  // Cycle 1
  conn.connect();
  mocks[0].readyState = 1;
  mocks[0].emit('open');
  // onReady → async fetch chain → settle
  await new Promise((r) => setTimeout(r, 10));

  // After cycle 1: device-A served by server (key-Y1); env key-X shadowed;
  // device-C still env-only.
  assert.equal(store.verifyKey({ deviceId: 'device-A', key: 'key-Y1' }), true);
  assert.equal(store.verifyKey({ deviceId: 'device-A', key: 'key-X' }), false);
  assert.equal(store.verifyKey({ deviceId: 'device-C', key: 'key-C' }), true);
  assert.equal(store.verifyKey({ deviceId: 'device-B', key: 'key-K' }), false);

  // Force reconnect.
  mocks[0].emit('close', 1006);
  await new Promise((r) => setTimeout(r, 25));
  assert.equal(mocks.length, 2, 'reconnect must spawn a fresh ws');
  mocks[1].readyState = 1;
  mocks[1].emit('open');
  await new Promise((r) => setTimeout(r, 10));

  // After cycle 2: device-A rotated to key-Y2; device-B added; device-C
  // env-only still works; old key-Y1 and env key-X both rejected.
  assert.equal(store.verifyKey({ deviceId: 'device-A', key: 'key-Y2' }), true, 'rotated server key must verify');
  assert.equal(store.verifyKey({ deviceId: 'device-A', key: 'key-Y1' }), false, 'stale server key must reject');
  assert.equal(store.verifyKey({ deviceId: 'device-A', key: 'key-X' }), false, 'env fallback must stay shadowed');
  assert.equal(store.verifyKey({ deviceId: 'device-B', key: 'key-K' }), true, 'newly issued device must verify after re-pull');
  assert.equal(store.verifyKey({ deviceId: 'device-C', key: 'key-C' }), true, 'pure env id must keep working');

  conn.stop();
});

test('T78: reconnect re-pull tombstones a device dropped from the new server set', async () => {
  // Cycle 1 has device-A:key-Y; Cycle 2 returns []. After cycle 2, device-A
  // is tombstoned and env fallback for device-A:key-X is rejected — this is
  // the recovery path when a device.revoked WS push was lost (server-side
  // outbox is "log + reconnect re-pull").
  const scripts = [
    () => jsonResponse(200, { ok: true, daemon_id: 'daemon-T78' }),
    () => jsonResponse(200, { devices: [{ device_id: 'device-A', api_key: 'key-Y' }] }),
    () => jsonResponse(200, { ok: true, daemon_id: 'daemon-T78' }),
    () => jsonResponse(200, { devices: [] }),
  ];
  const fetchImpl = makeFetchImpl(scripts);
  const store = new DeviceStore({ envEntries: new Map([['device-A', 'key-X']]) });

  let resolvedDaemonId = '';
  const bootstrap = async () => {
    const reg = await registerDaemon({
      serverUrl: 'http://srv',
      machineApiKey: 'mk',
      daemonId: resolvedDaemonId,
      port: 1234, // T81: port now required by registrar contract
      capabilities: [],
      fetchImpl,
    });
    resolvedDaemonId = reg.daemon_id;
    const { devices, revokedDeviceIds } = await fetchDevices({
      serverUrl: 'http://srv',
      machineApiKey: 'mk',
      daemonId: resolvedDaemonId,
      fetchImpl,
    });
    store.replaceServer(devices, revokedDeviceIds);
  };

  const mocks = [];
  const conn = new DaemonConnection({
    serverUrl: 'http://srv',
    machineApiKey: 'mk',
    onMessage: () => {},
    onReady: () => bootstrap(),
    wsFactory: (url) => { const ws = new MockWebSocket(url); mocks.push(ws); return ws; },
    reconnectInitialMs: 1,
    reconnectMaxMs: 1,
  });

  conn.connect();
  mocks[0].readyState = 1;
  mocks[0].emit('open');
  await new Promise((r) => setTimeout(r, 10));
  assert.equal(store.verifyKey({ deviceId: 'device-A', key: 'key-Y' }), true);

  mocks[0].emit('close', 1006);
  await new Promise((r) => setTimeout(r, 25));
  mocks[1].readyState = 1;
  mocks[1].emit('open');
  await new Promise((r) => setTimeout(r, 10));

  // device-A dropped from new server set → tombstoned. Both server key and
  // env key are rejected.
  assert.equal(store.revokedServerIds.has('device-A'), true);
  assert.equal(store.verifyKey({ deviceId: 'device-A', key: 'key-Y' }), false);
  assert.equal(store.verifyKey({ deviceId: 'device-A', key: 'key-X' }), false);

  conn.stop();
});

test('T81 M1.2-FIX-F: PUBLIC_PORT==null + DEVICE_SOURCE=both → register skipped, env fallback intact', async () => {
  // T81 hard-rule: when the daemon has no PUBLIC_PORT it must NOT issue a
  // register call. The server now returns 400 for null port, and firing the
  // request just buries the real "missing PUBLIC_PORT" config issue under
  // register_failed errors. With DEVICE_SOURCE='both' the env-seeded keys
  // remain authoritative until the operator configures the port.
  let fetchCalls = 0;
  const fetchImpl = async () => {
    fetchCalls += 1;
    throw new Error('fetchImpl should not be invoked when port is missing');
  };
  const logs = [];
  const log = (...args) => logs.push(args.join(' '));

  const store = new DeviceStore({
    envEntries: new Map([['device-env', 'env-key']]),
  });
  const bootstrap = createBootstrapDeviceSync({
    deviceSource: 'both',
    envDeviceKeysSize: 1,
    serverUrl: 'http://srv',
    machineApiKey: 'mk',
    publicHost: 'h',
    publicPort: null, // operator forgot to set COAGENT_DAEMON_HTTP_PORT
    publicScheme: null,
    capabilities: [],
    deviceStore: store,
    registerDaemonImpl: (...args) => registerDaemon({ ...args[0], fetchImpl }),
    fetchDevicesImpl: (...args) => fetchDevices({ ...args[0], fetchImpl }),
    log,
  });

  await bootstrap();
  // No fetch issued — register call short-circuited.
  assert.equal(fetchCalls, 0, 'register must not fire when publicPort is null');
  // Env fallback still authoritative.
  assert.equal(store.verifyKey({ deviceId: 'device-env', key: 'env-key' }), true);
  // Operator-facing log explains the skip + names the offending env vars.
  const skipLog = logs.find((l) => l.includes('register skipped'));
  assert.ok(skipLog, 'expected an explicit register-skipped log line');
  assert.match(skipLog, /COAGENT_DAEMON_HTTP_PORT|COAGENT_DAEMON_PUBLIC_PORT/);
});

test('T81 M1.2-FIX-F: PUBLIC_PORT==null + DEVICE_SOURCE=server → register skipped, store stays empty', async () => {
  // With source=server there is no env fallback; the device map simply
  // stays empty (verifyKey rejects all) until config is fixed. The point
  // is that we do NOT fire a doomed register request.
  let fetchCalls = 0;
  const fetchImpl = async () => {
    fetchCalls += 1;
    throw new Error('fetchImpl should not be invoked when port is missing');
  };
  const logs = [];
  const log = (...args) => logs.push(args.join(' '));

  const store = new DeviceStore({ envEntries: new Map() });
  const bootstrap = createBootstrapDeviceSync({
    deviceSource: 'server',
    envDeviceKeysSize: 0,
    serverUrl: 'http://srv',
    machineApiKey: 'mk',
    publicHost: 'h',
    publicPort: null,
    publicScheme: null,
    capabilities: [],
    deviceStore: store,
    registerDaemonImpl: (...args) => registerDaemon({ ...args[0], fetchImpl }),
    fetchDevicesImpl: (...args) => fetchDevices({ ...args[0], fetchImpl }),
    log,
  });

  await bootstrap();
  assert.equal(fetchCalls, 0, 'register must not fire when publicPort is null');
  assert.equal(store.verifyKey({ deviceId: 'whatever', key: 'whatever' }), false);
  assert.ok(logs.find((l) => l.includes('register skipped')));
});

test('T81 M1.2-FIX-F: PUBLIC_PORT==null + DEVICE_SOURCE=env → existing env-only short-circuit (no skip log)', async () => {
  // env source already short-circuits before the port check. This test
  // pins that behavior so it stays orthogonal to the new skip-on-missing
  // path.
  let fetchCalls = 0;
  const fetchImpl = async () => { fetchCalls += 1; return null; };
  const logs = [];
  const store = new DeviceStore({
    envEntries: new Map([['device-env', 'env-key']]),
  });
  const bootstrap = createBootstrapDeviceSync({
    deviceSource: 'env',
    envDeviceKeysSize: 1,
    serverUrl: 'http://srv',
    machineApiKey: 'mk',
    publicHost: 'h',
    publicPort: null,
    capabilities: [],
    deviceStore: store,
    registerDaemonImpl: (args) => registerDaemon({ ...args, fetchImpl }),
    fetchDevicesImpl: (args) => fetchDevices({ ...args, fetchImpl }),
    log: (...args) => logs.push(args.join(' ')),
  });

  await bootstrap();
  assert.equal(fetchCalls, 0);
  assert.equal(store.verifyKey({ deviceId: 'device-env', key: 'env-key' }), true);
  assert.ok(logs.find((l) => l.includes('source=env')));
  assert.equal(logs.find((l) => l.includes('register skipped')), undefined,
    'env path uses its own log line, not the missing-port skip log');
});

test('T82 M1.2-FIX-G: fresh DeviceStore + envKey for revoked id + server pull with revoked_device_ids → verifyKey rejects env key', async () => {
  // Fresh-boot scenario: daemon was restarted; in-memory DeviceStore comes
  // up empty; env still carries a stale key for device-A; server has
  // already revoked device-A. The boot pull's revoked_device_ids list lets
  // the daemon seed the tombstone deterministically — without it, env
  // fallback would silently re-authenticate device-A.
  const fetchImpl = async (url) => {
    if (url.endsWith('/api/daemon/register')) {
      return { ok: true, status: 200, json: async () => ({ ok: true, daemon_id: 'd-FRESH' }), text: async () => '' };
    }
    return {
      ok: true,
      status: 200,
      json: async () => ({ devices: [], revoked_device_ids: ['device-A'] }),
      text: async () => '',
    };
  };

  // Stale env entry persists across the daemon restart (operator never
  // rotated COAGENT_DEVICE_KEYS after the server-side revoke).
  const store = new DeviceStore({
    envEntries: new Map([['device-A', 'stale-env-key']]),
  });
  const bootstrap = createBootstrapDeviceSync({
    deviceSource: 'both',
    envDeviceKeysSize: 1,
    serverUrl: 'http://srv',
    machineApiKey: 'mk',
    publicHost: 'h',
    publicPort: 9501,
    capabilities: [],
    deviceStore: store,
    registerDaemonImpl: (args) => registerDaemon({ ...args, fetchImpl }),
    fetchDevicesImpl: (args) => fetchDevices({ ...args, fetchImpl }),
    log: () => {},
  });

  // Pre-bootstrap: env-only fallback would still verify (no server context yet).
  assert.equal(store.verifyKey({ deviceId: 'device-A', key: 'stale-env-key' }), true);

  await bootstrap();

  // Post-bootstrap: tombstone seeded from revoked_device_ids; env fallback
  // is now blocked. This is the central T82 invariant.
  assert.equal(store.serverManagedIds.has('device-A'), true);
  assert.equal(store.revokedServerIds.has('device-A'), true);
  assert.equal(store.verifyKey({ deviceId: 'device-A', key: 'stale-env-key' }), false,
    'env fallback for server-revoked id must be rejected after fresh-boot pull');
});

test('T82 M1.2-FIX-G: bootstrap forwards revoked_device_ids to DeviceStore.replaceServer second arg', async () => {
  // Wire-level check: the registrar parses revoked_device_ids and the
  // bootstrap closure forwards it as the 2nd arg of replaceServer.
  const replaceCalls = [];
  const fakeStore = {
    replaceServer: (entries, revokedIds) => replaceCalls.push({ entries, revokedIds }),
    size: () => 0,
  };
  const fetchImpl = async (url) => {
    if (url.endsWith('/api/daemon/register')) {
      return { ok: true, status: 200, json: async () => ({ ok: true, daemon_id: 'd-FWD' }), text: async () => '' };
    }
    return {
      ok: true,
      status: 200,
      json: async () => ({
        devices: [{ device_id: 'a', api_key: 'ka' }],
        revoked_device_ids: ['device-X', 'device-Y'],
      }),
      text: async () => '',
    };
  };
  const bootstrap = createBootstrapDeviceSync({
    deviceSource: 'both',
    envDeviceKeysSize: 0,
    serverUrl: 'http://srv',
    machineApiKey: 'mk',
    publicHost: 'h',
    publicPort: 9501,
    capabilities: [],
    deviceStore: fakeStore,
    registerDaemonImpl: (args) => registerDaemon({ ...args, fetchImpl }),
    fetchDevicesImpl: (args) => fetchDevices({ ...args, fetchImpl }),
    log: () => {},
  });
  await bootstrap();
  assert.equal(replaceCalls.length, 1);
  assert.deepEqual(replaceCalls[0].revokedIds, ['device-X', 'device-Y']);
  assert.equal(replaceCalls[0].entries.length, 1);
  assert.equal(replaceCalls[0].entries[0].device_id, 'a');
});

test('T82 M1.2-FIX-G: bootstrap against old server (no revoked_device_ids) defaults to [] — no regression', async () => {
  // Mixed-deploy guard: an older server returning only `{ devices }` must
  // not break the daemon. revokedDeviceIds defaults to [].
  const replaceCalls = [];
  const fakeStore = {
    replaceServer: (entries, revokedIds) => replaceCalls.push({ entries, revokedIds }),
    size: () => 0,
  };
  const fetchImpl = async (url) => {
    if (url.endsWith('/api/daemon/register')) {
      return { ok: true, status: 200, json: async () => ({ ok: true, daemon_id: 'd-OLD' }), text: async () => '' };
    }
    return {
      ok: true,
      status: 200,
      // legacy payload — no revoked_device_ids
      json: async () => ({ devices: [{ device_id: 'a', api_key: 'ka' }] }),
      text: async () => '',
    };
  };
  const bootstrap = createBootstrapDeviceSync({
    deviceSource: 'both',
    envDeviceKeysSize: 0,
    serverUrl: 'http://srv',
    machineApiKey: 'mk',
    publicHost: 'h',
    publicPort: 9501,
    capabilities: [],
    deviceStore: fakeStore,
    registerDaemonImpl: (args) => registerDaemon({ ...args, fetchImpl }),
    fetchDevicesImpl: (args) => fetchDevices({ ...args, fetchImpl }),
    log: () => {},
  });
  await bootstrap();
  assert.equal(replaceCalls.length, 1);
  assert.deepEqual(replaceCalls[0].revokedIds, []);
});

test('T81 M1.2-FIX-F: bootstrap with PUBLIC_PORT set still registers + pulls (smoke regression)', async () => {
  // Sanity-check that the new skip path does not regress the happy case.
  const calls = [];
  const fetchImpl = async (url) => {
    calls.push(url);
    if (url.endsWith('/api/daemon/register')) {
      return { ok: true, status: 200, json: async () => ({ ok: true, daemon_id: 'd-OK' }), text: async () => '' };
    }
    return { ok: true, status: 200, json: async () => ({ devices: [{ device_id: 'srv-1', api_key: 'sk1' }] }), text: async () => '' };
  };

  const store = new DeviceStore({ envEntries: new Map() });
  const bootstrap = createBootstrapDeviceSync({
    deviceSource: 'both',
    envDeviceKeysSize: 0,
    serverUrl: 'http://srv',
    machineApiKey: 'mk',
    publicHost: 'h',
    publicPort: 9501,
    capabilities: [],
    deviceStore: store,
    registerDaemonImpl: (args) => registerDaemon({ ...args, fetchImpl }),
    fetchDevicesImpl: (args) => fetchDevices({ ...args, fetchImpl }),
    log: () => {},
  });

  await bootstrap();
  assert.equal(calls.length, 2);
  assert.match(calls[0], /\/api\/daemon\/register$/);
  assert.match(calls[1], /\/api\/daemon\/d-OK\/devices$/);
  assert.equal(store.verifyKey({ deviceId: 'srv-1', key: 'sk1' }), true);
});
