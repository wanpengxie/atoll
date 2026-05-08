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
    const devices = await fetchDevices({
      serverUrl: 'http://srv',
      machineApiKey: 'mk',
      daemonId: resolvedDaemonId,
      fetchImpl,
    });
    store.replaceServer(devices);
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
      capabilities: [],
      fetchImpl,
    });
    resolvedDaemonId = reg.daemon_id;
    const devices = await fetchDevices({
      serverUrl: 'http://srv',
      machineApiKey: 'mk',
      daemonId: resolvedDaemonId,
      fetchImpl,
    });
    store.replaceServer(devices);
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
