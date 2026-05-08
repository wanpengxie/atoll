// DaemonConnection.onReady — bootstrap hook for re-register + re-pull on every
// WS open (T74 §1, §3). connect() is invoked initially and on each reconnect
// schedule, so onReady must fire once per ws-open lifecycle.
//
// Strategy: inject a synchronous wsFactory yielding a controllable EventEmitter
// stand-in for the `ws` library. Verifies onReady is called when the mock emits
// `open`, and (after manual reconnect) is called again with a fresh ws instance.

import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';

import { DaemonConnection } from '../src/connection.js';

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

test('DaemonConnection.onReady fires once on ws open', async () => {
  const mocks = [];
  const wsFactory = (url) => {
    const ws = new MockWebSocket(url);
    mocks.push(ws);
    return ws;
  };
  const readyArgs = [];
  const conn = new DaemonConnection({
    serverUrl: 'http://srv',
    machineApiKey: 'mk',
    onMessage: () => {},
    onReady: (info) => readyArgs.push(info),
    wsFactory,
  });
  conn.connect();
  assert.equal(mocks.length, 1);
  mocks[0].readyState = 1; // OPEN
  mocks[0].emit('open');
  // onReady runs synchronously (or via microtask) — flush
  await Promise.resolve();
  assert.equal(readyArgs.length, 1);
  conn.stop();
});

test('DaemonConnection.onReady fires again after reconnect (fresh ws-open lifecycle)', async () => {
  const mocks = [];
  const wsFactory = (url) => {
    const ws = new MockWebSocket(url);
    mocks.push(ws);
    return ws;
  };
  let readyCount = 0;
  const conn = new DaemonConnection({
    serverUrl: 'http://srv',
    machineApiKey: 'mk',
    onMessage: () => {},
    onReady: () => { readyCount += 1; },
    wsFactory,
    // shorten reconnect timer for the test (impl detail; default is 1000ms exp backoff)
    reconnectInitialMs: 1,
    reconnectMaxMs: 1,
  });

  conn.connect();
  mocks[0].readyState = 1;
  mocks[0].emit('open');
  await Promise.resolve();
  assert.equal(readyCount, 1);

  // Simulate close → triggers _scheduleReconnect → setTimeout fires → connect again
  mocks[0].emit('close', 1006);
  // wait long enough for reconnectInitialMs (1ms) to fire
  await new Promise((r) => setTimeout(r, 25));
  assert.equal(mocks.length, 2);
  mocks[1].readyState = 1;
  mocks[1].emit('open');
  await Promise.resolve();
  assert.equal(readyCount, 2);

  conn.stop();
});

test('DaemonConnection without onReady still works (backward compatible)', async () => {
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
    wsFactory,
  });
  conn.connect();
  mocks[0].readyState = 1;
  // should not throw
  mocks[0].emit('open');
  await Promise.resolve();
  conn.stop();
});
