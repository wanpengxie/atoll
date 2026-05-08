import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';
import test from 'node:test';
import WebSocket from 'ws';

import { DeviceWsServer, makeKeyVerifier, parseDeviceKeysEnv } from '../src/devices/ws-server.js';

async function startTestServer(verifyKey, options = {}) {
  const httpServer = http.createServer();
  await new Promise((resolve) => httpServer.listen(0, '127.0.0.1', resolve));
  const wss = new DeviceWsServer({ verifyKey, logEnabled: false, ...options });
  wss.attach(httpServer);
  const { port } = httpServer.address();
  return { httpServer, wss, port };
}

async function closeTestServer({ httpServer, wss }) {
  await wss.close();
  await new Promise((resolve) => httpServer.close(() => resolve()));
}

function connectClient(port, deviceId, key) {
  const url = `ws://127.0.0.1:${port}/device/${encodeURIComponent(deviceId)}?key=${encodeURIComponent(key ?? '')}`;
  return new WebSocket(url);
}

test('parseDeviceKeysEnv parses csv and json forms', () => {
  assert.deepEqual([...parseDeviceKeysEnv('').entries()], []);
  const csv = parseDeviceKeysEnv('a:1,b:2,  c : 3 ');
  assert.equal(csv.get('a'), '1');
  assert.equal(csv.get('b'), '2');
  assert.equal(csv.get('c'), '3');

  const json = parseDeviceKeysEnv(JSON.stringify({ x: 'k1', y: 'k2' }));
  assert.equal(json.get('x'), 'k1');
  assert.equal(json.get('y'), 'k2');

  // malformed entries dropped
  const bad = parseDeviceKeysEnv('a,b:,:c,valid:ok');
  assert.equal(bad.size, 1);
  assert.equal(bad.get('valid'), 'ok');
});

test('makeKeyVerifier honors per-device key and fallback', () => {
  const verifier = makeKeyVerifier({
    deviceKeys: new Map([['device-a', 'key-a']]),
    fallbackKey: 'global-fallback',
  });
  assert.equal(verifier({ deviceId: 'device-a', key: 'key-a' }), true);
  assert.equal(verifier({ deviceId: 'device-a', key: 'wrong' }), false);
  assert.equal(verifier({ deviceId: 'device-b', key: 'global-fallback' }), true);
  assert.equal(verifier({ deviceId: 'device-b', key: 'nope' }), false);
  assert.equal(verifier({ deviceId: '', key: 'key-a' }), false);
});

function awaitHandshakeRejection(ws) {
  return new Promise((resolve) => {
    let settled = false;
    const finish = (info) => {
      if (settled) return;
      settled = true;
      resolve(info);
    };
    ws.on('error', (err) => finish({ kind: 'error', message: err?.message ?? String(err) }));
    ws.on('unexpected-response', (_req, res) => finish({ kind: 'unexpected-response', status: res.statusCode }));
    ws.on('close', (code) => finish({ kind: 'close', code }));
  });
}

test('rejects connection with bad key (401)', async () => {
  const verifier = makeKeyVerifier({ deviceKeys: new Map([['d1', 'good']]) });
  const ctx = await startTestServer(verifier);
  try {
    const ws = connectClient(ctx.port, 'd1', 'bad');
    const info = await awaitHandshakeRejection(ws);
    if (info.kind === 'unexpected-response') {
      assert.equal(info.status, 401);
    } else if (info.kind === 'error') {
      assert.match(info.message, /401|Unauthorized|Unexpected/i);
    } else {
      assert.notEqual(info.code, 1000);
    }
    assert.equal(ctx.wss.isOnline('d1'), false);
  } finally {
    await closeTestServer(ctx);
  }
});

test('rejects malformed device path (404)', async () => {
  const verifier = makeKeyVerifier({ deviceKeys: new Map([['d1', 'good']]) });
  const ctx = await startTestServer(verifier);
  try {
    const ws = new WebSocket(`ws://127.0.0.1:${ctx.port}/device/`);
    const info = await awaitHandshakeRejection(ws);
    if (info.kind === 'unexpected-response') {
      assert.equal(info.status, 404);
    } else if (info.kind === 'error') {
      assert.match(info.message, /404|Not Found|Unexpected/i);
    } else {
      assert.notEqual(info.code, 1000);
    }
  } finally {
    await closeTestServer(ctx);
  }
});

test('accepts connection with good key and routes pushCommand', async () => {
  const verifier = makeKeyVerifier({ deviceKeys: new Map([['d1', 'good']]) });
  let presenceLog = [];
  const inboundFrames = [];
  const ctx = await startTestServer(verifier, {
    onPresence: (info) => presenceLog.push(info),
    onMessage: ({ frame }) => inboundFrames.push(frame),
  });
  try {
    const ws = connectClient(ctx.port, 'd1', 'good');
    await once(ws, 'open');

    // Wait until server registers the connection in its map (race: open happens after server side handleUpgrade).
    for (let i = 0; i < 50 && !ctx.wss.isOnline('d1'); i++) {
      await new Promise((r) => setTimeout(r, 5));
    }
    assert.equal(ctx.wss.isOnline('d1'), true);

    // pushCommand → client should receive it
    const incoming = once(ws, 'message');
    const result = ctx.wss.pushCommand('d1', { type: 'command', correlation_id: 'corr-1', cmd: 'publish' });
    assert.deepEqual(result, { ok: true });
    const [raw] = await incoming;
    const parsed = JSON.parse(raw.toString());
    assert.equal(parsed.correlation_id, 'corr-1');
    assert.equal(parsed.cmd, 'publish');

    // Inbound frame from client → server.onMessage callback
    ws.send(JSON.stringify({ type: 'ack', correlation_id: 'corr-1' }));
    for (let i = 0; i < 50 && inboundFrames.length === 0; i++) {
      await new Promise((r) => setTimeout(r, 5));
    }
    assert.equal(inboundFrames.length, 1);
    assert.equal(inboundFrames[0].type, 'ack');

    // Close → presence updated, removed from connections map
    ws.close(1000);
    await once(ws, 'close');
    for (let i = 0; i < 50 && ctx.wss.isOnline('d1'); i++) {
      await new Promise((r) => setTimeout(r, 5));
    }
    assert.equal(ctx.wss.isOnline('d1'), false);
    assert.ok(presenceLog.some((p) => p.event === 'connect'));
    assert.ok(presenceLog.some((p) => p.event === 'disconnect'));
  } finally {
    await closeTestServer(ctx);
  }
});

test('pushCommand returns device_offline when no client connected', async () => {
  const verifier = makeKeyVerifier({ deviceKeys: new Map([['d1', 'good']]) });
  const ctx = await startTestServer(verifier);
  try {
    const result = ctx.wss.pushCommand('d1', { hello: 'world' });
    assert.deepEqual(result, { ok: false, reason: 'device_offline' });
  } finally {
    await closeTestServer(ctx);
  }
});

test('replacing a connection closes the previous one', async () => {
  const verifier = makeKeyVerifier({ deviceKeys: new Map([['d1', 'good']]) });
  const ctx = await startTestServer(verifier);
  try {
    const a = connectClient(ctx.port, 'd1', 'good');
    await once(a, 'open');
    for (let i = 0; i < 50 && !ctx.wss.isOnline('d1'); i++) {
      await new Promise((r) => setTimeout(r, 5));
    }

    const b = connectClient(ctx.port, 'd1', 'good');
    await once(b, 'open');
    // wait for server-side replacement to happen
    for (let i = 0; i < 50 && a.readyState === WebSocket.OPEN; i++) {
      await new Promise((r) => setTimeout(r, 10));
    }
    assert.notEqual(a.readyState, WebSocket.OPEN);
    assert.equal(ctx.wss.isOnline('d1'), true);
    b.close(1000);
    await once(b, 'close');
  } finally {
    await closeTestServer(ctx);
  }
});

// ── Fix-T2 §1: makeKeyVerifier without fallback rejects every unconfigured device ─

test('makeKeyVerifier without fallback rejects unconfigured devices', () => {
  const verifier = makeKeyVerifier({ deviceKeys: new Map([['device-a', 'key-a']]) });
  assert.equal(verifier({ deviceId: 'device-a', key: 'key-a' }), true);
  assert.equal(verifier({ deviceId: 'device-a', key: 'wrong' }), false);
  // unconfigured device: no fallback → must reject regardless of key value
  assert.equal(verifier({ deviceId: 'device-b', key: 'key-a' }), false);
  assert.equal(verifier({ deviceId: 'device-b', key: 'random-key' }), false);
});

test('makeKeyVerifier rejects empty deviceId / key', () => {
  const verifier = makeKeyVerifier({ deviceKeys: new Map([['d1', 'good']]) });
  assert.equal(verifier({}), false);
  assert.equal(verifier({ deviceId: 'd1', key: '' }), false);
  assert.equal(verifier({ deviceId: '', key: 'good' }), false);
  assert.equal(verifier({ deviceId: 'd1' }), false);
});

// ── parseDeviceIdFromPath edge cases (covered indirectly via handshake) ─

test('rejects connection on multi-segment device path (404)', async () => {
  const verifier = makeKeyVerifier({ deviceKeys: new Map([['d1', 'good']]) });
  const ctx = await startTestServer(verifier);
  try {
    // /device/foo/bar should not match /device/{id}/?$ → 404
    const ws = new WebSocket(`ws://127.0.0.1:${ctx.port}/device/foo/bar?key=good`);
    const info = await awaitHandshakeRejection(ws);
    if (info.kind === 'unexpected-response') {
      assert.equal(info.status, 404);
    } else {
      assert.notEqual(info.code, 1000);
    }
  } finally {
    await closeTestServer(ctx);
  }
});

test('accepts trailing slash in device path', async () => {
  const verifier = makeKeyVerifier({ deviceKeys: new Map([['dx', 'k']]) });
  const ctx = await startTestServer(verifier);
  try {
    const ws = new WebSocket(`ws://127.0.0.1:${ctx.port}/device/dx/?key=k`);
    await once(ws, 'open');
    for (let i = 0; i < 50 && !ctx.wss.isOnline('dx'); i++) {
      await new Promise((r) => setTimeout(r, 5));
    }
    assert.equal(ctx.wss.isOnline('dx'), true);
    ws.close(1000);
    await once(ws, 'close');
  } finally {
    await closeTestServer(ctx);
  }
});

test('accepts URL-encoded device id with special chars', async () => {
  const verifier = makeKeyVerifier({ deviceKeys: new Map([['dev/A', 'k']]) });
  const ctx = await startTestServer(verifier);
  try {
    const ws = connectClient(ctx.port, 'dev/A', 'k');
    await once(ws, 'open');
    for (let i = 0; i < 50 && !ctx.wss.isOnline('dev/A'); i++) {
      await new Promise((r) => setTimeout(r, 5));
    }
    assert.equal(ctx.wss.isOnline('dev/A'), true);
    ws.close(1000);
    await once(ws, 'close');
  } finally {
    await closeTestServer(ctx);
  }
});

// ── pushCommand error branches ─

test('pushCommand returns payload_serialization_failed for cyclic structures', async () => {
  const verifier = makeKeyVerifier({ deviceKeys: new Map([['d1', 'good']]) });
  const ctx = await startTestServer(verifier);
  try {
    const ws = connectClient(ctx.port, 'd1', 'good');
    await once(ws, 'open');
    for (let i = 0; i < 50 && !ctx.wss.isOnline('d1'); i++) {
      await new Promise((r) => setTimeout(r, 5));
    }
    const payload = {};
    payload.self = payload; // cycle → JSON.stringify throws
    const result = ctx.wss.pushCommand('d1', payload);
    assert.equal(result.ok, false);
    assert.equal(result.reason, 'payload_serialization_failed');
    ws.close(1000);
    await once(ws, 'close');
  } finally {
    await closeTestServer(ctx);
  }
});

test('listOnline only includes OPEN sockets', async () => {
  const verifier = makeKeyVerifier({ deviceKeys: new Map([['d1', 'good'], ['d2', 'good']]) });
  const ctx = await startTestServer(verifier);
  try {
    assert.deepEqual(ctx.wss.listOnline(), []);
    const a = connectClient(ctx.port, 'd1', 'good');
    await once(a, 'open');
    for (let i = 0; i < 50 && !ctx.wss.isOnline('d1'); i++) {
      await new Promise((r) => setTimeout(r, 5));
    }
    assert.deepEqual(ctx.wss.listOnline().sort(), ['d1']);
    a.close(1000);
    await once(a, 'close');
    for (let i = 0; i < 50 && ctx.wss.isOnline('d1'); i++) {
      await new Promise((r) => setTimeout(r, 5));
    }
    assert.deepEqual(ctx.wss.listOnline(), []);
  } finally {
    await closeTestServer(ctx);
  }
});

// ── T74 §2: server revoke must force-disconnect already-connected device ──

test('disconnect(deviceId) closes the existing ws and removes it from connections', async () => {
  const verifier = makeKeyVerifier({ deviceKeys: new Map([['d1', 'good']]) });
  const presenceLog = [];
  const ctx = await startTestServer(verifier, {
    onPresence: (info) => presenceLog.push(info),
  });
  try {
    const ws = connectClient(ctx.port, 'd1', 'good');
    await once(ws, 'open');
    for (let i = 0; i < 50 && !ctx.wss.isOnline('d1'); i++) {
      await new Promise((r) => setTimeout(r, 5));
    }
    assert.equal(ctx.wss.isOnline('d1'), true);

    const result = ctx.wss.disconnect('d1', 'revoked');
    assert.equal(result, true);

    // client side observes close
    await once(ws, 'close');
    // server-side close handler fires asynchronously after the close handshake;
    // poll on presenceLog rather than isOnline (readyState flips to CLOSING
    // before the 'close' listener — and presence/connections.delete — runs).
    for (let i = 0; i < 80 && !presenceLog.some((p) => p.event === 'disconnect' && p.deviceId === 'd1'); i++) {
      await new Promise((r) => setTimeout(r, 5));
    }
    assert.equal(ctx.wss.isOnline('d1'), false);
    assert.ok(presenceLog.some((p) => p.event === 'disconnect' && p.deviceId === 'd1'));
  } finally {
    await closeTestServer(ctx);
  }
});

test('disconnect(deviceId) returns false when device not connected', async () => {
  const verifier = makeKeyVerifier({ deviceKeys: new Map([['d1', 'good']]) });
  const ctx = await startTestServer(verifier);
  try {
    assert.equal(ctx.wss.disconnect('ghost'), false);
  } finally {
    await closeTestServer(ctx);
  }
});

test('non-json frames are dropped without affecting onMessage', async () => {
  const verifier = makeKeyVerifier({ deviceKeys: new Map([['d1', 'good']]) });
  const inboundFrames = [];
  const ctx = await startTestServer(verifier, {
    onMessage: ({ frame }) => inboundFrames.push(frame),
  });
  try {
    const ws = connectClient(ctx.port, 'd1', 'good');
    await once(ws, 'open');
    for (let i = 0; i < 50 && !ctx.wss.isOnline('d1'); i++) {
      await new Promise((r) => setTimeout(r, 5));
    }
    ws.send('not json at all');
    ws.send(JSON.stringify({ type: 'valid', n: 1 }));
    for (let i = 0; i < 80 && inboundFrames.length === 0; i++) {
      await new Promise((r) => setTimeout(r, 5));
    }
    assert.equal(inboundFrames.length, 1);
    assert.equal(inboundFrames[0].type, 'valid');
    ws.close(1000);
    await once(ws, 'close');
  } finally {
    await closeTestServer(ctx);
  }
});
