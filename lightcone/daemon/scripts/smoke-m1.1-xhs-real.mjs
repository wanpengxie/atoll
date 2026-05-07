// M1.1-T3 e2e smoke (no chrome required).
//
// Flow exercised end-to-end:
//   1. Spin up ChannelManager + RpcServer + DeviceWsServer in-process on a
//      random HTTP port + unix socket under a tmp project dir.
//   2. Create an xhs-creator channel with human + device members.
//   3. Connect a node mock device WS client to /device/{id}.
//   4. Mock device: receive command frame → POST /api/device/{id}/session
//      then POST /api/device/{id}/callback with a fake xhs publish result.
//   5. Trigger device.command.send via /rpc.
//   6. Wait for callback to land; assert messages.sqlite holds dispatch.start
//      + dispatch.completed for the same correlation_id and result.url is
//      preserved; assert session file exists with logged_in state.
//
// Run via: node lightcone/daemon/scripts/smoke-m1.1-xhs-real.mjs
//      or: scripts/smoke/m1.1-xhs-real.sh

import assert from 'node:assert/strict';
import http from 'node:http';
import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import WebSocket from 'ws';
import { fileURLToPath } from 'node:url';

import { ChannelManager } from '../src/channel-manager.js';
import { DeviceWsServer, makeKeyVerifier } from '../src/devices/ws-server.js';
import { RpcServer } from '../src/rpc-server.js';
import { openMessageStore, queryStoredMessages } from '../src/message-store.js';

const tempRoot = mkdtempSync(path.join(os.tmpdir(), 'm1-1-smoke-'));
const projectKey = `smoke-${process.pid}-${Date.now()}`;
process.env.COAGENT_PROJECT_KEY = projectKey;
const baseDir = path.join(tempRoot, 'coagent');
const socketPath = path.join(baseDir, 'daemon.sock');
const httpPort = 24500 + Math.floor(Math.random() * 500);
const daemonToken = 'smoke-daemon-token';
const deviceId = 'smoke-device';
const deviceKey = 'smoke-device-key';
const userId = 'user-smoke';
const channelId = 'ch-smoke';

let cleanups = [];
function onCleanup(fn) { cleanups.push(fn); }
async function cleanup() {
  for (const fn of cleanups.reverse()) {
    try { await fn(); } catch (err) { console.error('cleanup error:', err?.message ?? err); }
  }
  rmSync(tempRoot, { recursive: true, force: true });
}

function rpcCall(method, params, opts = {}) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({ method, params });
    const req = http.request({
      host: '127.0.0.1',
      port: httpPort,
      method: 'POST',
      path: '/rpc',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body),
        Authorization: `Bearer ${opts.token ?? daemonToken}`,
      },
    }, (res) => {
      let raw = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => { raw += chunk; });
      res.on('end', () => {
        try { resolve({ statusCode: res.statusCode, body: JSON.parse(raw) }); }
        catch (err) { reject(new Error(`bad rpc body: ${err.message}: ${raw}`)); }
      });
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

function deviceHttp(action, body, { token = deviceKey } = {}) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const req = http.request({
      host: '127.0.0.1',
      port: httpPort,
      method: 'POST',
      path: `/api/device/${encodeURIComponent(deviceId)}/${action}`,
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(data),
        Authorization: `Bearer ${token}`,
      },
    }, (res) => {
      let raw = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => { raw += chunk; });
      res.on('end', () => {
        try { resolve({ statusCode: res.statusCode, body: raw ? JSON.parse(raw) : null }); }
        catch (err) { reject(new Error(`bad device response: ${err.message}: ${raw}`)); }
      });
    });
    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

function readDispatchMessages(workdir) {
  const store = openMessageStore(workdir);
  try {
    return queryStoredMessages(store, { channel_id: channelId, order: 'asc', limit: 200 })
      .filter((m) => String(m.payload_type).startsWith('dispatch.'));
  } finally { store.close(); }
}

async function main() {
  console.log(`[smoke] tempRoot=${tempRoot} httpPort=${httpPort}`);

  // 1. wire managers
  const verifyDeviceKey = makeKeyVerifier({ deviceKeys: new Map([[deviceId, deviceKey]]) });
  const deviceWsServer = new DeviceWsServer({
    verifyKey: verifyDeviceKey,
    logEnabled: false,
    onMessage: ({ deviceId: id, frame }) => {
      console.log(`[smoke] device frame ← ${id}: ${frame?.type}`);
    },
  });
  const channelManager = new ChannelManager({
    serverUrl: 'http://localhost:0',
    machineApiKey: 'machine-key',
    daemonSocketPath: socketPath,
    daemonHttpUrl: `http://127.0.0.1:${httpPort}`,
    daemonToken,
    baseDir,
    dueMessagePollMs: 0,
    deviceWsServer,
  });
  const rpcServer = new RpcServer({
    channelManager,
    socketPath,
    httpPort,
    httpHost: '127.0.0.1',
    authToken: daemonToken,
    deviceWsServer,
    verifyDeviceKey,
  });
  await rpcServer.start();
  onCleanup(() => rpcServer.stop());

  // 2. create channel
  const created = await channelManager.createChannel({
    channelId,
    workspaceId: 'ws-smoke',
    daemonId: 'machine-smoke',
    name: 'M1.1 smoke channel',
    type: 'xhs-creator',
    status: 'created',
    members: [
      { memberType: 'human', memberId: userId, displayName: 'Smoke Owner' },
      { memberType: 'device', memberId: deviceId, displayName: 'Smoke Device' },
    ],
  });
  const node = channelManager._requireNode(created.channel_id);
  console.log(`[smoke] channel ${node.channelId} workdir=${node.workdir}`);

  // 3. mock device WS client + handler
  let receivedCorrelationId = null;
  let callbackCompleted = false;
  const wsUrl = `ws://127.0.0.1:${httpPort}/device/${encodeURIComponent(deviceId)}?key=${encodeURIComponent(deviceKey)}`;
  const deviceClient = new WebSocket(wsUrl);
  onCleanup(() => { try { deviceClient.close(1000); } catch {} });
  await new Promise((resolve, reject) => {
    deviceClient.once('open', resolve);
    deviceClient.once('error', reject);
  });
  console.log(`[smoke] mock device ws connected`);
  deviceClient.on('message', async (raw) => {
    let frame;
    try { frame = JSON.parse(raw.toString()); } catch { return; }
    if (frame.type !== 'command') return;
    receivedCorrelationId = frame.correlation_id;
    console.log(`[smoke] mock device received command correlation_id=${frame.correlation_id} cmd=${frame.cmd}`);

    // simulate quick login → sync session
    const sessionRes = await deviceHttp('session', {
      user_id: userId,
      cookies: [{ name: 'sid', value: 'smoke', domain: '.xiaohongshu.com' }],
      login_state: 'logged_in',
    });
    assert.equal(sessionRes.statusCode, 200, 'session post must 200');

    // simulate "user finished publishing in xhs.com"
    const callbackRes = await deviceHttp('callback', {
      correlation_id: frame.correlation_id,
      status: 'ok',
      result: {
        url: `https://www.xiaohongshu.com/explore/smoke-${frame.correlation_id.slice(0, 8)}`,
        note_id: `smoke-${frame.correlation_id.slice(0, 8)}`,
      },
    });
    assert.equal(callbackRes.statusCode, 200, 'callback post must 200');
    callbackCompleted = true;
  });

  // give the WS server a tick to register the connection
  for (let i = 0; i < 50 && !deviceWsServer.isOnline(deviceId); i++) {
    await delay(10);
  }
  assert.equal(deviceWsServer.isOnline(deviceId), true, 'device should be online');

  // 4. trigger the dispatch
  const sendRes = await rpcCall('device.command.send', {
    channel_id: channelId,
    type: 'xhs.publish',
    params: { title: 'Smoke note', content: '/tmp/note.md', images: [], tags: ['smoke'] },
  });
  assert.equal(sendRes.statusCode, 200, `device.command.send must 200, got ${sendRes.statusCode}`);
  assert.equal(sendRes.body.ok, true, `device.command.send envelope: ${JSON.stringify(sendRes.body)}`);
  const correlationId = sendRes.body.result.correlation_id;
  console.log(`[smoke] dispatch.start correlation_id=${correlationId} push=${JSON.stringify(sendRes.body.result.push)}`);
  assert.equal(sendRes.body.result.push.ok, true, 'ws pushCommand should succeed');
  assert.equal(sendRes.body.result.device_id, deviceId);
  assert.equal(sendRes.body.result.user_id, userId);

  // 5. wait for the device to call back
  for (let i = 0; i < 200 && !callbackCompleted; i++) {
    await delay(20);
  }
  assert.equal(callbackCompleted, true, 'mock device must complete the callback round-trip');
  assert.equal(receivedCorrelationId, correlationId, 'mock device should observe same correlation_id');

  // give triggerGateway a tick to land the dispatch.completed message
  await delay(50);

  // 6. assertions
  const dispatched = readDispatchMessages(node.workdir);
  const start = dispatched.find((m) => m.payload_type === 'dispatch.start' && m.correlation_id === correlationId);
  const done = dispatched.find((m) => m.payload_type === 'dispatch.completed' && m.correlation_id === correlationId);
  assert.ok(start, 'dispatch.start must be in messages.sqlite');
  assert.ok(done, 'dispatch.completed must be in messages.sqlite');
  assert.equal(done.sender_kind, 'external');
  const result = done.payload_body?.result;
  assert.ok(result?.url, 'dispatch.completed result must include url');
  assert.match(result.url, /xiaohongshu\.com/);

  const sessionFile = path.join(baseDir, 'users', userId, 'xhs-session.json');
  assert.ok(existsSync(sessionFile), `session file must exist: ${sessionFile}`);
  const session = JSON.parse(readFileSync(sessionFile, 'utf8'));
  assert.equal(session.user_id, userId);
  assert.equal(session.login_state, 'logged_in');
  assert.equal(session.cookies?.[0]?.name, 'sid');

  console.log('\n[smoke] ✓ M1.1-T3 e2e smoke passed.');
  console.log(`        correlation_id=${correlationId}`);
  console.log(`        url=${result.url}`);
  console.log(`        session=${sessionFile}`);
}

main()
  .then(async () => { await cleanup(); process.exit(0); })
  .catch(async (err) => {
    console.error('\n[smoke] ✗ failed:', err?.stack ?? err);
    await cleanup();
    process.exit(1);
  });
