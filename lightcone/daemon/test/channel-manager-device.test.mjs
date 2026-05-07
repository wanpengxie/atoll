// Tests for device.* RPC: deviceCommandSend / deviceSessionGet / deviceSessionUpdate
// + deviceCallback. Use a real ChannelManager with a fake DeviceWsServer + a
// stand-alone tmp baseDir so persistence is isolated.

import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { ChannelManager } from '../src/channel-manager.js';
import { queryStoredMessages, openMessageStore } from '../src/message-store.js';

class FakeWsServer {
  constructor() { this.calls = []; this.online = new Set(); this.nextResult = null; }
  pushCommand(deviceId, payload) {
    this.calls.push({ deviceId, payload });
    if (this.nextResult) {
      const r = this.nextResult;
      this.nextResult = null;
      return r;
    }
    return this.online.has(deviceId) ? { ok: true } : { ok: false, reason: 'device_offline' };
  }
  goOnline(deviceId) { this.online.add(deviceId); }
}

function readDispatchMessages(node) {
  const store = openMessageStore(node.workdir);
  try {
    return queryStoredMessages(store, { channel_id: node.channelId, order: 'asc', limit: 500 })
      .filter((m) => String(m.payload_type).startsWith('dispatch.'));
  } finally {
    store.close();
  }
}

async function createHarness(t) {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'cm-device-'));
  t.after(() => rmSync(tempHome, { recursive: true, force: true }));
  const ws = new FakeWsServer();
  const cm = new ChannelManager({
    serverUrl: 'http://localhost:3001',
    machineApiKey: 'machine-key',
    daemonSocketPath: path.join(tempHome, '.coagent', 'daemon.sock'),
    daemonHttpUrl: 'http://127.0.0.1:3002',
    daemonToken: 'daemon-token',
    baseDir: path.join(tempHome, 'coagent'),
    dueMessagePollMs: 0,
    deviceWsServer: ws,
  });
  const created = await cm.createChannel({
    channelId: 'ch-xhs-1',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'XHS Channel',
    type: 'xhs-creator',
    status: 'created',
    members: [
      { memberType: 'human', memberId: 'user-001', displayName: 'Owner' },
      { memberType: 'device', memberId: 'device-A', displayName: 'Browser' },
    ],
  });
  const node = cm._requireNode(created.channel_id);
  return { cm, ws, node, tempHome };
}

test('deviceCommandSend resolves device/user from members, emits dispatch.start + schedules self-check + pushes ws', async (t) => {
  const ctx = await createHarness(t);
  ctx.ws.goOnline('device-A');

  const result = await ctx.cm.deviceCommandSend({
    channel_id: 'ch-xhs-1',
    type: 'xhs.publish',
    params: { title: 'hello', content: '/path/to.md', images: [], tags: [] },
  });

  assert.ok(result.correlation_id);
  assert.equal(result.device_id, 'device-A');
  assert.equal(result.user_id, 'user-001');
  assert.deepEqual(result.push, { ok: true });
  assert.ok(result.dispatch);
  assert.ok(result.self_check);
  assert.equal(result.dispatch.payloadType, 'dispatch.start');
  assert.equal(result.self_check.payloadType, 'dispatch.self_check_due');

  // ws called with right shape
  assert.equal(ctx.ws.calls.length, 1);
  const wsCall = ctx.ws.calls[0];
  assert.equal(wsCall.deviceId, 'device-A');
  assert.equal(wsCall.payload.type, 'command');
  assert.equal(wsCall.payload.correlation_id, result.correlation_id);
  assert.equal(wsCall.payload.cmd, 'publish'); // xhs. prefix stripped
  assert.equal(wsCall.payload.session, null); // no session yet

  // dispatchRouter has the entry
  const route = ctx.cm.dispatchRouter.lookup(result.correlation_id);
  assert.ok(route);
  assert.equal(route.device_id, 'device-A');
  assert.equal(route.user_id, 'user-001');
  assert.equal(route.channel_id, 'ch-xhs-1');

  // dispatch.start written into messages.sqlite
  const dispatched = readDispatchMessages(ctx.node);
  const start = dispatched.find((m) => m.payload_type === 'dispatch.start');
  assert.ok(start);
  assert.equal(start.correlation_id, result.correlation_id);
});

test('deviceCommandSend forwards stored session to ws frame', async (t) => {
  const ctx = await createHarness(t);
  ctx.ws.goOnline('device-A');

  ctx.cm.sessionManager.updateSession('user-001', {
    cookies: [{ name: 'sid', value: 'abc' }],
    login_state: 'logged_in',
  });

  const result = await ctx.cm.deviceCommandSend({
    channel_id: 'ch-xhs-1',
    type: 'xhs.search',
    params: { keyword: 'foo' },
  });
  assert.equal(ctx.ws.calls.length, 1);
  const session = ctx.ws.calls[0].payload.session;
  assert.ok(session);
  assert.equal(session.user_id, 'user-001');
  assert.equal(session.login_state, 'logged_in');
  assert.equal(result.push.ok, true);
});

test('deviceCommandSend throws device_offline when ws not connected (Fix-T2 §2)', async (t) => {
  const ctx = await createHarness(t);
  // ws.online is empty → pushCommand returns offline → RPC must error.
  await assert.rejects(
    ctx.cm.deviceCommandSend({
      channel_id: 'ch-xhs-1',
      type: 'xhs.publish',
      params: { title: 'hi', content: 'body' },
    }),
    (err) => err.code === 'device_offline' && err.statusCode === 503,
  );
  // Audit trail: dispatch.start emitted + dispatch.failed audit message
  // recorded; router entry cleaned up so future correlation_id reuse is safe.
  const dispatched = readDispatchMessages(ctx.node);
  assert.ok(dispatched.some((m) => m.payload_type === 'dispatch.start'));
  const failed = dispatched.find((m) => m.payload_type === 'dispatch.failed');
  assert.ok(failed, 'dispatch.failed audit message expected on offline');
  assert.equal(failed.payload_body?.error?.code, 'device_offline');
  assert.equal(ctx.cm.dispatchRouter.lookup(failed.correlation_id), null);
});

test('deviceCommandSend allows explicit override device_id / user_id', async (t) => {
  const ctx = await createHarness(t);
  ctx.ws.goOnline('device-X');
  const result = await ctx.cm.deviceCommandSend({
    channel_id: 'ch-xhs-1',
    type: 'xhs.publish',
    params: { title: 'hi', content: 'body' },
    device_id: 'device-X',
    user_id: 'user-other',
  });
  assert.equal(result.device_id, 'device-X');
  assert.equal(result.user_id, 'user-other');
  assert.equal(ctx.ws.calls[0].deviceId, 'device-X');
});

test('deviceCommandSend errors when channel has no device member and no env default', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'cm-device-empty-'));
  t.after(() => rmSync(tempHome, { recursive: true, force: true }));
  const cm = new ChannelManager({
    serverUrl: 'http://localhost:3001',
    machineApiKey: 'machine-key',
    daemonSocketPath: path.join(tempHome, '.coagent', 'daemon.sock'),
    baseDir: path.join(tempHome, 'coagent'),
    dueMessagePollMs: 0,
    deviceWsServer: new FakeWsServer(),
    defaultDeviceId: '',
    defaultUserId: '',
  });
  await cm.createChannel({
    channelId: 'ch-empty',
    name: 'No members',
    type: 'xhs-creator',
    status: 'created',
  });
  await assert.rejects(
    cm.deviceCommandSend({ channel_id: 'ch-empty', type: 'xhs.publish', params: { title: 'hi', content: 'body' } }),
    (err) => err.code === 'device_unavailable',
  );
});

test('deviceSessionUpdate persists patch and deviceSessionGet returns flat shape (spec §4.1)', async (t) => {
  const ctx = await createHarness(t);
  const upd = await ctx.cm.deviceSessionUpdate({
    user_id: 'user-001',
    cookies: [{ name: 'sid', value: 'v9' }],
    login_state: 'logged_in',
    expires_at: 1234567890,
  });
  assert.equal(upd.session.user_id, 'user-001');
  assert.equal(upd.session.login_state, 'logged_in');
  assert.equal(upd.session.cookies.length, 1);

  const got = await ctx.cm.deviceSessionGet({ user_id: 'user-001' });
  // Fix-T2 §4: flat shape, no `{exists, session}` wrapper.
  assert.equal(got.user_id, 'user-001');
  assert.equal(got.login_state, 'logged_in');
  assert.equal(Array.isArray(got.cookies), true);
  assert.equal(got.cookies.length, 1);
  assert.equal(got.expires_at, 1234567890);
  assert.equal(typeof got.last_updated_at, 'number');

  // Missing → null (envelope writes {ok:true,result:null}, mirrors publish-status).
  const missing = await ctx.cm.deviceSessionGet({ user_id: 'user-999' });
  assert.equal(missing, null);
});

test('deviceSessionUpdate accepts the HTTP-forwarded {deviceId,userId,patch} shape', async (t) => {
  const ctx = await createHarness(t);
  const upd = await ctx.cm.deviceSessionUpdate({
    deviceId: 'device-A',
    userId: 'user-007',
    patch: { cookies: [{ name: 'a', value: 'b' }], login_state: 'logged_in' },
  });
  assert.equal(upd.session.user_id, 'user-007');
  assert.equal(upd.session.cookies.length, 1);
});

test('deviceCallback emits dispatch.completed and clears router', async (t) => {
  const ctx = await createHarness(t);
  ctx.ws.goOnline('device-A');
  const send = await ctx.cm.deviceCommandSend({
    channel_id: 'ch-xhs-1',
    type: 'xhs.publish',
    params: { title: 't', content: 'body' },
  });

  const cb = await ctx.cm.deviceCallback({
    deviceId: 'device-A',
    correlationId: send.correlation_id,
    status: 'ok',
    result: { url: 'https://xhs.com/note/abc', note_id: 'abc' },
  });
  assert.equal(cb.payload_type, 'dispatch.completed');

  const dispatched = readDispatchMessages(ctx.node);
  const completed = dispatched.find((m) => m.payload_type === 'dispatch.completed');
  assert.ok(completed);
  assert.equal(completed.correlation_id, send.correlation_id);
  assert.deepEqual(completed.payload_body.result, { url: 'https://xhs.com/note/abc', note_id: 'abc' });

  // router cleared
  assert.equal(ctx.cm.dispatchRouter.lookup(send.correlation_id), null);
});

test('deviceCallback emits dispatch.failed when status=error', async (t) => {
  const ctx = await createHarness(t);
  ctx.ws.goOnline('device-A');
  const send = await ctx.cm.deviceCommandSend({
    channel_id: 'ch-xhs-1',
    type: 'xhs.publish',
    params: { title: 'hi', content: 'body' },
  });
  const cb = await ctx.cm.deviceCallback({
    deviceId: 'device-A',
    correlationId: send.correlation_id,
    status: 'error',
    error: { code: 'auth_expired', message: 'login required' },
  });
  assert.equal(cb.payload_type, 'dispatch.failed');
  const dispatched = readDispatchMessages(ctx.node);
  const failed = dispatched.find((m) => m.payload_type === 'dispatch.failed');
  assert.ok(failed);
  assert.equal(failed.payload_body.error?.code, 'auth_expired');
});

test('deviceCallback rejects unknown correlation_id (404)', async (t) => {
  const ctx = await createHarness(t);
  await assert.rejects(
    ctx.cm.deviceCallback({ deviceId: 'device-A', correlationId: 'no-such', status: 'ok' }),
    (err) => err.code === 'correlation_unknown' && err.statusCode === 404,
  );
});

test('deviceCallback rejects mismatched device_id (403)', async (t) => {
  const ctx = await createHarness(t);
  ctx.ws.goOnline('device-A');
  const send = await ctx.cm.deviceCommandSend({
    channel_id: 'ch-xhs-1',
    type: 'xhs.publish',
    params: { title: 'hi', content: 'body' },
  });
  await assert.rejects(
    ctx.cm.deviceCallback({ deviceId: 'rogue-device', correlationId: send.correlation_id, status: 'ok' }),
    (err) => err.code === 'forbidden' && err.statusCode === 403,
  );
});

test('rpcCall switch dispatches device.* methods', async (t) => {
  const ctx = await createHarness(t);
  ctx.ws.goOnline('device-A');
  const send = await ctx.cm.rpcCall('device.command.send', {
    channel_id: 'ch-xhs-1',
    type: 'xhs.publish',
    params: { title: 't', content: 'body' },
  });
  assert.ok(send.correlation_id);
  const got = await ctx.cm.rpcCall('device.session.get', { user_id: 'user-001' });
  assert.equal(got, null); // Fix-T2 §4: missing → null (no wrapper)
  const upd = await ctx.cm.rpcCall('device.session.update', {
    user_id: 'user-001',
    cookies: [{ name: 'k', value: 'v' }],
    login_state: 'logged_in',
  });
  assert.equal(upd.session.login_state, 'logged_in');
});
