// Tests for device.* RPC: deviceCommandSend / deviceSessionGet / deviceSessionUpdate
// + deviceCallback. Use a real ChannelManager with a fake DeviceWsServer + a
// stand-alone tmp baseDir so persistence is isolated.

import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { ChannelManager, validateDeviceCommand, DEVICE_COMMAND_TYPES } from '../src/channel-manager.js';
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
  // R3-T4 FX7: deviceSessionUpdate 返回脱敏 envelope（不含 cookies/session），
  // 但持久化文件保留 cookie 真值，deviceSessionGet 仍可读出全量。
  const upd = await ctx.cm.deviceSessionUpdate({
    user_id: 'user-001',
    cookies: [{ name: 'sid', value: 'v9' }],
    login_state: 'logged_in',
    expires_at: 1234567890,
  });
  assert.equal(upd.user_id, 'user-001');
  assert.equal(upd.login_state, 'logged_in');
  assert.equal(upd.cookie_count, 1);
  assert.equal(upd.expires_at, 1234567890);
  assert.equal(typeof upd.last_updated_at, 'number');

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
  assert.equal(upd.user_id, 'user-007');
  assert.equal(upd.login_state, 'logged_in');
  assert.equal(upd.cookie_count, 1);
});

// R3-T4 FX7 / round-2 review codex#t59.2：返回 envelope 必须不泄漏完整 cookies / session 字段。
// daemon 端 HTTP 透传（rpc-server.js writeJson 整段返回 result），任何 cookies / session 字段都
// 会被 extension 端 console.log + UI message 打印出去。
test('deviceSessionUpdate returns redacted envelope without raw cookies / session fields', async (t) => {
  const ctx = await createHarness(t);
  const upd = await ctx.cm.deviceSessionUpdate({
    user_id: 'user-002',
    cookies: [
      { name: 'web_session', value: 'SECRET_SESSION_VALUE' },
      { name: 'access-token', value: 'SECRET_TOKEN_VALUE' },
    ],
    login_state: 'logged_in',
  });
  // 正向：脱敏字段在
  assert.equal(upd.cookie_count, 2);
  assert.equal(upd.login_state, 'logged_in');
  // 反向：禁止出现 cookies / session 完整结构
  assert.ok(!('session' in upd), 'response must not include session wrapper');
  assert.ok(!('cookies' in upd), 'response must not include raw cookies array');
  // 整段序列化也不能出现 cookie 真值
  const serialized = JSON.stringify(upd);
  assert.ok(!serialized.includes('SECRET_SESSION_VALUE'), 'raw cookie value leaked into envelope');
  assert.ok(!serialized.includes('SECRET_TOKEN_VALUE'), 'raw cookie value leaked into envelope');
  assert.ok(!serialized.includes('web_session'), 'cookie name leaked into envelope');
  assert.ok(!serialized.includes('access-token'), 'cookie name leaked into envelope');
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
  // R3-T4 FX7: rpcCall 透传脱敏 envelope；不含 session/cookies 字段。
  assert.equal(upd.login_state, 'logged_in');
  assert.equal(upd.cookie_count, 1);
  assert.ok(!('session' in upd));
  assert.ok(!('cookies' in upd));
});

// ── Fix-T2 §3 payload allowlist + per-type schema ────────────────────────────

test('validateDeviceCommand: rejects unknown / non-allowlisted types', () => {
  for (const bad of ['', 'xhs.unknown', 'foo', 'XHS.PUBLISH', null, undefined]) {
    assert.throws(() => validateDeviceCommand(bad, {}), (err) => err.code === 'bad_request');
  }
  // Snapshot the full allowlist so future drift is loud.
  assert.deepEqual([...DEVICE_COMMAND_TYPES], [
    'xhs.publish', 'xhs.search', 'xhs.get-my-recent', 'xhs.get-note', 'xhs.publish-status',
  ]);
});

test('validateDeviceCommand: rejects non-object params', () => {
  for (const bad of [null, 'a string', 42, true, []]) {
    assert.throws(() => validateDeviceCommand('xhs.search', bad), (err) => err.code === 'bad_request');
  }
});

test('validateDeviceCommand: xhs.publish needs title + (content|content_path)', () => {
  assert.throws(() => validateDeviceCommand('xhs.publish', {}), /title/);
  assert.throws(() => validateDeviceCommand('xhs.publish', { title: 'hi' }), /content or content_path/);
  // valid via inline content
  validateDeviceCommand('xhs.publish', { title: 'hi', content: 'body' });
  // valid via path
  validateDeviceCommand('xhs.publish', { title: 'hi', content_path: '/abs/path.md' });
  // images / tags must be arrays when present
  assert.throws(() => validateDeviceCommand('xhs.publish', { title: 'hi', content: 'b', images: 'nope' }), /images must be an array/);
  assert.throws(() => validateDeviceCommand('xhs.publish', { title: 'hi', content: 'b', tags: {} }), /tags must be an array/);
});

test('validateDeviceCommand: xhs.search / xhs.get-note / xhs.publish-status', () => {
  assert.throws(() => validateDeviceCommand('xhs.search', {}), /keyword/);
  validateDeviceCommand('xhs.search', { keyword: 'foo' });
  validateDeviceCommand('xhs.search', { keyword: 'foo', limit: 5 });
  assert.throws(() => validateDeviceCommand('xhs.search', { keyword: 'foo', limit: -1 }), /limit/);

  // get-my-recent only checks limit type.
  validateDeviceCommand('xhs.get-my-recent', {});
  assert.throws(() => validateDeviceCommand('xhs.get-my-recent', { limit: 'lots' }), /limit/);

  // get-note: url OR xsec_token required
  assert.throws(() => validateDeviceCommand('xhs.get-note', {}), /url or xsec_token/);
  validateDeviceCommand('xhs.get-note', { url: 'https://xhs.com/note/x' });
  validateDeviceCommand('xhs.get-note', { xsec_token: 'abc' });

  // publish-status: note_id required (FX3 round-2 codex#t57.1).
  // correlation_id 是 dispatch envelope 字段（外层），不是 device-command params 字段。
  assert.throws(() => validateDeviceCommand('xhs.publish-status', {}), /note_id/);
  assert.throws(
    () => validateDeviceCommand('xhs.publish-status', { correlation_id: 'corr-1' }),
    /note_id/,
  );
  validateDeviceCommand('xhs.publish-status', { note_id: 'n1' });
});

test('validateDeviceCommand: xhs.publish content_path must be absolute', () => {
  // FX1: real-mode CLI 只发 absolute path；relative 拒绝。
  assert.throws(
    () => validateDeviceCommand('xhs.publish', { title: 'hi', content_path: 'relative/path.md' }),
    /content_path must be an absolute path/,
  );
  assert.throws(
    () => validateDeviceCommand('xhs.publish', { title: 'hi', content_path: './path.md' }),
    /content_path must be an absolute path/,
  );
  // 绝对路径校验通过（不读盘）。
  validateDeviceCommand('xhs.publish', { title: 'hi', content_path: '/abs/path.md' });
});

test('deviceCommandSend rejects unknown type before resolving device/user', async (t) => {
  const ctx = await createHarness(t);
  await assert.rejects(
    ctx.cm.deviceCommandSend({ channel_id: 'ch-xhs-1', type: 'xhs.bogus', params: {} }),
    (err) => err.code === 'bad_request' && /allowed/.test(err.message),
  );
  // No dispatch.start should be emitted on schema rejection.
  const dispatched = readDispatchMessages(ctx.node);
  assert.equal(dispatched.length, 0);
});

test('deviceCommandSend rejects xhs.publish missing title', async (t) => {
  const ctx = await createHarness(t);
  ctx.ws.goOnline('device-A');
  await assert.rejects(
    ctx.cm.deviceCommandSend({
      channel_id: 'ch-xhs-1',
      type: 'xhs.publish',
      params: { content: 'body' },
    }),
    (err) => err.code === 'bad_request' && /title/.test(err.message),
  );
});

// ── FX1 (round-2 codex#t56.1): content_path materialize ─────────────────────

test('deviceCommandSend: xhs.publish materializes content_path → content before push', async (t) => {
  const ctx = await createHarness(t);
  ctx.ws.goOnline('device-A');
  // 写一个临时 markdown 作 content 源。
  const tmp = mkdtempSync(path.join(os.tmpdir(), 'cm-publish-content-'));
  t.after(() => rmSync(tmp, { recursive: true, force: true }));
  const contentPath = path.join(tmp, 'note.md');
  const expectedBody = '# 标题\n\n正文内容来自文件\n';
  writeFileSync(contentPath, expectedBody, 'utf8');

  const result = await ctx.cm.deviceCommandSend({
    channel_id: 'ch-xhs-1',
    type: 'xhs.publish',
    params: { title: '物化', content_path: contentPath, tags: ['t1'] },
  });
  assert.equal(result.push.ok, true);

  // ws push 收到 inline content（非 path）。
  assert.equal(ctx.ws.calls.length, 1);
  const pushed = ctx.ws.calls[0].payload.params;
  assert.equal(pushed.content, expectedBody, 'device 端应收到 inline content');
  assert.equal(pushed.content_path, undefined, 'device 端不应再带 content_path');
  assert.equal(pushed.title, '物化');
  assert.deepEqual(pushed.tags, ['t1']);
});

test('deviceCommandSend: xhs.publish ENOENT → dispatch.failed + content_read_failed (no push)', async (t) => {
  const ctx = await createHarness(t);
  ctx.ws.goOnline('device-A');
  const missing = path.join(os.tmpdir(), `cm-missing-${Date.now()}-${Math.random().toString(36).slice(2)}.md`);

  await assert.rejects(
    ctx.cm.deviceCommandSend({
      channel_id: 'ch-xhs-1',
      type: 'xhs.publish',
      params: { title: 'oops', content_path: missing },
    }),
    (err) => err.code === 'content_read_failed' && err.statusCode === 400,
  );

  // dispatch.start emitted（先于物化）+ dispatch.failed 审计 + 没有 push 给 device。
  const dispatched = readDispatchMessages(ctx.node);
  const failed = dispatched.find((m) => m.payload_type === 'dispatch.failed');
  assert.ok(failed, 'dispatch.failed 审计 should be emitted on content_read_failed');
  assert.equal(failed.payload_body?.error?.code, 'content_read_failed');
  assert.equal(ctx.ws.calls.length, 0, 'pushCommand 不应被调用');
  // router 已清；correlation_id 可被复用。
  assert.equal(ctx.cm.dispatchRouter.lookup(failed.correlation_id), null);
});

test('deviceCommandSend: xhs.publish content + content_path 双给时取 content（back-compat）', async (t) => {
  const ctx = await createHarness(t);
  ctx.ws.goOnline('device-A');
  const tmp = mkdtempSync(path.join(os.tmpdir(), 'cm-publish-both-'));
  t.after(() => rmSync(tmp, { recursive: true, force: true }));
  const contentPath = path.join(tmp, 'note.md');
  writeFileSync(contentPath, 'FROM_FILE_SHOULD_NOT_BE_USED', 'utf8');

  const result = await ctx.cm.deviceCommandSend({
    channel_id: 'ch-xhs-1',
    type: 'xhs.publish',
    params: { title: 'hi', content: 'INLINE_WINS', content_path: contentPath },
  });
  assert.equal(result.push.ok, true);
  const pushed = ctx.ws.calls[0].payload.params;
  assert.equal(pushed.content, 'INLINE_WINS');
  assert.equal(pushed.content_path, undefined, 'path 应被剥离，device 端只见 inline');
});

test('deviceCommandSend: xhs.publish content_path 必须 absolute（validator 层早拒绝）', async (t) => {
  const ctx = await createHarness(t);
  ctx.ws.goOnline('device-A');
  await assert.rejects(
    ctx.cm.deviceCommandSend({
      channel_id: 'ch-xhs-1',
      type: 'xhs.publish',
      params: { title: 'hi', content_path: 'relative/path.md' },
    }),
    (err) => err.code === 'bad_request' && /content_path must be an absolute path/.test(err.message),
  );
  // validator 阶段拒绝 → 没有 dispatch.start 也没有 push。
  assert.equal(ctx.ws.calls.length, 0);
  const dispatched = readDispatchMessages(ctx.node);
  assert.equal(dispatched.length, 0);
});

test('deviceCommandSend: xhs.publish-status note_id required（FX3）', async (t) => {
  const ctx = await createHarness(t);
  ctx.ws.goOnline('device-A');
  // 不带 note_id → 400 bad_request
  await assert.rejects(
    ctx.cm.deviceCommandSend({
      channel_id: 'ch-xhs-1',
      type: 'xhs.publish-status',
      params: {},
    }),
    (err) => err.code === 'bad_request' && /note_id/.test(err.message),
  );
  // 带 note_id 通过；ws 收到 cmd='publish-status' + params.note_id。
  const result = await ctx.cm.deviceCommandSend({
    channel_id: 'ch-xhs-1',
    type: 'xhs.publish-status',
    params: { note_id: 'n-abc' },
  });
  assert.equal(result.push.ok, true);
  const pushed = ctx.ws.calls.at(-1);
  assert.equal(pushed.payload.cmd, 'publish-status');
  assert.equal(pushed.payload.params.note_id, 'n-abc');
});

test('deviceCommandSend bubbles up payload_serialization_failed reason as device_offline', async (t) => {
  const ctx = await createHarness(t);
  ctx.ws.goOnline('device-A');
  ctx.ws.nextResult = { ok: false, reason: 'payload_serialization_failed', message: 'cyclic' };
  await assert.rejects(
    ctx.cm.deviceCommandSend({
      channel_id: 'ch-xhs-1',
      type: 'xhs.search',
      params: { keyword: 'q' },
    }),
    (err) => err.code === 'device_offline' && /payload_serialization_failed/.test(err.message),
  );
});

// ── M1.1 Fix-T3 §3 daemon callback dedupe + replay ───────────────────────────

test('deviceCallback: duplicate after success returns 200 with deduped:true (no second emit)', async (t) => {
  const ctx = await createHarness(t);
  ctx.ws.goOnline('device-A');
  const send = await ctx.cm.deviceCommandSend({
    channel_id: 'ch-xhs-1',
    type: 'xhs.publish',
    params: { title: 't', content: 'body' },
  });

  // First callback wins → emits dispatch.completed.
  const first = await ctx.cm.deviceCallback({
    deviceId: 'device-A',
    correlationId: send.correlation_id,
    status: 'ok',
    result: { url: 'https://xhs.com/note/abc', note_id: 'abc' },
  });
  assert.equal(first.payload_type, 'dispatch.completed');
  assert.notEqual(first.deduped, true);

  // Second callback (extension retry / replay) finds router empty AND
  // dispatch.completed in store → returns deduped:true.
  const second = await ctx.cm.deviceCallback({
    deviceId: 'device-A',
    correlationId: send.correlation_id,
    status: 'ok',
    result: { url: 'https://xhs.com/note/abc', note_id: 'abc' },
  });
  assert.equal(second.deduped, true);
  assert.equal(second.payload_type, 'dispatch.completed');
  assert.equal(second.correlation_id, send.correlation_id);

  // Only one dispatch.completed in the store — no duplicate emit.
  const dispatched = readDispatchMessages(ctx.node);
  const completions = dispatched.filter((m) => m.payload_type === 'dispatch.completed');
  assert.equal(completions.length, 1);
});

test('deviceCallback: duplicate after failure returns deduped:true with dispatch.failed', async (t) => {
  const ctx = await createHarness(t);
  ctx.ws.goOnline('device-A');
  const send = await ctx.cm.deviceCommandSend({
    channel_id: 'ch-xhs-1',
    type: 'xhs.publish',
    params: { title: 't', content: 'body' },
  });
  await ctx.cm.deviceCallback({
    deviceId: 'device-A',
    correlationId: send.correlation_id,
    status: 'error',
    error: { code: 'auth_expired', message: 'login required' },
  });
  const second = await ctx.cm.deviceCallback({
    deviceId: 'device-A',
    correlationId: send.correlation_id,
    status: 'error',
    error: { code: 'auth_expired', message: 'login required' },
  });
  assert.equal(second.deduped, true);
  assert.equal(second.payload_type, 'dispatch.failed');
});

test('deviceCallback: dedupe with mismatched device_id refuses (falls through to correlation_unknown 404)', async (t) => {
  const ctx = await createHarness(t);
  ctx.ws.goOnline('device-A');
  const send = await ctx.cm.deviceCommandSend({
    channel_id: 'ch-xhs-1',
    type: 'xhs.publish',
    params: { title: 't', content: 'body' },
  });
  await ctx.cm.deviceCallback({
    deviceId: 'device-A',
    correlationId: send.correlation_id,
    status: 'ok',
    result: { note_id: 'a' },
  });
  // Different device retrying — must not be granted dedupe.
  await assert.rejects(
    ctx.cm.deviceCallback({
      deviceId: 'rogue-device',
      correlationId: send.correlation_id,
      status: 'ok',
      result: { note_id: 'a' },
    }),
    (err) => err.code === 'correlation_unknown' && err.statusCode === 404,
  );
});

test('handleCallbackReplay: dispatches each payload through deviceCallback (mix of new + dedupe + unknown)', async (t) => {
  const ctx = await createHarness(t);
  ctx.ws.goOnline('device-A');
  // 1) Unfinished dispatch → replay payload completes it.
  const a = await ctx.cm.deviceCommandSend({
    channel_id: 'ch-xhs-1',
    type: 'xhs.publish',
    params: { title: 't', content: 'body' },
  });
  // 2) Already-completed dispatch → replay payload deduped.
  const b = await ctx.cm.deviceCommandSend({
    channel_id: 'ch-xhs-1',
    type: 'xhs.publish',
    params: { title: 't2', content: 'body' },
  });
  await ctx.cm.deviceCallback({
    deviceId: 'device-A',
    correlationId: b.correlation_id,
    status: 'ok',
    result: { note_id: 'b' },
  });

  // Snapshot ws calls so the ack-push assertion below can find the new frame.
  const wsCallsBefore = ctx.ws.calls.length;

  const summary = await ctx.cm.handleCallbackReplay({
    deviceId: 'device-A',
    payloads: [
      { correlation_id: a.correlation_id, status: 'ok', result: { note_id: 'a' } },
      { correlation_id: b.correlation_id, status: 'ok', result: { note_id: 'b' } },
      { correlation_id: 'never-existed', status: 'ok', result: {} },
      'not-an-object',
    ],
  });

  assert.equal(summary.accepted, 1);
  assert.equal(summary.deduped, 1);
  assert.equal(summary.failed, 2);
  assert.equal(summary.results.length, 4);
  // R3-T3: ack lists track results — accepted holds new + dedupe; rejected
  // holds invalid_payload + correlation_unknown with daemon error code.
  assert.deepEqual(summary.ack.accepted.sort(), [a.correlation_id, b.correlation_id].sort());
  assert.equal(summary.ack.rejected.length, 2);
  const rejectedCodes = summary.ack.rejected.map((r) => r.code).sort();
  assert.deepEqual(rejectedCodes, ['correlation_unknown', 'invalid_payload']);
  const rejectedById = new Map(summary.ack.rejected.map((r) => [r.correlation_id, r]));
  assert.ok(rejectedById.has('never-existed'));
  assert.equal(rejectedById.get('never-existed').code, 'correlation_unknown');
  assert.ok(rejectedById.has(''));
  assert.equal(rejectedById.get('').code, 'invalid_payload');

  // R3-T3: the daemon must have pushed exactly one new callback_replay_ack
  // frame back to device-A reflecting the same accepted / rejected lists.
  const ackCalls = ctx.ws.calls.slice(wsCallsBefore).filter(
    (c) => c.payload?.type === 'callback_replay_ack',
  );
  assert.equal(ackCalls.length, 1);
  const ackFrame = ackCalls[0];
  assert.equal(ackFrame.deviceId, 'device-A');
  assert.deepEqual(ackFrame.payload.accepted.sort(), [a.correlation_id, b.correlation_id].sort());
  assert.equal(ackFrame.payload.rejected.length, 2);

  // Original dispatch.completed for `a` exists exactly once.
  const dispatched = readDispatchMessages(ctx.node);
  const completedForA = dispatched.filter(
    (m) => m.payload_type === 'dispatch.completed' && m.correlation_id === a.correlation_id,
  );
  assert.equal(completedForA.length, 1);
});

test('handleCallbackReplay: empty / non-array payloads short-circuit', async (t) => {
  const ctx = await createHarness(t);
  assert.deepEqual(
    await ctx.cm.handleCallbackReplay({ deviceId: 'device-A', payloads: [] }),
    { accepted: 0, deduped: 0, failed: 0, results: [], ack: { accepted: [], rejected: [] } },
  );
  assert.deepEqual(
    await ctx.cm.handleCallbackReplay({ deviceId: 'device-A', payloads: undefined }),
    { accepted: 0, deduped: 0, failed: 0, results: [], ack: { accepted: [], rejected: [] } },
  );
});

test('handleCallbackReplay: ack push failure is recorded but never throws', async (t) => {
  const ctx = await createHarness(t);
  ctx.ws.goOnline('device-A');
  const a = await ctx.cm.deviceCommandSend({
    channel_id: 'ch-xhs-1',
    type: 'xhs.publish',
    params: { title: 't', content: 'body' },
  });
  // Force the next pushCommand (the ack frame) to fail — extension dropped
  // mid-replay; daemon must still complete handling without throwing.
  ctx.ws.nextResult = { ok: false, reason: 'device_offline' };
  const summary = await ctx.cm.handleCallbackReplay({
    deviceId: 'device-A',
    payloads: [
      { correlation_id: a.correlation_id, status: 'ok', result: { note_id: 'a' } },
    ],
  });
  assert.equal(summary.accepted, 1);
  assert.deepEqual(summary.ack.accepted, [a.correlation_id]);
  // ws-server got the ack push attempt even though it returned device_offline.
  const ackCalls = ctx.ws.calls.filter((c) => c.payload?.type === 'callback_replay_ack');
  assert.equal(ackCalls.length, 1);
});
