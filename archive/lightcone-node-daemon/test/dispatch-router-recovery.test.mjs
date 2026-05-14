// M1.1 Fix-T3 — DispatchRouter restart recovery + ChannelManager.start() rebuild.
//
// Coverage:
//   1. DispatchRouter.recoverFromRows
//      - empty / non-array / partial-row tolerated
//      - skips rows already registered (idempotent re-recovery)
//      - returns { recovered, skipped } summary
//      - per-row ttlMs honored
//   2. ChannelManager._recoverDispatchRouter
//      - in-flight dispatch.start without terminal → re-registered
//      - dispatch.start with paired dispatch.completed/failed/rejected → skipped
//      - rows older than the recovery window → skipped
//   3. End-to-end:
//      - simulate daemon restart by constructing a *fresh* ChannelManager
//        that points at the same baseDir and calling start(); verify that an
//        in-flight correlation looked up before deviceCallback returns the
//        right route.
//
// Tests intentionally drive ChannelManager via a stand-alone tmp baseDir +
// FakeWsServer (no real WS / RPC server) — same harness as
// channel-manager-device.test.mjs.

import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { ChannelManager } from '../src/channel-manager.js';
import { DispatchRouter, DEFAULT_RECOVERY_WINDOW_MS } from '../src/devices/dispatch-router.js';

class FakeWsServer {
  constructor() { this.calls = []; this.online = new Set(); }
  pushCommand(deviceId, payload) {
    this.calls.push({ deviceId, payload });
    return this.online.has(deviceId) ? { ok: true } : { ok: false, reason: 'device_offline' };
  }
  goOnline(deviceId) { this.online.add(deviceId); }
}

function newBaseDir(t, prefix = 'cm-recovery-') {
  const dir = mkdtempSync(path.join(os.tmpdir(), prefix));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  return dir;
}

async function makeChannelManager(t, { baseDir, ws } = {}) {
  const tempHome = baseDir ?? newBaseDir(t);
  const cm = new ChannelManager({
    serverUrl: 'http://localhost:3001',
    machineApiKey: 'machine-key',
    daemonSocketPath: path.join(tempHome, '.coagent', 'daemon.sock'),
    daemonHttpUrl: 'http://127.0.0.1:3002',
    daemonToken: 'daemon-token',
    baseDir: path.join(tempHome, 'coagent'),
    dueMessagePollMs: 0,
    deviceWsServer: ws ?? new FakeWsServer(),
  });
  return { cm, tempHome };
}

// ── 1. DispatchRouter.recoverFromRows unit ─────────────────────────────────

test('DispatchRouter.recoverFromRows: empty / invalid input → no-op', () => {
  const r = new DispatchRouter();
  assert.deepEqual(r.recoverFromRows([]), { recovered: 0, skipped: 0 });
  assert.deepEqual(r.recoverFromRows(null), { recovered: 0, skipped: 0 });
  assert.deepEqual(r.recoverFromRows(undefined), { recovered: 0, skipped: 0 });
  assert.deepEqual(r.recoverFromRows([null, undefined, 42, 'x']), {
    recovered: 0,
    skipped: 4,
  });
  assert.equal(r.size(), 0);
});

test('DispatchRouter.recoverFromRows: skips rows missing correlation_id or channel_id', () => {
  const r = new DispatchRouter();
  const out = r.recoverFromRows([
    { correlation_id: '', channel_id: 'ch', device_id: 'd', user_id: 'u' },
    { correlation_id: 'c1', channel_id: '', device_id: 'd', user_id: 'u' },
    { correlation_id: 'c2', channel_id: 'ch', device_id: 'd', user_id: 'u' },
  ]);
  assert.deepEqual(out, { recovered: 1, skipped: 2 });
  assert.ok(r.lookup('c2'));
});

test('DispatchRouter.recoverFromRows: idempotent — re-recovery skips already-registered', () => {
  const r = new DispatchRouter();
  r.register({ correlationId: 'c1', channelId: 'ch', deviceId: 'd', userId: 'u' });
  const out = r.recoverFromRows([
    { correlation_id: 'c1', channel_id: 'ch', device_id: 'd2', user_id: 'u2' },
    { correlation_id: 'c2', channel_id: 'ch', device_id: 'd', user_id: 'u' },
  ]);
  assert.deepEqual(out, { recovered: 1, skipped: 1 });
  // existing entry preserved (recovery doesn't overwrite live registrations)
  const got = r.lookup('c1');
  assert.equal(got.device_id, 'd');
  assert.equal(got.user_id, 'u');
});

test('DispatchRouter.recoverFromRows: per-row ttlMs honored', () => {
  let now = 1_000_000;
  const r = new DispatchRouter({ ttlMs: 10_000, now: () => now });
  r.recoverFromRows([
    { correlation_id: 'c1', channel_id: 'ch', device_id: 'd', user_id: 'u', ttlMs: 100 },
    { correlation_id: 'c2', channel_id: 'ch', device_id: 'd', user_id: 'u' },
  ]);
  now += 200; // beyond c1 ttl, within default
  assert.equal(r.lookup('c1'), null);
  assert.ok(r.lookup('c2'));
});

test('DispatchRouter.DEFAULT_RECOVERY_WINDOW_MS is 30 minutes', () => {
  assert.equal(DEFAULT_RECOVERY_WINDOW_MS, 30 * 60 * 1000);
});

// ── 2. ChannelManager._recoverDispatchRouter ─────────────────────────────

test('_recoverDispatchRouter: in-flight dispatch.start re-registered into router', async (t) => {
  const ws = new FakeWsServer();
  ws.goOnline('device-A');
  const { cm } = await makeChannelManager(t, { ws });
  await cm.createChannel({
    channelId: 'ch-recovery-1',
    name: 'Recovery 1',
    type: 'xhs-creator',
    status: 'created',
    members: [
      { memberType: 'human', memberId: 'user-001' },
      { memberType: 'device', memberId: 'device-A' },
    ],
  });

  const send = await cm.deviceCommandSend({
    channel_id: 'ch-recovery-1',
    type: 'xhs.publish',
    params: { title: 't', content: 'body' },
  });

  // Forcefully clear the live router to simulate a process restart. The
  // dispatch.start row is still in messages.sqlite.
  cm.dispatchRouter.remove(send.correlation_id);
  assert.equal(cm.dispatchRouter.lookup(send.correlation_id), null);

  const node = cm._requireNode('ch-recovery-1');
  const summary = cm._recoverDispatchRouter(node);
  assert.equal(summary.recovered, 1);
  assert.equal(summary.scanned, 1);

  const route = cm.dispatchRouter.lookup(send.correlation_id);
  assert.ok(route, 'router entry rebuilt from messages.sqlite');
  assert.equal(route.channel_id, 'ch-recovery-1');
  assert.equal(route.device_id, 'device-A');
  assert.equal(route.user_id, 'user-001');
});

test('_recoverDispatchRouter: dispatch.start with paired terminal is skipped', async (t) => {
  const ws = new FakeWsServer();
  ws.goOnline('device-A');
  const { cm } = await makeChannelManager(t, { ws });
  await cm.createChannel({
    channelId: 'ch-recovery-2',
    name: 'Recovery 2',
    type: 'xhs-creator',
    status: 'created',
    members: [
      { memberType: 'human', memberId: 'user-001' },
      { memberType: 'device', memberId: 'device-A' },
    ],
  });

  const finished = await cm.deviceCommandSend({
    channel_id: 'ch-recovery-2',
    type: 'xhs.publish',
    params: { title: 't', content: 'body' },
  });
  await cm.deviceCallback({
    deviceId: 'device-A',
    correlationId: finished.correlation_id,
    status: 'ok',
    result: { url: 'https://xhs.com/note/x', note_id: 'x' },
  });

  const inflight = await cm.deviceCommandSend({
    channel_id: 'ch-recovery-2',
    type: 'xhs.publish',
    params: { title: 't2', content: 'body2' },
  });

  // Reset router to mimic restart.
  cm.dispatchRouter.remove(finished.correlation_id);
  cm.dispatchRouter.remove(inflight.correlation_id);

  const node = cm._requireNode('ch-recovery-2');
  const summary = cm._recoverDispatchRouter(node);
  // Only the in-flight one is rebuilt; the finished one is skipped.
  assert.equal(summary.recovered, 1);
  assert.equal(summary.scanned, 2);
  assert.ok(cm.dispatchRouter.lookup(inflight.correlation_id));
  assert.equal(cm.dispatchRouter.lookup(finished.correlation_id), null);
});

test('_recoverDispatchRouter: dispatch.start outside the recovery window is skipped', async (t) => {
  const ws = new FakeWsServer();
  ws.goOnline('device-A');
  const { cm } = await makeChannelManager(t, { ws });
  await cm.createChannel({
    channelId: 'ch-recovery-3',
    name: 'Recovery 3',
    type: 'xhs-creator',
    status: 'created',
    members: [
      { memberType: 'human', memberId: 'user-001' },
      { memberType: 'device', memberId: 'device-A' },
    ],
  });

  const send = await cm.deviceCommandSend({
    channel_id: 'ch-recovery-3',
    type: 'xhs.publish',
    params: { title: 't', content: 'body' },
  });
  cm.dispatchRouter.remove(send.correlation_id);

  const node = cm._requireNode('ch-recovery-3');
  // Use a tiny window with future nowMs so the existing dispatch.start falls
  // outside the recovery horizon.
  const summary = cm._recoverDispatchRouter(node, {
    windowMs: 1, // 1ms
    nowMs: Date.now() + 60_000,
  });
  assert.equal(summary.recovered, 0);
  assert.equal(cm.dispatchRouter.lookup(send.correlation_id), null);
});

// ── 3. End-to-end restart simulation ─────────────────────────────────────

test('end-to-end: ChannelManager.start() rebuilds router for a restart-mid-dispatch scenario', async (t) => {
  const baseDir = newBaseDir(t, 'cm-recovery-e2e-');
  const ws1 = new FakeWsServer();
  ws1.goOnline('device-A');
  const { cm: cm1 } = await makeChannelManager(t, { baseDir, ws: ws1 });
  await cm1.createChannel({
    channelId: 'ch-e2e',
    name: 'E2E recovery',
    type: 'xhs-creator',
    status: 'created',
    members: [
      { memberType: 'human', memberId: 'user-001' },
      { memberType: 'device', memberId: 'device-A' },
    ],
  });
  const send = await cm1.deviceCommandSend({
    channel_id: 'ch-e2e',
    type: 'xhs.publish',
    params: { title: 't', content: 'body' },
  });
  // Tear down channel store handle so the next ChannelManager opens fresh.
  await cm1.stopAll();

  // Fresh process: brand-new ChannelManager pointed at the same baseDir.
  const ws2 = new FakeWsServer();
  ws2.goOnline('device-A');
  const { cm: cm2 } = await makeChannelManager(t, { baseDir, ws: ws2 });
  await cm2.start();

  const route = cm2.dispatchRouter.lookup(send.correlation_id);
  assert.ok(route, 'router entry rebuilt by start()');
  assert.equal(route.channel_id, 'ch-e2e');
  assert.equal(route.device_id, 'device-A');

  // Now the late-arriving callback succeeds (vs. 404 correlation_unknown
  // pre-recovery).
  const cb = await cm2.deviceCallback({
    deviceId: 'device-A',
    correlationId: send.correlation_id,
    status: 'ok',
    result: { url: 'https://xhs.com/note/late', note_id: 'late' },
  });
  assert.equal(cb.payload_type, 'dispatch.completed');
  assert.equal(cb.correlation_id, send.correlation_id);
  // Router cleared after terminal.
  assert.equal(cm2.dispatchRouter.lookup(send.correlation_id), null);
  await cm2.stopAll();
});
