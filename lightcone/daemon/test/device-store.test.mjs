// DeviceStore — in-memory device api-key store with env+server union (T74 §1, §5).
//
// Coverage:
//   - 构造时可选 envEntries 作为 dev fallback
//   - replaceServer(entries) 全量替换 server-managed entries（T74 §3 reconnect re-pull）
//   - upsert/remove/update 单条增量同步（T74 §2 device.*-event handler 入口）
//   - verifyKey 校验 server-managed + env fallback 取并集；同 deviceId server 优先
//   - onRevoke 回调用于 ws-server 主动断开（T74 §2 device.revoked 已连 disconnect）
//   - snapshot 返回 deviceId-keyed 副本

import test from 'node:test';
import assert from 'node:assert/strict';

import { DeviceStore } from '../src/devices/device-store.js';

test('DeviceStore env-only constructor exposes verifyKey', () => {
  const store = new DeviceStore({ envEntries: new Map([['xhs-001', 'env-key']]) });
  assert.equal(store.verifyKey({ deviceId: 'xhs-001', key: 'env-key' }), true);
  assert.equal(store.verifyKey({ deviceId: 'xhs-001', key: 'wrong' }), false);
  assert.equal(store.verifyKey({ deviceId: 'unknown', key: 'env-key' }), false);
});

test('DeviceStore replaceServer overwrites previous server entries (re-pull semantics)', () => {
  const store = new DeviceStore();
  store.replaceServer([
    { device_id: 'a', api_key: 'ka' },
    { device_id: 'b', api_key: 'kb' },
  ]);
  assert.equal(store.verifyKey({ deviceId: 'a', key: 'ka' }), true);
  assert.equal(store.verifyKey({ deviceId: 'b', key: 'kb' }), true);

  store.replaceServer([
    { device_id: 'b', api_key: 'kb-new' },
    { device_id: 'c', api_key: 'kc' },
  ]);
  // a removed by replace
  assert.equal(store.verifyKey({ deviceId: 'a', key: 'ka' }), false);
  // b updated
  assert.equal(store.verifyKey({ deviceId: 'b', key: 'kb' }), false);
  assert.equal(store.verifyKey({ deviceId: 'b', key: 'kb-new' }), true);
  // c added
  assert.equal(store.verifyKey({ deviceId: 'c', key: 'kc' }), true);
});

test('DeviceStore upsert / remove / update single entry', () => {
  const store = new DeviceStore();
  store.upsert({ device_id: 'a', api_key: 'k1' });
  assert.equal(store.verifyKey({ deviceId: 'a', key: 'k1' }), true);

  store.update({ device_id: 'a', api_key: 'k2' });
  assert.equal(store.verifyKey({ deviceId: 'a', key: 'k1' }), false);
  assert.equal(store.verifyKey({ deviceId: 'a', key: 'k2' }), true);

  store.remove('a');
  assert.equal(store.verifyKey({ deviceId: 'a', key: 'k2' }), false);
});

test('DeviceStore env+server union — server overrides env on shared deviceId', () => {
  const store = new DeviceStore({ envEntries: new Map([['shared', 'env-key'], ['only-env', 'ek']]) });
  store.replaceServer([{ device_id: 'shared', api_key: 'srv-key' }, { device_id: 'only-srv', api_key: 'sk' }]);

  // only-env still works (server didn't touch it)
  assert.equal(store.verifyKey({ deviceId: 'only-env', key: 'ek' }), true);
  // only-srv recognized
  assert.equal(store.verifyKey({ deviceId: 'only-srv', key: 'sk' }), true);
  // shared: server-supplied key wins
  assert.equal(store.verifyKey({ deviceId: 'shared', key: 'srv-key' }), true);
  assert.equal(store.verifyKey({ deviceId: 'shared', key: 'env-key' }), false);
});

test('DeviceStore.remove triggers onRevoke callback with deviceId', () => {
  const revoked = [];
  const store = new DeviceStore({ onRevoke: (id) => revoked.push(id) });
  store.upsert({ device_id: 'a', api_key: 'k' });
  store.remove('a');
  assert.deepEqual(revoked, ['a']);

  // remove on unknown id is silent (idempotent)
  store.remove('ghost');
  assert.deepEqual(revoked, ['a']);
});

test('DeviceStore.replaceServer fires onRevoke for entries dropped from server set', () => {
  const revoked = [];
  const store = new DeviceStore({ onRevoke: (id) => revoked.push(id) });
  store.replaceServer([{ device_id: 'a', api_key: 'ka' }, { device_id: 'b', api_key: 'kb' }]);
  store.replaceServer([{ device_id: 'a', api_key: 'ka' }]);
  assert.deepEqual(revoked, ['b']);
});

test('DeviceStore.snapshot returns plain object map keyed by device_id', () => {
  const store = new DeviceStore({ envEntries: new Map([['env1', 'ek']]) });
  store.replaceServer([{ device_id: 'srv1', api_key: 'sk', user_id: 'u1' }]);
  const snap = store.snapshot();
  assert.equal(snap.size, 2);
  assert.equal(snap.get('env1').source, 'env');
  assert.equal(snap.get('srv1').source, 'server');
  assert.equal(snap.get('srv1').user_id, 'u1');
});

test('DeviceStore.size reports total unique entries', () => {
  const store = new DeviceStore({ envEntries: new Map([['a', 'k']]) });
  assert.equal(store.size(), 1);
  store.replaceServer([{ device_id: 'b', api_key: 'kb' }]);
  assert.equal(store.size(), 2);
  store.replaceServer([{ device_id: 'a', api_key: 'srv-a' }]);
  // env 'a' is shadowed by server 'a' — total still 1 distinct id from server side, plus env-only ids
  // distinct deviceIds = {a (server), } → 1
  assert.equal(store.size(), 1);
});

// ── T78 (M1.2-FIX-C): server-revoke authoritative tombstone ─────────────────
// Without these guards, env fallback would silently re-authenticate a deviceId
// the server has already revoked — see DeviceStore header note for full
// rationale. Each test below pins one branch of the new verifyKey() logic.

test('T78: remove() tombstones server-managed id; env fallback for same id is rejected', () => {
  // Operator seeds env (dev fallback) with device-A:key-X; server pulls
  // device-A:key-Y; then revokes device-A. The original env key MUST NOT
  // re-authenticate after revoke.
  const revoked = [];
  const store = new DeviceStore({
    envEntries: new Map([['device-A', 'key-X']]),
    onRevoke: (id) => revoked.push(id),
  });
  store.upsert({ device_id: 'device-A', api_key: 'key-Y' });
  // sanity: server key works, env key shadowed
  assert.equal(store.verifyKey({ deviceId: 'device-A', key: 'key-Y' }), true);
  assert.equal(store.verifyKey({ deviceId: 'device-A', key: 'key-X' }), false);

  store.remove('device-A');
  // After revoke: neither key works. Env fallback explicitly disabled.
  assert.equal(store.verifyKey({ deviceId: 'device-A', key: 'key-Y' }), false);
  assert.equal(store.verifyKey({ deviceId: 'device-A', key: 'key-X' }), false);
  assert.deepEqual(revoked, ['device-A']);
  assert.equal(store.revokedServerIds.has('device-A'), true);
  assert.equal(store.serverManagedIds.has('device-A'), true);
});

test('T78: replaceServer() drop diff tombstones; server-managed id never falls back to env', () => {
  // Same scenario via replaceServer (reconnect re-pull dropping a device).
  const revoked = [];
  const store = new DeviceStore({
    envEntries: new Map([['device-A', 'key-X']]),
    onRevoke: (id) => revoked.push(id),
  });
  store.replaceServer([{ device_id: 'device-A', api_key: 'key-Y' }]);
  assert.equal(store.verifyKey({ deviceId: 'device-A', key: 'key-Y' }), true);
  assert.equal(store.verifyKey({ deviceId: 'device-A', key: 'key-X' }), false);

  // Re-pull no longer includes device-A (server-side revoke between syncs).
  store.replaceServer([]);
  assert.equal(store.verifyKey({ deviceId: 'device-A', key: 'key-Y' }), false);
  assert.equal(store.verifyKey({ deviceId: 'device-A', key: 'key-X' }), false);
  assert.deepEqual(revoked, ['device-A']);
  assert.equal(store.revokedServerIds.has('device-A'), true);
});

test('T78: re-issuing a tombstoned id via upsert lifts the tombstone', () => {
  // Operator revoke + re-create same device_id flow: the new server key must
  // verify, env still must not.
  const store = new DeviceStore({ envEntries: new Map([['device-A', 'key-X']]) });
  store.upsert({ device_id: 'device-A', api_key: 'key-Y' });
  store.remove('device-A');
  assert.equal(store.revokedServerIds.has('device-A'), true);

  store.upsert({ device_id: 'device-A', api_key: 'key-Z' });
  assert.equal(store.revokedServerIds.has('device-A'), false, 'tombstone must clear on re-issue');
  assert.equal(store.verifyKey({ deviceId: 'device-A', key: 'key-Z' }), true);
  // Old server key + env key still rejected.
  assert.equal(store.verifyKey({ deviceId: 'device-A', key: 'key-Y' }), false);
  assert.equal(store.verifyKey({ deviceId: 'device-A', key: 'key-X' }), false);
});

test('T78: re-issuing a tombstoned id via replaceServer lifts the tombstone', () => {
  // Same as above but via the bulk re-pull path (reconnect re-creates device).
  const store = new DeviceStore({ envEntries: new Map([['device-A', 'key-X']]) });
  store.replaceServer([{ device_id: 'device-A', api_key: 'key-Y' }]);
  store.replaceServer([]); // drop → tombstoned
  assert.equal(store.revokedServerIds.has('device-A'), true);

  store.replaceServer([{ device_id: 'device-A', api_key: 'key-Z' }]);
  assert.equal(store.revokedServerIds.has('device-A'), false);
  assert.equal(store.verifyKey({ deviceId: 'device-A', key: 'key-Z' }), true);
  assert.equal(store.verifyKey({ deviceId: 'device-A', key: 'key-X' }), false);
});

test('T78: env-only ids (server never touched) keep working — fallback only blocked for server-managed ids', () => {
  // Regression guard: tombstone semantics only apply to ids the server has
  // actually managed. Pure env entries must continue to verify.
  const store = new DeviceStore({
    envEntries: new Map([
      ['env-only', 'env-key'],
      ['shared', 'env-shared'],
    ]),
  });
  // server only manages 'shared' (with a different key); 'env-only' remains
  // pure env.
  store.upsert({ device_id: 'shared', api_key: 'srv-shared' });
  assert.equal(store.verifyKey({ deviceId: 'env-only', key: 'env-key' }), true);
  assert.equal(store.verifyKey({ deviceId: 'shared', key: 'srv-shared' }), true);
  assert.equal(store.verifyKey({ deviceId: 'shared', key: 'env-shared' }), false);

  // Revoke 'shared': env-only still fine, shared fully tombstoned.
  store.remove('shared');
  assert.equal(store.verifyKey({ deviceId: 'env-only', key: 'env-key' }), true);
  assert.equal(store.verifyKey({ deviceId: 'shared', key: 'env-shared' }), false);
  assert.equal(store.verifyKey({ deviceId: 'shared', key: 'srv-shared' }), false);
});

test('T78: snapshot omits env entries shadowed by server authority (active or revoked)', () => {
  // Snapshot should reflect what verifyKey() would accept — env entries for
  // any id the server has managed are shadowed entirely.
  const store = new DeviceStore({
    envEntries: new Map([
      ['env-only', 'ek'],
      ['shared', 'env-shared'],
    ]),
  });
  store.upsert({ device_id: 'shared', api_key: 'srv-shared' });
  let snap = store.snapshot();
  assert.equal(snap.size, 2);
  assert.equal(snap.get('env-only').source, 'env');
  assert.equal(snap.get('shared').source, 'server');

  store.remove('shared');
  snap = store.snapshot();
  // 'shared' has no server entry and env is shadowed → omitted entirely.
  assert.equal(snap.has('shared'), false);
  assert.equal(snap.get('env-only').source, 'env');
  assert.equal(snap.size, 1);
});
