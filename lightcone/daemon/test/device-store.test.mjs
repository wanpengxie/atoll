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
