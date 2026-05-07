// Unit tests for DispatchRouter (M1.1 Fix-T2 §8 / round-1 review codex#14).
// Coverage: register/lookup/remove, TTL expiry on lookup, idempotent remove,
// custom now/ttl injection, lazy sweep beyond size threshold.

import assert from 'node:assert/strict';
import test from 'node:test';

import { DispatchRouter } from '../src/devices/dispatch-router.js';

test('register + lookup returns a copy with channel/device/user fields', () => {
  const r = new DispatchRouter();
  r.register({
    correlationId: 'corr-1',
    channelId: 'ch-1',
    deviceId: 'dev-1',
    userId: 'user-1',
  });
  const got = r.lookup('corr-1');
  assert.deepEqual(
    Object.keys(got).sort(),
    ['channel_id', 'device_id', 'expires_at', 'user_id'],
  );
  assert.equal(got.channel_id, 'ch-1');
  assert.equal(got.device_id, 'dev-1');
  assert.equal(got.user_id, 'user-1');
  // mutating the returned snapshot must not poison internal state.
  got.channel_id = 'mutated';
  const second = r.lookup('corr-1');
  assert.equal(second.channel_id, 'ch-1');
});

test('register requires non-empty correlationId', () => {
  const r = new DispatchRouter();
  assert.throws(() => r.register({ correlationId: '', channelId: 'c', deviceId: 'd', userId: 'u' }), /correlationId required/);
  assert.throws(() => r.register({ correlationId: '   ', channelId: 'c', deviceId: 'd', userId: 'u' }), /correlationId required/);
});

test('lookup returns null for unknown / blank correlation id', () => {
  const r = new DispatchRouter();
  assert.equal(r.lookup('missing'), null);
  assert.equal(r.lookup(''), null);
  assert.equal(r.lookup(null), null);
  assert.equal(r.lookup(undefined), null);
});

test('lookup deletes expired entries (TTL elapsed)', () => {
  let now = 1_000_000;
  const r = new DispatchRouter({ ttlMs: 100, now: () => now });
  r.register({ correlationId: 'c1', channelId: 'ch', deviceId: 'd', userId: 'u' });
  assert.ok(r.lookup('c1'));
  now += 200; // past TTL
  assert.equal(r.lookup('c1'), null);
  assert.equal(r.size(), 0, 'expired entry must be evicted on lookup');
});

test('per-call ttlMs overrides default', () => {
  let now = 1_000_000;
  const r = new DispatchRouter({ ttlMs: 100, now: () => now });
  r.register({ correlationId: 'c1', channelId: 'ch', deviceId: 'd', userId: 'u', ttlMs: 1000 });
  now += 500; // beyond default ttl, within override
  assert.ok(r.lookup('c1'));
  now += 600; // beyond override
  assert.equal(r.lookup('c1'), null);
});

test('remove is idempotent — true once, false thereafter', () => {
  const r = new DispatchRouter();
  r.register({ correlationId: 'c2', channelId: 'ch', deviceId: 'd', userId: 'u' });
  assert.equal(r.size(), 1);
  assert.equal(r.remove('c2'), true);
  assert.equal(r.size(), 0);
  assert.equal(r.remove('c2'), false);
  assert.equal(r.remove('never-existed'), false);
});

test('register triggers lazy sweep when size >= 256 entries', () => {
  let now = 1_000_000;
  const r = new DispatchRouter({ ttlMs: 50, now: () => now });
  // Insert ~260 entries
  for (let i = 0; i < 260; i++) {
    r.register({ correlationId: `c-${i}`, channelId: 'ch', deviceId: 'd', userId: 'u' });
  }
  assert.equal(r.size(), 260);
  now += 1000; // expire all but the next-registered entry
  // Trigger sweep via a fresh register — sweep runs after set, so the new entry survives.
  r.register({ correlationId: 'fresh', channelId: 'ch', deviceId: 'd', userId: 'u' });
  assert.equal(r.size(), 1);
  assert.ok(r.lookup('fresh'));
});

test('register replaces an existing entry for the same correlationId', () => {
  const r = new DispatchRouter();
  r.register({ correlationId: 'corr', channelId: 'ch1', deviceId: 'd1', userId: 'u1' });
  r.register({ correlationId: 'corr', channelId: 'ch2', deviceId: 'd2', userId: 'u2' });
  const got = r.lookup('corr');
  assert.equal(got.channel_id, 'ch2');
  assert.equal(got.device_id, 'd2');
  assert.equal(got.user_id, 'u2');
});
