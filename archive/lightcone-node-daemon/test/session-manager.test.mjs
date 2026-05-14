import assert from 'node:assert/strict';
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { SessionManager } from '../src/devices/session-manager.js';

function tmpBase() {
  return mkdtempSync(path.join(os.tmpdir(), 'coagent-session-'));
}

test('SessionManager.getSession returns null when file missing', () => {
  const baseDir = tmpBase();
  try {
    const sm = new SessionManager({ baseDir });
    assert.equal(sm.getSession('user-001'), null);
  } finally {
    rmSync(baseDir, { recursive: true, force: true });
  }
});

test('updateSession persists shallow merge with last_updated_at, atomic rename', () => {
  const baseDir = tmpBase();
  try {
    const sm = new SessionManager({ baseDir });
    const first = sm.updateSession('user-001', {
      cookies: [{ name: 'sid', value: 'abc', domain: '.xiaohongshu.com' }],
      login_state: 'logged_in',
      expires_at: 1234567890,
    });
    assert.equal(first.user_id, 'user-001');
    assert.equal(first.login_state, 'logged_in');
    assert.equal(first.cookies.length, 1);
    assert.equal(typeof first.last_updated_at, 'number');
    assert.equal(first.expires_at, 1234567890);

    const stored = JSON.parse(readFileSync(sm.sessionPath('user-001'), 'utf8'));
    assert.deepEqual(stored, first);

    // patch only login_state — cookies preserved.
    const second = sm.updateSession('user-001', { login_state: 'expired' });
    assert.equal(second.login_state, 'expired');
    assert.equal(second.cookies.length, 1, 'cookies preserved across merge');
    assert.ok(second.last_updated_at >= first.last_updated_at);

    const noTmp = readdirSync(sm.userDir('user-001')).filter((name) => name.includes('.tmp-'));
    assert.equal(noTmp.length, 0, 'no tmp files left behind');
  } finally {
    rmSync(baseDir, { recursive: true, force: true });
  }
});

test('updateSession defaults login_state to unknown when missing', () => {
  const baseDir = tmpBase();
  try {
    const sm = new SessionManager({ baseDir });
    const sess = sm.updateSession('user-002', { cookies: [] });
    assert.equal(sess.login_state, 'unknown');
  } finally {
    rmSync(baseDir, { recursive: true, force: true });
  }
});

test('updateSession rejects invalid cookies / expires_at', () => {
  const baseDir = tmpBase();
  try {
    const sm = new SessionManager({ baseDir });
    assert.throws(() => sm.updateSession('user-001', { cookies: 'not-an-array' }), /cookies must be an array/);
    assert.throws(() => sm.updateSession('user-001', { expires_at: 'soon' }), /expires_at must be a finite number/);
  } finally {
    rmSync(baseDir, { recursive: true, force: true });
  }
});

test('user_id pattern blocks path traversal', () => {
  const baseDir = tmpBase();
  try {
    const sm = new SessionManager({ baseDir });
    for (const bad of ['../escape', 'a/b', '..', '', '   ', 'has space', 'a:b', 'x'.repeat(65)]) {
      assert.throws(() => sm.updateSession(bad, { cookies: [] }), /invalid user_id/);
      assert.throws(() => sm.getSession(bad), /invalid user_id/);
    }
  } finally {
    rmSync(baseDir, { recursive: true, force: true });
  }
});

test('deleteSession removes file when present and returns false on missing', () => {
  const baseDir = tmpBase();
  try {
    const sm = new SessionManager({ baseDir });
    sm.updateSession('user-009', { cookies: [] });
    assert.ok(existsSync(sm.sessionPath('user-009')));
    assert.equal(sm.deleteSession('user-009'), true);
    assert.equal(existsSync(sm.sessionPath('user-009')), false);
    assert.equal(sm.deleteSession('user-009'), false);
  } finally {
    rmSync(baseDir, { recursive: true, force: true });
  }
});

test('getSession throws session_corrupted on malformed json', () => {
  const baseDir = tmpBase();
  try {
    const sm = new SessionManager({ baseDir });
    sm.updateSession('user-010', { cookies: [] });
    // simulate corruption
    writeFileSync(sm.sessionPath('user-010'), '{not json');
    assert.throws(() => sm.getSession('user-010'), (err) => err.code === 'session_corrupted');
  } finally {
    rmSync(baseDir, { recursive: true, force: true });
  }
});

// ── Fix-T2 §7: per-user dirs locked down 0o700 (claude-M6) ─

test('updateSession creates baseDir + per-user dirs with mode 0o700', () => {
  // mkdtempSync drops a temp dir at default umask, so use a fresh nested baseDir
  // we never created so the SessionManager is responsible for both levels.
  const root = mkdtempSync(path.join(os.tmpdir(), 'coagent-session-mode-'));
  const baseDir = path.join(root, 'users');
  try {
    const sm = new SessionManager({ baseDir });
    sm.updateSession('user-mode', { cookies: [] });
    const baseStat = statSync(baseDir);
    const userStat = statSync(sm.userDir('user-mode'));
    // mask & 0o777 to ignore S_IFDIR bits
    assert.equal((baseStat.mode & 0o777), 0o700, `baseDir mode=${(baseStat.mode & 0o777).toString(8)}, want 0o700`);
    assert.equal((userStat.mode & 0o777), 0o700, `userDir mode=${(userStat.mode & 0o777).toString(8)}, want 0o700`);
    // session file mode 0o600 is the existing contract — verify still holds.
    const fileStat = statSync(sm.sessionPath('user-mode'));
    assert.equal((fileStat.mode & 0o777), 0o600);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

// Round-2 review codex-#t57.2 / FX4 / R3-T2: pre-existing dirs at 0o755 (left
// by an older release) must be tightened to 0o700 on the next updateSession
// call. mkdirSync's `mode` only applies on creation, so a chmodSync follow-up
// is required for the upgrade path.
test('updateSession tightens pre-existing 0o755 baseDir + userDir to 0o700', () => {
  const root = mkdtempSync(path.join(os.tmpdir(), 'coagent-session-upgrade-'));
  const baseDir = path.join(root, 'users');
  const userId = 'user-upgrade';
  const userDir = path.join(baseDir, userId);
  try {
    // Simulate an older release's footprint: both dirs already exist at 0o755.
    mkdirSync(baseDir, { recursive: true, mode: 0o755 });
    mkdirSync(userDir, { recursive: true, mode: 0o755 });
    // umask can mask the mkdir mode bits, so chmod explicitly to 0o755 to make
    // the regression starting point deterministic.
    chmodSync(baseDir, 0o755);
    chmodSync(userDir, 0o755);
    assert.equal((statSync(baseDir).mode & 0o777), 0o755, 'baseDir starts at 0o755');
    assert.equal((statSync(userDir).mode & 0o777), 0o755, 'userDir starts at 0o755');

    const sm = new SessionManager({ baseDir });
    sm.updateSession(userId, { cookies: [] });

    const baseStat = statSync(baseDir);
    const userStat = statSync(userDir);
    assert.equal(
      (baseStat.mode & 0o777),
      0o700,
      `baseDir mode=${(baseStat.mode & 0o777).toString(8)}, want 0o700 after upgrade`,
    );
    assert.equal(
      (userStat.mode & 0o777),
      0o700,
      `userDir mode=${(userStat.mode & 0o777).toString(8)}, want 0o700 after upgrade`,
    );
    const fileStat = statSync(sm.sessionPath(userId));
    assert.equal(
      (fileStat.mode & 0o777),
      0o600,
      `session file mode=${(fileStat.mode & 0o777).toString(8)}, want 0o600`,
    );
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

// ── Extra defensive cases for assertUserId ─

test('assertUserId rejects exactly 65 chars but accepts 64', () => {
  const sm = new SessionManager({ baseDir: tmpBase() });
  const sixtyFour = 'a'.repeat(64);
  const sixtyFive = 'a'.repeat(65);
  assert.equal(SessionManager.assertUserId(sixtyFour), sixtyFour);
  assert.throws(() => SessionManager.assertUserId(sixtyFive), /invalid user_id/);
});

test('assertUserId rejects directory-like strings', () => {
  const sm = new SessionManager({ baseDir: tmpBase() });
  for (const bad of ['../escape', './self', '..', '.', 'a/b', 'a\\b', 'a b', '#anchor', '?q=1']) {
    assert.throws(() => sm.userDir(bad), /invalid user_id/);
  }
});
