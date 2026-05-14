// Tests for cli/bin/xhs shim. We import the shim module (it auto-detects CLI
// invocation and only runs when executed directly), then exercise its
// exported helpers + runShim with a fake spawn impl.

import assert from 'node:assert/strict';
import { chmodSync, existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

const shimPath = path.resolve(new URL('../bin/xhs', import.meta.url).pathname);
const shim = await import(shimPath);
const {
  COAGENT_XHS_BINARY,
  inferDefaultBackend,
  buildSpawnEnv,
  resolveCoagentXhsBinary,
  runShim,
  findOnPath,
} = shim;

function makeFakeBinary(dir, name = COAGENT_XHS_BINARY) {
  mkdirSync(dir, { recursive: true });
  const target = path.join(dir, name);
  writeFileSync(target, '#!/bin/sh\nexit 0\n');
  chmodSync(target, 0o755);
  return target;
}

test('inferDefaultBackend honors explicit env then falls back to daemon presence', () => {
  assert.equal(inferDefaultBackend({ COAGENT_XHS_BACKEND: 'mock' }), 'mock');
  assert.equal(inferDefaultBackend({ COAGENT_DAEMON_HTTP: 'http://x' }), 'real');
  assert.equal(inferDefaultBackend({}), 'mock');
});

test('buildSpawnEnv injects COAGENT_XHS_BACKEND without clobbering existing', () => {
  const out = buildSpawnEnv({ FOO: 'bar', COAGENT_XHS_BACKEND: 'real' });
  assert.equal(out.FOO, 'bar');
  assert.equal(out.COAGENT_XHS_BACKEND, 'real');

  const out2 = buildSpawnEnv({ COAGENT_DAEMON_HTTP: 'http://x' });
  assert.equal(out2.COAGENT_XHS_BACKEND, 'real');
});

test('resolveCoagentXhsBinary picks up COAGENT_XHS_BIN absolute path', () => {
  const tmp = mkdtempSync(path.join(os.tmpdir(), 'xhs-shim-'));
  try {
    const bin = makeFakeBinary(tmp);
    const r = resolveCoagentXhsBinary({
      binDir: '/nowhere',
      env: { COAGENT_XHS_BIN: bin },
    });
    assert.equal(r.path, bin);
    assert.equal(r.source, 'env_absolute');
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
});

test('resolveCoagentXhsBinary returns missing when env path absent', () => {
  const r = resolveCoagentXhsBinary({
    binDir: '/nowhere',
    env: { COAGENT_XHS_BIN: '/does-not-exist/coagent-xhs' },
  });
  assert.equal(r.path, null);
  assert.equal(r.source, 'env_absolute');
  assert.equal(r.missing, '/does-not-exist/coagent-xhs');
});

test('resolveCoagentXhsBinary prefers <repo>/xhs-cli/bin when present', () => {
  const tmp = mkdtempSync(path.join(os.tmpdir(), 'xhs-shim-repo-'));
  try {
    // simulate repo layout: <tmp>/cli/bin (binDir) → <tmp>/xhs-cli/bin/coagent-xhs
    const binDir = path.join(tmp, 'cli', 'bin');
    mkdirSync(binDir, { recursive: true });
    const xhsBinDir = path.join(tmp, 'xhs-cli', 'bin');
    const bin = makeFakeBinary(xhsBinDir);
    const r = resolveCoagentXhsBinary({ binDir, env: {} });
    assert.equal(r.path, bin);
    assert.equal(r.source, 'repo_bin');
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
});

test('resolveCoagentXhsBinary falls back to PATH', () => {
  const tmp = mkdtempSync(path.join(os.tmpdir(), 'xhs-shim-path-'));
  try {
    const bin = makeFakeBinary(tmp);
    const r = resolveCoagentXhsBinary({
      binDir: path.join(tmp, 'cli', 'bin'), // no repo bin here
      env: { PATH: tmp },
    });
    assert.equal(r.path, bin);
    assert.equal(r.source, 'path');
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
});

test('resolveCoagentXhsBinary reports not_found when nothing matches', () => {
  const tmp = mkdtempSync(path.join(os.tmpdir(), 'xhs-shim-none-'));
  try {
    const r = resolveCoagentXhsBinary({
      binDir: path.join(tmp, 'cli', 'bin'),
      env: { PATH: '/no/such/dir' },
    });
    assert.equal(r.path, null);
    assert.equal(r.source, 'not_found');
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
});

test('findOnPath resolves binary in custom PATH', () => {
  const tmp = mkdtempSync(path.join(os.tmpdir(), 'xhs-shim-findpath-'));
  try {
    const bin = makeFakeBinary(tmp, 'custom-bin');
    const found = findOnPath('custom-bin', { PATH: tmp });
    assert.equal(found, bin);
    assert.equal(findOnPath('nope', { PATH: tmp }), null);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
});

test('runShim with COAGENT_XHS_PROVIDER=ts-mock invokes runXhs fallback', async (t) => {
  // Patch process.exit to prevent the test runner from being killed and capture exit codes.
  const originalExit = process.exit;
  let exitCode;
  process.exit = (code = 0) => { exitCode = code; throw new Error(`__exit_${code}__`); };
  t.after(() => { process.exit = originalExit; });

  // We need a stub dist/index.js. Use a tmp binDir whose ../dist/index.js
  // resolves to a written file exporting runXhs.
  const tmp = mkdtempSync(path.join(os.tmpdir(), 'xhs-shim-tsmock-'));
  try {
    const binDir = path.join(tmp, 'bin');
    mkdirSync(binDir, { recursive: true });
    mkdirSync(path.join(tmp, 'dist'), { recursive: true });
    const distPath = path.join(tmp, 'dist', 'index.js');
    writeFileSync(distPath, `
      export const runXhs = async (args) => {
        process.stdout.write(JSON.stringify({ ok: true, args }));
      };
    `);

    let captured = '';
    const originalWrite = process.stdout.write.bind(process.stdout);
    process.stdout.write = (chunk) => { captured += String(chunk); return true; };
    t.after(() => { process.stdout.write = originalWrite; });

    await runShim({
      binDir,
      args: ['publish', '--title', 't'],
      env: { COAGENT_XHS_PROVIDER: 'ts-mock' },
      spawnImpl: () => { throw new Error('spawnImpl should not be called in ts-mock path'); },
    });
    // runShim should NOT call process.exit in the happy ts-mock path, so we
    // shouldn't have thrown. exitCode should be undefined.
    assert.equal(exitCode, undefined);
    const parsed = JSON.parse(captured);
    assert.equal(parsed.ok, true);
    assert.deepEqual(parsed.args, ['publish', '--title', 't']);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
});

test('runShim spawn happy path passes through args + injects backend', async (t) => {
  const originalExit = process.exit;
  let exitCode;
  process.exit = (code = 0) => { exitCode = code; throw new Error(`__exit_${code}__`); };
  t.after(() => { process.exit = originalExit; });

  const tmp = mkdtempSync(path.join(os.tmpdir(), 'xhs-shim-spawn-'));
  try {
    const binDir = path.join(tmp, 'cli', 'bin');
    mkdirSync(binDir, { recursive: true });
    const xhsBinDir = path.join(tmp, 'xhs-cli', 'bin');
    const bin = makeFakeBinary(xhsBinDir);

    let spawnCall;
    const fakeSpawn = (cmd, args, opts) => {
      spawnCall = { cmd, args, opts };
      return { status: 0 };
    };

    await assert.rejects(runShim({
      binDir,
      args: ['publish', '--title', 'hi'],
      env: { PATH: '/usr/bin', COAGENT_DAEMON_HTTP: 'http://localhost:7070' },
      spawnImpl: fakeSpawn,
    }), /__exit_0__/);
    assert.equal(exitCode, 0);
    assert.equal(spawnCall.cmd, bin);
    assert.deepEqual(spawnCall.args, ['publish', '--title', 'hi']);
    assert.equal(spawnCall.opts.stdio, 'inherit');
    assert.equal(spawnCall.opts.env.COAGENT_XHS_BACKEND, 'real');
    assert.equal(spawnCall.opts.env.COAGENT_DAEMON_HTTP, 'http://localhost:7070');
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
});

test('runShim exits 1 with helpful stderr when binary missing', async (t) => {
  const originalExit = process.exit;
  let exitCode;
  process.exit = (code = 0) => { exitCode = code; throw new Error(`__exit_${code}__`); };
  t.after(() => { process.exit = originalExit; });

  let stderr = '';
  const originalErr = process.stderr.write.bind(process.stderr);
  process.stderr.write = (chunk) => { stderr += String(chunk); return true; };
  t.after(() => { process.stderr.write = originalErr; });

  const tmp = mkdtempSync(path.join(os.tmpdir(), 'xhs-shim-missing-'));
  try {
    await assert.rejects(runShim({
      binDir: path.join(tmp, 'cli', 'bin'),
      args: ['publish'],
      env: { PATH: '/no/such/dir' },
      spawnImpl: () => { throw new Error('should not spawn'); },
    }), /__exit_1__/);
    assert.equal(exitCode, 1);
    assert.match(stderr, /cannot locate coagent-xhs binary/);
    assert.match(stderr, /go build -o xhs-cli\/bin\/coagent-xhs/);
    assert.match(stderr, /COAGENT_XHS_PROVIDER=ts-mock/);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
});

test('runShim propagates child non-zero exit code', async (t) => {
  const originalExit = process.exit;
  let exitCode;
  process.exit = (code = 0) => { exitCode = code; throw new Error(`__exit_${code}__`); };
  t.after(() => { process.exit = originalExit; });

  const tmp = mkdtempSync(path.join(os.tmpdir(), 'xhs-shim-exit-'));
  try {
    const binDir = path.join(tmp, 'cli', 'bin');
    mkdirSync(binDir, { recursive: true });
    const xhsBinDir = path.join(tmp, 'xhs-cli', 'bin');
    makeFakeBinary(xhsBinDir);
    const fakeSpawn = () => ({ status: 7 });
    await assert.rejects(runShim({
      binDir,
      args: ['publish'],
      env: { PATH: '/usr/bin' },
      spawnImpl: fakeSpawn,
    }), /__exit_7__/);
    assert.equal(exitCode, 7);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
});
