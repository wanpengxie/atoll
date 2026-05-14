import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { once } from 'node:events';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:http';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';

import { existingKeyLooksRegistered } from './register-machine.mjs';

const execFileAsync = promisify(execFile);
const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

function tempDir(t) {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'register-machine-test-'));
  t.after(() => {
    rmSync(dir, { recursive: true, force: true });
  });
  return dir;
}

function writeJson(res, statusCode, body) {
  res.statusCode = statusCode;
  res.setHeader('content-type', 'application/json');
  res.end(JSON.stringify(body));
}

async function closeServer(server) {
  await new Promise((resolve, reject) => {
    server.close((err) => {
      if (err) reject(err);
      else resolve();
    });
  });
}

async function withTcpServer(handler, run) {
  const server = createServer(handler);
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const { port } = server.address();
  try {
    await run(`http://127.0.0.1:${port}`);
  } finally {
    await closeServer(server);
  }
}

async function closedTcpServerUrl() {
  const server = createServer((_req, res) => res.end());
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const { port } = server.address();
  await closeServer(server);
  return `http://127.0.0.1:${port}`;
}

async function withSocketServer(socketPath, handler, run) {
  const server = createServer(handler);
  server.listen(socketPath);
  await once(server, 'listening');
  try {
    await run();
  } finally {
    await closeServer(server);
  }
}

async function runRegisterMachine(env, options = {}) {
  return await execFileAsync(process.execPath, ['ops/register-machine.mjs'], {
    cwd: repoRoot,
    env: {
      ...process.env,
      ...env,
    },
    ...options,
  });
}

async function expectRegisterMachineFailure(env, options = {}) {
  let failure;
  try {
    await runRegisterMachine(env, options);
  } catch (err) {
    failure = err;
  }
  assert.ok(failure, 'register-machine should fail');
  return failure;
}

test('existingKeyLooksRegistered rejects whoami key from another server', async () => {
  await withTcpServer((req, res) => {
    assert.equal(req.url, '/api/machines/whoami');
    assert.equal(req.headers.authorization, 'Bearer sk_machine_valid');
    writeJson(res, 200, {
      key_valid: true,
      server_id: 'server-b',
      machine_id: 'machine-from-other-server',
    });
  }, async (baseUrl) => {
    const result = await existingKeyLooksRegistered({
      SERVER_URL: baseUrl,
      DEFAULT_SERVER_ID: 'server-a',
    }, 'sk_machine_valid');

    assert.deepEqual(result, { valid: false, via: null, reason: 'invalid_authoritative' });
  });
});

test('existingKeyLooksRegistered accepts whoami key from target server', async () => {
  await withTcpServer((_req, res) => {
    writeJson(res, 200, {
      key_valid: true,
      server_id: 'server-a',
    });
  }, async (baseUrl) => {
    const result = await existingKeyLooksRegistered({
      SERVER_URL: baseUrl,
      DEFAULT_SERVER_ID: 'server-a',
    }, 'sk_machine_valid');

    assert.deepEqual(result, { valid: true, via: '/api/machines/whoami' });
  });
});

test('existingKeyLooksRegistered ignores local daemon and validates only via server whoami', async (t) => {
  const dir = tempDir(t);
  const socketPath = path.join(dir, 'daemon.sock');
  let daemonRequests = 0;

  await withTcpServer((req, res) => {
    assert.equal(req.url, '/api/machines/whoami');
    assert.equal(req.headers.authorization, 'Bearer sk_machine_valid');
    writeJson(res, 401, { error: 'Invalid machine API key' });
  }, async (baseUrl) => {
    await withSocketServer(socketPath, (req, res) => {
      daemonRequests += 1;
      writeJson(res, 200, {
        ok: true,
        server_url: baseUrl,
        project_key: 'project-a',
      });
    }, async () => {
      const result = await existingKeyLooksRegistered({
        SERVER_URL: baseUrl,
        DEFAULT_SERVER_ID: 'server-a',
        COAGENT_DAEMON_SOCKET: socketPath,
        COAGENT_PROJECT_KEY: 'project-a',
      }, 'sk_machine_valid');

      assert.deepEqual(result, { valid: false, via: null, reason: 'invalid_authoritative' });
      assert.equal(daemonRequests, 0);
    });
  });
});

test('register-machine CLI skips existing key when whoami matches target server without ADMIN_TOKEN', async (t) => {
  const dir = tempDir(t);
  const keyPath = path.join(dir, 'machine.key');
  writeFileSync(keyPath, 'sk_machine_valid\n', { mode: 0o600 });

  await withTcpServer((_req, res) => {
    writeJson(res, 200, {
      key_valid: true,
      server_id: 'server-a',
    });
  }, async (baseUrl) => {
    const { stdout, stderr } = await runRegisterMachine({
      SERVER_URL: baseUrl,
      DEFAULT_SERVER_ID: 'server-a',
      COAGENT_PROJECT_KEY: 'project-a',
      COAGENT_MACHINE_KEY_PATH: keyPath,
      ADMIN_TOKEN: '',
    });

    assert.equal(stderr, '');
    const body = JSON.parse(stdout);
    assert.equal(body.ok, true);
    assert.equal(body.skipped, true);
    assert.equal(body.reason, 'already registered, key valid via /api/machines/whoami');
    assert.equal(body.keyPath, keyPath);
    assert.equal(body.projectKey, 'project-a');
    assert.equal(Object.hasOwn(body, 'machineId'), false);
    assert.equal(readFileSync(keyPath, 'utf8'), 'sk_machine_valid\n');
  });
});

test('register-machine CLI keeps existing key when whoami is unreachable without ADMIN_TOKEN', async (t) => {
  const dir = tempDir(t);
  const keyPath = path.join(dir, 'machine.key');
  const originalKey = 'sk_machine_existing\n';
  writeFileSync(keyPath, originalKey, { mode: 0o600 });
  const serverUrl = await closedTcpServerUrl();

  const failure = await expectRegisterMachineFailure({
    SERVER_URL: serverUrl,
    DEFAULT_SERVER_ID: 'server-a',
    COAGENT_PROJECT_KEY: 'project-a',
    COAGENT_MACHINE_KEY_PATH: keyPath,
    ADMIN_TOKEN: '',
    REGISTER_MACHINE_TIMEOUT_MS: '50',
  }, { timeout: 2000 });

  assert.equal(failure.code, 1);
  const body = JSON.parse(failure.stderr);
  assert.equal(body.ok, false);
  assert.match(body.error, /cannot validate existing machine key/);
  assert.match(body.error, /keeping .*machine\.key intact/);
  assert.match(body.error, /retry once server is reachable/);
  assert.equal(existsSync(keyPath), true);
  assert.equal(readFileSync(keyPath, 'utf8'), originalKey);
});

test('register-machine CLI keeps existing key when whoami returns 5xx without ADMIN_TOKEN', async (t) => {
  const dir = tempDir(t);
  const keyPath = path.join(dir, 'machine.key');
  const originalKey = 'sk_machine_existing\n';
  const requests = [];
  writeFileSync(keyPath, originalKey, { mode: 0o600 });

  await withTcpServer((req, res) => {
    requests.push({ method: req.method, url: req.url });
    assert.equal(req.url, '/api/machines/whoami');
    writeJson(res, 503, { error: 'temporarily unavailable' });
  }, async (baseUrl) => {
    const failure = await expectRegisterMachineFailure({
      SERVER_URL: baseUrl,
      DEFAULT_SERVER_ID: 'server-a',
      COAGENT_PROJECT_KEY: 'project-a',
      COAGENT_MACHINE_KEY_PATH: keyPath,
      ADMIN_TOKEN: '',
    });

    assert.equal(failure.code, 1);
    const body = JSON.parse(failure.stderr);
    assert.equal(body.ok, false);
    assert.match(body.error, /cannot validate existing machine key/);
    assert.match(body.error, /503/);
    assert.match(body.error, /retry once server is reachable/);
    assert.equal(existsSync(keyPath), true);
    assert.equal(readFileSync(keyPath, 'utf8'), originalKey);
    assert.deepEqual(requests.map((request) => request.url), ['/api/machines/whoami']);
  });
});

test('register-machine CLI deletes existing key when whoami returns 401 without ADMIN_TOKEN', async (t) => {
  const dir = tempDir(t);
  const keyPath = path.join(dir, 'machine.key');
  const requests = [];
  writeFileSync(keyPath, 'sk_machine_stale\n', { mode: 0o600 });

  await withTcpServer((req, res) => {
    requests.push({ method: req.method, url: req.url });
    assert.equal(req.url, '/api/machines/whoami');
    writeJson(res, 401, { error: 'Invalid machine API key' });
  }, async (baseUrl) => {
    const failure = await expectRegisterMachineFailure({
      SERVER_URL: baseUrl,
      DEFAULT_SERVER_ID: 'server-a',
      COAGENT_PROJECT_KEY: 'project-a',
      COAGENT_MACHINE_KEY_PATH: keyPath,
      ADMIN_TOKEN: '',
    });

    assert.equal(failure.code, 1);
    const body = JSON.parse(failure.stderr);
    assert.equal(body.ok, false);
    assert.equal(body.error, 'ADMIN_TOKEN is required in .env before registering a machine');
    assert.equal(existsSync(keyPath), false);
    assert.deepEqual(requests.map((request) => request.url), ['/api/machines/whoami']);
  });
});

test('register-machine CLI deletes stale key and registers with ADMIN_TOKEN', async (t) => {
  const dir = tempDir(t);
  const keyPath = path.join(dir, 'machine.key');
  writeFileSync(keyPath, 'sk_machine_stale\n', { mode: 0o600 });

  const requests = [];
  await withTcpServer((req, res) => {
    requests.push({ method: req.method, url: req.url, authorization: req.headers.authorization });
    if (req.url === '/api/machines/whoami') {
      writeJson(res, 401, { error: 'Invalid machine API key' });
      return;
    }
    assert.equal(req.method, 'POST');
    assert.equal(req.url, '/api/servers/server-a/machines');
    assert.equal(req.headers.authorization, 'Bearer admin-secret');
    writeJson(res, 200, {
      id: 'machine-new',
      apiKey: 'sk_machine_new_valid',
      apiKeyPrefix: 'sk_machine_new',
    });
  }, async (baseUrl) => {
    const { stdout, stderr } = await runRegisterMachine({
      SERVER_URL: baseUrl,
      DEFAULT_SERVER_ID: 'server-a',
      COAGENT_PROJECT_KEY: 'project-a',
      COAGENT_MACHINE_KEY_PATH: keyPath,
      ADMIN_TOKEN: 'admin-secret',
    });

    assert.equal(stderr, '');
    const body = JSON.parse(stdout);
    assert.equal(body.ok, true);
    assert.equal(body.skipped, false);
    assert.equal(body.machineId, 'machine-new');
    assert.equal(readFileSync(keyPath, 'utf8'), 'sk_machine_new_valid\n');
    assert.deepEqual(requests.map((request) => request.url), [
      '/api/machines/whoami',
      '/api/servers/server-a/machines',
    ]);
  });
});

test('register-machine CLI times out registration request with clear SERVER_URL error', async (t) => {
  const dir = tempDir(t);
  const keyPath = path.join(dir, 'machine.key');

  await withTcpServer((_req, _res) => {
    // Intentionally keep the request open until AbortSignal.timeout aborts fetch.
  }, async (baseUrl) => {
    const failure = await expectRegisterMachineFailure({
      SERVER_URL: baseUrl,
      DEFAULT_SERVER_ID: 'server-a',
      COAGENT_PROJECT_KEY: 'project-a',
      COAGENT_MACHINE_KEY_PATH: keyPath,
      ADMIN_TOKEN: 'admin-secret',
      REGISTER_MACHINE_TIMEOUT_MS: '50',
    }, {
      timeout: 2000,
    });

    assert.equal(failure.code, 1);
    const body = JSON.parse(failure.stderr);
    assert.equal(body.ok, false);
    assert.match(body.error, /注册请求 0\.05 秒超时/);
    assert.match(body.error, new RegExp(`SERVER_URL=${baseUrl.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}`));
    assert.equal(existsSync(keyPath), false);
  });
});
