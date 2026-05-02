import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { once } from 'node:events';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
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

test('existingKeyLooksRegistered rejects whoami key from another server', async (t) => {
  const dir = tempDir(t);
  const missingSocket = path.join(dir, 'missing.sock');

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
      COAGENT_DAEMON_SOCKET: missingSocket,
      COAGENT_PROJECT_KEY: 'project-a',
    }, 'sk_machine_valid');

    assert.deepEqual(result, { valid: false, via: null });
  });
});

test('existingKeyLooksRegistered accepts whoami key from target server', async (t) => {
  const dir = tempDir(t);
  const missingSocket = path.join(dir, 'missing.sock');

  await withTcpServer((_req, res) => {
    writeJson(res, 200, {
      key_valid: true,
      server_id: 'server-a',
    });
  }, async (baseUrl) => {
    const result = await existingKeyLooksRegistered({
      SERVER_URL: baseUrl,
      DEFAULT_SERVER_ID: 'server-a',
      COAGENT_DAEMON_SOCKET: missingSocket,
      COAGENT_PROJECT_KEY: 'project-a',
    }, 'sk_machine_valid');

    assert.deepEqual(result, { valid: true, via: '/api/machines/whoami' });
  });
});

test('existingKeyLooksRegistered accepts matching daemon admin status', async (t) => {
  const dir = tempDir(t);
  const socketPath = path.join(dir, 'daemon.sock');

  await withSocketServer(socketPath, (req, res) => {
    assert.equal(req.url, '/admin/status');
    assert.equal(req.headers.authorization, 'Bearer sk_machine_valid');
    writeJson(res, 200, {
      ok: true,
      server_url: 'http://server.local/',
      project_key: 'project-a',
    });
  }, async () => {
    const result = await existingKeyLooksRegistered({
      SERVER_URL: 'http://server.local',
      DEFAULT_SERVER_ID: 'server-a',
      COAGENT_DAEMON_SOCKET: socketPath,
      COAGENT_PROJECT_KEY: 'project-a',
    }, 'sk_machine_valid');

    assert.deepEqual(result, { valid: true, via: '/admin/status' });
  });
});

test('existingKeyLooksRegistered rejects daemon admin status for another project', async (t) => {
  const dir = tempDir(t);
  const socketPath = path.join(dir, 'daemon.sock');

  await withTcpServer((_req, res) => {
    writeJson(res, 401, { error: 'Invalid machine API key' });
  }, async (baseUrl) => {
    await withSocketServer(socketPath, (_req, res) => {
      writeJson(res, 200, {
        ok: true,
        server_url: baseUrl,
        project_key: 'project-b',
      });
    }, async () => {
      const result = await existingKeyLooksRegistered({
        SERVER_URL: baseUrl,
        DEFAULT_SERVER_ID: 'server-a',
        COAGENT_DAEMON_SOCKET: socketPath,
        COAGENT_PROJECT_KEY: 'project-a',
      }, 'sk_machine_valid');

      assert.deepEqual(result, { valid: false, via: null });
    });
  });
});

test('register-machine CLI skips existing key when whoami matches target server', async (t) => {
  const dir = tempDir(t);
  const keyPath = path.join(dir, 'machine.key');
  const missingSocket = path.join(dir, 'missing.sock');
  writeFileSync(keyPath, 'sk_machine_valid\n', { mode: 0o600 });

  await withTcpServer((_req, res) => {
    writeJson(res, 200, {
      key_valid: true,
      server_id: 'server-a',
    });
  }, async (baseUrl) => {
    const { stdout, stderr } = await execFileAsync(process.execPath, ['ops/register-machine.mjs'], {
      cwd: repoRoot,
      env: {
        ...process.env,
        SERVER_URL: baseUrl,
        DEFAULT_SERVER_ID: 'server-a',
        COAGENT_PROJECT_KEY: 'project-a',
        COAGENT_MACHINE_KEY_PATH: keyPath,
        COAGENT_DAEMON_SOCKET: missingSocket,
        ADMIN_TOKEN: 'unused-for-skip',
      },
    });

    assert.equal(stderr, '');
    const body = JSON.parse(stdout);
    assert.equal(body.ok, true);
    assert.equal(body.skipped, true);
    assert.equal(body.reason, 'already registered, key valid via /api/machines/whoami');
    assert.equal(body.keyPath, keyPath);
    assert.equal(body.projectKey, 'project-a');
    assert.equal(Object.hasOwn(body, 'machineId'), false);
  });
});
