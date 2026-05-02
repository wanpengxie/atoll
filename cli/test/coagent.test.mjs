import assert from 'node:assert/strict';
import { execFile, execFileSync } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';

const execFileAsync = promisify(execFile);

const testDir = path.dirname(fileURLToPath(import.meta.url));
const cliDir = path.resolve(testDir, '..');
const binShim = path.join(cliDir, 'bin', 'coagent');
const distEntry = path.join(cliDir, 'dist', 'cmd', 'coagent.js');

function runCli(args, env = {}, cwd = cliDir) {
  const stdout = execFileSync(binShim, args, {
    cwd,
    env: { ...process.env, ...env },
    encoding: 'utf8',
  });
  return JSON.parse(stdout);
}

async function runCliAsync(args, env = {}, cwd = cliDir) {
  const { stdout } = await execFileAsync(binShim, args, {
    cwd,
    env: { ...process.env, ...env },
    encoding: 'utf8',
  });
  return JSON.parse(stdout);
}

function withRpcServer(handler) {
  const requests = [];
  const server = http.createServer((req, res) => {
    let raw = '';
    req.setEncoding('utf8');
    req.on('data', (chunk) => { raw += chunk; });
    req.on('end', () => {
      requests.push({
        method: req.method,
        url: req.url,
        headers: req.headers,
        payload: raw ? JSON.parse(raw) : {},
      });
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify(handler(requests.at(-1))));
    });
  });

  return new Promise((resolve, reject) => {
    server.listen(0, '127.0.0.1', async () => {
      try {
        const address = server.address();
        const value = await Promise.resolve(resolve({ server, port: address.port, requests }));
        return value;
      } catch (err) {
        reject(err);
      }
    });
  });
}

test('build artifacts exist for coagent', () => {
  assert.doesNotThrow(() => execFileSync(process.execPath, ['-e', `require('fs').accessSync(${JSON.stringify(distEntry)})`]));
});

test('coagent help lists the business command tree', () => {
  const help = execFileSync(binShim, ['--help'], { cwd: cliDir, encoding: 'utf8' });
  assert.match(help, /channel\s+Manage coagent channels/);
  assert.match(help, /message\s+Send and inspect channel messages/);
  assert.match(help, /admin\s+Inspect the local daemon/);
  assert.match(help, /xhs\s+Run Xiaohongshu business commands/);
});

test('coagent admin status calls daemon RPC', async () => {
  const { server, port, requests } = await withRpcServer(() => ({
    ok: true,
    result: { ok: true, channels_count: 0 },
  }));
  try {
    const body = await runCliAsync(['admin', 'status'], {
      COAGENT_DAEMON_HTTP: `http://127.0.0.1:${port}`,
      COAGENT_DAEMON_TOKEN: 'test-token',
      COAGENT_DAEMON_SOCKET: '',
    });
    assert.equal(body.ok, true);
    assert.equal(body.data.channels_count, 0);
    assert.equal(requests[0].payload.method, 'admin.status');
    assert.equal(requests[0].headers.authorization, 'Bearer test-token');
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test('coagent daemon RPC reads machine.key token without mutating env', async (t) => {
  const tempDir = mkdtempSync(path.join(os.tmpdir(), 'coagent-main-token-'));
  t.after(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });
  const keyPath = path.join(tempDir, 'machine.key');
  writeFileSync(keyPath, 'machine-key-from-file\n', 'utf8');

  const { configureDaemonRpcEnv } = await import(path.join(cliDir, 'dist', 'lib', 'coagent-env.js'));
  const env = {
    COAGENT_DAEMON_HTTP: 'http://127.0.0.1:12345',
    COAGENT_DAEMON_SOCKET: '',
    COAGENT_MACHINE_KEY_PATH: keyPath,
  };
  const config = configureDaemonRpcEnv(env);
  assert.equal(config.token, 'machine-key-from-file');
  assert.equal('COAGENT_DAEMON_TOKEN' in env, false);

  const { server, port, requests } = await withRpcServer(() => ({
    ok: true,
    result: { ok: true },
  }));
  try {
    const body = await runCliAsync(['admin', 'status'], {
      COAGENT_DAEMON_HTTP: `http://127.0.0.1:${port}`,
      COAGENT_DAEMON_SOCKET: '',
      COAGENT_MACHINE_KEY_PATH: keyPath,
      COAGENT_DAEMON_TOKEN: '',
    });
    assert.equal(body.ok, true);
    assert.equal(requests[0].headers.authorization, 'Bearer machine-key-from-file');
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test('coagent xhs routes to existing xhs commands', () => {
  const tempDir = mkdtempSync(path.join(os.tmpdir(), 'coagent-main-xhs-'));
  const contentPath = path.join(tempDir, 'note.md');
  writeFileSync(contentPath, '# hello\n', 'utf8');
  const body = runCli([
    'xhs',
    'publish',
    '--title',
    'Hello',
    '--content',
    contentPath,
    '--images',
    '/tmp/a.png',
  ], {}, tempDir);
  assert.equal(body.ok, true);
  assert.equal(typeof body.data.note_id, 'string');
});
