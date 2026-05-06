import assert from 'node:assert/strict';
import { execFile, spawnSync } from 'node:child_process';
import { accessSync, constants as fsConstants, mkdtempSync, mkdirSync, writeFileSync } from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';

const testDir = path.dirname(fileURLToPath(import.meta.url));
const cliDir = path.resolve(testDir, '..');
const binShim = path.join(cliDir, 'bin', 'coagent-kernel');
const distEntry = path.join(cliDir, 'dist', 'kernel', 'index.js');
const execFileAsync = promisify(execFile);

function runCli(args, env = {}, cwd = cliDir) {
  return spawnSync(binShim, args, {
    cwd,
    encoding: 'utf8',
    env: {
      ...process.env,
      ...env,
    },
  });
}

async function runCliAsync(args, env = {}, cwd = cliDir) {
  return execFileAsync(binShim, args, {
    cwd,
    encoding: 'utf8',
    env: {
      ...process.env,
      ...env,
    },
  });
}

function parseJson(stdout) {
  assert.notEqual(stdout, '', 'CLI should write a JSON envelope to stdout');
  return JSON.parse(stdout);
}

function createChannelWorkdir() {
  const workdir = mkdtempSync(path.join(os.tmpdir(), 'coagent-kernel-cli-'));
  mkdirSync(path.join(workdir, 'schedules'), { recursive: true });
  writeFileSync(path.join(workdir, 'channel.yaml'), `${JSON.stringify({
    channel_id: 'channel-kernel',
    name: 'Kernel Channel',
    type: 'xhs-creator',
    status: 'active',
    capability_set: { cli_binaries: ['xhs', 'coagent-kernel'] },
    members: [
      {
        member_type: 'human',
        member_id: 'user-1',
        display_name: 'User One',
        joined_at: '2026-04-29T12:00:00.000Z',
      },
    ],
    created_at: '2026-04-29T12:00:00.000Z',
  }, null, 2)}\n`);
  writeFileSync(path.join(workdir, 'schedules', 'sched-1.yaml'), `${JSON.stringify({
    id: 'sched-1',
    created_at: '2026-04-29T12:30:00.000Z',
    reason: 'demo',
  }, null, 2)}\n`);
  return workdir;
}

async function withRpcServer(handler, fn) {
  const requests = [];
  const server = http.createServer((req, res) => {
    let raw = '';
    req.setEncoding('utf8');
    req.on('data', (chunk) => {
      raw += chunk;
    });
    req.on('end', () => {
      const payload = raw ? JSON.parse(raw) : {};
      requests.push({
        payload,
        headers: req.headers,
      });
      const body = handler(payload, req.headers);
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify(body));
    });
  });

  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  const baseUrl = `http://127.0.0.1:${address.port}`;

  try {
    await fn({ baseUrl, requests });
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
}

test('build artifacts exist for coagent-kernel', () => {
  accessSync(distEntry, fsConstants.F_OK);
  accessSync(binShim, fsConstants.X_OK);
});

test('channel-info/member-list/capability-list/list-schedules read local workdir truth', () => {
  const workdir = createChannelWorkdir();
  const env = {
    COAGENT_WORKDIR: workdir,
    COAGENT_CHANNEL_ID: 'channel-kernel',
  };

  const info = parseJson(runCli(['channel-info'], env, workdir).stdout);
  assert.equal(info.ok, true);
  assert.equal(info.data.name, 'Kernel Channel');
  assert.equal(info.data.members_count, 1);

  const members = parseJson(runCli(['member-list'], env, workdir).stdout);
  assert.equal(members.ok, true);
  assert.equal(members.data.members[0].member_id, 'user-1');

  const capabilities = parseJson(runCli(['capability-list'], env, workdir).stdout);
  assert.equal(capabilities.ok, true);
  assert.deepEqual(capabilities.data.cli_binaries, ['xhs', 'coagent-kernel']);

  const schedules = parseJson(runCli(['list-schedules'], env, workdir).stdout);
  assert.equal(schedules.ok, true);
  assert.equal(schedules.data.schedules[0].id, 'sched-1');
});

test('schedule-cron and cancel-schedule call daemon RPC and map responses', async () => {
  const workdir = createChannelWorkdir();

  await withRpcServer((payload, headers) => {
    assert.equal(headers.authorization, 'Bearer kernel-token');
    if (payload.method === 'schedule.cron') {
      return { ok: true, result: { id: 'sched-rpc' } };
    }
    if (payload.method === 'schedule.cancel') {
      return { ok: true, result: { schedule_id: 'sched-rpc', canceled: true } };
    }
    throw new Error(`unexpected RPC method: ${payload.method}`);
  }, async ({ baseUrl, requests }) => {
    const env = {
      COAGENT_WORKDIR: workdir,
      COAGENT_CHANNEL_ID: 'channel-kernel',
      COAGENT_DAEMON_HTTP: baseUrl,
      COAGENT_DAEMON_TOKEN: 'kernel-token',
    };

    const createResult = parseJson((await runCliAsync([
      'schedule-cron',
      '--id',
      'sched-rpc',
      '--cron',
      '0 9 * * *',
      '--reason',
      'wake up',
      '--payload',
      '{"kind":"demo"}',
      '--upsert',
    ], env, workdir)).stdout);
    assert.equal(createResult.ok, true);
    assert.equal(createResult.data.schedule_id, 'sched-rpc');

    const cancelResult = parseJson((await runCliAsync([
      'cancel-schedule',
      '--id',
      'sched-rpc',
    ], env, workdir)).stdout);
    assert.equal(cancelResult.ok, true);
    assert.equal(cancelResult.data.canceled, true);

    assert.equal(requests.length, 2);
    assert.equal(requests[0].payload.method, 'schedule.cron');
    assert.deepEqual(requests[0].payload.params, {
      channel_id: 'channel-kernel',
      id: 'sched-rpc',
      cron: '0 9 * * *',
      reason: 'wake up',
      payload: { kind: 'demo' },
      upsert: true,
    });
    assert.equal(requests[1].payload.method, 'schedule.cancel');
  });
});

test('sub-agent-spawn returns not_implemented', () => {
  const workdir = createChannelWorkdir();
  const result = runCli(['sub-agent-spawn', '--name', 'writer', '--kind', 'named'], {
    COAGENT_WORKDIR: workdir,
    COAGENT_CHANNEL_ID: 'channel-kernel',
  }, workdir);

  assert.equal(result.status, 1);
  const body = parseJson(result.stdout);
  assert.equal(body.ok, false);
  assert.equal(body.error.code, 'not_implemented');
});
