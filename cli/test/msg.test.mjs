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
const binShim = path.join(cliDir, 'bin', 'coagent-msg');
const distEntry = path.join(cliDir, 'dist', 'msg', 'index.js');
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

function createMessageWorkdir() {
  const workdir = mkdtempSync(path.join(os.tmpdir(), 'coagent-msg-cli-'));
  mkdirSync(path.join(workdir, 'messages'), { recursive: true });
  writeFileSync(path.join(workdir, 'channel.yaml'), `${JSON.stringify({
    channel_id: 'channel-msg',
    name: 'Message Channel',
    type: 'xhs-creator',
    status: 'active',
    capability_set: { cli_binaries: ['xhs', 'coagent-kernel', 'coagent-msg'] },
    members: [],
    created_at: '2026-04-29T12:00:00.000Z',
  }, null, 2)}\n`);
  writeFileSync(path.join(workdir, 'messages', '2026-04-29.jsonl'), [
    JSON.stringify({ content: 'hello world', createdAt: '2026-04-29T12:00:00.000Z' }),
    JSON.stringify({ content: 'follow up smoke', createdAt: '2026-04-29T12:05:00.000Z' }),
  ].join('\n'));
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

test('build artifacts exist for coagent-msg', () => {
  accessSync(distEntry, fsConstants.F_OK);
  accessSync(binShim, fsConstants.X_OK);
});

test('check and search read local channel messages', () => {
  const workdir = createMessageWorkdir();
  const env = {
    COAGENT_WORKDIR: workdir,
    COAGENT_CHANNEL_ID: 'channel-msg',
  };

  const check = parseJson(runCli(['check', '--since', '2026-04-29T12:01:00.000Z', '--limit', '5'], env, workdir).stdout);
  assert.equal(check.ok, true);
  assert.equal(check.data.messages.length, 1);
  assert.equal(check.data.messages[0].content, 'follow up smoke');

  const search = parseJson(runCli(['search', '--keyword', 'smoke', '--limit', '5'], env, workdir).stdout);
  assert.equal(search.ok, true);
  assert.equal(search.data.messages.length, 1);
  assert.equal(search.data.messages[0].content, 'follow up smoke');
});

test('send resolves file content and calls daemon RPC', async () => {
  const workdir = createMessageWorkdir();
  const contentPath = path.join(workdir, 'reply.md');
  writeFileSync(contentPath, 'reply from file\n', 'utf8');

  await withRpcServer((payload, headers) => {
    assert.equal(headers.authorization, 'Bearer msg-token');
    return {
      ok: true,
      result: {
        message_id: 'msg-1',
        channel_id: payload.params.channel_id,
        content: payload.params.content,
      },
    };
  }, async ({ baseUrl, requests }) => {
    const env = {
      COAGENT_WORKDIR: workdir,
      COAGENT_CHANNEL_ID: 'channel-msg',
      COAGENT_DAEMON_HTTP: baseUrl,
      COAGENT_DAEMON_TOKEN: 'msg-token',
    };

    const result = parseJson((await runCliAsync([
      'send',
      '--content',
      contentPath,
      '--attachments',
      '/tmp/a.png,/tmp/b.png',
    ], env, workdir)).stdout);

    assert.equal(result.ok, true);
    assert.equal(result.data.content, 'reply from file\n');
    assert.equal(requests.length, 1);
    assert.equal(requests[0].payload.method, 'message.send');
    assert.equal(requests[0].payload.params.attachments.length, 2);
    assert.equal(requests[0].payload.params.sender_kind, 'agent');
    assert.equal(requests[0].payload.params.payload_type, 'agent.text');
    assert.deepEqual(requests[0].payload.params.payload_body, {
      text: 'reply from file\n',
      attachments: ['/tmp/a.png', '/tmp/b.png'],
    });
  });
});
