import assert from 'node:assert/strict';
import { execFile, execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
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

async function runCliFailure(args, env = {}, cwd = cliDir) {
  try {
    await execFileAsync(binShim, args, {
      cwd,
      env: { ...process.env, ...env },
      encoding: 'utf8',
    });
  } catch (err) {
    return {
      code: err.code,
      stdout: err.stdout,
      body: JSON.parse(err.stdout),
    };
  }
  assert.fail('expected CLI command to fail');
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
  assert.match(help, /emit \[options\]\s+Emit an envelope message/);
  assert.match(help, /query \[options\]\s+Query channel messages/);
  assert.match(help, /dispatch\s+Dispatch promise-chain helpers/);
  assert.match(help, /task\s+Open and inspect task entities/);
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

test('coagent business subcommands call expected daemon RPC methods', async () => {
  const { server, port, requests } = await withRpcServer((request) => ({
    ok: true,
    result: { echoed_method: request.payload.method, echoed_params: request.payload.params },
  }));
  const env = {
    COAGENT_DAEMON_HTTP: `http://127.0.0.1:${port}`,
    COAGENT_DAEMON_TOKEN: 'test-token',
    COAGENT_DAEMON_SOCKET: '',
  };
  const cases = [
    { args: ['channel', 'ls'], method: 'channel.list', params: {} },
    { args: ['channel', 'show', 'channel-a'], method: 'channel.info', params: { channel_id: 'channel-a' } },
    { args: ['channel', 'start', 'channel-a'], method: 'channel.start', params: { channel_id: 'channel-a' } },
    { args: ['channel', 'restart', 'channel-a'], method: 'channel.restart', params: { channel_id: 'channel-a' } },
    { args: ['channel', 'stop', 'channel-a'], method: 'channel.stop', params: { channel_id: 'channel-a' } },
    { args: ['channel', 'archive', 'channel-a'], method: 'channel.archive', params: { channel_id: 'channel-a' } },
    {
      args: ['message', 'send', '--channel', 'channel-a', '--text', 'hello', '--attachments', '/tmp/a.png,/tmp/b.png'],
      method: 'message.send',
      params: {
        channel_id: 'channel-a',
        content: 'hello',
        attachments: ['/tmp/a.png', '/tmp/b.png'],
        sender_type: 'human',
        sender_kind: 'human',
        sender_id: 'cli',
        sender_name: 'CLI',
        payload_type: 'user.text',
        payload_body: { text: 'hello', attachments: ['/tmp/a.png', '/tmp/b.png'] },
      },
    },
    { args: ['message', 'history', '--channel', 'channel-a', '--limit', '7'], method: 'message.list', params: { channel_id: 'channel-a', limit: 7 } },
    { args: ['message', 'search', '--channel', 'channel-a', '--query', 'hello', '--limit', '3'], method: 'message.search', params: { channel_id: 'channel-a', query: 'hello', limit: 3 } },
    {
      args: ['emit', '--channel', 'channel-a', '--payload-type', 'agent.text', '--payload', '{"text":"hi"}'],
      method: 'message.emit',
      params: {
        channel_id: 'channel-a',
        sender_kind: 'agent',
        sender_type: 'channel_agent',
        sender_id: 'channel-agent',
        sender_name: 'channel-agent',
        message_type: 'agent.text',
        payload_type: 'agent.text',
        payload_body: { text: 'hi' },
        content: 'hi',
        audience: ['channel'],
      },
    },
    {
      args: [
        'query',
        '--channel', 'channel-a',
        '--correlation-id', 'corr-a',
        '--payload-type', 'dispatch.completed',
        '--sender-kind', 'external',
        '--not-before', '2026-05-06T00:00:00.000Z',
        '--text', 'done',
        '--unread',
        '--limit', '5',
      ],
      method: 'message.query',
      params: {
        channel_id: 'channel-a',
        correlation_id: 'corr-a',
        payload_type: 'dispatch.completed',
        sender_kind: 'external',
        not_before_lte: Date.parse('2026-05-06T00:00:00.000Z'),
        text: 'done',
        unread: true,
        limit: 5,
      },
    },
    {
      args: ['query', '--channel', 'channel-a', '--unread', '--include-future'],
      method: 'message.query',
      params: {
        channel_id: 'channel-a',
        unread: true,
        include_future: true,
        limit: 20,
      },
    },
    {
      args: ['schedule', '--channel', 'channel-a', '--not-before', '1760000000000', '--payload', '{"reason":"check"}'],
      method: 'message.schedule',
      params: {
        channel_id: 'channel-a',
        not_before: 1760000000000,
        payload_type: 'dispatch.self_check_due',
        payload_body: { reason: 'check' },
      },
    },
    {
      args: ['schedule', '--channel', 'channel-a', '--not-before', '1760000000000', '--payload', '{"reason":"check"}', '--in-task', 'task-a'],
      method: 'message.schedule',
      params: {
        channel_id: 'channel-a',
        not_before: 1760000000000,
        payload_type: 'dispatch.self_check_due',
        payload_body: { reason: 'check' },
        task_id: 'task-a',
      },
    },
    {
      args: ['schedule', '--channel', 'channel-a', '--not-before', '1760000000000', '--payload', '{"reason":"check"}', '--audience', 'channel'],
      method: 'message.schedule',
      params: {
        channel_id: 'channel-a',
        not_before: 1760000000000,
        payload_type: 'dispatch.self_check_due',
        payload_body: { reason: 'check' },
        audience: ['channel'],
      },
    },
    {
      args: ['dispatch', 'start', '--channel', 'channel-a', '--target', 'external:device:x', '--type', 'xhs.publish', '--params', '{"title":"hi"}', '--in-task', 'task-a', '--check-after', '5m'],
      method: 'dispatch.start',
      params: {
        channel_id: 'channel-a',
        target: 'external:device:x',
        type: 'xhs.publish',
        params: { title: 'hi' },
        in_task: 'task-a',
        check_after_ms: 300000,
      },
    },
    { args: ['dispatch', 'check', '--channel', 'channel-a', '--correlation-id', 'corr-a'], method: 'dispatch.check', params: { channel_id: 'channel-a', correlation_id: 'corr-a' } },
    { args: ['dispatch', 'renew', '--channel', 'channel-a', '--correlation-id', 'corr-a', '--check-after', '30s'], method: 'dispatch.renew', params: { channel_id: 'channel-a', correlation_id: 'corr-a', check_after_ms: 30000 } },
    { args: ['dispatch', 'ls', '--channel', 'channel-a', '--task-id', 'task-a', '--status', 'pending'], method: 'dispatch.list', params: { channel_id: 'channel-a', task_id: 'task-a', status: 'pending' } },
    {
      args: ['memo', '--channel', 'channel-a', '--tag', 'rule', '--scope', 'forever', '--doc', 'notes/rule.md', '--correlation-id', 'corr-a', '--in-task', 'task-a', 'Remember this'],
      method: 'memo.create',
      params: {
        channel_id: 'channel-a',
        tag: 'rule',
        scope: 'forever',
        doc: 'notes/rule.md',
        correlation_id: 'corr-a',
        task_id: 'task-a',
        summary: 'Remember this',
      },
    },
    { args: ['task', 'ls', '--channel', 'channel-a', '--status', 'active', '--mine', '--parent', 'task-parent'], method: 'task.list', params: { channel_id: 'channel-a', status: 'active', mine: true, parent_task_id: 'task-parent' } },
    { args: ['task', 'show', '--channel', 'channel-a', 'task-a'], method: 'task.show', params: { channel_id: 'channel-a', task_id: 'task-a' } },
    { args: ['task', 'tree', '--channel', 'channel-a', '--root', 'task-a'], method: 'task.tree', params: { channel_id: 'channel-a', root_task_id: 'task-a' } },
    { args: ['recall', '--channel', 'channel-a', '--tag', 'rule', '--limit', '3', '--status', 'all'], method: 'memo.recall', params: { channel_id: 'channel-a', tag: 'rule', limit: 3, status: 'all' } },
    { args: ['admin', 'machines'], method: 'admin.machines', params: {} },
  ];

  try {
    for (const item of cases) {
      const body = await runCliAsync(item.args, env);
      assert.equal(body.ok, true);
      assert.equal(body.data.echoed_method, item.method);
      assert.deepEqual(body.data.echoed_params, item.params);
    }

    assert.equal(requests.length, cases.length);
    for (const [index, item] of cases.entries()) {
      assert.equal(requests[index].payload.method, item.method);
      assert.deepEqual(requests[index].payload.params, item.params);
      assert.equal(requests[index].headers.authorization, 'Bearer test-token');
    }
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

test('coagent emit --text treats file-like values as literal text', async (t) => {
  const tempDir = mkdtempSync(path.join(os.tmpdir(), 'coagent-emit-text-literal-'));
  t.after(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });
  writeFileSync(path.join(tempDir, 'README.md'), 'file contents should not be used\n', 'utf8');

  const { server, port, requests } = await withRpcServer((request) => ({
    ok: true,
    result: { echoed_method: request.payload.method, echoed_params: request.payload.params },
  }));
  try {
    const body = await runCliAsync([
      'emit',
      '--channel',
      'channel-a',
      '--payload-type',
      'agent.text',
      '--payload',
      '{}',
      '--text',
      'README.md',
    ], {
      COAGENT_DAEMON_HTTP: `http://127.0.0.1:${port}`,
      COAGENT_DAEMON_SOCKET: '',
    }, tempDir);

    assert.equal(body.ok, true);
    assert.equal(requests[0].payload.method, 'message.emit');
    assert.equal(requests[0].payload.params.content, 'README.md');
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test('coagent memo-write writes a local doc and emits a memo RPC', async (t) => {
  const tempDir = mkdtempSync(path.join(os.tmpdir(), 'coagent-memo-write-'));
  t.after(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });
  writeFileSync(path.join(tempDir, 'channel.yaml'), JSON.stringify({ channel_id: 'channel-memo' }), 'utf8');

  const { server, port, requests } = await withRpcServer((request) => ({
    ok: true,
    result: { echoed_method: request.payload.method, echoed_params: request.payload.params },
  }));
  try {
    const body = await runCliAsync([
      'memo-write',
      '--doc',
      'notes/tasks/task.md',
      '--content',
      '# Task Title\n\nbody\n',
    ], {
      COAGENT_DAEMON_HTTP: `http://127.0.0.1:${port}`,
      COAGENT_DAEMON_SOCKET: '',
    }, tempDir);

    assert.equal(body.ok, true);
    assert.equal(readFileSync(path.join(tempDir, 'notes', 'tasks', 'task.md'), 'utf8'), '# Task Title\n\nbody\n');
    assert.equal(requests[0].payload.method, 'memo.create');
    assert.deepEqual(requests[0].payload.params, {
      channel_id: 'channel-memo',
      tag: 'pending_action',
      scope: 'channel',
      doc: 'notes/tasks/task.md',
      summary: 'Task Title',
    });
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test('coagent memo-write --content is literal and --content-file reads explicitly', async (t) => {
  const tempDir = mkdtempSync(path.join(os.tmpdir(), 'coagent-memo-write-content-file-'));
  t.after(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });
  writeFileSync(path.join(tempDir, 'channel.yaml'), JSON.stringify({ channel_id: 'channel-memo' }), 'utf8');
  writeFileSync(path.join(tempDir, 'README.md'), '# File Title\n\nfile body\n', 'utf8');

  const { server, port, requests } = await withRpcServer((request) => ({
    ok: true,
    result: { echoed_method: request.payload.method, echoed_params: request.payload.params },
  }));
  try {
    const env = {
      COAGENT_DAEMON_HTTP: `http://127.0.0.1:${port}`,
      COAGENT_DAEMON_SOCKET: '',
    };
    const literal = await runCliAsync([
      'memo-write',
      '--doc',
      'notes/tasks/literal.md',
      '--content',
      'README.md',
    ], env, tempDir);
    const fromFile = await runCliAsync([
      'memo-write',
      '--doc',
      'notes/tasks/from-file.md',
      '--content-file',
      'README.md',
    ], env, tempDir);

    assert.equal(literal.ok, true);
    assert.equal(fromFile.ok, true);
    assert.equal(readFileSync(path.join(tempDir, 'notes', 'tasks', 'literal.md'), 'utf8'), 'README.md');
    assert.equal(readFileSync(path.join(tempDir, 'notes', 'tasks', 'from-file.md'), 'utf8'), '# File Title\n\nfile body\n');
    assert.equal(requests[0].payload.params.summary, 'README.md');
    assert.equal(requests[1].payload.params.summary, 'File Title');
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test('coagent task open emits task.open RPC and leaves doc writes to daemon', async (t) => {
  const tempDir = mkdtempSync(path.join(os.tmpdir(), 'coagent-task-open-'));
  t.after(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });
  writeFileSync(path.join(tempDir, 'channel.yaml'), JSON.stringify({ channel_id: 'channel-task' }), 'utf8');

  const { server, port, requests } = await withRpcServer((request) => ({
    ok: true,
    result: { echoed_method: request.payload.method, echoed_params: request.payload.params },
  }));
  try {
    const body = await runCliAsync([
      'task',
      'open',
      '--type',
      'note.publish',
      '--title',
      'Publish Launch Note!',
      '--rationale',
      'needs tracking',
    ], {
      COAGENT_DAEMON_HTTP: `http://127.0.0.1:${port}`,
      COAGENT_DAEMON_SOCKET: '',
    }, tempDir);

    assert.equal(body.ok, true);
    assert.match(body.data.task_id, /^[0-9a-f-]{36}$/);
    assert.match(body.data.doc_ref, /^notes\/tasks\/\d{4}-\d{2}-\d{2}-publish-launch-note\.md$/);
    assert.equal(existsSync(path.join(tempDir, body.data.doc_ref)), false);
    assert.equal(requests[0].payload.method, 'task.open');
    assert.deepEqual(requests[0].payload.params, {
      channel_id: 'channel-task',
      task_id: body.data.task_id,
      type: 'note.publish',
      title: 'Publish Launch Note!',
      doc_ref: body.data.doc_ref,
      rationale: 'needs tracking',
    });
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test('coagent task open does not leave a task doc when RPC fails', async (t) => {
  const tempDir = mkdtempSync(path.join(os.tmpdir(), 'coagent-task-open-rpc-fail-'));
  t.after(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });
  writeFileSync(path.join(tempDir, 'channel.yaml'), JSON.stringify({ channel_id: 'channel-task' }), 'utf8');

  const { server, port, requests } = await withRpcServer(() => ({
    ok: false,
    error: { code: 'conflict', message: 'task already exists' },
  }));
  try {
    const body = await runCliFailure([
      'task',
      'open',
      '--type',
      'note.publish',
      '--title',
      'Publish Launch Note!',
    ], {
      COAGENT_DAEMON_HTTP: `http://127.0.0.1:${port}`,
      COAGENT_DAEMON_SOCKET: '',
    }, tempDir);

    assert.equal(body.code, 1);
    assert.equal(body.body.ok, false);
    assert.equal(body.body.error.code, 'conflict');
    assert.equal(requests.length, 1);
    assert.equal(requests[0].payload.method, 'task.open');
    assert.equal(existsSync(path.join(tempDir, 'notes')), false);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test('coagent task append and close delegate doc updates to daemon', async (t) => {
  const tempDir = mkdtempSync(path.join(os.tmpdir(), 'coagent-task-edit-'));
  t.after(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });
  writeFileSync(path.join(tempDir, 'channel.yaml'), JSON.stringify({ channel_id: 'channel-task' }), 'utf8');
  const docRef = 'notes/tasks/2026-05-06-existing.md';
  const initialDoc = '# Existing\n\n## Timeline\n\n- opened\n\n## Status\n\nStatus: opened\n';
  mkdirSync(path.dirname(path.join(tempDir, docRef)), { recursive: true });
  writeFileSync(path.join(tempDir, docRef), initialDoc, 'utf8');

  const { server, port, requests } = await withRpcServer((request) => {
    if (request.payload.method === 'task.append') {
      return {
        ok: true,
        result: {
          task_id: request.payload.params.task_id,
          doc_ref: docRef,
        },
      };
    }
    if (request.payload.method === 'task.close') {
      return {
        ok: true,
        result: {
          task_id: request.payload.params.task_id,
          doc_ref: docRef,
          status: request.payload.params.status,
          task: { task_id: request.payload.params.task_id, doc_ref: docRef },
        },
      };
    }
    return { ok: true, result: { echoed_method: request.payload.method, echoed_params: request.payload.params } };
  });
  const env = {
    COAGENT_DAEMON_HTTP: `http://127.0.0.1:${port}`,
    COAGENT_DAEMON_SOCKET: '',
  };
  try {
    const appended = await runCliAsync(['task', 'append', 'task-a', 'Draft ready'], env, tempDir);
    assert.equal(appended.ok, true);
    assert.equal(appended.data.doc_ref, docRef);
    assert.equal(readFileSync(path.join(tempDir, docRef), 'utf8'), initialDoc);
    assert.equal(requests[0].payload.method, 'task.append');
    assert.deepEqual(requests[0].payload.params, {
      channel_id: 'channel-task',
      task_id: 'task-a',
      summary: 'Draft ready',
    });

    const closed = await runCliAsync([
      'task',
      'close',
      'task-a',
      '--status',
      'completed',
      '--summary',
      'Finished',
      '--result-ref',
      'artifact://note',
    ], env, tempDir);
    assert.equal(closed.ok, true);
    assert.equal(closed.data.doc_ref, docRef);
    const closedDoc = readFileSync(path.join(tempDir, docRef), 'utf8');
    assert.doesNotMatch(closedDoc, /Summary: Finished/);
    assert.doesNotMatch(closedDoc, /Status: completed/);
    assert.equal(requests[1].payload.method, 'task.close');
    assert.deepEqual(requests[1].payload.params, {
      channel_id: 'channel-task',
      task_id: 'task-a',
      status: 'completed',
      summary: 'Finished',
      result_ref: 'artifact://note',
    });
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test('coagent task close rejects invalid status before daemon RPC', async (t) => {
  const tempDir = mkdtempSync(path.join(os.tmpdir(), 'coagent-task-close-status-'));
  t.after(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });
  writeFileSync(path.join(tempDir, 'channel.yaml'), JSON.stringify({ channel_id: 'channel-task' }), 'utf8');

  const { server, port, requests } = await withRpcServer(() => ({
    ok: true,
    result: {},
  }));
  try {
    const body = await runCliFailure([
      'task',
      'close',
      'task-a',
      '--channel',
      'channel-task',
      '--status',
      'weird',
    ], {
      COAGENT_DAEMON_HTTP: `http://127.0.0.1:${port}`,
      COAGENT_DAEMON_SOCKET: '',
    }, tempDir);

    assert.equal(body.code, 2);
    assert.equal(body.body.ok, false);
    assert.equal(body.body.error.code, 'invalid_arguments');
    assert.equal(requests.length, 0);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test('coagent env resolves PROJECT_KEY and socket from nearest .env without overriding env', async (t) => {
  const tempDir = mkdtempSync(path.join(os.tmpdir(), 'coagent-env-dotenv-'));
  t.after(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });
  const nestedDir = path.join(tempDir, 'nested', 'project');
  mkdirSync(nestedDir, { recursive: true });
  writeFileSync(path.join(tempDir, '.env'), 'COAGENT_PROJECT_KEY=test_proj\n', 'utf8');

  const previousCwd = process.cwd();
  process.chdir(nestedDir);
  t.after(() => {
    process.chdir(previousCwd);
  });

  const { resolveProjectKey, resolveDaemonSocketPath } = await import(path.join(cliDir, 'dist', 'lib', 'coagent-env.js'));
  assert.equal(resolveProjectKey({}), 'test_proj');
  assert.equal(resolveDaemonSocketPath({}), path.join(os.homedir(), '.coagent', 'test_proj', 'daemon.sock'));
  assert.equal(resolveProjectKey({ COAGENT_PROJECT_KEY: 'env_proj' }), 'env_proj');
});

test('coagent daemon unavailable error includes PROJECT_KEY and socket hint', async (t) => {
  const tempDir = mkdtempSync(path.join(os.tmpdir(), 'coagent-daemon-hint-'));
  t.after(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });
  writeFileSync(path.join(tempDir, '.env'), 'COAGENT_PROJECT_KEY=hint_proj\n', 'utf8');

  const result = await runCliFailure(['admin', 'status'], {
    COAGENT_DAEMON_HTTP: '',
    COAGENT_DAEMON_SOCKET: '',
    COAGENT_DAEMON_TOKEN: 'test-token',
  }, tempDir);

  assert.equal(result.code, 1);
  assert.equal(result.body.ok, false);
  assert.equal(result.body.error.code, 'daemon_request_failed');
  assert.match(result.body.error.message, /PROJECT_KEY=hint_proj/);
  assert.match(result.body.error.message, /\.coagent\/hint_proj\/daemon\.sock/);
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
