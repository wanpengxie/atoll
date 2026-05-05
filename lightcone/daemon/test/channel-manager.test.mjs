import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { mkdtempSync, readFileSync, readdirSync, rmSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { ChannelManager } from '../src/channel-manager.js';
import { readStoredMessages } from '../src/message-store.js';

function captureStdoutWrites(t) {
  const previousWrite = process.stdout.write;
  const writes = [];
  process.stdout.write = (chunk, encoding, callback) => {
    writes.push(String(chunk));
    if (typeof encoding === 'function') encoding();
    if (typeof callback === 'function') callback();
    return true;
  };

  t.after(() => {
    process.stdout.write = previousWrite;
  });

  return writes;
}

function createStdoutHarness(t, channelId) {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-stdout-'));
  t.after(() => {
    rmSync(tempHome, { recursive: true, force: true });
  });

  const channelManager = new ChannelManager({
    serverUrl: 'http://localhost:3001',
    machineApiKey: 'machine-key',
    daemonSocketPath: path.join(tempHome, '.coagent', 'daemon.sock'),
    daemonHttpUrl: 'http://127.0.0.1:3002',
    daemonToken: 'daemon-token',
    baseDir: path.join(tempHome, 'coagent'),
  });
  const proc = new EventEmitter();
  proc.stdout = new EventEmitter();
  proc.stderr = new EventEmitter();
  const node = { channelId };

  channelManager._wireProcess(node, proc, { restoring: false });
  return { channelManager, proc, node };
}

test('channel:message.send writes local truth before view sync and reports success', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-human-message-'));
  const previousHome = process.env.HOME;
  process.env.HOME = tempHome;

  t.after(() => {
    process.env.HOME = previousHome;
    rmSync(tempHome, { recursive: true, force: true });
  });

  let persistedBeforeViewSync = false;
  let workdir = '';
  const sentLog = [];
  const connection = {
    send(message) {
      sentLog.push(message);
    },
    async request({ message, expect }) {
      const files = readdirSync(path.join(workdir, 'messages')).filter((entry) => entry.endsWith('.jsonl'));
      assert.equal(files.length, 1);

      const contents = readFileSync(path.join(workdir, 'messages', files[0]), 'utf8');
      persistedBeforeViewSync = contents.includes('"content":"hello from server"');
      assert.equal(message.sender_kind, 'human');
      assert.equal(message.payload_type, 'user.text');
      assert.equal(message.payload_body.text, 'hello from server');
      assert.equal(message.envelope.sender.kind, 'human');
      assert.equal(message.envelope.sender.id, 'user-a');
      assert.equal(typeof message.ts_received, 'number');

      return { type: expect.type, requestId: expect.requestId, ok: true, echoedType: message.type };
    },
  };

  const channelManager = new ChannelManager({
    serverUrl: 'http://localhost:3001',
    machineApiKey: 'machine-key',
    daemonSocketPath: path.join(tempHome, '.coagent', 'daemon.sock'),
    daemonHttpUrl: 'http://127.0.0.1:3002',
    daemonToken: 'daemon-token',
  });
  channelManager.setConnection(connection);

  const created = await channelManager.createChannel({
    channelId: 'channel-1',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Channel One',
    type: 'xhs-creator',
    status: 'active',
    members: [{ memberType: 'human', memberId: 'user-a', displayName: 'User A' }],
  });
  workdir = created.workdir;

  const result = await channelManager.handle({
    type: 'channel:message.send',
    requestId: 'req-1',
    channelId: 'channel-1',
    senderType: 'human',
    senderId: 'user-a',
    senderName: 'User A',
    content: 'hello from server',
    attachments: [],
  }, connection);

  assert.equal(persistedBeforeViewSync, true);
  assert.equal(result.content, 'hello from server');
  assert.equal(result.senderType, 'human');
  assert.equal(result.senderId, 'user-a');
  assert.equal(result.senderName, 'User A');

  const files = readdirSync(path.join(workdir, 'messages')).filter((entry) => entry.endsWith('.jsonl'));
  assert.equal(files.length, 1);
  const stored = readFileSync(path.join(workdir, 'messages', files[0]), 'utf8')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line));
  assert.equal(stored.length, 1);
  assert.equal(stored[0].content, 'hello from server');
  assert.equal(stored[0].senderType, 'human');
  assert.equal(stored[0].senderId, 'user-a');
  assert.equal(stored[0].senderName, 'User A');
  assert.equal(stored[0].senderKind, 'human');
  assert.equal(stored[0].payloadType, 'user.text');
  assert.equal(stored[0].payload.body.text, 'hello from server');

  assert.deepEqual(sentLog, [{
    type: 'channel:message.send.result',
    requestId: 'req-1',
    ok: true,
    message: result,
  }]);
});

test('appendMessage double-writes 100 messages to jsonl and sqlite consistently', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-sqlite-double-write-'));
  t.after(() => {
    rmSync(tempHome, { recursive: true, force: true });
  });

  const channelManager = new ChannelManager({
    serverUrl: 'http://localhost:3001',
    machineApiKey: 'machine-key',
    daemonSocketPath: path.join(tempHome, '.coagent', 'daemon.sock'),
    daemonHttpUrl: 'http://127.0.0.1:3002',
    daemonToken: 'daemon-token',
    baseDir: path.join(tempHome, 'coagent'),
  });

  const created = await channelManager.createChannel({
    channelId: 'channel-sqlite',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'SQLite Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const node = channelManager._requireNode(created.channel_id);

  for (let index = 0; index < 100; index += 1) {
    await channelManager._appendMessage(node, {
      messageId: `message-${index}`,
      channelId: node.channelId,
      senderType: 'human',
      senderId: 'user-a',
      senderName: 'User A',
      content: `message ${index}`,
      createdAt: new Date(Date.UTC(2026, 4, 6, 0, 0, index)).toISOString(),
      source: 'test',
    });
  }

  const jsonlMessages = readdirSync(path.join(created.workdir, 'messages'))
    .filter((entry) => entry.endsWith('.jsonl'))
    .flatMap((fileName) => readFileSync(path.join(created.workdir, 'messages', fileName), 'utf8')
      .split('\n')
      .map((line) => line.trim())
      .filter(Boolean)
      .map((line) => JSON.parse(line)));
  const sqliteMessages = readStoredMessages(node.messageStore);

  assert.equal(jsonlMessages.length, 100);
  assert.equal(sqliteMessages.length, 100);
  for (let index = 0; index < 100; index += 1) {
    assert.equal(sqliteMessages[index].id, jsonlMessages[index].messageId);
    assert.equal(sqliteMessages[index].legacy.content, jsonlMessages[index].content);
    assert.equal(sqliteMessages[index].sender_kind, jsonlMessages[index].senderKind);
    assert.equal(sqliteMessages[index].payload_type, jsonlMessages[index].payloadType);
    assert.deepEqual(sqliteMessages[index].payload.body, jsonlMessages[index].payload.body);
  }
});

test('channel:message.send keeps daemon truth and reports ok when view sync ack fails', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-view-sync-fail-'));
  const previousHome = process.env.HOME;
  process.env.HOME = tempHome;

  t.after(() => {
    process.env.HOME = previousHome;
    rmSync(tempHome, { recursive: true, force: true });
  });

  const sentLog = [];
  const connection = {
    send(message) {
      sentLog.push(message);
    },
    async request({ expect }) {
      return {
        type: expect.type,
        requestId: expect.requestId,
        ok: false,
        error: 'simulated view sync failure',
      };
    },
  };

  const channelManager = new ChannelManager({
    serverUrl: 'http://localhost:3001',
    machineApiKey: 'machine-key',
    daemonSocketPath: path.join(tempHome, '.coagent', 'daemon.sock'),
    daemonHttpUrl: 'http://127.0.0.1:3002',
    daemonToken: 'daemon-token',
  });
  channelManager.setConnection(connection);

  const created = await channelManager.createChannel({
    channelId: 'channel-2',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Channel Two',
    type: 'xhs-creator',
    status: 'active',
    members: [{ memberType: 'human', memberId: 'user-a', displayName: 'User A' }],
  });
  const { workdir } = created;

  const result = await channelManager.handle({
    type: 'channel:message.send',
    requestId: 'req-ack-fail',
    channelId: 'channel-2',
    senderType: 'human',
    senderId: 'user-a',
    senderName: 'User A',
    content: 'message survives ack failure',
    attachments: [],
  }, connection);

  assert.equal(result.content, 'message survives ack failure');

  const messageFiles = readdirSync(path.join(workdir, 'messages')).filter((entry) => entry.endsWith('.jsonl'));
  assert.equal(messageFiles.length, 1);
  const stored = readFileSync(path.join(workdir, 'messages', messageFiles[0]), 'utf8')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line));
  assert.equal(stored.length, 1);
  assert.equal(stored[0].content, 'message survives ack failure');

  const pendingDir = path.join(workdir, 'pending-view-sync');
  const pendingFiles = readdirSync(pendingDir).filter((entry) => entry.endsWith('.json'));
  assert.equal(pendingFiles.length, 1);
  const pending = JSON.parse(readFileSync(path.join(pendingDir, pendingFiles[0]), 'utf8'));
  assert.equal(pending.reason, 'simulated view sync failure');
  assert.equal(pending.payload.type, 'message.append');
  assert.equal(pending.payload.message_id, result.messageId);
  assert.equal(pending.payload.content, 'message survives ack failure');

  assert.deepEqual(sentLog, [{
    type: 'channel:message.send.result',
    requestId: 'req-ack-fail',
    ok: true,
    message: result,
  }]);
});

test('channel:message.send keeps daemon truth and reports ok when view sync request throws', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-view-sync-throw-'));
  const previousHome = process.env.HOME;
  process.env.HOME = tempHome;

  t.after(() => {
    process.env.HOME = previousHome;
    rmSync(tempHome, { recursive: true, force: true });
  });

  const sentLog = [];
  const connection = {
    send(message) {
      sentLog.push(message);
    },
    async request() {
      throw new Error('connection lost mid-request');
    },
  };

  const channelManager = new ChannelManager({
    serverUrl: 'http://localhost:3001',
    machineApiKey: 'machine-key',
    daemonSocketPath: path.join(tempHome, '.coagent', 'daemon.sock'),
    daemonHttpUrl: 'http://127.0.0.1:3002',
    daemonToken: 'daemon-token',
  });
  channelManager.setConnection(connection);

  const created = await channelManager.createChannel({
    channelId: 'channel-3',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Channel Three',
    type: 'xhs-creator',
    status: 'active',
    members: [{ memberType: 'human', memberId: 'user-a', displayName: 'User A' }],
  });
  const { workdir } = created;

  const result = await channelManager.handle({
    type: 'channel:message.send',
    requestId: 'req-throw',
    channelId: 'channel-3',
    senderType: 'human',
    senderId: 'user-a',
    senderName: 'User A',
    content: 'survives connection error',
    attachments: [],
  }, connection);

  assert.equal(result.content, 'survives connection error');

  const stored = readFileSync(
    path.join(workdir, 'messages', readdirSync(path.join(workdir, 'messages')).find((entry) => entry.endsWith('.jsonl'))),
    'utf8',
  ).trim();
  assert.match(stored, /survives connection error/);

  const pendingFiles = readdirSync(path.join(workdir, 'pending-view-sync')).filter((entry) => entry.endsWith('.json'));
  assert.equal(pendingFiles.length, 1);
  const pending = JSON.parse(readFileSync(path.join(workdir, 'pending-view-sync', pendingFiles[0]), 'utf8'));
  assert.equal(pending.reason, 'connection lost mid-request');

  assert.deepEqual(sentLog, [{
    type: 'channel:message.send.result',
    requestId: 'req-throw',
    ok: true,
    message: result,
  }]);
});

test('agent stdout is forwarded as raw JSON Lines without prefix or truncation', (t) => {
  const writes = captureStdoutWrites(t);
  const { proc } = createStdoutHarness(t, 'channel-stdout');
  const longDetail = 'x'.repeat(800);
  const line = JSON.stringify({ event: 'agent.status', detail: longDetail });

  proc.stdout.emit('data', `${line}\n`);

  assert.equal(writes.join(''), `${line}\n`);
});

test('agent stdout split across chunks is buffered until a full JSON Line arrives', (t) => {
  const writes = captureStdoutWrites(t);
  const { proc } = createStdoutHarness(t, 'channel-stdout-chunks');
  const line = JSON.stringify({ event: 'agent.activity', detail: 'chunked' });
  const cut = Math.floor(line.length / 2);

  proc.stdout.emit('data', line.slice(0, cut));
  assert.deepEqual(writes, []);

  proc.stdout.emit('data', `${line.slice(cut)}\n`);
  assert.deepEqual(writes, [`${line}\n`]);
});

test('agent stdout forwards multiple JSON Lines from a single chunk separately', (t) => {
  const writes = captureStdoutWrites(t);
  const { proc } = createStdoutHarness(t, 'channel-stdout-multiline');
  const lines = [
    JSON.stringify({ event: 'agent.status', detail: 'a' }),
    JSON.stringify({ event: 'agent.status', detail: 'b' }),
    JSON.stringify({ event: 'agent.status', detail: 'c' }),
  ];

  proc.stdout.emit('data', `${lines.join('\n')}\n`);

  assert.deepEqual(writes, lines.map((line) => `${line}\n`));
});

test('agent stdout flushes an unterminated line on exit even when proc was replaced', (t) => {
  const writes = captureStdoutWrites(t);
  const { channelManager, proc, node } = createStdoutHarness(t, 'channel-stdout-exit-flush');
  const line = JSON.stringify({ event: 'agent.status', detail: 'unterminated' });
  const replacementProc = new EventEmitter();

  proc.stdout.emit('data', line);
  channelManager.channels.set(node.channelId, { ...node, proc: replacementProc });
  proc.emit('exit', 1, null);

  assert.deepEqual(writes, [`${line}\n`]);
});
