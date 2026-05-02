import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { mkdtempSync, readFileSync, readdirSync, rmSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { ChannelManager } from '../src/channel-manager.js';

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

  assert.deepEqual(sentLog, [{
    type: 'channel:message.send.result',
    requestId: 'req-1',
    ok: true,
    message: result,
  }]);
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
  });
  const proc = new EventEmitter();
  proc.stdout = new EventEmitter();
  proc.stderr = new EventEmitter();
  const node = { channelId: 'channel-stdout' };
  const longDetail = 'x'.repeat(800);
  const line = JSON.stringify({ event: 'agent.status', detail: longDetail });

  channelManager._wireProcess(node, proc, { restoring: false });
  proc.stdout.emit('data', `${line}\n`);

  assert.equal(writes.join(''), `${line}\n`);
});
