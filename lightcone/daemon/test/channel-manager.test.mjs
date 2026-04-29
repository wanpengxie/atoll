import assert from 'node:assert/strict';
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
