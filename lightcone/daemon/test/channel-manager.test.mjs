import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { ChannelManager } from '../src/channel-manager.js';
import { getStoredTask, readAgentCursor, readDueMessages, readStoredMessages } from '../src/message-store.js';

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

function readJsonlMessages(workdir) {
  const messagesDir = path.join(workdir, 'messages');
  if (!existsSync(messagesDir)) return [];
  return readdirSync(messagesDir)
    .filter((entry) => entry.endsWith('.jsonl'))
    .flatMap((fileName) => readFileSync(path.join(messagesDir, fileName), 'utf8')
      .split('\n')
      .map((line) => line.trim())
      .filter(Boolean)
      .map((line) => JSON.parse(line)));
}

function readTraceEntries(node) {
  const tracePath = path.join(node.workdir, 'agents', node.agentName, 'trace', 'pending.jsonl');
  if (!existsSync(tracePath)) return [];
  return readFileSync(tracePath, 'utf8')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

function createDeferred() {
  let resolve;
  const promise = new Promise((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

function parseStdoutEvents(writes) {
  return writes
    .flatMap((chunk) => chunk.split('\n').map((line) => line.trim()).filter(Boolean))
    .map((line) => JSON.parse(line));
}

function insertLegacyDueMessage(db, node, {
  id,
  now,
  notBefore = now - 1,
  deliveryAttempts = 7,
  lastError = 'legacy_failure',
  correlationId = `corr-${id}`,
  payloadBody = { reason: 'legacy over limit' },
} = {}) {
  const createdAt = new Date(now).toISOString();
  const payload = { type: 'dispatch.self_check_due', body: payloadBody };
  const envelope = {
    id,
    sender: { kind: 'agent', id: node.agentName, name: node.agentName },
    audience: ['self'],
    correlation_id: correlationId,
    origin: 'self',
    not_before: notBefore,
    ts: now,
    ts_received: now,
  };
  const legacy = {
    messageId: id,
    channelId: node.channelId,
    senderKind: 'agent',
    senderType: 'channel_agent',
    senderId: node.agentName,
    senderName: node.agentName,
    messageType: payload.type,
    payloadType: payload.type,
    payloadBody,
    payload,
    content: JSON.stringify(payloadBody),
    correlationId,
    audience: ['self'],
    origin: 'self',
    notBefore,
    createdAt,
    source: 'test',
  };

  db.prepare(`
    INSERT INTO messages (
      id, channel_id, ts, ts_received, sender_kind, sender_id, sender_name,
      payload_type, payload_body, parent_id, correlation_id, task_id, thread_id,
      audience, mentions, origin, not_before, expires_at, delivered_at,
      delivery_failed_at, delivery_attempts, last_attempt_at, last_error,
      envelope_json, payload_json, legacy_json, created_at
    )
    VALUES (
      @id, @channel_id, @ts, @ts_received, @sender_kind, @sender_id, @sender_name,
      @payload_type, @payload_body, @parent_id, @correlation_id, @task_id, @thread_id,
      @audience, @mentions, @origin, @not_before, @expires_at, @delivered_at,
      @delivery_failed_at, @delivery_attempts, @last_attempt_at, @last_error,
      @envelope_json, @payload_json, @legacy_json, @created_at
    )
  `).run({
    id,
    channel_id: node.channelId,
    ts: now,
    ts_received: now,
    sender_kind: 'agent',
    sender_id: node.agentName,
    sender_name: node.agentName,
    payload_type: payload.type,
    payload_body: JSON.stringify(payloadBody),
    parent_id: null,
    correlation_id: correlationId,
    task_id: null,
    thread_id: null,
    audience: 'self',
    mentions: JSON.stringify(null),
    origin: 'self',
    not_before: notBefore,
    expires_at: null,
    delivered_at: null,
    delivery_failed_at: null,
    delivery_attempts: deliveryAttempts,
    last_attempt_at: null,
    last_error: lastError,
    envelope_json: JSON.stringify(envelope),
    payload_json: JSON.stringify(payload),
    legacy_json: JSON.stringify(legacy),
    created_at: createdAt,
  });
}

async function createUnreadQueryHarness(t, channelId, tempPrefix) {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), `${tempPrefix}-`));
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
    dueMessagePollMs: 0,
  });

  const created = await channelManager.createChannel({
    channelId,
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Unread Query Channel',
    type: 'xhs-creator',
    status: 'created',
  });

  return {
    channelManager,
    node: channelManager._requireNode(created.channel_id),
  };
}

async function appendUnreadQueryRows(channelManager, node, rows, baseTs) {
  for (const [index, row] of rows.entries()) {
    const isDeferred = row.notBefore != null;
    const payloadType = row.payloadType ?? (isDeferred ? 'agent.text' : 'user.text');
    await channelManager._appendMessage(node, {
      messageId: row.id,
      channelId: node.channelId,
      senderType: row.senderType ?? (isDeferred ? 'channel_agent' : 'human'),
      senderKind: row.senderKind ?? (isDeferred ? 'agent' : undefined),
      senderId: row.senderId ?? (isDeferred ? node.agentName : 'user-a'),
      senderName: row.senderName ?? (isDeferred ? node.agentName : 'User A'),
      messageType: payloadType,
      payloadType,
      payloadBody: row.payloadBody ?? (row.status != null
        ? { text: row.content ?? row.id, status: row.status }
        : undefined),
      content: row.content ?? row.id,
      audience: row.audience ?? (isDeferred ? ['self'] : undefined),
      origin: row.origin ?? (isDeferred ? 'self' : undefined),
      notBefore: row.notBefore,
      createdAt: new Date(baseTs + index).toISOString(),
      tsReceived: baseTs + index,
      source: 'test',
    });
  }
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

test('_appendMessage keeps jsonl and sqlite aligned when sqlite projection fails', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-sqlite-projection-failure-'));
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
    dueMessagePollMs: 0,
  });

  const created = await channelManager.createChannel({
    channelId: 'channel-projection-failure',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Projection Failure Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const node = channelManager._requireNode(created.channel_id);

  await assert.rejects(
    () => channelManager._appendMessage(node, {
      messageId: 'msg-orphan-task',
      channelId: node.channelId,
      senderType: 'channel_agent',
      senderKind: 'agent',
      senderId: node.agentName,
      senderName: node.agentName,
      messageType: 'task.opened',
      payload: {
        type: 'task.opened',
        body: {
          type: 'research',
          title: 'Orphan child',
          parent_task_id: 'missing-parent',
          doc_ref: 'notes/tasks/2026-05-06-orphan-child.md',
        },
      },
      content: 'Orphan child',
      taskId: 'task-orphan',
      audience: ['self'],
      origin: 'self',
      createdAt: new Date(Date.UTC(2026, 4, 6, 0, 0, 0)).toISOString(),
      source: 'test',
    }),
    /FOREIGN KEY|constraint/i,
  );

  assert.equal(readJsonlMessages(created.workdir).length, 0);
  assert.equal(readStoredMessages(channelManager._openMessageStore(node)).length, 0);
});

test('emitChannelMessage rejects unknown payload types before writing local truth', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-unknown-payload-'));
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
    dueMessagePollMs: 0,
  });
  const created = await channelManager.createChannel({
    channelId: 'channel-unknown-payload',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Unknown Payload Channel',
    type: 'xhs-creator',
    status: 'created',
  });

  await assert.rejects(
    () => channelManager.emitChannelMessage({
      channelId: created.channel_id,
      payloadType: 'whatever.unknown',
      payloadBody: { text: 'unsupported' },
      content: 'unsupported',
    }),
    (err) => err.code === 'bad_request' && /unsupported payload_type/.test(err.message),
  );
  assert.deepEqual(readdirSync(path.join(created.workdir, 'messages')).filter((entry) => entry.endsWith('.jsonl')), []);
});

test('delayed channel-audience messages are rejected before local writes', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-invalid-delayed-envelope-'));
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
    dueMessagePollMs: 0,
  });
  const created = await channelManager.createChannel({
    channelId: 'channel-invalid-delayed-envelope',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Invalid Delayed Envelope Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const node = channelManager._requireNode(created.channel_id);

  await assert.rejects(
    () => channelManager.scheduleChannelMessage({
      channelId: node.channelId,
      notBefore: Date.now() + 3_600_000,
      payloadBody: { reason: 'broadcast later' },
      audience: ['channel'],
    }),
    (err) => err.code === 'invalid_envelope' && err.statusCode === 400,
  );

  assert.equal(readJsonlMessages(created.workdir).length, 0);
  assert.equal(readStoredMessages(channelManager._openMessageStore(node)).length, 0);
});

test('scheduled task lifecycle payloads are rejected before local writes', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-scheduled-task-lifecycle-'));
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
    dueMessagePollMs: 0,
  });
  const created = await channelManager.createChannel({
    channelId: 'channel-scheduled-task-lifecycle',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Scheduled Task Lifecycle Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const node = channelManager._requireNode(created.channel_id);

  const notBefore = Date.now() + 3_600_000;
  const cases = [
    {
      payloadType: 'task.opened',
      taskId: 'task-open-later',
      payloadBody: {
        type: 'research',
        title: 'Open later',
        doc_ref: 'notes/tasks/2026-05-06-open-later.md',
      },
    },
    {
      payloadType: 'task.appended',
      taskId: 'task-append-later',
      payloadBody: { summary: 'append later' },
    },
    {
      payloadType: 'task.closed',
      taskId: 'task-close-later',
      payloadBody: { status: 'completed', summary: 'close later' },
    },
  ];

  for (const item of cases) {
    await assert.rejects(
      () => channelManager.scheduleChannelMessage({
        channelId: node.channelId,
        notBefore,
        payloadType: item.payloadType,
        payloadBody: item.payloadBody,
        taskId: item.taskId,
      }),
      (err) => err.code === 'invalid_envelope'
        && err.statusCode === 400
        && /task lifecycle payload cannot be scheduled/.test(err.message),
    );
  }

  assert.equal(readJsonlMessages(created.workdir).length, 0);
  assert.equal(readStoredMessages(channelManager._openMessageStore(node)).length, 0);
});

test('task close and append reject terminal tasks and direct emit cannot mutate projection', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-terminal-task-'));
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
    dueMessagePollMs: 0,
  });
  const created = await channelManager.createChannel({
    channelId: 'channel-terminal-task',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Terminal Task Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const node = channelManager._requireNode(created.channel_id);

  await channelManager.openTask({
    channelId: node.channelId,
    taskId: 'task-terminal',
    type: 'research',
    title: 'Terminal task',
    docRef: 'notes/tasks/2026-05-06-terminal.md',
  });
  await channelManager.closeTask({
    channelId: node.channelId,
    taskId: 'task-terminal',
    status: 'completed',
    summary: 'done',
  });
  const closedTask = getStoredTask(node.messageStore, 'task-terminal', node.channelId);

  await assert.rejects(
    () => channelManager.closeTask({
      channelId: node.channelId,
      taskId: 'task-terminal',
      status: 'failed',
      summary: 'late failure',
    }),
    (err) => err.code === 'task_already_terminal' && err.statusCode === 409,
  );
  await assert.rejects(
    () => channelManager.appendTask({
      channelId: node.channelId,
      taskId: 'task-terminal',
      summary: 'late append',
    }),
    (err) => err.code === 'task_already_terminal' && err.statusCode === 409,
  );
  await assert.rejects(
    () => channelManager.emitChannelMessage({
      channelId: node.channelId,
      messageId: 'msg-direct-terminal-close',
      payloadType: 'task.closed',
      payloadBody: { status: 'failed', summary: 'direct close' },
      content: 'direct close',
      taskId: 'task-terminal',
      audience: ['self'],
      origin: 'self',
    }),
    (err) => err.code === 'task_already_terminal' && err.statusCode === 409,
  );

  const task = getStoredTask(node.messageStore, 'task-terminal', node.channelId);
  assert.equal(task.status, 'completed');
  assert.equal(task.closed_at, closedTask.closed_at);
  assert.equal(task.last_event_at, closedTask.last_event_at);
  assert.equal(readStoredMessages(node.messageStore).length, 2);
});

test('message.schedule and due processing deliver only due D messages once', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-due-messages-'));
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
    dueMessagePollMs: 0,
  });

  const created = await channelManager.createChannel({
    channelId: 'channel-due',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Due Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const node = channelManager._requireNode(created.channel_id);
  const deliveredLines = [];
  node.status = 'active';
  node.proc = { stdin: { write: (line) => deliveredLines.push(JSON.parse(line)) } };

  const now = Date.UTC(2026, 4, 6, 1, 0, 0);
  await channelManager.scheduleChannelMessage({
    channelId: node.channelId,
    notBefore: now - 1,
    correlationId: 'corr-due',
    payloadBody: { reason: 'due now' },
  });
  await channelManager.scheduleChannelMessage({
    channelId: node.channelId,
    notBefore: now + 60_000,
    correlationId: 'corr-later',
    payloadBody: { reason: 'later' },
  });

  const first = await channelManager.processDueMessages(node.channelId, now);
  const second = await channelManager.processDueMessages(node.channelId, now);

  assert.equal(first.delivered.length, 1);
  assert.equal(first.delivered[0].decision, 'react');
  assert.equal(second.delivered.length, 0);
  assert.equal(deliveredLines.length, 1);
  assert.equal(deliveredLines[0].event.payload.message.correlationId, 'corr-due');

  const stored = readStoredMessages(node.messageStore);
  const due = stored.find((message) => message.correlation_id === 'corr-due');
  const later = stored.find((message) => message.correlation_id === 'corr-later');
  assert.equal(typeof due.delivered_at, 'number');
  assert.equal(later.delivered_at, null);
  assert.equal(readAgentCursor(node.workdir, node.agentName).last_seen_seq, due.seq);
});

test('react delivery advances per-agent cursor after stdin accepts the event', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-react-cursor-'));
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
    dueMessagePollMs: 0,
  });

  const created = await channelManager.createChannel({
    channelId: 'channel-react-cursor',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'React Cursor Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const node = channelManager._requireNode(created.channel_id);
  const deliveredLines = [];
  node.status = 'active';
  node.proc = { stdin: { write: (line) => deliveredLines.push(JSON.parse(line)) } };

  const result = await channelManager.handleEvent({
    channelId: node.channelId,
    event: {
      type: 'user.message.posted',
      source: 'test',
      payload: {
        senderType: 'human',
        senderId: 'user-a',
        senderName: 'User A',
        content: 'wake the agent',
      },
    },
  });

  assert.equal(result.decision, 'react');
  assert.equal(deliveredLines.length, 1);
  const stored = readStoredMessages(node.messageStore);
  assert.equal(stored.length, 1);
  assert.equal(readAgentCursor(node.workdir, node.agentName).last_seen_seq, stored[0].seq);
});

test('react delivery failure leaves per-agent cursor unchanged', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-react-cursor-failed-'));
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
    dueMessagePollMs: 0,
  });

  const created = await channelManager.createChannel({
    channelId: 'channel-react-cursor-failed',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'React Cursor Failed Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const node = channelManager._requireNode(created.channel_id);
  node.status = 'active';
  node.proc = {};

  const result = await channelManager.handleEvent({
    channelId: node.channelId,
    event: {
      type: 'user.message.posted',
      source: 'test',
      payload: {
        senderType: 'human',
        senderId: 'user-a',
        senderName: 'User A',
        content: 'wake the agent',
      },
    },
  });

  assert.equal(result.decision, 'react');
  assert.equal(readStoredMessages(node.messageStore).length, 1);
  assert.equal(readAgentCursor(node.workdir, node.agentName).last_seen_seq, 0);
});

test('stdin write throw during react delivery leaves per-agent cursor unchanged', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-react-cursor-throw-'));
  t.after(() => {
    rmSync(tempHome, { recursive: true, force: true });
  });

  const previousError = console.error;
  const errorLines = [];
  console.error = (...args) => {
    errorLines.push(args.map((item) => String(item)).join(' '));
  };
  t.after(() => {
    console.error = previousError;
  });

  const channelManager = new ChannelManager({
    serverUrl: 'http://localhost:3001',
    machineApiKey: 'machine-key',
    daemonSocketPath: path.join(tempHome, '.coagent', 'daemon.sock'),
    daemonHttpUrl: 'http://127.0.0.1:3002',
    daemonToken: 'daemon-token',
    baseDir: path.join(tempHome, 'coagent'),
    dueMessagePollMs: 0,
  });

  const created = await channelManager.createChannel({
    channelId: 'channel-react-cursor-throw',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'React Cursor Throw Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const node = channelManager._requireNode(created.channel_id);
  node.status = 'active';
  node.proc = {
    stdin: {
      write: () => {
        throw new Error('stdin boom');
      },
    },
  };

  const result = await channelManager.handleEvent({
    channelId: node.channelId,
    event: {
      type: 'user.message.posted',
      source: 'test',
      payload: {
        senderType: 'human',
        senderId: 'user-a',
        senderName: 'User A',
        content: 'wake the agent',
      },
    },
  });

  assert.equal(result.decision, 'react');
  assert.match(errorLines.join('\n'), /stdin boom/);
  assert.equal(readStoredMessages(node.messageStore).length, 1);
  assert.equal(readAgentCursor(node.workdir, node.agentName).last_seen_seq, 0);
});

test('due processing keeps inactive channel messages unread and records failed attempt', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-due-inactive-'));
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
    dueMessagePollMs: 0,
  });

  const created = await channelManager.createChannel({
    channelId: 'channel-due-inactive',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Due Inactive Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const node = channelManager._requireNode(created.channel_id);
  const now = Date.UTC(2026, 4, 6, 1, 0, 0);
  await channelManager.scheduleChannelMessage({
    channelId: node.channelId,
    notBefore: now - 1,
    correlationId: 'corr-inactive',
    payloadBody: { reason: 'due while inactive' },
  });

  const result = await channelManager.processDueMessages(node.channelId, now);

  assert.equal(result.delivered.length, 0);
  assert.equal(result.failed.length, 1);
  assert.equal(result.failed[0].reason, 'channel_inactive');
  const stored = readStoredMessages(node.messageStore).find((message) => message.correlation_id === 'corr-inactive');
  assert.equal(stored.delivered_at, null);
  assert.equal(stored.delivery_attempts, 1);
  assert.equal(stored.last_error, 'channel_inactive');
});

test('due processing dead-letters permanent delivery failures after the retry limit', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-due-dead-letter-'));
  t.after(() => {
    rmSync(tempHome, { recursive: true, force: true });
  });

  const stdoutWrites = captureStdoutWrites(t);
  const previousError = console.error;
  const errorLines = [];
  console.error = (...args) => {
    errorLines.push(args.map((item) => String(item)).join(' '));
  };
  t.after(() => {
    console.error = previousError;
  });

  const channelManager = new ChannelManager({
    serverUrl: 'http://localhost:3001',
    machineApiKey: 'machine-key',
    daemonSocketPath: path.join(tempHome, '.coagent', 'daemon.sock'),
    daemonHttpUrl: 'http://127.0.0.1:3002',
    daemonToken: 'daemon-token',
    baseDir: path.join(tempHome, 'coagent'),
    dueMessagePollMs: 0,
  });

  const created = await channelManager.createChannel({
    channelId: 'channel-due-dead-letter',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Due Dead Letter Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const node = channelManager._requireNode(created.channel_id);
  node.status = 'active';

  let deliveryCalls = 0;
  channelManager._deliverEvent = async () => {
    deliveryCalls += 1;
    return { ok: false, reason: 'permanent_failure' };
  };

  const now = Date.UTC(2026, 4, 6, 1, 0, 0);
  await channelManager.scheduleChannelMessage({
    channelId: node.channelId,
    messageId: 'message-dead-letter',
    notBefore: now - 1,
    correlationId: 'corr-dead-letter',
    payloadBody: { reason: 'permanent failure' },
  });

  const attemptsByPoll = [];
  for (let index = 0; index < 6; index += 1) {
    const result = await channelManager.processDueMessages(node.channelId, now + index);
    attemptsByPoll.push(result.failed[0]?.attempts ?? null);
  }

  assert.deepEqual(attemptsByPoll, [1, 2, 3, 4, 5, null]);
  assert.equal(deliveryCalls, 5);

  const stored = readStoredMessages(node.messageStore).find((message) => message.id === 'message-dead-letter');
  assert.equal(stored.delivered_at, null);
  assert.equal(stored.delivery_attempts, 5);
  assert.equal(stored.last_error, 'permanent_failure');
  assert.equal(typeof stored.delivery_failed_at, 'number');
  assert.equal(readDueMessages(node.messageStore, now + 6, 100).some((message) => message.id === 'message-dead-letter'), false);

  const tracePath = path.join(node.workdir, 'agents', node.agentName, 'trace', 'pending.jsonl');
  const traceEntries = readFileSync(tracePath, 'utf8')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line));
  const deadLetterEntries = traceEntries.filter((entry) => entry.event === 'delivery_dead_letter');
  assert.equal(deadLetterEntries.length, 1);
  assert.equal(deadLetterEntries[0].message_id, 'message-dead-letter');
  assert.equal(deadLetterEntries[0].last_error, 'permanent_failure');
  assert.equal(deadLetterEntries[0].attempts, 5);

  const stdoutEvents = stdoutWrites
    .flatMap((chunk) => chunk.split('\n').map((line) => line.trim()).filter(Boolean))
    .map((line) => JSON.parse(line));
  const inboxEvents = stdoutEvents.filter((entry) => entry.event === 'inbox.created');
  assert.equal(inboxEvents.length, 1);
  assert.equal(inboxEvents[0].severity, 'blocker');
  assert.equal(inboxEvents[0].reason, 'incident');
  assert.equal(inboxEvents[0].body.message_id, 'message-dead-letter');
  assert.equal(inboxEvents[0].body.last_error, 'permanent_failure');
  assert.equal(errorLines.filter((line) => line.includes('Message delivery dead-lettered')).length, 1);
});

test('due processing dead-letters legacy over-limit messages on the next poll', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-due-dead-letter-legacy-'));
  t.after(() => {
    rmSync(tempHome, { recursive: true, force: true });
  });

  const stdoutWrites = captureStdoutWrites(t);
  const previousError = console.error;
  const errorLines = [];
  console.error = (...args) => {
    errorLines.push(args.map((item) => String(item)).join(' '));
  };
  t.after(() => {
    console.error = previousError;
  });

  const channelManager = new ChannelManager({
    serverUrl: 'http://localhost:3001',
    machineApiKey: 'machine-key',
    daemonSocketPath: path.join(tempHome, '.coagent', 'daemon.sock'),
    daemonHttpUrl: 'http://127.0.0.1:3002',
    daemonToken: 'daemon-token',
    baseDir: path.join(tempHome, 'coagent'),
    dueMessagePollMs: 0,
  });

  const created = await channelManager.createChannel({
    channelId: 'channel-due-dead-letter-legacy',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Due Dead Letter Legacy Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const node = channelManager._requireNode(created.channel_id);
  node.status = 'active';

  let deliveryCalls = 0;
  channelManager._deliverEvent = async () => {
    deliveryCalls += 1;
    return { ok: false, reason: 'permanent_failure' };
  };

  const now = Date.UTC(2026, 4, 6, 1, 0, 0);
  insertLegacyDueMessage(channelManager._openMessageStore(node), node, {
    id: 'message-dead-letter-legacy',
    now,
    deliveryAttempts: 7,
    lastError: 'legacy_failure',
  });

  const result = await channelManager.processDueMessages(node.channelId, now);
  const second = await channelManager.processDueMessages(node.channelId, now + 1);
  const third = await channelManager.processDueMessages(node.channelId, now + 2);

  assert.equal(result.delivered.length, 0);
  assert.equal(result.failed.length, 1);
  assert.equal(second.delivered.length, 0);
  assert.equal(second.failed.length, 0);
  assert.equal(third.delivered.length, 0);
  assert.equal(third.failed.length, 0);
  assert.equal(result.failed[0].attempts, 8);
  assert.equal(result.failed[0].last_error, 'permanent_failure');
  assert.equal(typeof result.failed[0].delivery_failed_at, 'number');
  assert.equal(deliveryCalls, 1);

  const stored = readStoredMessages(node.messageStore).find((message) => message.id === 'message-dead-letter-legacy');
  assert.equal(stored.delivered_at, null);
  assert.equal(stored.delivery_attempts, 8);
  assert.equal(stored.last_error, 'permanent_failure');
  assert.equal(typeof stored.delivery_failed_at, 'number');
  assert.equal(readDueMessages(node.messageStore, now + 1, 100).some((message) => message.id === 'message-dead-letter-legacy'), false);

  const deadLetterEntries = readTraceEntries(node).filter((entry) => entry.event === 'delivery_dead_letter');
  assert.equal(deadLetterEntries.length, 1);
  assert.equal(deadLetterEntries[0].message_id, 'message-dead-letter-legacy');
  assert.equal(deadLetterEntries[0].last_error, 'permanent_failure');
  assert.equal(deadLetterEntries[0].attempts, 8);

  const inboxEvents = parseStdoutEvents(stdoutWrites).filter((entry) => entry.event === 'inbox.created');
  assert.equal(inboxEvents.length, 1);
  assert.equal(inboxEvents[0].body.message_id, 'message-dead-letter-legacy');
  assert.equal(inboxEvents[0].body.last_error, 'permanent_failure');
  assert.equal(errorLines.filter((line) => line.includes('Message delivery dead-lettered')).length, 1);
});

test('due processing dead-letter side effects fire exactly once across overlapping polls', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-due-dead-letter-once-'));
  t.after(() => {
    rmSync(tempHome, { recursive: true, force: true });
  });

  const stdoutWrites = captureStdoutWrites(t);
  const previousError = console.error;
  const errorLines = [];
  console.error = (...args) => {
    errorLines.push(args.map((item) => String(item)).join(' '));
  };
  t.after(() => {
    console.error = previousError;
  });

  const channelManager = new ChannelManager({
    serverUrl: 'http://localhost:3001',
    machineApiKey: 'machine-key',
    daemonSocketPath: path.join(tempHome, '.coagent', 'daemon.sock'),
    daemonHttpUrl: 'http://127.0.0.1:3002',
    daemonToken: 'daemon-token',
    baseDir: path.join(tempHome, 'coagent'),
    dueMessagePollMs: 0,
  });

  const created = await channelManager.createChannel({
    channelId: 'channel-due-dead-letter-once',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Due Dead Letter Once Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const node = channelManager._requireNode(created.channel_id);
  node.status = 'active';

  let releaseDelivery;
  let deliveryCalls = 0;
  const deliveryGate = new Promise((resolve) => {
    releaseDelivery = resolve;
  });
  channelManager._dispatchDueMessage = async () => {
    deliveryCalls += 1;
    await deliveryGate;
    return { ok: false, reason: 'permanent_failure' };
  };

  const now = Date.UTC(2026, 4, 6, 1, 0, 0);
  insertLegacyDueMessage(channelManager._openMessageStore(node), node, {
    id: 'message-dead-letter-once',
    now,
    deliveryAttempts: 7,
    lastError: 'legacy_failure',
  });

  const polls = [
    channelManager.processDueMessages(node.channelId, now),
    channelManager.processDueMessages(node.channelId, now + 1),
    channelManager.processDueMessages(node.channelId, now + 2),
  ];
  assert.equal(deliveryCalls, 3);
  releaseDelivery();
  const results = await Promise.all(polls);

  assert.equal(results.flatMap((result) => result.delivered).length, 0);
  assert.equal(results.flatMap((result) => result.failed).length, 3);
  assert.equal(deliveryCalls, 3);

  const stored = readStoredMessages(node.messageStore).find((message) => message.id === 'message-dead-letter-once');
  assert.equal(stored.delivered_at, null);
  assert.equal(stored.delivery_attempts, 10);
  assert.equal(stored.last_error, 'permanent_failure');
  assert.equal(typeof stored.delivery_failed_at, 'number');
  assert.equal(readDueMessages(node.messageStore, now + 3, 100).some((message) => message.id === 'message-dead-letter-once'), false);

  const deadLetterEntries = readTraceEntries(node).filter((entry) => entry.event === 'delivery_dead_letter');
  assert.equal(deadLetterEntries.length, 1);
  assert.equal(deadLetterEntries[0].message_id, 'message-dead-letter-once');

  const inboxEvents = parseStdoutEvents(stdoutWrites).filter((entry) => entry.event === 'inbox.created');
  assert.equal(inboxEvents.length, 1);
  assert.equal(inboxEvents[0].body.message_id, 'message-dead-letter-once');
  assert.equal(errorLines.filter((line) => line.includes('Message delivery dead-lettered')).length, 1);
});

test('message.query --unread does not advance cursor and repeats until explicit ack', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-unread-cursor-'));
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
    dueMessagePollMs: 0,
  });

  const created = await channelManager.createChannel({
    channelId: 'channel-unread',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Unread Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const node = channelManager._requireNode(created.channel_id);
  await channelManager._appendMessage(node, {
    messageId: 'message-unread-1',
    channelId: node.channelId,
    senderType: 'human',
    senderId: 'user-a',
    senderName: 'User A',
    content: 'first unread',
    createdAt: new Date(Date.UTC(2026, 4, 6, 1, 0, 0)).toISOString(),
    source: 'test',
  });

  const first = await channelManager.queryChannelMessages({ channelId: node.channelId, unread: true, order: 'asc' });
  const second = await channelManager.queryChannelMessages({ channelId: node.channelId, unread: true, order: 'asc' });

  assert.deepEqual(first.messages.map((message) => message.id), ['message-unread-1']);
  assert.deepEqual(second.messages.map((message) => message.id), ['message-unread-1']);
  assert.equal(readAgentCursor(node.workdir, node.agentName).last_seen_seq, 0);

  const ack = await channelManager.ackChannelMessages({
    channelId: node.channelId,
    untilSeq: first.messages[0].seq,
  });
  const afterAck = await channelManager.queryChannelMessages({ channelId: node.channelId, unread: true, order: 'asc' });

  assert.equal(ack.last_seen_seq, first.messages[0].seq);
  assert.equal(ack.advanced, true);
  assert.equal(afterAck.messages.length, 0);
});

test('message.query --unread limit is peek-only until explicit ack advances cursor', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-unread-limit-cursor-'));
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
    dueMessagePollMs: 0,
  });

  const created = await channelManager.createChannel({
    channelId: 'channel-unread-limit',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Unread Limit Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const node = channelManager._requireNode(created.channel_id);
  const baseTs = Date.UTC(2026, 4, 6, 1, 0, 0);

  for (let index = 0; index < 25; index += 1) {
    await channelManager._appendMessage(node, {
      messageId: `message-unread-limit-${index}`,
      channelId: node.channelId,
      senderType: 'human',
      senderId: 'user-a',
      senderName: 'User A',
      content: `unread ${index}`,
      createdAt: new Date(baseTs + index).toISOString(),
      tsReceived: baseTs + index,
      source: 'test',
    });
  }

  const first = await channelManager.queryChannelMessages({ channelId: node.channelId, unread: true, limit: 20 });
  const second = await channelManager.queryChannelMessages({ channelId: node.channelId, unread: true, limit: 20 });

  assert.deepEqual(
    first.messages.map((message) => message.id),
    Array.from({ length: 20 }, (_, index) => `message-unread-limit-${index}`),
  );
  assert.deepEqual(
    second.messages.map((message) => message.id),
    Array.from({ length: 20 }, (_, index) => `message-unread-limit-${index}`),
  );
  assert.equal(readAgentCursor(node.workdir, node.agentName).last_seen_seq, 0);

  await channelManager.ackChannelMessages({
    channelId: node.channelId,
    untilSeq: first.messages.at(-1).seq,
  });
  const afterAck = await channelManager.queryChannelMessages({ channelId: node.channelId, unread: true, limit: 20 });
  assert.deepEqual(
    afterAck.messages.map((message) => message.id),
    Array.from({ length: 5 }, (_, index) => `message-unread-limit-${index + 20}`),
  );
  assert.equal(readAgentCursor(node.workdir, node.agentName).last_seen_seq, first.messages.at(-1).seq);
});

test('message.ack through_message_id cumulatively advances cursor to the stored message sequence', async (t) => {
  const { channelManager, node } = await createUnreadQueryHarness(
    t,
    'channel-ack-through-message-id',
    'channel-manager-ack-through-message-id',
  );
  const baseTs = Date.UTC(2026, 4, 6, 1, 0, 0);
  const rows = [
    { id: 'message-ack-id-1' },
    { id: 'message-ack-id-2' },
    { id: 'message-ack-id-3' },
  ];
  await appendUnreadQueryRows(channelManager, node, rows, baseTs);
  const beforeAck = await channelManager.queryChannelMessages({
    channelId: node.channelId,
    unread: true,
    limit: 10,
  });
  const targetSeq = beforeAck.messages.find((message) => message.id === 'message-ack-id-2').seq;

  const ack = await channelManager.ackChannelMessages({
    channelId: node.channelId,
    throughMessageId: 'message-ack-id-2',
  });
  const unread = await channelManager.queryChannelMessages({
    channelId: node.channelId,
    unread: true,
    limit: 10,
  });

  assert.equal(ack.through_message_id, 'message-ack-id-2');
  assert.equal(ack.acked_through_seq, targetSeq);
  assert.equal(ack.previous_last_seen_seq, 0);
  assert.equal(ack.last_seen_seq, targetSeq);
  assert.deepEqual(unread.messages.map((message) => message.id), ['message-ack-id-3']);
});

test('message.ack rejects malformed until_seq values without advancing cursor', async (t) => {
  const { channelManager, node } = await createUnreadQueryHarness(
    t,
    'channel-ack-bad-seq',
    'channel-manager-ack-bad-seq',
  );
  const baseTs = Date.UTC(2026, 4, 6, 1, 0, 0);
  await appendUnreadQueryRows(channelManager, node, [{ id: 'message-ack-bad-seq-1' }], baseTs);

  const cases = [
    ['zero', 0],
    ['negative', -1],
    ['fraction', '1.5'],
    ['suffix', '1abc'],
  ];

  for (const [name, untilSeq] of cases) {
    await assert.rejects(
      () => channelManager.ackChannelMessages({
        channelId: node.channelId,
        untilSeq,
      }),
      (err) => {
        assert.equal(err.code, 'bad_request', name);
        assert.match(err.message, /until_seq must be a positive integer/, name);
        return true;
      },
    );
    assert.equal(readAgentCursor(node.workdir, node.agentName).last_seen_seq, 0, name);
  }
});

test('message.ack rejects future until_seq without advancing cursor', async (t) => {
  const { channelManager, node } = await createUnreadQueryHarness(
    t,
    'channel-ack-future-seq',
    'channel-manager-ack-future-seq',
  );
  const baseTs = Date.UTC(2026, 4, 6, 1, 0, 0);
  await appendUnreadQueryRows(channelManager, node, [{ id: 'message-ack-future-seq-1' }], baseTs);
  const existing = await channelManager.queryChannelMessages({
    channelId: node.channelId,
    unread: true,
    limit: 10,
  });

  await assert.rejects(
    () => channelManager.ackChannelMessages({
      channelId: node.channelId,
      untilSeq: existing.messages[0].seq + 1,
    }),
    (err) => {
      assert.equal(err.code, 'bad_request');
      assert.match(err.message, /until_seq must not exceed current max seq/);
      return true;
    },
  );
  assert.equal(readAgentCursor(node.workdir, node.agentName).last_seen_seq, 0);
});

test('message.ack rejects invalid argument combinations and unknown through_message_id', async (t) => {
  const { channelManager, node } = await createUnreadQueryHarness(
    t,
    'channel-ack-invalid-args',
    'channel-manager-ack-invalid-args',
  );
  const baseTs = Date.UTC(2026, 4, 6, 1, 0, 0);
  await appendUnreadQueryRows(channelManager, node, [{ id: 'message-ack-invalid-args-1' }], baseTs);

  await assert.rejects(
    () => channelManager.ackChannelMessages({ channelId: node.channelId }),
    (err) => {
      assert.equal(err.code, 'bad_request');
      assert.match(err.message, /exactly one of until_seq or through_message_id is required/);
      return true;
    },
  );
  await assert.rejects(
    () => channelManager.ackChannelMessages({
      channelId: node.channelId,
      untilSeq: 1,
      throughMessageId: 'message-ack-invalid-args-1',
    }),
    (err) => {
      assert.equal(err.code, 'bad_request');
      assert.match(err.message, /exactly one of until_seq or through_message_id is required/);
      return true;
    },
  );
  await assert.rejects(
    () => channelManager.ackChannelMessages({
      channelId: node.channelId,
      throughMessageId: 'message-ack-missing',
    }),
    (err) => {
      assert.equal(err.code, 'not_found');
      assert.match(err.message, /message not found: message-ack-missing/);
      return true;
    },
  );
  assert.equal(readAgentCursor(node.workdir, node.agentName).last_seen_seq, 0);
});

test('message.query --unread with content filters peeks without advancing cursor', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-unread-filter-cursor-'));
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
    dueMessagePollMs: 0,
  });

  const created = await channelManager.createChannel({
    channelId: 'channel-unread-filter',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Unread Filter Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const node = channelManager._requireNode(created.channel_id);
  const baseTs = Date.UTC(2026, 4, 6, 1, 0, 0);
  const rows = [
    { id: 'message-unread-filter-1', content: 'alpha', payloadType: 'user.text' },
    { id: 'message-unread-filter-2', content: 'dispatch done 1', payloadType: 'dispatch.completed' },
    { id: 'message-unread-filter-3', content: 'beta', payloadType: 'user.text' },
    { id: 'message-unread-filter-4', content: 'dispatch done 2', payloadType: 'dispatch.completed' },
    { id: 'message-unread-filter-5', content: 'gamma', payloadType: 'user.text' },
  ];

  for (const [index, row] of rows.entries()) {
    await channelManager._appendMessage(node, {
      messageId: row.id,
      channelId: node.channelId,
      senderType: 'human',
      senderId: 'user-a',
      senderName: 'User A',
      messageType: row.payloadType,
      payloadType: row.payloadType,
      content: row.content,
      createdAt: new Date(baseTs + index).toISOString(),
      tsReceived: baseTs + index,
      source: 'test',
    });
  }

  const dispatchMessages = await channelManager.queryChannelMessages({
    channelId: node.channelId,
    unread: true,
    payload_type: 'dispatch.completed',
    limit: 10,
  });
  assert.deepEqual(
    dispatchMessages.messages.map((message) => message.id),
    ['message-unread-filter-2', 'message-unread-filter-4'],
  );
  assert.equal(readAgentCursor(node.workdir, node.agentName).last_seen_seq, 0);

  const alphaMessages = await channelManager.queryChannelMessages({
    channelId: node.channelId,
    unread: true,
    text: 'alpha',
    limit: 10,
  });
  assert.deepEqual(alphaMessages.messages.map((message) => message.id), ['message-unread-filter-1']);
  assert.equal(readAgentCursor(node.workdir, node.agentName).last_seen_seq, 0);

  const allUnread = await channelManager.queryChannelMessages({
    channelId: node.channelId,
    unread: true,
    limit: 10,
  });
  assert.deepEqual(allUnread.messages.map((message) => message.id), rows.map((row) => row.id));
  assert.equal(readAgentCursor(node.workdir, node.agentName).last_seen_seq, 0);
});

test('message.query --unread with status all does not advance cursor', async (t) => {
  const { channelManager, node } = await createUnreadQueryHarness(
    t,
    'channel-unread-status-all',
    'channel-manager-unread-status-all',
  );
  const baseTs = Date.UTC(2026, 4, 6, 1, 0, 0);
  const rows = [
    { id: 'message-unread-status-all-1', status: 'active' },
    { id: 'message-unread-status-all-2', status: 'done' },
    { id: 'message-unread-status-all-3', status: 'active' },
  ];
  await appendUnreadQueryRows(channelManager, node, rows, baseTs);

  const allStatusMessages = await channelManager.queryChannelMessages({
    channelId: node.channelId,
    unread: true,
    status: 'all',
    limit: 10,
  });
  assert.deepEqual(allStatusMessages.messages.map((message) => message.id), rows.map((row) => row.id));
  assert.equal(readAgentCursor(node.workdir, node.agentName).last_seen_seq, 0);

  const repeated = await channelManager.queryChannelMessages({
    channelId: node.channelId,
    unread: true,
    status: 'all',
    limit: 10,
  });
  assert.deepEqual(repeated.messages.map((message) => message.id), rows.map((row) => row.id));
});

test('message.query --unread with status filter peeks without advancing cursor', async (t) => {
  const { channelManager, node } = await createUnreadQueryHarness(
    t,
    'channel-unread-status-active',
    'channel-manager-unread-status-active',
  );
  const baseTs = Date.UTC(2026, 4, 6, 1, 0, 0);
  const rows = [
    { id: 'message-unread-status-active-1', status: 'active' },
    { id: 'message-unread-status-active-2', status: 'done' },
    { id: 'message-unread-status-active-3', status: 'active' },
  ];
  await appendUnreadQueryRows(channelManager, node, rows, baseTs);

  const activeMessages = await channelManager.queryChannelMessages({
    channelId: node.channelId,
    unread: true,
    status: 'active',
    limit: 10,
  });
  assert.deepEqual(
    activeMessages.messages.map((message) => message.id),
    ['message-unread-status-active-1', 'message-unread-status-active-3'],
  );
  assert.equal(readAgentCursor(node.workdir, node.agentName).last_seen_seq, 0);

  const allUnread = await channelManager.queryChannelMessages({
    channelId: node.channelId,
    unread: true,
    limit: 10,
  });
  assert.deepEqual(allUnread.messages.map((message) => message.id), rows.map((row) => row.id));
  assert.equal(readAgentCursor(node.workdir, node.agentName).last_seen_seq, 0);
});

test('message.query --unread with empty status values does not advance cursor', async (t) => {
  for (const [label, status] of [['empty', ''], ['null', null]]) {
    const { channelManager, node } = await createUnreadQueryHarness(
      t,
      `channel-unread-status-${label}`,
      `channel-manager-unread-status-${label}`,
    );
    const baseTs = Date.UTC(2026, 4, 6, 1, 0, 0);
    const rows = [
      { id: `message-unread-status-${label}-1` },
      { id: `message-unread-status-${label}-2` },
      { id: `message-unread-status-${label}-3` },
    ];
    await appendUnreadQueryRows(channelManager, node, rows, baseTs);

    const messages = await channelManager.queryChannelMessages({
      channelId: node.channelId,
      unread: true,
      status,
      limit: 10,
    });
    assert.deepEqual(messages.messages.map((message) => message.id), rows.map((row) => row.id));
    assert.equal(readAgentCursor(node.workdir, node.agentName).last_seen_seq, 0);
  }
});

test('message.query --unread with audience peeks without advancing cursor', async (t) => {
  const { channelManager, node } = await createUnreadQueryHarness(
    t,
    'channel-unread-audience-filter',
    'channel-manager-unread-audience-filter',
  );
  const baseTs = Date.UTC(2026, 4, 6, 1, 0, 0);
  const rows = [
    { id: 'message-unread-audience-1', audience: ['*'] },
    { id: 'message-unread-audience-2', audience: ['agent:foo'] },
    { id: 'message-unread-audience-3', audience: ['*'] },
    { id: 'message-unread-audience-4', audience: ['agent:foo'] },
    { id: 'message-unread-audience-5', audience: ['*'] },
  ];
  await appendUnreadQueryRows(channelManager, node, rows, baseTs);

  const audienceMessages = await channelManager.queryChannelMessages({
    channelId: node.channelId,
    unread: true,
    audience: 'agent:foo',
    limit: 10,
  });
  assert.deepEqual(
    audienceMessages.messages.map((message) => message.id),
    ['message-unread-audience-2', 'message-unread-audience-4'],
  );
  assert.equal(readAgentCursor(node.workdir, node.agentName).last_seen_seq, 0);

  const allUnread = await channelManager.queryChannelMessages({
    channelId: node.channelId,
    unread: true,
    limit: 10,
  });
  assert.deepEqual(allUnread.messages.map((message) => message.id), rows.map((row) => row.id));
  assert.equal(readAgentCursor(node.workdir, node.agentName).last_seen_seq, 0);
});

test('message.query --unread with not_before_lte peeks without advancing cursor', async (t) => {
  const { channelManager, node } = await createUnreadQueryHarness(
    t,
    'channel-unread-not-before-lte-filter',
    'channel-manager-unread-not-before-lte-filter',
  );
  const baseTs = Date.UTC(2026, 4, 6, 1, 0, 0);
  const now = baseTs + 10_000;
  const rows = [
    { id: 'message-unread-not-before-lte-1', notBefore: now - 3_000 },
    { id: 'message-unread-not-before-lte-2', notBefore: now - 2_000 },
    { id: 'message-unread-not-before-lte-3', notBefore: now + 1_000 },
    { id: 'message-unread-not-before-lte-4', notBefore: now - 1_000 },
    { id: 'message-unread-not-before-lte-5', notBefore: now + 2_000 },
  ];
  await appendUnreadQueryRows(channelManager, node, rows, baseTs);

  const dueMessages = await channelManager.queryChannelMessages({
    channelId: node.channelId,
    unread: true,
    not_before_lte: now,
    nowMs: now,
    limit: 10,
  });
  assert.deepEqual(
    dueMessages.messages.map((message) => message.id),
    [
      'message-unread-not-before-lte-1',
      'message-unread-not-before-lte-2',
      'message-unread-not-before-lte-4',
    ],
  );
  assert.equal(readAgentCursor(node.workdir, node.agentName).last_seen_seq, 0);

  const allUnreadLater = await channelManager.queryChannelMessages({
    channelId: node.channelId,
    unread: true,
    nowMs: now + 3_000,
    limit: 10,
  });
  assert.deepEqual(allUnreadLater.messages.map((message) => message.id), rows.map((row) => row.id));
  assert.equal(readAgentCursor(node.workdir, node.agentName).last_seen_seq, 0);
});

test('message.query --unread with not_before_gte peeks without advancing cursor', async (t) => {
  const { channelManager, node } = await createUnreadQueryHarness(
    t,
    'channel-unread-not-before-gte-filter',
    'channel-manager-unread-not-before-gte-filter',
  );
  const baseTs = Date.UTC(2026, 4, 6, 1, 0, 0);
  const now = baseTs + 10_000;
  const rows = [
    { id: 'message-unread-not-before-gte-1', notBefore: now - 3_000 },
    { id: 'message-unread-not-before-gte-2', notBefore: now + 1_000 },
    { id: 'message-unread-not-before-gte-3', notBefore: now - 2_000 },
    { id: 'message-unread-not-before-gte-4', notBefore: now + 2_000 },
    { id: 'message-unread-not-before-gte-5', notBefore: now - 1_000 },
  ];
  await appendUnreadQueryRows(channelManager, node, rows, baseTs);

  const futureMessages = await channelManager.queryChannelMessages({
    channelId: node.channelId,
    unread: true,
    not_before_gte: now,
    nowMs: now + 3_000,
    limit: 10,
  });
  assert.deepEqual(
    futureMessages.messages.map((message) => message.id),
    ['message-unread-not-before-gte-2', 'message-unread-not-before-gte-4'],
  );
  assert.equal(readAgentCursor(node.workdir, node.agentName).last_seen_seq, 0);

  const allUnreadLater = await channelManager.queryChannelMessages({
    channelId: node.channelId,
    unread: true,
    nowMs: now + 3_000,
    limit: 10,
  });
  assert.deepEqual(allUnreadLater.messages.map((message) => message.id), rows.map((row) => row.id));
  assert.equal(readAgentCursor(node.workdir, node.agentName).last_seen_seq, 0);
});

test('message.query --unread with unknown filter key peeks without advancing cursor', async (t) => {
  const { channelManager, node } = await createUnreadQueryHarness(
    t,
    'channel-unread-unknown-filter',
    'channel-manager-unread-unknown-filter',
  );
  const baseTs = Date.UTC(2026, 4, 6, 1, 0, 0);
  const rows = [
    { id: 'message-unread-unknown-1' },
    { id: 'message-unread-unknown-2' },
    { id: 'message-unread-unknown-3' },
  ];
  await appendUnreadQueryRows(channelManager, node, rows, baseTs);

  const peek = await channelManager.queryChannelMessages({
    channelId: node.channelId,
    unread: true,
    __future_filter: 'agent:foo',
    limit: 10,
  });
  assert.deepEqual(peek.messages.map((message) => message.id), rows.map((row) => row.id));
  assert.equal(readAgentCursor(node.workdir, node.agentName).last_seen_seq, 0);

  const allUnread = await channelManager.queryChannelMessages({
    channelId: node.channelId,
    unread: true,
    limit: 10,
  });
  assert.deepEqual(allUnread.messages.map((message) => message.id), rows.map((row) => row.id));
  assert.equal(readAgentCursor(node.workdir, node.agentName).last_seen_seq, 0);
});

test('message.query --unread hides future D messages unless include_future is set', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-unread-future-'));
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
    dueMessagePollMs: 0,
  });

  const created = await channelManager.createChannel({
    channelId: 'channel-future',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Future Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const node = channelManager._requireNode(created.channel_id);
  const now = Date.UTC(2026, 4, 6, 1, 0, 0);
  await channelManager.scheduleChannelMessage({
    channelId: node.channelId,
    notBefore: now + 3_600_000,
    correlationId: 'corr-future',
    payloadBody: { reason: 'later' },
  });

  const hidden = await channelManager.queryChannelMessages({ channelId: node.channelId, unread: true, nowMs: now });
  const visible = await channelManager.queryChannelMessages({
    channelId: node.channelId,
    unread: true,
    includeFuture: true,
    nowMs: now,
  });

  assert.equal(hidden.messages.length, 0);
  assert.deepEqual(visible.messages.map((message) => message.correlation_id), ['corr-future']);
  assert.equal(readAgentCursor(node.workdir, node.agentName).last_seen_seq, 0);
});

test('dispatch and memo RPC helpers write and summarize sqlite protocol messages', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-pattern-cli-'));
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
    dueMessagePollMs: 0,
  });

  const created = await channelManager.createChannel({
    channelId: 'channel-pattern',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Pattern Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const node = channelManager._requireNode(created.channel_id);

  const started = await channelManager.dispatchStart({
    channelId: node.channelId,
    target: 'external:device:test',
    type: 'xhs.publish',
    params: { title: 'hello' },
    inTask: 'task-1',
    checkAfterMs: 60_000,
  });
  assert.equal(typeof started.correlation_id, 'string');

  const afterStart = readStoredMessages(node.messageStore);
  assert.equal(afterStart.filter((message) => message.correlation_id === started.correlation_id).length, 2);
  assert.equal(afterStart.some((message) => message.payload_type === 'dispatch.start' && message.task_id === 'task-1'), true);
  assert.equal(afterStart.some((message) => message.payload_type === 'dispatch.self_check_due' && message.not_before > Date.now()), true);

  const noResponse = await channelManager.dispatchCheck({
    channelId: node.channelId,
    correlationId: started.correlation_id,
  });
  assert.equal(noResponse.status, 'no_response');

  await channelManager.emitChannelMessage({
    channelId: node.channelId,
    senderKind: 'external',
    senderType: 'external',
    senderId: 'external:device:test',
    senderName: 'device',
    payloadType: 'dispatch.completed',
    payloadBody: { result: { url: 'https://example.test/note' } },
    content: 'completed',
    correlationId: started.correlation_id,
    origin: 'external',
  });
  const completed = await channelManager.dispatchCheck({
    channelId: node.channelId,
    correlationId: started.correlation_id,
  });
  assert.equal(completed.status, 'completed');
  assert.deepEqual(completed.result, { url: 'https://example.test/note' });

  const memo = await channelManager.createMemo({
    channelId: node.channelId,
    tag: 'pending_action',
    summary: 'Publish note result is ready',
    doc: 'notes/tasks/publish.md',
    correlationId: started.correlation_id,
  });
  assert.equal(memo.payloadType, 'self.memo');

  const recalled = await channelManager.recallMemo({
    channelId: node.channelId,
    tag: 'pending_action',
  });
  assert.equal(recalled.memos.length, 1);
  assert.equal(recalled.memos[0].doc_ref, 'notes/tasks/publish.md');
});

test('registerSchedule rejects unsafe IDs before writing outside schedules', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-schedule-unsafe-'));
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
    dueMessagePollMs: 0,
  });
  const created = await channelManager.createChannel({
    channelId: 'channel-schedule-unsafe',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Schedule Unsafe Channel',
    type: 'xhs-creator',
    status: 'created',
  });

  await assert.rejects(
    () => channelManager.registerSchedule({
      channelId: created.channel_id,
      id: '../foo',
      cron: '0 9 * * *',
      reason: 'unsafe',
    }, 'cron'),
    (err) => err.code === 'bad_request' && /schedule id/.test(err.message),
  );

  assert.equal(existsSync(path.join(created.workdir, 'foo.yaml')), false);
  assert.deepEqual(readdirSync(path.join(created.workdir, 'schedules')).filter((entry) => entry.endsWith('.yaml')), []);
  await assert.rejects(
    () => channelManager.cancelSchedule(created.channel_id, '../foo'),
    (err) => err.code === 'bad_request' && /schedule id/.test(err.message),
  );
});

test('registerSchedule rejects duplicate IDs unless upsert is explicit', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-schedule-upsert-'));
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
    dueMessagePollMs: 0,
  });
  const created = await channelManager.createChannel({
    channelId: 'channel-schedule-upsert',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Schedule Upsert Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const schedulePath = path.join(created.workdir, 'schedules', 'daily.yaml');

  await channelManager.registerSchedule({
    channelId: created.channel_id,
    id: 'daily',
    cron: '0 9 * * *',
    reason: 'first',
  }, 'cron');
  await assert.rejects(
    () => channelManager.registerSchedule({
      channelId: created.channel_id,
      id: 'daily',
      cron: '0 10 * * *',
      reason: 'second',
    }, 'cron'),
    (err) => err.code === 'schedule_exists' && err.statusCode === 409,
  );
  assert.equal(JSON.parse(readFileSync(schedulePath, 'utf8')).reason, 'first');

  await channelManager.registerSchedule({
    channelId: created.channel_id,
    id: 'daily',
    cron: '0 10 * * *',
    reason: 'second',
    upsert: true,
  }, 'cron');
  assert.equal(JSON.parse(readFileSync(schedulePath, 'utf8')).reason, 'second');
});

test('task RPC helpers project task rows and expose show/tree views', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-task-family-'));
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
    dueMessagePollMs: 0,
  });

  const created = await channelManager.createChannel({
    channelId: 'channel-task-family',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Task Family Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const node = channelManager._requireNode(created.channel_id);
  const docRef = 'notes/tasks/2026-05-06-publish-plan.md';
  mkdirSync(path.dirname(path.join(created.workdir, docRef)), { recursive: true });
  writeFileSync(path.join(created.workdir, docRef), '# Publish plan\n', 'utf8');

  const opened = await channelManager.openTask({
    channelId: node.channelId,
    taskId: 'task-parent',
    type: 'note.publish',
    title: 'Publish plan',
    docRef,
    rationale: 'track launch',
  });
  assert.equal(opened.task_id, 'task-parent');
  assert.equal(opened.task.status, 'opened');

  await channelManager.openTask({
    channelId: node.channelId,
    taskId: 'task-child',
    type: 'research',
    title: 'Collect refs',
    parentTaskId: 'task-parent',
    docRef: 'notes/tasks/2026-05-06-collect-refs.md',
  });
  await channelManager.appendTask({
    channelId: node.channelId,
    taskId: 'task-parent',
    summary: 'Draft completed',
  });
  const closed = await channelManager.closeTask({
    channelId: node.channelId,
    taskId: 'task-parent',
    status: 'completed',
    summary: 'Published',
  });
  assert.equal(closed.task.status, 'completed');

  const listed = await channelManager.listTasks({ channelId: node.channelId, status: 'completed' });
  assert.deepEqual(listed.tasks.map((task) => task.task_id), ['task-parent']);

  const rpcResponses = [];
  await channelManager.handle({
    type: 'channel:rpc',
    requestId: 'req-task-list',
    method: 'task.list',
    channelId: node.channelId,
    params: { status: 'completed' },
  }, {
    send(message) {
      rpcResponses.push(message);
    },
  });
  assert.equal(rpcResponses[0].type, 'channel:rpc.result');
  assert.equal(rpcResponses[0].ok, true);
  assert.deepEqual(rpcResponses[0].result.tasks.map((task) => task.task_id), ['task-parent']);

  const shown = await channelManager.showTask({ channelId: node.channelId, taskId: 'task-parent' });
  assert.match(shown.doc.content, /^# Publish plan\n/);
  assert.match(shown.doc.content, /Status: completed/);
  assert.equal(shown.children[0].task_id, 'task-child');
  assert.equal(shown.messages.some((message) => message.payload_type === 'task.appended'), true);

  const tree = await channelManager.taskTree({ channelId: node.channelId });
  assert.equal(tree.tasks[0].task_id, 'task-parent');
  assert.equal(tree.tasks[0].children[0].task_id, 'task-child');
});

test('task open and close write task docs inside daemon RPC boundary', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-task-docs-'));
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
    dueMessagePollMs: 0,
  });
  const created = await channelManager.createChannel({
    channelId: 'channel-task-docs',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Task Docs Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const node = channelManager._requireNode(created.channel_id);
  const docRef = 'notes/tasks/2026-05-06-daemon-doc.md';
  const docPath = path.join(created.workdir, docRef);

  const opened = await channelManager.openTask({
    channelId: node.channelId,
    taskId: 'task-docs',
    type: 'note.publish',
    title: 'Daemon doc task',
    docRef,
    rationale: 'track daemon doc writes',
  });
  assert.equal(opened.doc_ref, docRef);
  const openedDoc = readFileSync(docPath, 'utf8');
  assert.match(openedDoc, /^# Daemon doc task/);
  assert.match(openedDoc, /track daemon doc writes/);
  assert.match(openedDoc, /Status: opened/);

  const closed = await channelManager.closeTask({
    channelId: node.channelId,
    taskId: 'task-docs',
    status: 'completed',
    summary: 'Finished',
    resultRef: 'artifact://note',
  });
  assert.equal(closed.doc_ref, docRef);
  const closedDoc = readFileSync(docPath, 'utf8');
  assert.match(closedDoc, /Summary: Finished/);
  assert.match(closedDoc, /Result ref: artifact:\/\/note/);
  assert.equal(closedDoc.trim().endsWith('Status: completed'), true);
  assert.equal(getStoredTask(node.messageStore, 'task-docs', node.channelId).status, 'completed');
});

test('task append writes task doc inside daemon RPC boundary', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-task-doc-append-'));
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
    dueMessagePollMs: 0,
  });
  const created = await channelManager.createChannel({
    channelId: 'channel-task-doc-append',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Task Doc Append Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const node = channelManager._requireNode(created.channel_id);
  const docRef = 'notes/tasks/2026-05-06-append.md';
  const docPath = path.join(created.workdir, docRef);

  await channelManager.openTask({
    channelId: node.channelId,
    taskId: 'task-append-doc',
    type: 'research',
    title: 'Append doc task',
    docRef,
  });

  const appended = await channelManager.appendTask({
    channelId: node.channelId,
    taskId: 'task-append-doc',
    summary: 'Draft ready',
  });

  assert.equal(appended.doc_ref, docRef);
  const appendedDoc = readFileSync(docPath, 'utf8');
  assert.match(appendedDoc, /- .* - Draft ready/);
  assert.equal(appendedDoc.indexOf('Draft ready') < appendedDoc.indexOf('## Status'), true);
  assert.equal(readStoredMessages(node.messageStore)
    .filter((message) => message.payload_type === 'task.appended').length, 1);
});

test('task append doc write failure does not leave daemon append projection', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-task-doc-append-fail-'));
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
    dueMessagePollMs: 0,
  });
  const created = await channelManager.createChannel({
    channelId: 'channel-task-doc-append-fail',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Task Doc Append Fail Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const node = channelManager._requireNode(created.channel_id);
  const docRef = 'notes/tasks/2026-05-06-append-fail.md';
  const docPath = path.join(created.workdir, docRef);

  await channelManager.openTask({
    channelId: node.channelId,
    taskId: 'task-append-doc-fail',
    type: 'research',
    title: 'Append doc fail',
    docRef,
  });
  const originalDoc = readFileSync(docPath, 'utf8');
  const originalTask = getStoredTask(node.messageStore, 'task-append-doc-fail', node.channelId);
  const originalWriteTaskDocAtomic = channelManager._writeTaskDocAtomic;
  let sentAppend = false;
  channelManager._writeTaskDocAtomic = () => {
    throw new Error('mock doc write failed');
  };
  const originalSendChannelMessage = channelManager.sendChannelMessage.bind(channelManager);
  channelManager.sendChannelMessage = async (params, options) => {
    if (params.messageType === 'task.appended') sentAppend = true;
    return originalSendChannelMessage(params, options);
  };

  await assert.rejects(
    () => channelManager.appendTask({
      channelId: node.channelId,
      taskId: 'task-append-doc-fail',
      summary: 'should not persist',
    }),
    /mock doc write failed/,
  );
  channelManager._writeTaskDocAtomic = originalWriteTaskDocAtomic;

  assert.equal(sentAppend, false);
  assert.equal(readFileSync(docPath, 'utf8'), originalDoc);
  assert.equal(readStoredMessages(node.messageStore)
    .filter((message) => message.payload_type === 'task.appended').length, 0);
  const task = getStoredTask(node.messageStore, 'task-append-doc-fail', node.channelId);
  assert.equal(task.last_event_at, originalTask.last_event_at);
});

test('task append restores doc when RPC send fails after doc write', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-task-doc-append-rollback-'));
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
    dueMessagePollMs: 0,
  });
  const created = await channelManager.createChannel({
    channelId: 'channel-task-doc-append-rollback',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Task Doc Append Rollback Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const node = channelManager._requireNode(created.channel_id);
  const docRef = 'notes/tasks/2026-05-06-append-rollback.md';
  const docPath = path.join(created.workdir, docRef);

  await channelManager.openTask({
    channelId: node.channelId,
    taskId: 'task-append-doc-rollback',
    type: 'research',
    title: 'Append doc rollback',
    docRef,
  });
  const originalDoc = readFileSync(docPath, 'utf8');
  const originalSendChannelMessage = channelManager.sendChannelMessage.bind(channelManager);
  let appendSendAttempted = false;
  channelManager.sendChannelMessage = async (params, options) => {
    if (params.messageType === 'task.appended') {
      appendSendAttempted = true;
      throw new Error('mock append send failed');
    }
    return originalSendChannelMessage(params, options);
  };

  await assert.rejects(
    () => channelManager.appendTask({
      channelId: node.channelId,
      taskId: 'task-append-doc-rollback',
      summary: 'rolled back',
    }),
    /mock append send failed/,
  );

  assert.equal(appendSendAttempted, true);
  assert.equal(readFileSync(docPath, 'utf8'), originalDoc);
  assert.equal(readStoredMessages(node.messageStore)
    .filter((message) => message.payload_type === 'task.appended').length, 0);
});

test('concurrent task appends serialize doc writes and projections', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-task-append-concurrent-'));
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
    dueMessagePollMs: 0,
  });
  const created = await channelManager.createChannel({
    channelId: 'channel-task-append-concurrent',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Task Append Concurrent Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const node = channelManager._requireNode(created.channel_id);
  const docRef = 'notes/tasks/2026-05-06-append-concurrent.md';
  const docPath = path.join(created.workdir, docRef);

  await channelManager.openTask({
    channelId: node.channelId,
    taskId: 'task-append-concurrent',
    type: 'research',
    title: 'Append concurrent',
    docRef,
  });

  const firstEntered = createDeferred();
  const firstRelease = createDeferred();
  const appendSendOrder = [];
  const originalSendChannelMessage = channelManager.sendChannelMessage.bind(channelManager);
  channelManager.sendChannelMessage = async (params, options) => {
    if (params.messageType === 'task.appended') {
      appendSendOrder.push(params.content);
      if (params.content === 'First concurrent append') {
        firstEntered.resolve();
        await firstRelease.promise;
      }
    }
    return originalSendChannelMessage(params, options);
  };

  const firstAppend = channelManager.appendTask({
    channelId: node.channelId,
    taskId: 'task-append-concurrent',
    summary: 'First concurrent append',
  });
  await firstEntered.promise;
  const secondAppend = channelManager.appendTask({
    channelId: node.channelId,
    taskId: 'task-append-concurrent',
    summary: 'Second concurrent append',
  });
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(appendSendOrder, ['First concurrent append']);

  firstRelease.resolve();
  await Promise.all([firstAppend, secondAppend]);

  const doc = readFileSync(docPath, 'utf8');
  assert.match(doc, /- .* - First concurrent append/);
  assert.match(doc, /- .* - Second concurrent append/);
  assert.equal(doc.indexOf('First concurrent append') < doc.indexOf('Second concurrent append'), true);
  const appends = readStoredMessages(node.messageStore)
    .filter((message) => message.payload_type === 'task.appended');
  assert.equal(appends.length, 2);
  assert.equal(appends.some((message) => message.payload_body.summary === 'First concurrent append'), true);
  assert.equal(appends.some((message) => message.payload_body.summary === 'Second concurrent append'), true);
  assert.equal(channelManager._taskDocMutex.size, 0);
});

test('concurrent task append failed first send does not restore over successful second append', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-task-append-first-fails-'));
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
    dueMessagePollMs: 0,
  });
  const created = await channelManager.createChannel({
    channelId: 'channel-task-append-first-fails',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Task Append First Fails Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const node = channelManager._requireNode(created.channel_id);
  const docRef = 'notes/tasks/2026-05-06-append-first-fails.md';
  const docPath = path.join(created.workdir, docRef);

  await channelManager.openTask({
    channelId: node.channelId,
    taskId: 'task-append-first-fails',
    type: 'research',
    title: 'Append first fails',
    docRef,
  });

  const firstEntered = createDeferred();
  const firstRelease = createDeferred();
  const appendSendOrder = [];
  const originalSendChannelMessage = channelManager.sendChannelMessage.bind(channelManager);
  channelManager.sendChannelMessage = async (params, options) => {
    if (params.messageType === 'task.appended') {
      appendSendOrder.push(params.content);
      if (params.content === 'First append will fail') {
        firstEntered.resolve();
        await firstRelease.promise;
        throw new Error('mock first append send failed');
      }
    }
    return originalSendChannelMessage(params, options);
  };

  const firstAppend = channelManager.appendTask({
    channelId: node.channelId,
    taskId: 'task-append-first-fails',
    summary: 'First append will fail',
  });
  await firstEntered.promise;
  const secondAppend = channelManager.appendTask({
    channelId: node.channelId,
    taskId: 'task-append-first-fails',
    summary: 'Second append survives',
  });
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(appendSendOrder, ['First append will fail']);

  firstRelease.resolve();
  const results = await Promise.allSettled([firstAppend, secondAppend]);
  assert.equal(results[0].status, 'rejected');
  assert.match(results[0].reason.message, /mock first append send failed/);
  assert.equal(results[1].status, 'fulfilled');

  const doc = readFileSync(docPath, 'utf8');
  assert.equal(doc.includes('First append will fail'), false);
  assert.match(doc, /- .* - Second append survives/);
  const appends = readStoredMessages(node.messageStore)
    .filter((message) => message.payload_type === 'task.appended');
  assert.equal(appends.length, 1);
  assert.equal(appends[0].payload_body.summary, 'Second append survives');
  assert.equal(channelManager._taskDocMutex.size, 0);
});

test('concurrent task appends both failing sends restore the initial doc', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-task-append-both-fail-'));
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
    dueMessagePollMs: 0,
  });
  const created = await channelManager.createChannel({
    channelId: 'channel-task-append-both-fail',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Task Append Both Fail Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const node = channelManager._requireNode(created.channel_id);
  const docRef = 'notes/tasks/2026-05-06-append-both-fail.md';
  const docPath = path.join(created.workdir, docRef);

  await channelManager.openTask({
    channelId: node.channelId,
    taskId: 'task-append-both-fail',
    type: 'research',
    title: 'Append both fail',
    docRef,
  });
  const originalDoc = readFileSync(docPath, 'utf8');

  const firstEntered = createDeferred();
  const firstRelease = createDeferred();
  const appendSendOrder = [];
  channelManager.sendChannelMessage = async (params) => {
    if (params.messageType === 'task.appended') {
      appendSendOrder.push(params.content);
      if (params.content === 'First failing append') {
        firstEntered.resolve();
        await firstRelease.promise;
      }
      throw new Error(`mock send failed for ${params.content}`);
    }
    throw new Error('unexpected message send in test');
  };

  const firstAppend = channelManager.appendTask({
    channelId: node.channelId,
    taskId: 'task-append-both-fail',
    summary: 'First failing append',
  });
  await firstEntered.promise;
  const secondAppend = channelManager.appendTask({
    channelId: node.channelId,
    taskId: 'task-append-both-fail',
    summary: 'Second failing append',
  });
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(appendSendOrder, ['First failing append']);

  firstRelease.resolve();
  const results = await Promise.allSettled([firstAppend, secondAppend]);
  assert.equal(results[0].status, 'rejected');
  assert.equal(results[1].status, 'rejected');

  assert.equal(readFileSync(docPath, 'utf8'), originalDoc);
  assert.equal(readStoredMessages(node.messageStore)
    .filter((message) => message.payload_type === 'task.appended').length, 0);
  assert.equal(channelManager._taskDocMutex.size, 0);
});

test('concurrent task append and close serialize on the shared doc ref', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-task-append-close-concurrent-'));
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
    dueMessagePollMs: 0,
  });
  const created = await channelManager.createChannel({
    channelId: 'channel-task-append-close-concurrent',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Task Append Close Concurrent Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const node = channelManager._requireNode(created.channel_id);
  const docRef = 'notes/tasks/2026-05-06-append-close-concurrent.md';
  const docPath = path.join(created.workdir, docRef);

  await channelManager.openTask({
    channelId: node.channelId,
    taskId: 'task-append-close-concurrent',
    type: 'research',
    title: 'Append close concurrent',
    docRef,
  });

  const appendEntered = createDeferred();
  const appendRelease = createDeferred();
  const sendOrder = [];
  const originalSendChannelMessage = channelManager.sendChannelMessage.bind(channelManager);
  channelManager.sendChannelMessage = async (params, options) => {
    if (params.messageType === 'task.appended') {
      sendOrder.push('append');
      appendEntered.resolve();
      await appendRelease.promise;
    }
    if (params.messageType === 'task.closed') {
      sendOrder.push('close');
    }
    return originalSendChannelMessage(params, options);
  };

  const appendPromise = channelManager.appendTask({
    channelId: node.channelId,
    taskId: 'task-append-close-concurrent',
    summary: 'Append before close',
  });
  await appendEntered.promise;
  const closePromise = channelManager.closeTask({
    channelId: node.channelId,
    taskId: 'task-append-close-concurrent',
    status: 'completed',
    summary: 'Closed after append',
  });
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(sendOrder, ['append']);

  appendRelease.resolve();
  await Promise.all([appendPromise, closePromise]);

  const doc = readFileSync(docPath, 'utf8');
  assert.match(doc, /- .* - Append before close/);
  assert.match(doc, /Summary: Closed after append/);
  assert.equal(doc.trim().endsWith('Status: completed'), true);
  assert.equal(doc.indexOf('Append before close') < doc.indexOf('Status: completed'), true);
  assert.equal(getStoredTask(node.messageStore, 'task-append-close-concurrent', node.channelId).status, 'completed');
  const messages = readStoredMessages(node.messageStore);
  assert.equal(messages.filter((message) => message.payload_type === 'task.appended').length, 1);
  assert.equal(messages.filter((message) => message.payload_type === 'task.closed').length, 1);
  assert.equal(channelManager._taskDocMutex.size, 0);
});

test('task open doc write failure does not leave daemon task projection', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-task-doc-open-fail-'));
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
    dueMessagePollMs: 0,
  });
  const created = await channelManager.createChannel({
    channelId: 'channel-task-doc-open-fail',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Task Doc Open Fail Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const node = channelManager._requireNode(created.channel_id);
  writeFileSync(path.join(created.workdir, 'notes', 'tasks'), 'not a directory', 'utf8');

  await assert.rejects(
    () => channelManager.openTask({
      channelId: node.channelId,
      taskId: 'task-doc-fail',
      type: 'research',
      title: 'Doc fail',
      docRef: 'notes/tasks/fail.md',
    }),
    /not a directory|ENOTDIR|EEXIST/,
  );

  const db = channelManager._openMessageStore(node);
  assert.equal(getStoredTask(db, 'task-doc-fail', node.channelId), null);
  assert.equal(readStoredMessages(db).length, 0);
});

test('task close doc write failure leaves daemon task non-terminal', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-task-doc-close-fail-'));
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
    dueMessagePollMs: 0,
  });
  const created = await channelManager.createChannel({
    channelId: 'channel-task-doc-close-fail',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Task Doc Close Fail Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const node = channelManager._requireNode(created.channel_id);
  const docRef = 'notes/tasks/2026-05-06-close-fail.md';
  const docPath = path.join(created.workdir, docRef);

  await channelManager.openTask({
    channelId: node.channelId,
    taskId: 'task-close-doc-fail',
    type: 'research',
    title: 'Close doc fail',
    docRef,
  });
  rmSync(docPath, { force: true });
  mkdirSync(docPath);

  await assert.rejects(
    () => channelManager.closeTask({
      channelId: node.channelId,
      taskId: 'task-close-doc-fail',
      status: 'completed',
      summary: 'done',
    }),
    /illegal operation on a directory|EISDIR|ENOTDIR/,
  );

  const task = getStoredTask(node.messageStore, 'task-close-doc-fail', node.channelId);
  assert.equal(task.status, 'opened');
  assert.equal(task.closed_at, null);
  assert.equal(readStoredMessages(node.messageStore).length, 1);
});

test('channel RPC result includes structured error statusCode from daemon errors', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-rpc-status-'));
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
    dueMessagePollMs: 0,
  });
  channelManager.rpcCall = async () => {
    const err = new Error('task already terminal: task-a');
    err.code = 'task_already_terminal';
    err.statusCode = 409;
    throw err;
  };

  const rpcResponses = [];
  await channelManager.handle({
    type: 'channel:rpc',
    requestId: 'req-terminal-task',
    method: 'task.list',
    channelId: 'channel-a',
    params: {},
  }, {
    send(message) {
      rpcResponses.push(message);
    },
  });

  assert.equal(rpcResponses[0].type, 'channel:rpc.result');
  assert.equal(rpcResponses[0].ok, false);
  assert.equal(rpcResponses[0].code, 'task_already_terminal');
  assert.equal(rpcResponses[0].statusCode, 409);
  assert.deepEqual(rpcResponses[0].error, {
    code: 'task_already_terminal',
    message: 'task already terminal: task-a',
    statusCode: 409,
  });
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

test('pending view sync is replayed on reconnect and removed after ack', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-view-sync-replay-'));
  const previousHome = process.env.HOME;
  process.env.HOME = tempHome;

  t.after(() => {
    process.env.HOME = previousHome;
    rmSync(tempHome, { recursive: true, force: true });
  });

  const sentLog = [];
  const failingConnection = {
    send(message) {
      sentLog.push(message);
    },
    async request({ expect }) {
      return {
        type: expect.type,
        requestId: expect.requestId,
        ok: false,
        error: 'server unavailable',
      };
    },
  };
  const replayedRequests = [];
  const healthyConnection = {
    send(message) {
      sentLog.push(message);
    },
    async request({ message, expect }) {
      replayedRequests.push(message);
      return { type: expect.type, requestId: expect.requestId, ok: true };
    },
  };

  const channelManager = new ChannelManager({
    serverUrl: 'http://localhost:3001',
    machineApiKey: 'machine-key',
    daemonSocketPath: path.join(tempHome, '.coagent', 'daemon.sock'),
    daemonHttpUrl: 'http://127.0.0.1:3002',
    daemonToken: 'daemon-token',
  });
  channelManager.setConnection(failingConnection);

  const created = await channelManager.createChannel({
    channelId: 'channel-replay',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Replay Channel',
    type: 'xhs-creator',
    status: 'active',
    members: [{ memberType: 'human', memberId: 'user-a', displayName: 'User A' }],
  });
  const { workdir } = created;

  const result = await channelManager.handle({
    type: 'channel:message.send',
    requestId: 'req-replay',
    channelId: 'channel-replay',
    senderType: 'human',
    senderId: 'user-a',
    senderName: 'User A',
    content: 'message replayed after reconnect',
    attachments: [],
  }, failingConnection);

  assert.equal(result.content, 'message replayed after reconnect');
  assert.equal(readdirSync(path.join(workdir, 'pending-view-sync')).filter((entry) => entry.endsWith('.json')).length, 1);

  channelManager.setConnection(healthyConnection);
  await channelManager.pendingViewSyncReplay;

  assert.equal(readdirSync(path.join(workdir, 'pending-view-sync')).filter((entry) => entry.endsWith('.json')).length, 0);
  assert.equal(replayedRequests.length, 1);
  assert.equal(replayedRequests[0].requestId, 'req-replay');
  assert.equal(replayedRequests[0].message_id, result.messageId);

  channelManager.setConnection(healthyConnection);
  await channelManager.pendingViewSyncReplay;
  assert.equal(replayedRequests.length, 1);
});

test('_recordTrace failures do not abort handleEvent delivery path', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-trace-fail-'));
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
    dueMessagePollMs: 0,
  });

  const created = await channelManager.createChannel({
    channelId: 'channel-trace-fail',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Trace Fail Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const node = channelManager._requireNode(created.channel_id);
  const deliveredLines = [];
  node.status = 'active';
  node.proc = { stdin: { write: (line) => deliveredLines.push(JSON.parse(line)) } };
  const traceDir = path.join(node.workdir, 'agents', node.agentName, 'trace');
  rmSync(traceDir, { recursive: true, force: true });
  writeFileSync(traceDir, 'not a directory', 'utf8');

  await channelManager.handleEvent({
    channelId: node.channelId,
    event: {
      type: 'user.message.posted',
      source: 'test',
      payload: {
        senderType: 'human',
        senderId: 'user-a',
        senderName: 'User A',
        content: 'trace failure should not abort',
      },
    },
  });

  assert.equal(deliveredLines.length, 1);
  assert.equal(deliveredLines[0].event.payload.content, 'trace failure should not abort');
});

test('agents workdir change events stay blocked and recorded in trace', async (t) => {
  const tempHome = mkdtempSync(path.join(os.tmpdir(), 'channel-manager-agents-block-'));
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
    dueMessagePollMs: 0,
  });

  const created = await channelManager.createChannel({
    channelId: 'channel-agents-block',
    workspaceId: 'workspace-1',
    daemonId: 'machine-a',
    name: 'Agents Block Channel',
    type: 'xhs-creator',
    status: 'created',
  });
  const node = channelManager._requireNode(created.channel_id);

  await channelManager.handleEvent({
    channelId: node.channelId,
    event: {
      type: 'workdir.changed',
      source: 'test',
      payload: {
        op: 'change',
        path: 'agents/channel-agent/notes/note.md',
      },
    },
  });

  const tracePath = path.join(node.workdir, 'agents', node.agentName, 'trace', 'pending.jsonl');
  const traceEntries = readFileSync(tracePath, 'utf8')
    .trim()
    .split('\n')
    .map((line) => JSON.parse(line));
  assert.deepEqual(traceEntries.map((entry) => [entry.kind, entry.decision, entry.reason]), [
    ['trigger', 'block', 'workdir_agents_block'],
  ]);
  assert.equal(traceEntries[0].event.payload.path, 'agents/channel-agent/notes/note.md');
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
