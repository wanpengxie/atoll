import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import {
  appendMessageToStore,
  getStoredTask,
  openMessageStore,
  queryStoredTasks,
  readStoredMessages,
} from '../src/message-store.js';

function makeMessage({
  id,
  channelId = 'channel-tasks',
  taskId = null,
  payloadType = 'agent.text',
  body = { text: 'hello' },
  audience = ['channel'],
  correlationId = null,
  ts = Date.UTC(2026, 4, 6, 0, 0, 0),
}) {
  return {
    messageId: id,
    channelId,
    senderKind: 'agent',
    senderId: 'channel-agent',
    senderName: 'channel-agent',
    payloadType,
    payloadBody: body,
    payload: { type: payloadType, body },
    content: body.title ?? body.summary ?? body.text ?? JSON.stringify(body),
    correlationId,
    taskId,
    audience,
    origin: audience.includes('self') ? 'self' : 'channel',
    tsReceived: ts,
    createdAt: new Date(ts).toISOString(),
    envelope: {
      id,
      ts,
      ts_received: ts,
      sender: { kind: 'agent', id: 'channel-agent', name: 'channel-agent' },
      audience,
      origin: audience.includes('self') ? 'self' : 'channel',
      ...(correlationId ? { correlation_id: correlationId } : {}),
      ...(taskId ? { task_id: taskId } : {}),
    },
  };
}

function withStore(t) {
  const tempDir = mkdtempSync(path.join(os.tmpdir(), 'task-projection-'));
  t.after(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });
  const db = openMessageStore(tempDir);
  t.after(() => db.close());
  return db;
}

test('task projection creates schema rows and updates last_event_at on task messages only', (t) => {
  const db = withStore(t);
  const openedAt = Date.UTC(2026, 4, 6, 0, 0, 0);
  const appendedAt = openedAt + 30_000;
  const unrelatedAt = openedAt + 60_000;
  const closedAt = openedAt + 90_000;

  appendMessageToStore(db, makeMessage({
    id: 'msg-open',
    taskId: 'task-a',
    payloadType: 'task.opened',
    body: {
      type: 'note.publish',
      title: 'Publish launch note',
      doc_ref: 'notes/tasks/2026-05-06-publish-launch-note.md',
    },
    audience: ['self'],
    correlationId: 'corr-open',
    ts: openedAt,
  }));
  appendMessageToStore(db, makeMessage({
    id: 'msg-agent',
    taskId: 'task-a',
    payloadType: 'agent.text',
    body: { text: 'working' },
    ts: appendedAt,
  }));
  appendMessageToStore(db, makeMessage({
    id: 'msg-dispatch-unrelated',
    taskId: null,
    payloadType: 'dispatch.start',
    body: { type: 'xhs.publish' },
    correlationId: 'corr-unrelated',
    ts: unrelatedAt,
  }));
  appendMessageToStore(db, makeMessage({
    id: 'msg-close',
    taskId: 'task-a',
    payloadType: 'task.closed',
    body: { status: 'completed', summary: 'done' },
    audience: ['self'],
    ts: closedAt,
  }));

  const task = getStoredTask(db, 'task-a', 'channel-tasks');
  assert.equal(task.task_id, 'task-a');
  assert.equal(task.type, 'note.publish');
  assert.equal(task.status, 'completed');
  assert.equal(task.opened_at, openedAt);
  assert.equal(task.last_event_at, closedAt);
  assert.equal(task.closed_at, closedAt);
  assert.equal(task.primary_correlation, 'corr-open');
  assert.equal(queryStoredTasks(db, { channel_id: 'channel-tasks', status: 'active' }).length, 0);
  assert.equal(queryStoredTasks(db, { channel_id: 'channel-tasks', status: 'completed' }).length, 1);
});

test('appendMessageToStore rejects unknown payload types', (t) => {
  const db = withStore(t);

  assert.throws(
    () => appendMessageToStore(db, makeMessage({
      id: 'msg-unknown',
      payloadType: 'whatever.unknown',
      body: { text: 'unsupported' },
    })),
    (err) => err.code === 'bad_request' && /unsupported payload_type/.test(err.message),
  );
  assert.equal(readStoredMessages(db).length, 0);
});

test('appendMessageToStore treats duplicate message ids as idempotent', (t) => {
  const db = withStore(t);

  const first = appendMessageToStore(db, makeMessage({
    id: 'msg-once',
    body: { text: 'first' },
  }));
  const second = appendMessageToStore(db, makeMessage({
    id: 'msg-once',
    body: { text: 'retry with different body ignored' },
  }));

  assert.equal(first.inserted, true);
  assert.equal(second.inserted, false);
  const messages = readStoredMessages(db);
  assert.equal(messages.length, 1);
  assert.equal(messages[0].id, 'msg-once');
  assert.equal(messages[0].payload_body.text, 'first');
});

test('tasks table enforces parent FK and unique task_id atomically with message insert', (t) => {
  const db = withStore(t);

  assert.throws(() => appendMessageToStore(db, makeMessage({
    id: 'msg-orphan',
    taskId: 'task-orphan',
    payloadType: 'task.opened',
    body: {
      type: 'research',
      title: 'Orphan child',
      parent_task_id: 'missing-parent',
      doc_ref: 'notes/tasks/2026-05-06-orphan-child.md',
    },
    audience: ['self'],
  })), /FOREIGN KEY|constraint/i);
  assert.equal(readStoredMessages(db).length, 0);

  appendMessageToStore(db, makeMessage({
    id: 'msg-parent',
    taskId: 'task-parent',
    payloadType: 'task.opened',
    body: {
      type: 'research',
      title: 'Parent',
      doc_ref: 'notes/tasks/2026-05-06-parent.md',
    },
    audience: ['self'],
  }));
  appendMessageToStore(db, makeMessage({
    id: 'msg-child',
    taskId: 'task-child',
    payloadType: 'task.opened',
    body: {
      type: 'research',
      title: 'Child',
      parent_task_id: 'task-parent',
      doc_ref: 'notes/tasks/2026-05-06-child.md',
    },
    audience: ['self'],
  }));

  assert.equal(getStoredTask(db, 'task-child', 'channel-tasks').parent_task_id, 'task-parent');
  assert.throws(() => appendMessageToStore(db, makeMessage({
    id: 'msg-duplicate',
    taskId: 'task-child',
    payloadType: 'task.opened',
    body: {
      type: 'research',
      title: 'Duplicate',
      doc_ref: 'notes/tasks/2026-05-06-duplicate.md',
    },
    audience: ['self'],
  })), /UNIQUE|constraint/i);
  assert.equal(readStoredMessages(db).length, 2);
});

test('task projection refuses terminal task close and append mutations', (t) => {
  const db = withStore(t);
  const openedAt = Date.UTC(2026, 4, 6, 0, 0, 0);
  const closedAt = openedAt + 30_000;

  appendMessageToStore(db, makeMessage({
    id: 'msg-open-terminal',
    taskId: 'task-terminal',
    payloadType: 'task.opened',
    body: {
      type: 'research',
      title: 'Terminal task',
      doc_ref: 'notes/tasks/2026-05-06-terminal.md',
    },
    audience: ['self'],
    ts: openedAt,
  }));
  appendMessageToStore(db, makeMessage({
    id: 'msg-close-terminal',
    taskId: 'task-terminal',
    payloadType: 'task.closed',
    body: { status: 'completed', summary: 'done' },
    audience: ['self'],
    ts: closedAt,
  }));

  assert.throws(
    () => appendMessageToStore(db, makeMessage({
      id: 'msg-close-terminal-again',
      taskId: 'task-terminal',
      payloadType: 'task.closed',
      body: { status: 'failed', summary: 'late failure' },
      audience: ['self'],
      ts: closedAt + 30_000,
    })),
    (err) => err.code === 'task_already_terminal' && err.statusCode === 409,
  );
  assert.throws(
    () => appendMessageToStore(db, makeMessage({
      id: 'msg-append-terminal',
      taskId: 'task-terminal',
      payloadType: 'task.appended',
      body: { summary: 'late append' },
      audience: ['self'],
      ts: closedAt + 60_000,
    })),
    (err) => err.code === 'task_already_terminal' && err.statusCode === 409,
  );

  const task = getStoredTask(db, 'task-terminal', 'channel-tasks');
  assert.equal(task.status, 'completed');
  assert.equal(task.closed_at, closedAt);
  assert.equal(task.last_event_at, closedAt);
  assert.equal(readStoredMessages(db).length, 2);
});
