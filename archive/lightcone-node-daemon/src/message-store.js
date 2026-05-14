import Database from 'better-sqlite3';
import { mkdirSync, readFileSync, writeFileSync } from 'fs';
import path from 'path';
import { PayloadType, isPayloadType } from '@coagent/payload-types';
import { nowIso } from './time.js';

const DEFAULT_AGENT_NAME = 'channel-agent';
const TERMINAL_TASK_STATUSES = new Set(['completed', 'failed', 'abandoned', 'archived']);
const REMOVED_MESSAGE_COLUMN = ['legacy', 'json'].join('_');
const MESSAGE_COLUMNS = Object.freeze([
  'id',
  'channel_id',
  'ts',
  'ts_received',
  'sender_kind',
  'sender_id',
  'sender_name',
  'payload_type',
  'payload_body',
  'parent_id',
  'correlation_id',
  'task_id',
  'thread_id',
  'audience',
  'mentions',
  'origin',
  'not_before',
  'expires_at',
  'delivered_at',
  'delivery_failed_at',
  'delivery_attempts',
  'last_attempt_at',
  'last_error',
  'envelope_json',
  'payload_json',
  'created_at',
]);

function toJson(value) {
  return JSON.stringify(value ?? null);
}

function parseJsonStrict(value, fieldName) {
  if (value == null) throw badRequest(`${fieldName} is required`);
  try {
    return JSON.parse(value);
  } catch (err) {
    throw badRequest(`invalid ${fieldName} JSON: ${err.message}`);
  }
}

function parseJsonNullable(value) {
  if (value == null) return null;
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

function toEpochMs(value, fallback = Date.now()) {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string' && value.trim()) {
    const numeric = Number(value);
    if (Number.isFinite(numeric)) return numeric;
    const parsed = Date.parse(value);
    if (!Number.isNaN(parsed)) return parsed;
  }
  return fallback;
}

function optionalEpochMs(value) {
  if (value == null || value === '') return null;
  const parsed = toEpochMs(value, null);
  return Number.isFinite(parsed) ? parsed : null;
}

function toBoolean(value) {
  if (value === true) return true;
  if (value === false || value == null) return false;
  return String(value).trim().toLowerCase() === 'true';
}

function toSeq(value, fallback = 0) {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function badRequest(message) {
  const err = new Error(message);
  err.code = 'bad_request';
  return err;
}

function conflict(code, message) {
  const err = new Error(message);
  err.code = code;
  err.statusCode = 409;
  return err;
}

function assertKnownPayloadType(payloadType) {
  if (!isPayloadType(payloadType)) {
    throw badRequest(`unsupported payload_type: ${payloadType}`);
  }
}

function isTerminalTask(row) {
  if (!row) return false;
  return row.closed_at != null || TERMINAL_TASK_STATUSES.has(String(row.status ?? '').trim());
}

function getTaskProjectionRow(db, taskId, channelId) {
  return db.prepare(`
    SELECT * FROM tasks
    WHERE task_id = @task_id
      AND channel_id = @channel_id
  `).get({
    task_id: taskId,
    channel_id: channelId,
  }) ?? null;
}

function assertTaskProjectionMutable(db, taskId, channelId) {
  const task = getTaskProjectionRow(db, taskId, channelId);
  if (!task) return null;
  if (isTerminalTask(task)) {
    throw conflict('task_already_terminal', `task already terminal: ${taskId}`);
  }
  return task;
}

function ensureColumn(db, table, column, definition) {
  const columns = db.prepare(`PRAGMA table_info(${table})`).all();
  if (columns.some((row) => row.name === column)) return;
  db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
}

function quoteIdentifier(identifier) {
  const name = String(identifier ?? '').trim();
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
    throw new Error(`invalid sqlite identifier: ${identifier}`);
  }
  return `"${name}"`;
}

function tableColumns(db, table) {
  return db.prepare(`PRAGMA table_info(${quoteIdentifier(table)})`).all();
}

function createMessagesTableSql(tableName = 'messages', { ifNotExists = true } = {}) {
  return `
    CREATE TABLE ${ifNotExists ? 'IF NOT EXISTS ' : ''}${quoteIdentifier(tableName)} (
      id TEXT PRIMARY KEY,
      channel_id TEXT NOT NULL,
      ts INTEGER NOT NULL,
      ts_received INTEGER NOT NULL,
      sender_kind TEXT NOT NULL,
      sender_id TEXT NOT NULL,
      sender_name TEXT DEFAULT '',
      payload_type TEXT NOT NULL,
      payload_body TEXT NOT NULL,
      parent_id TEXT DEFAULT NULL,
      correlation_id TEXT DEFAULT NULL,
      task_id TEXT DEFAULT NULL,
      thread_id TEXT DEFAULT NULL,
      audience TEXT NOT NULL DEFAULT 'channel',
      mentions TEXT DEFAULT NULL,
      origin TEXT DEFAULT NULL,
      not_before INTEGER DEFAULT NULL,
      expires_at INTEGER DEFAULT NULL,
      delivered_at INTEGER DEFAULT NULL,
      delivery_failed_at INTEGER DEFAULT NULL,
      delivery_attempts INTEGER NOT NULL DEFAULT 0,
      last_attempt_at INTEGER DEFAULT NULL,
      last_error TEXT DEFAULT NULL,
      envelope_json TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      created_at TEXT NOT NULL
    );
  `;
}

function createMessageIndexes(db) {
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_messages_channel_ts_received ON messages(channel_id, ts_received);
    CREATE INDEX IF NOT EXISTS idx_messages_payload_type ON messages(payload_type, ts_received);
    CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages(sender_kind, sender_id, ts_received);
    CREATE INDEX IF NOT EXISTS idx_messages_correlation ON messages(correlation_id, ts_received);
    CREATE INDEX IF NOT EXISTS idx_messages_task ON messages(task_id, ts_received);
    CREATE INDEX IF NOT EXISTS idx_messages_parent ON messages(parent_id);
    CREATE INDEX IF NOT EXISTS idx_messages_not_before ON messages(not_before, ts_received);
    CREATE INDEX IF NOT EXISTS idx_messages_due ON messages(not_before, delivered_at);
    CREATE INDEX IF NOT EXISTS idx_messages_due_pending ON messages(not_before, delivered_at, delivery_failed_at);
  `);
}

function rebuildMessagesTableWithoutColumn(db, columnName) {
  const existingColumns = new Set(tableColumns(db, 'messages').map((row) => row.name));
  const missingColumns = MESSAGE_COLUMNS.filter((column) => !existingColumns.has(column));
  if (missingColumns.length > 0) {
    throw new Error(`messages table is missing required columns: ${missingColumns.join(', ')}`);
  }

  const tempTable = `messages_rebuild_${Date.now()}`;
  const columnsSql = MESSAGE_COLUMNS.map(quoteIdentifier).join(', ');
  db.transaction(() => {
    db.exec(createMessagesTableSql(tempTable, { ifNotExists: false }));
    db.exec(`
      INSERT INTO ${quoteIdentifier(tempTable)} (${columnsSql})
      SELECT ${columnsSql}
      FROM messages
    `);
    db.exec('DROP TABLE messages');
    db.exec(`ALTER TABLE ${quoteIdentifier(tempTable)} RENAME TO messages`);
  })();
  if (tableColumns(db, 'messages').some((row) => row.name === columnName)) {
    throw new Error('failed to remove obsolete messages column');
  }
}

function dropRemovedMessageColumn(db) {
  const columnName = REMOVED_MESSAGE_COLUMN;
  if (!tableColumns(db, 'messages').some((row) => row.name === columnName)) return;

  try {
    db.exec(`ALTER TABLE messages DROP COLUMN ${quoteIdentifier(columnName)}`);
  } catch {
    rebuildMessagesTableWithoutColumn(db, columnName);
  }

  if (tableColumns(db, 'messages').some((row) => row.name === columnName)) {
    throw new Error('failed to remove obsolete messages column');
  }
}

function rowToMessage(row) {
  return {
    ...row,
    payload_body: parseJsonStrict(row.payload_body, 'payload_body'),
    mentions: parseJsonNullable(row.mentions),
    envelope: parseJsonStrict(row.envelope_json, 'envelope_json'),
    payload: parseJsonStrict(row.payload_json, 'payload_json'),
  };
}

function rowToAppendResult(row, inserted) {
  const message = rowToMessage(row);
  Object.defineProperty(message, 'inserted', {
    value: inserted,
    enumerable: false,
  });
  return message;
}

function rowToTask(row) {
  return {
    task_id: row.task_id,
    channel_id: row.channel_id,
    parent_task_id: row.parent_task_id,
    type: row.type,
    title: row.title,
    initiator_kind: row.initiator_kind,
    initiator_id: row.initiator_id,
    status: row.status,
    opened_at: row.opened_at,
    last_event_at: row.last_event_at,
    closed_at: row.closed_at,
    doc_ref: row.doc_ref,
    primary_correlation: row.primary_correlation,
  };
}

export function messageStorePath(workdir) {
  return path.join(workdir, 'messages.sqlite');
}

export function agentCursorPath(workdir, agentName = DEFAULT_AGENT_NAME) {
  return path.join(workdir, 'agents', String(agentName || DEFAULT_AGENT_NAME), 'cursor.json');
}

export function readAgentCursor(workdir, agentName = DEFAULT_AGENT_NAME) {
  const filePath = agentCursorPath(workdir, agentName);
  try {
    const raw = JSON.parse(readFileSync(filePath, 'utf8'));
    return {
      last_seen_seq: toSeq(raw?.last_seen_seq ?? raw?.lastSeenSeq, 0),
      updated_at: raw?.updated_at ?? null,
    };
  } catch {
    return { last_seen_seq: 0, updated_at: null };
  }
}

export function writeAgentCursor(workdir, agentName = DEFAULT_AGENT_NAME, cursor = {}) {
  const filePath = agentCursorPath(workdir, agentName);
  mkdirSync(path.dirname(filePath), { recursive: true });
  const next = {
    last_seen_seq: toSeq(cursor.last_seen_seq ?? cursor.lastSeenSeq, 0),
    updated_at: cursor.updated_at ?? nowIso(),
  };
  writeFileSync(filePath, `${JSON.stringify(next, null, 2)}\n`, 'utf8');
  return next;
}

export function advanceAgentCursor(workdir, agentName = DEFAULT_AGENT_NAME, seq) {
  const current = readAgentCursor(workdir, agentName);
  const nextSeq = toSeq(seq, current.last_seen_seq);
  if (nextSeq <= current.last_seen_seq) return current;
  return writeAgentCursor(workdir, agentName, {
    last_seen_seq: nextSeq,
    updated_at: nowIso(),
  });
}

export function openMessageStore(workdir) {
  const db = new Database(messageStorePath(workdir));
  db.pragma('foreign_keys = ON');
  db.pragma('journal_mode = WAL');
  db.exec(`
    ${createMessagesTableSql()}

    CREATE TABLE IF NOT EXISTS tasks (
      task_id TEXT PRIMARY KEY,
      channel_id TEXT NOT NULL,
      parent_task_id TEXT DEFAULT NULL REFERENCES tasks(task_id),
      type TEXT NOT NULL,
      title TEXT NOT NULL,
      initiator_kind TEXT NOT NULL,
      initiator_id TEXT NOT NULL,
      status TEXT NOT NULL,
      opened_at INTEGER NOT NULL,
      last_event_at INTEGER NOT NULL,
      closed_at INTEGER DEFAULT NULL,
      doc_ref TEXT NOT NULL,
      primary_correlation TEXT DEFAULT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_tasks_channel_status ON tasks(channel_id, status);
    CREATE INDEX IF NOT EXISTS idx_tasks_parent ON tasks(parent_task_id);
    CREATE INDEX IF NOT EXISTS idx_tasks_last_event_at ON tasks(last_event_at);
  `);
  ensureColumn(db, 'messages', 'delivered_at', 'INTEGER DEFAULT NULL');
  ensureColumn(db, 'messages', 'delivery_failed_at', 'INTEGER DEFAULT NULL');
  ensureColumn(db, 'messages', 'delivery_attempts', 'INTEGER NOT NULL DEFAULT 0');
  ensureColumn(db, 'messages', 'last_attempt_at', 'INTEGER DEFAULT NULL');
  ensureColumn(db, 'messages', 'last_error', 'TEXT DEFAULT NULL');
  dropRemovedMessageColumn(db);
  createMessageIndexes(db);
  return db;
}

export function appendMessageToStore(db, message) {
  const envelope = message.envelope ?? {};
  const payload = message.payload ?? {};
  const sender = envelope.sender ?? {};
  const audience = Array.isArray(envelope.audience)
    ? envelope.audience[0]
    : (envelope.audience ?? message.audience ?? 'channel');

  const row = {
    id: envelope.id ?? message.messageId,
    channel_id: message.channelId,
    ts: toEpochMs(envelope.ts ?? message.createdAt),
    ts_received: toEpochMs(envelope.ts_received ?? message.tsReceived),
    sender_kind: sender.kind ?? message.senderKind,
    sender_id: sender.id ?? message.senderId,
    sender_name: sender.name ?? message.senderName ?? '',
    payload_type: payload.type ?? message.payloadType,
    payload_body: toJson(payload.body ?? message.payloadBody ?? {}),
    parent_id: envelope.parent_id ?? message.parentId ?? null,
    correlation_id: envelope.correlation_id ?? message.correlationId ?? null,
    task_id: envelope.task_id ?? message.taskId ?? null,
    thread_id: envelope.thread_id ?? message.threadId ?? null,
    audience: String(audience ?? 'channel'),
    mentions: toJson(envelope.mentions ?? message.mentions ?? null),
    origin: envelope.origin ?? message.origin ?? null,
    not_before: envelope.not_before ?? message.notBefore ?? null,
    expires_at: envelope.expires_at ?? message.expiresAt ?? null,
    envelope_json: toJson(envelope),
    payload_json: toJson(payload),
    created_at: message.createdAt,
  };
  assertKnownPayloadType(row.payload_type);

  const selectMessage = db.prepare('SELECT rowid AS seq, * FROM messages WHERE id = @id');
  const insertMessage = db.prepare(`
    INSERT INTO messages (
      id, channel_id, ts, ts_received, sender_kind, sender_id, sender_name,
      payload_type, payload_body, parent_id, correlation_id, task_id, thread_id,
      audience, mentions, origin, not_before, expires_at, envelope_json,
      payload_json, created_at
    )
    VALUES (
      @id, @channel_id, @ts, @ts_received, @sender_kind, @sender_id, @sender_name,
      @payload_type, @payload_body, @parent_id, @correlation_id, @task_id, @thread_id,
      @audience, @mentions, @origin, @not_before, @expires_at, @envelope_json,
      @payload_json, @created_at
    )
  `);

  return db.transaction(() => {
    const existing = selectMessage.get({ id: row.id });
    if (existing) return rowToAppendResult(existing, false);
    insertMessage.run(row);
    projectTaskFromMessageRow(db, row);
    return rowToAppendResult(selectMessage.get({ id: row.id }), true);
  })();
}

export function projectTaskFromMessageRow(db, row) {
  const payloadBody = parseJsonStrict(row.payload_body, 'payload_body');
  const envelope = parseJsonStrict(row.envelope_json, 'envelope_json');
  const rawAudience = envelope?.audience ?? row.audience;
  const audience = Array.isArray(rawAudience)
    ? rawAudience.map((item) => String(item))
    : [String(rawAudience ?? row.audience ?? '')];
  const isSelfAudience = audience.includes('self');
  const taskId = row.task_id ? String(row.task_id) : '';

  if (row.payload_type === PayloadType.TASK_OPENED && isSelfAudience) {
    if (!taskId) throw new Error('task.opened requires envelope.task_id');
    const title = String(payloadBody?.title ?? '').trim();
    const docRef = String(payloadBody?.doc_ref ?? payloadBody?.doc ?? '').trim();
    if (!title) throw new Error('task.opened requires payload.body.title');
    if (!docRef) throw new Error('task.opened requires payload.body.doc_ref');

    db.prepare(`
      INSERT INTO tasks (
        task_id, channel_id, parent_task_id, type, title, initiator_kind, initiator_id,
        status, opened_at, last_event_at, closed_at, doc_ref, primary_correlation
      )
      VALUES (
        @task_id, @channel_id, @parent_task_id, @type, @title, @initiator_kind, @initiator_id,
        @status, @opened_at, @last_event_at, NULL, @doc_ref, @primary_correlation
      )
    `).run({
      task_id: taskId,
      channel_id: row.channel_id,
      parent_task_id: payloadBody?.parent_task_id ?? payloadBody?.parentTaskId ?? null,
      type: String(payloadBody?.type ?? 'free').trim() || 'free',
      title,
      initiator_kind: row.sender_kind,
      initiator_id: row.sender_id,
      status: String(payloadBody?.status ?? 'opened').trim() || 'opened',
      opened_at: row.ts_received,
      last_event_at: row.ts_received,
      doc_ref: docRef,
      primary_correlation: row.correlation_id ?? null,
    });
  }

  if (row.payload_type === PayloadType.TASK_CLOSED && isSelfAudience && taskId) {
    assertTaskProjectionMutable(db, taskId, row.channel_id);
    const status = String(payloadBody?.status ?? '').trim();
    if (!status) throw new Error('task.closed requires payload.body.status');
    db.prepare(`
      UPDATE tasks
      SET status = @status,
          closed_at = @closed_at
      WHERE task_id = @task_id
        AND channel_id = @channel_id
    `).run({
      status,
      closed_at: row.ts_received,
      task_id: taskId,
      channel_id: row.channel_id,
    });
  }

  if (row.payload_type === PayloadType.TASK_APPENDED && isSelfAudience && taskId) {
    assertTaskProjectionMutable(db, taskId, row.channel_id);
  }

  if ([PayloadType.TASK_OPENED, PayloadType.TASK_CLOSED, PayloadType.TASK_APPENDED].includes(row.payload_type) && taskId) {
    db.prepare(`
      UPDATE tasks
      SET last_event_at = @last_event_at
      WHERE task_id = @task_id
        AND channel_id = @channel_id
    `).run({
      last_event_at: row.ts_received,
      task_id: taskId,
      channel_id: row.channel_id,
    });
  }
}

export function readStoredMessages(db) {
  return db.prepare('SELECT rowid AS seq, * FROM messages ORDER BY ts_received ASC').all().map(rowToMessage);
}

export function getStoredMessage(db, messageId) {
  const id = String(messageId ?? '').trim();
  if (!id) return null;
  const row = db.prepare('SELECT rowid AS seq, * FROM messages WHERE id = @id').get({ id });
  return row ? rowToMessage(row) : null;
}

export function getMaxStoredMessageSeq(db) {
  const row = db.prepare('SELECT max(rowid) AS seq FROM messages').get();
  return toSeq(row?.seq, 0);
}

export function queryStoredMessages(db, filters = {}) {
  const where = [];
  const params = {};
  const unread = toBoolean(filters.unread);

  const equals = [
    ['channel_id', 'channelId', 'channel_id'],
    ['correlation_id', 'correlationId', 'correlation_id'],
    ['task_id', 'taskId', 'task_id'],
    ['payload_type', 'payloadType', 'payload_type'],
    ['sender_kind', 'senderKind', 'sender_kind'],
    ['sender_id', 'senderId', 'sender_id'],
    ['audience', 'audience', 'audience'],
  ];

  for (const [column, camelKey, snakeKey] of equals) {
    const value = filters[snakeKey] ?? filters[camelKey];
    if (value == null || value === '') continue;
    where.push(`${column} = @${column}`);
    params[column] = String(value);
  }

  const notBeforeLte = optionalEpochMs(filters.not_before_lte ?? filters.notBeforeLte ?? filters.not_before_before);
  if (notBeforeLte != null) {
    where.push('not_before IS NOT NULL AND not_before <= @not_before_lte');
    params.not_before_lte = notBeforeLte;
  }

  const notBeforeGte = optionalEpochMs(filters.not_before_gte ?? filters.notBeforeGte ?? filters.not_before_after);
  if (notBeforeGte != null) {
    where.push('not_before IS NOT NULL AND not_before >= @not_before_gte');
    params.not_before_gte = notBeforeGte;
  }

  if (unread) {
    params.cursor_seq = toSeq(
      filters.cursor_seq ?? filters.cursorSeq ?? filters.last_seen_seq ?? filters.lastSeenSeq,
      0,
    );
    where.push('rowid > @cursor_seq');
    if (!toBoolean(filters.include_future ?? filters.includeFuture)) {
      params.now_ms = optionalEpochMs(filters.now_ms ?? filters.nowMs) ?? Date.now();
      where.push('(not_before IS NULL OR not_before <= @now_ms)');
    }
  } else if (filters.delivered === false) {
    where.push('delivered_at IS NULL');
  } else if (filters.delivered === true) {
    where.push('delivered_at IS NOT NULL');
  }

  const text = String(filters.text ?? filters.query ?? '').trim().toLowerCase();
  if (text) {
    where.push(`(
      instr(lower(coalesce(payload_json, '')), @text) > 0
      OR instr(lower(coalesce(payload_body, '')), @text) > 0
    )`);
    params.text = text;
  }

  const tag = filters.tag;
  if (tag != null && tag !== '') {
    where.push("trim(CAST(json_extract(payload_body, '$.tag') AS TEXT)) = @payload_tag");
    params.payload_tag = String(tag).trim();
  }

  const status = filters.status && filters.status !== 'all' ? filters.status : null;
  if (status != null && status !== '') {
    where.push("trim(CAST(json_extract(payload_body, '$.status') AS TEXT)) = @payload_status");
    params.payload_status = String(status).trim();
  }

  const order = String(filters.order ?? 'desc').toLowerCase() === 'asc' ? 'ASC' : 'DESC';
  const limit = Math.max(1, Math.min(Number.parseInt(filters.limit, 10) || 50, 500));
  const orderBy = unread ? 'rowid ASC' : `ts_received ${order}`;
  const sql = [
    'SELECT rowid AS seq, * FROM messages',
    where.length > 0 ? `WHERE ${where.join(' AND ')}` : '',
    `ORDER BY ${orderBy}`,
    'LIMIT @limit',
  ].filter(Boolean).join(' ');

  return db.prepare(sql).all({ ...params, limit }).map(rowToMessage);
}

export function queryStoredTasks(db, filters = {}) {
  const where = [];
  const params = {};

  const channelId = filters.channel_id ?? filters.channelId;
  if (channelId != null && channelId !== '') {
    where.push('channel_id = @channel_id');
    params.channel_id = String(channelId);
  }

  const parentTaskId = filters.parent_task_id ?? filters.parentTaskId ?? filters.parent;
  if (parentTaskId === null) {
    where.push('parent_task_id IS NULL');
  } else if (parentTaskId != null && parentTaskId !== '') {
    where.push('parent_task_id = @parent_task_id');
    params.parent_task_id = String(parentTaskId);
  }

  const status = String(filters.status ?? '').trim();
  if (status === 'active') {
    where.push("status IN ('opened', 'active', 'blocked') AND closed_at IS NULL");
  } else if (status) {
    where.push('status = @status');
    params.status = status;
  }

  if (filters.mine === true || filters.mine === 'true') {
    where.push('initiator_kind = @initiator_kind');
    params.initiator_kind = String(filters.initiator_kind ?? filters.initiatorKind ?? 'agent');
    const initiatorId = filters.initiator_id ?? filters.initiatorId;
    if (initiatorId != null && initiatorId !== '') {
      where.push('initiator_id = @initiator_id');
      params.initiator_id = String(initiatorId);
    }
  }

  const order = String(filters.order ?? 'last_event_desc').toLowerCase();
  const orderSql = order === 'opened_asc'
    ? 'opened_at ASC'
    : order === 'last_event_asc'
      ? 'last_event_at ASC'
      : 'last_event_at DESC';
  const limit = Math.max(1, Math.min(Number.parseInt(filters.limit, 10) || 200, 500));

  const sql = [
    'SELECT * FROM tasks',
    where.length > 0 ? `WHERE ${where.join(' AND ')}` : '',
    `ORDER BY ${orderSql}`,
    'LIMIT @limit',
  ].filter(Boolean).join(' ');

  return db.prepare(sql).all({ ...params, limit }).map(rowToTask);
}

export function getStoredTask(db, taskId, channelId = null) {
  const id = String(taskId ?? '').trim();
  if (!id) return null;
  const row = channelId == null || channelId === ''
    ? db.prepare('SELECT * FROM tasks WHERE task_id = @task_id').get({ task_id: id })
    : db.prepare('SELECT * FROM tasks WHERE task_id = @task_id AND channel_id = @channel_id').get({
      task_id: id,
      channel_id: String(channelId),
    });
  return row ? rowToTask(row) : null;
}

export function readDueMessages(db, nowMs = Date.now(), limit = 50) {
  return db.prepare(`
    SELECT rowid AS seq, * FROM messages
    WHERE not_before IS NOT NULL
      AND not_before <= @nowMs
      AND delivered_at IS NULL
      AND delivery_failed_at IS NULL
    ORDER BY not_before ASC, ts_received ASC
    LIMIT @limit
  `).all({
    nowMs: toEpochMs(nowMs),
    limit: Math.max(1, Math.min(Number.parseInt(limit, 10) || 50, 500)),
  }).map(rowToMessage);
}

export function markMessageDelivered(db, messageId, deliveredAt = Date.now()) {
  const deliveredAtMs = toEpochMs(deliveredAt);
  db.prepare(`
    UPDATE messages
    SET delivered_at = @delivered_at,
        delivery_attempts = delivery_attempts + 1,
        last_attempt_at = @delivered_at,
        last_error = NULL
    WHERE id = @id
  `).run({
    id: messageId,
    delivered_at: deliveredAtMs,
  });
  const row = db.prepare('SELECT rowid AS seq, * FROM messages WHERE id = @id').get({ id: messageId });
  return row ? rowToMessage(row) : null;
}

export function markMessageDeliveryFailed(db, messageId, failedAt = Date.now()) {
  const failedAtMs = toEpochMs(failedAt);
  const result = db.prepare(`
    UPDATE messages
    SET delivery_failed_at = @delivery_failed_at
    WHERE id = @id
      AND delivery_failed_at IS NULL
  `).run({
    id: messageId,
    delivery_failed_at: failedAtMs,
  });
  const row = db.prepare('SELECT rowid AS seq, * FROM messages WHERE id = @id').get({ id: messageId });
  if (!row) return null;
  const message = rowToMessage(row);
  Object.defineProperties(message, {
    deliveryFailedChanged: {
      value: result.changes > 0,
      enumerable: false,
    },
    delivery_failed_changed: {
      value: result.changes > 0,
      enumerable: false,
    },
    delivery_failed_changes: {
      value: result.changes,
      enumerable: false,
    },
  });
  return message;
}

export function markMessageDeliveryAttempt(db, messageId, {
  attemptedAt = Date.now(),
  error = 'delivery failed',
} = {}) {
  db.prepare(`
    UPDATE messages
    SET delivery_attempts = delivery_attempts + 1,
        last_attempt_at = @last_attempt_at,
        last_error = @last_error
    WHERE id = @id
  `).run({
    id: messageId,
    last_attempt_at: toEpochMs(attemptedAt),
    last_error: String(error ?? 'delivery failed').slice(0, 1000),
  });
  const row = db.prepare('SELECT rowid AS seq, * FROM messages WHERE id = @id').get({ id: messageId });
  return row ? rowToMessage(row) : null;
}
