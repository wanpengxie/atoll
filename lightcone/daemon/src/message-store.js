import Database from 'better-sqlite3';
import path from 'path';

function toJson(value) {
  return JSON.stringify(value ?? null);
}

function fromJson(value, fallback = null) {
  if (value == null) return fallback;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
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

export function messageStorePath(workdir) {
  return path.join(workdir, 'messages.sqlite');
}

export function openMessageStore(workdir) {
  const db = new Database(messageStorePath(workdir));
  db.pragma('journal_mode = WAL');
  db.exec(`
    CREATE TABLE IF NOT EXISTS messages (
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
      envelope_json TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      legacy_json TEXT NOT NULL,
      created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_messages_channel_ts_received ON messages(channel_id, ts_received);
    CREATE INDEX IF NOT EXISTS idx_messages_payload_type ON messages(payload_type, ts_received);
    CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages(sender_kind, sender_id, ts_received);
    CREATE INDEX IF NOT EXISTS idx_messages_correlation ON messages(correlation_id, ts_received);
    CREATE INDEX IF NOT EXISTS idx_messages_task ON messages(task_id, ts_received);
    CREATE INDEX IF NOT EXISTS idx_messages_parent ON messages(parent_id);
    CREATE INDEX IF NOT EXISTS idx_messages_not_before ON messages(not_before, ts_received);
  `);
  return db;
}

export function appendMessageToStore(db, message) {
  const envelope = message.envelope ?? {};
  const payload = message.payload ?? {};
  const sender = envelope.sender ?? {};
  const audience = Array.isArray(envelope.audience)
    ? envelope.audience[0]
    : (envelope.audience ?? message.audience ?? 'channel');

  db.prepare(`
    INSERT INTO messages (
      id, channel_id, ts, ts_received, sender_kind, sender_id, sender_name,
      payload_type, payload_body, parent_id, correlation_id, task_id, thread_id,
      audience, mentions, origin, not_before, expires_at, envelope_json,
      payload_json, legacy_json, created_at
    )
    VALUES (
      @id, @channel_id, @ts, @ts_received, @sender_kind, @sender_id, @sender_name,
      @payload_type, @payload_body, @parent_id, @correlation_id, @task_id, @thread_id,
      @audience, @mentions, @origin, @not_before, @expires_at, @envelope_json,
      @payload_json, @legacy_json, @created_at
    )
  `).run({
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
    legacy_json: toJson(message),
    created_at: message.createdAt,
  });
}

export function readStoredMessages(db) {
  return db.prepare('SELECT * FROM messages ORDER BY ts_received ASC').all().map((row) => ({
    ...row,
    payload_body: fromJson(row.payload_body, {}),
    mentions: fromJson(row.mentions, null),
    envelope: fromJson(row.envelope_json, {}),
    payload: fromJson(row.payload_json, {}),
    legacy: fromJson(row.legacy_json, {}),
  }));
}
