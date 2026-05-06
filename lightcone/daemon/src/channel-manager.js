import { spawn } from 'child_process';
import { randomUUID } from 'crypto';
import {
  appendFileSync,
  cpSync,
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  renameSync,
  statSync,
  unlinkSync,
  writeFileSync,
} from 'fs';
import { homedir } from 'os';
import path from 'path';
import { fileURLToPath } from 'url';
import { PayloadType, SenderKind, isPayloadType } from '@coagent/payload-types';
import { HEALTHY_UPTIME_MS, CronScheduler } from './cron-scheduler.js';
import { buildCoagentSpawn } from './drivers/coagent.js';
import { emitJsonEvent } from './events.js';
import {
  advanceAgentCursor,
  appendMessageToStore,
  getStoredTask,
  markMessageDeliveryAttempt,
  markMessageDeliveryFailed,
  markMessageDelivered,
  openMessageStore,
  queryStoredMessages,
  queryStoredTasks,
  readAgentCursor,
  readDueMessages,
} from './message-store.js';
import { coagentProjectDir, normalizeProjectKey } from './paths.js';
import { TriggerGateway } from './trigger-gateway.js';
import { WorkdirWatcher } from './workdir-watcher.js';

const DEFAULT_AGENT_NAME = 'channel-agent';
const SHUTDOWN_GRACE_MS = 5_000;
const DEFAULT_DUE_MESSAGE_POLL_MS = 1_000;
const DEFAULT_DELIVERY_FAILURE_LIMIT = 5;
const VIEW_SYNC_RETRY_LIMIT = 10;
const SCHEDULE_ID_PATTERN = /^[A-Za-z0-9_-]{1,128}$/;
const TERMINAL_TASK_STATUSES = new Set(['completed', 'failed', 'abandoned', 'archived']);

function nowIso() {
  return new Date().toISOString();
}

function toRpcError(code, message, statusCode = 400) {
  const err = new Error(message);
  err.code = code;
  err.statusCode = statusCode;
  return err;
}

function normalizeCapabilitySet(raw) {
  const cliBinaries = Array.isArray(raw?.cli_binaries)
    ? raw.cli_binaries
    : Array.isArray(raw?.cliBinaries)
      ? raw.cliBinaries
      : [];

  return {
    cli_binaries: [...new Set(cliBinaries.map((item) => String(item).trim()).filter(Boolean))],
  };
}

function normalizeChannelTypeConfig(raw) {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return null;
  return raw;
}

function normalizeMembers(rawMembers) {
  const members = Array.isArray(rawMembers) ? rawMembers : [];
  const seen = new Set();
  const normalized = [];

  for (const member of members) {
    const memberType = String(member?.memberType ?? member?.member_type ?? '').trim();
    const memberId = String(member?.memberId ?? member?.member_id ?? '').trim();
    if (!memberType || !memberId) continue;

    const key = `${memberType}:${memberId}`;
    if (seen.has(key)) continue;
    seen.add(key);
    normalized.push({
      memberType,
      memberId,
      displayName: member?.displayName ?? member?.display_name ?? memberId,
      joinedAt: member?.joinedAt ?? member?.joined_at ?? nowIso(),
    });
  }

  return normalized;
}

function normalizeChannelPayload(input) {
  const channelId = String(input?.channelId ?? input?.channel_id ?? input?.id ?? '').trim();
  if (!channelId) throw toRpcError('bad_request', 'channel_id is required');

  return {
    channelId,
    workspaceId: String(input?.workspaceId ?? input?.workspace_id ?? '').trim(),
    daemonId: String(input?.daemonId ?? input?.daemon_id ?? '').trim(),
    name: String(input?.name ?? '').trim(),
    type: String(input?.type ?? 'xhs-creator').trim(),
    channelTypeConfig: normalizeChannelTypeConfig(input?.channelTypeConfig ?? input?.channel_type_config),
    status: String(input?.status ?? 'created').trim(),
    capabilitySet: normalizeCapabilitySet(input?.capabilitySet ?? input?.capability_set ?? {}),
    members: normalizeMembers(input?.members),
    agentName: String(input?.agentName ?? input?.agent_name ?? DEFAULT_AGENT_NAME).trim() || DEFAULT_AGENT_NAME,
    createdAt: input?.createdAt ?? input?.created_at ?? nowIso(),
    archivedAt: input?.archivedAt ?? input?.archived_at ?? null,
  };
}

function normalizeEvent(rawEvent) {
  const event = rawEvent?.event ?? rawEvent;
  const type = String(event?.type ?? '').trim();
  if (!type) throw toRpcError('bad_request', 'event.type is required');

  return {
    type,
    payload: event?.payload ?? {},
    source: event?.source ?? 'server',
    createdAt: event?.createdAt ?? event?.created_at ?? nowIso(),
  };
}

function parseJsonString(value, fallback) {
  if (typeof value !== 'string') return value ?? fallback;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function toEpochMs(value, fallback = Date.now()) {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (value instanceof Date && !Number.isNaN(value.getTime())) return value.getTime();
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

function isTruthyOption(value) {
  return value === true || String(value ?? '').trim().toLowerCase() === 'true';
}

function normalizeScheduleId(value) {
  const scheduleId = String(value ?? '').trim();
  if (!SCHEDULE_ID_PATTERN.test(scheduleId)) {
    throw toRpcError('bad_request', 'schedule id must match ^[A-Za-z0-9_-]{1,128}$');
  }
  return scheduleId;
}

function assertPathInsideDirectory(rootDir, candidatePath) {
  const root = path.resolve(rootDir);
  const candidate = path.resolve(candidatePath);
  const relative = path.relative(root, candidate);
  if (!relative || relative.startsWith('..') || path.isAbsolute(relative)) {
    throw toRpcError('bad_request', 'schedule path must stay inside schedules directory');
  }
}

function safeRelativePath(value) {
  const text = String(value ?? '').trim();
  if (!text || path.isAbsolute(text)) return null;
  const parts = text.split(/[\\/]+/);
  if (parts.includes('..')) return null;
  return parts.join(path.sep);
}

function readDocIfPresent(workdir, docRef) {
  const relative = safeRelativePath(docRef);
  if (!relative) return null;
  const absolute = path.resolve(workdir, relative);
  const root = path.resolve(workdir);
  if (absolute !== root && !absolute.startsWith(`${root}${path.sep}`)) return null;
  if (!existsSync(absolute)) return null;
  return readFileSync(absolute, 'utf8');
}

function resolveDocPath(workdir, docRef) {
  const relative = safeRelativePath(docRef);
  if (!relative) {
    throw toRpcError('bad_request', 'doc_ref must be a relative path inside the channel workdir');
  }
  const absolute = path.resolve(workdir, relative);
  const root = path.resolve(workdir);
  if (absolute === root || !absolute.startsWith(`${root}${path.sep}`)) {
    throw toRpcError('bad_request', 'doc_ref must stay inside the channel workdir');
  }
  return absolute;
}

function writeTaskDocAtomic(workdir, docRef, content, { overwrite }) {
  const absolute = resolveDocPath(workdir, docRef);
  mkdirSync(path.dirname(absolute), { recursive: true });
  const existedBefore = existsSync(absolute);
  if (!overwrite && existedBefore) {
    return { absolute, wrote: false, created: false, previous: null };
  }

  const previous = overwrite && existedBefore ? readFileSync(absolute, 'utf8') : null;
  const tmpPath = `${absolute}.tmp-${process.pid}-${Date.now()}-${randomUUID()}`;
  try {
    writeFileSync(tmpPath, content, 'utf8');
    if (!overwrite && existsSync(absolute)) {
      unlinkSync(tmpPath);
      return { absolute, wrote: false, created: false, previous: null };
    }
    renameSync(tmpPath, absolute);
    return { absolute, wrote: true, created: !existedBefore, previous };
  } catch (err) {
    try {
      if (existsSync(tmpPath)) unlinkSync(tmpPath);
    } catch {
      // Preserve the original filesystem failure.
    }
    throw err;
  }
}

function removeCreatedTaskDoc(writeResult) {
  if (!writeResult?.created) return;
  try {
    if (existsSync(writeResult.absolute)) unlinkSync(writeResult.absolute);
  } catch {
    // Best-effort rollback; surface the original RPC failure.
  }
}

function restoreTaskDoc(workdir, docRef, previous) {
  try {
    if (previous == null) {
      const absolute = resolveDocPath(workdir, docRef);
      if (existsSync(absolute)) unlinkSync(absolute);
      return;
    }
    writeTaskDocAtomic(workdir, docRef, previous, { overwrite: true });
  } catch {
    // Best-effort rollback; surface the original RPC failure.
  }
}

function taskDocTemplate({ taskId, type, title, parentTaskId, rationale }) {
  const timestamp = nowIso();
  return [
    `# ${title}`,
    '',
    '## Brief',
    '',
    rationale || 'TBD',
    '',
    '## Stakeholders',
    '',
    '- Initiator: channel-agent',
    '',
    '## Decisions',
    '',
    '- TBD',
    '',
    '## Constraints',
    '',
    '- TBD',
    '',
    '## Refs',
    '',
    `- Task ID: ${taskId}`,
    `- Type: ${type}`,
    ...(parentTaskId ? [`- Parent Task: ${parentTaskId}`] : []),
    '',
    '## Timeline',
    '',
    `- ${timestamp} - Task opened.`,
    '',
    '## Status',
    '',
    'Status: opened',
    '',
  ].join('\n');
}

function appendClosedStatus(content, status, summary, resultRef) {
  const lines = [
    '## Status',
    '',
    ...(summary ? [`Summary: ${summary}`] : []),
    ...(resultRef ? [`Result ref: ${resultRef}`] : []),
    `Status: ${status}`,
  ];
  return `${content.trimEnd()}\n\n${lines.join('\n')}\n`;
}

function appendTimeline(content, summary) {
  const line = `- ${nowIso()} - ${summary}`;
  const statusMarker = '\n## Status';
  const statusIndex = content.lastIndexOf(statusMarker);
  if (statusIndex >= 0) {
    return `${content.slice(0, statusIndex).trimEnd()}\n${line}\n${content.slice(statusIndex)}`
      .replace(/\n?$/, '\n');
  }
  return `${content.trimEnd()}\n\n## Timeline\n\n${line}\n`;
}

function buildTaskTree(tasks, rootTaskId = null) {
  const byParent = new Map();
  for (const task of tasks) {
    const parent = task.parent_task_id ?? null;
    const children = byParent.get(parent) ?? [];
    children.push(task);
    byParent.set(parent, children);
  }

  const makeNode = (task) => ({
    ...task,
    children: (byParent.get(task.task_id) ?? []).map(makeNode),
  });

  if (rootTaskId) {
    const root = tasks.find((task) => task.task_id === rootTaskId);
    return root ? [makeNode(root)] : [];
  }

  return (byParent.get(null) ?? []).map(makeNode);
}

function parseDurationMs(value, fallback = 10 * 60 * 1000) {
  if (typeof value === 'number' && Number.isFinite(value) && value > 0) return value;
  const text = String(value ?? '').trim();
  if (!text) return fallback;
  const numeric = Number(text);
  if (Number.isFinite(numeric) && numeric > 0) return numeric;
  const match = text.match(/^(\d+(?:\.\d+)?)(ms|s|m|h|d)$/i);
  if (!match) return fallback;
  const amount = Number(match[1]);
  const unit = match[2].toLowerCase();
  const multipliers = { ms: 1, s: 1000, m: 60_000, h: 3_600_000, d: 86_400_000 };
  return Math.round(amount * multipliers[unit]);
}

function normalizePayloadInput(input, defaultType = PayloadType.AGENT_TEXT) {
  const payload = input?.payload ?? {};
  const payloadType = String(
    payload.type
      ?? input?.payloadType
      ?? input?.payload_type
      ?? defaultType,
  ).trim();
  const payloadBody = payload.body
    ?? input?.payloadBody
    ?? input?.payload_body
    ?? (payload.type || payload.body ? {} : payload)
    ?? {};
  return { payloadType, payloadBody };
}

const UNFILTERED_UNREAD_CURSOR_ADVANCE_KEYS = new Set([
  'channel_id',
  'channelId',
  'unread',
  'include_future',
  'includeFuture',
  'limit',
  'order',
  'cursor_seq',
  'cursorSeq',
  'now_ms',
  'nowMs',
]);

function hasNonEmptyQueryValue(value) {
  return value != null && String(value).trim() !== '';
}

function isUnreadFullScanQuery(params = {}) {
  return Object.entries(params).every(([key, value]) => (
    UNFILTERED_UNREAD_CURSOR_ADVANCE_KEYS.has(key) || !hasNonEmptyQueryValue(value)
  ));
}

function normalizeSenderKind(rawKind, rawSenderType) {
  const kind = String(rawKind ?? '').trim();
  if (Object.values(SenderKind).includes(kind)) return kind;

  switch (String(rawSenderType ?? '').trim()) {
    case 'human':
    case 'user':
      return SenderKind.HUMAN;
    case 'agent':
    case 'channel_agent':
    case 'sub_agent':
    case 'worker':
      return SenderKind.AGENT;
    case 'system':
      return SenderKind.SYSTEM;
    case 'external':
    case 'device':
      return SenderKind.EXTERNAL;
    default:
      return SenderKind.HUMAN;
  }
}

function normalizeLegacySenderType(senderKind, provided) {
  const senderType = String(provided ?? '').trim();
  if (senderType) return senderType;
  if (senderKind === SenderKind.AGENT) return 'channel_agent';
  if (senderKind === SenderKind.HUMAN) return 'human';
  return senderKind;
}

function normalizeMentions(value) {
  const parsed = parseJsonString(value, value);
  return Array.isArray(parsed) ? parsed.map((item) => String(item).trim()).filter(Boolean) : null;
}

function normalizeAudience(value) {
  const parsed = parseJsonString(value, value);
  const values = Array.isArray(parsed) ? parsed : [parsed ?? 'channel'];
  const normalized = values.map((item) => String(item).trim()).filter(Boolean);
  return normalized.length > 0 ? normalized : ['channel'];
}

function isSelfAudienceOnly(audience) {
  return Array.isArray(audience) && audience.length === 1 && audience[0] === 'self';
}

function inferPayloadType(message, senderKind, messageType) {
  const explicit = message?.payload?.type ?? message?.payloadType ?? message?.payload_type;
  if (explicit) return String(explicit).trim();
  const legacyType = String(messageType ?? '').trim();
  if (legacyType.includes('.')) return legacyType;
  if (!legacyType || legacyType === 'chat') {
    return senderKind === SenderKind.AGENT ? PayloadType.AGENT_TEXT : PayloadType.USER_TEXT;
  }
  return legacyType;
}

function assertKnownPayloadType(payloadType) {
  if (!isPayloadType(payloadType)) {
    throw toRpcError('bad_request', `unsupported payload_type: ${payloadType}`);
  }
}

function normalizePayloadBody(message, content, attachments) {
  const explicit = message?.payload?.body ?? message?.payloadBody ?? message?.payload_body;
  if (explicit != null) return parseJsonString(explicit, explicit);
  return {
    text: content,
    ...(attachments.length > 0 ? { attachments } : {}),
  };
}

function inferOrigin(message, senderKind) {
  const explicit = message?.envelope?.origin ?? message?.origin;
  if (explicit) return String(explicit).trim();
  if (senderKind === SenderKind.SYSTEM) return 'system';
  if (senderKind === SenderKind.AGENT) return 'self';
  return 'external';
}

function normalizeMessage(channelId, message, defaults = {}) {
  const sender = message?.envelope?.sender ?? {};
  const senderKind = normalizeSenderKind(
    sender.kind ?? message?.senderKind ?? message?.sender_kind,
    message?.senderType ?? message?.sender_type ?? defaults.senderType,
  );
  const senderType = normalizeLegacySenderType(
    senderKind,
    message?.senderType ?? message?.sender_type ?? defaults.senderType,
  );
  const senderId = String(sender.id ?? message?.senderId ?? message?.sender_id ?? defaults.senderId ?? DEFAULT_AGENT_NAME);
  const senderName = String(sender.name ?? message?.senderName ?? message?.sender_name ?? defaults.senderName ?? defaults.senderId ?? senderId);
  const attachments = Array.isArray(message?.attachments) ? message.attachments : [];
  const payloadType = inferPayloadType(message, senderKind, message?.messageType ?? message?.message_type);
  assertKnownPayloadType(payloadType);
  const payloadBody = normalizePayloadBody(message, String(message?.content ?? message?.payload?.body?.text ?? '').trim(), attachments);
  const content = String(message?.content ?? payloadBody?.text ?? '').trim()
    || (message?.payload ? JSON.stringify(payloadBody) : '');
  if (!content && !message?.payload && message?.payloadBody == null && message?.payload_body == null) {
    throw toRpcError('bad_request', 'message content is required');
  }

  const nowMs = Date.now();
  const ts = toEpochMs(message?.envelope?.ts ?? message?.ts ?? message?.createdAt ?? message?.created_at, nowMs);
  const tsReceived = toEpochMs(message?.envelope?.ts_received ?? message?.tsReceived ?? message?.ts_received, nowMs);
  const createdAt = message?.createdAt ?? message?.created_at ?? new Date(ts).toISOString();
  const messageId = String(message?.envelope?.id ?? message?.messageId ?? message?.message_id ?? randomUUID());
  const mentions = normalizeMentions(message?.envelope?.mentions ?? message?.mentions);
  const audience = normalizeAudience(message?.envelope?.audience ?? message?.audience);
  const parentId = message?.envelope?.parent_id ?? message?.parentId ?? message?.parent_id ?? null;
  const correlationId = message?.envelope?.correlation_id ?? message?.correlationId ?? message?.correlation_id ?? null;
  const taskId = message?.envelope?.task_id ?? message?.taskId ?? message?.task_id ?? null;
  const threadId = message?.envelope?.thread_id ?? message?.threadId ?? message?.thread_id ?? null;
  const notBefore = optionalEpochMs(message?.envelope?.not_before ?? message?.notBefore ?? message?.not_before);
  const expiresAt = optionalEpochMs(message?.envelope?.expires_at ?? message?.expiresAt ?? message?.expires_at);
  const origin = inferOrigin(message, senderKind);
  const envelope = {
    id: messageId,
    ts,
    ts_received: tsReceived,
    sender: {
      kind: senderKind,
      id: senderId,
      ...(senderName ? { name: senderName } : {}),
    },
    audience,
    origin,
    ...(parentId ? { parent_id: parentId } : {}),
    ...(correlationId ? { correlation_id: correlationId } : {}),
    ...(taskId ? { task_id: taskId } : {}),
    ...(threadId ? { thread_id: threadId } : {}),
    ...(mentions ? { mentions } : {}),
    ...(notBefore != null ? { not_before: notBefore } : {}),
    ...(expiresAt != null ? { expires_at: expiresAt } : {}),
  };
  const payload = {
    type: payloadType,
    body: payloadBody,
  };

  return {
    messageId,
    channelId,
    senderType,
    senderId,
    senderName,
    content,
    attachments,
    messageType: String(message?.messageType ?? message?.message_type ?? 'chat'),
    createdAt,
    source: message?.source ?? defaults.source ?? 'daemon',
    senderKind,
    payloadType,
    payloadBody,
    parentId,
    correlationId,
    taskId,
    threadId,
    audience,
    mentions,
    origin,
    notBefore,
    expiresAt,
    tsReceived,
    envelope,
    payload,
  };
}

function assertDeferredMessageEnvelope(message) {
  if (message.notBefore == null) return;
  if (message.payloadType?.startsWith?.('task.')) {
    throw toRpcError(
      'invalid_envelope',
      'task lifecycle payload cannot be scheduled (not_before forbidden)',
    );
  }
  if (isSelfAudienceOnly(message.audience) || message.origin === 'system') return;
  throw toRpcError(
    'invalid_envelope',
    'not_before requires audience=[self] or origin=system',
  );
}

function isTerminalTask(task) {
  return task?.closed_at != null || TERMINAL_TASK_STATUSES.has(String(task?.status ?? '').trim());
}

function assertTaskMutable(task, taskId) {
  if (isTerminalTask(task)) {
    throw toRpcError('task_already_terminal', `task already terminal: ${taskId}`, 409);
  }
}

function parseStructuredFile(filePath) {
  return JSON.parse(readFileSync(filePath, 'utf8'));
}

function writeStructuredFile(filePath, data, options = {}) {
  if (options.rootDir) {
    assertPathInsideDirectory(options.rootDir, filePath);
  }
  writeFileSync(filePath, `${JSON.stringify(data, null, 2)}\n`, 'utf8');
}

function ensureDirectory(dirPath) {
  mkdirSync(dirPath, { recursive: true });
  return dirPath;
}

function repoRoot() {
  return path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');
}

function sortByCreatedAt(items) {
  return [...items].sort((left, right) => {
    const a = new Date(left.createdAt ?? left.created_at ?? 0).getTime();
    const b = new Date(right.createdAt ?? right.created_at ?? 0).getTime();
    return a - b;
  });
}

function legacyMessageOutput(stored) {
  if (stored?.legacy && Object.keys(stored.legacy).length > 0) return stored.legacy;
  return stored;
}

export class ChannelManager {
  constructor({
    serverUrl,
    machineApiKey,
    daemonSocketPath,
    daemonHttpUrl = '',
    daemonToken = '',
    projectKey = process.env.COAGENT_PROJECT_KEY,
    baseDir = null,
    dueMessagePollMs = DEFAULT_DUE_MESSAGE_POLL_MS,
  }) {
    this.serverUrl = serverUrl;
    this.machineApiKey = machineApiKey;
    this.projectKey = normalizeProjectKey(projectKey);
    this.daemonSocketPath = daemonSocketPath;
    this.daemonHttpUrl = daemonHttpUrl;
    this.daemonToken = daemonToken;
    this.connection = null;
    this.channels = new Map();
    this.baseDir = ensureDirectory(baseDir ?? coagentProjectDir(this.projectKey));
    this.channelsDir = ensureDirectory(path.join(this.baseDir, 'channels'));
    this.archivedDir = ensureDirectory(path.join(this.baseDir, 'archived'));
    this.workspaceTemplateDir = path.join(repoRoot(), 'workspace-template');
    this.dueMessagePollMs = dueMessagePollMs;
    this.dueMessageTimer = null;
    this.pendingViewSyncReplay = null;
    this._taskDocMutex = new Map();
    this.cronScheduler = new CronScheduler({
      onTick: async (tick) => {
        await this._handleCronTick(tick);
      },
    });
    this.triggerGateway = new TriggerGateway({
      onReact: async (channel, event, outcome) => {
        await this._recordTrace(channel, {
          kind: 'trigger',
          decision: 'react',
          reason: outcome.reason,
          event,
        });
        return this._deliverEvent(channel, event);
      },
      onLogOnly: async (channel, event, outcome) => {
        await this._recordTrace(channel, {
          kind: 'trigger',
          decision: 'log_only',
          reason: outcome.reason,
          event,
        });
      },
      onBlock: async (channel, event, outcome) => {
        await this._recordTrace(channel, {
          kind: 'trigger',
          decision: 'block',
          reason: outcome.reason,
          event,
        });
      },
    });
  }

  setConnection(connection) {
    this.connection = connection;
    if (connection) {
      this._schedulePendingViewSyncReplay();
    }
  }

  _readTaskDocIfPresent(workdir, docRef) {
    return readDocIfPresent(workdir, docRef);
  }

  _writeTaskDocAtomic(workdir, docRef, content, options) {
    return writeTaskDocAtomic(workdir, docRef, content, options);
  }

  _restoreTaskDoc(workdir, docRef, previous) {
    restoreTaskDoc(workdir, docRef, previous);
  }

  _taskDocLockKey(channelId, docRef) {
    const relativeDocRef = safeRelativePath(docRef) ?? String(docRef ?? '').trim();
    return `${String(channelId ?? '').trim()}\0${relativeDocRef}`;
  }

  async _withTaskDocLock(channelId, docRef, fn) {
    const key = this._taskDocLockKey(channelId, docRef);
    const previous = this._taskDocMutex.get(key) ?? Promise.resolve();
    let release;
    const gate = new Promise((resolve) => {
      release = resolve;
    });
    const tail = previous.catch(() => {}).then(() => gate);
    this._taskDocMutex.set(key, tail);

    await previous.catch(() => {});
    try {
      return await fn();
    } finally {
      release();
      if (this._taskDocMutex.get(key) === tail) {
        this._taskDocMutex.delete(key);
      }
    }
  }

  _schedulePendingViewSyncReplay() {
    if (this.pendingViewSyncReplay) return;
    this.pendingViewSyncReplay = this._replayPendingViewSyncForAll()
      .catch((err) => {
        console.error('[ChannelManager] Pending view sync replay failed:', err.message);
      })
      .finally(() => {
        this.pendingViewSyncReplay = null;
      });
  }

  canHandle(message) {
    return [
      'channel:create',
      'channel:start',
      'channel:pause',
      'channel:resume',
      'channel:archive',
      'channel:event',
      'channel:message.send',
      'channel:rpc',
    ].includes(message?.type);
  }

  async start() {
    if (!existsSync(this.channelsDir)) return;

    for (const entry of readdirSync(this.channelsDir)) {
      const workdir = path.join(this.channelsDir, entry);
      if (!statSync(workdir).isDirectory()) continue;

      try {
        const node = this._loadNodeFromDisk(workdir);
        this.channels.set(node.channelId, node);
        if (node.status === 'active') {
          await this.startChannel({ channelId: node.channelId }, { restoring: true, notifyStatus: false });
        }
      } catch (err) {
        console.error(`[ChannelManager] Failed to restore ${entry}:`, err.message);
      }
    }
  }

  async stopAll() {
    this.cronScheduler.stop();
    this._stopDueMessageLoop();

    for (const node of this.channels.values()) {
      this._stopWorkdirWatcher(node);
      await this._stopProcess(node, 'SIGTERM');
      this._closeMessageStore(node);
    }
  }

  async handle(message, connection = this.connection) {
    if (connection) this.setConnection(connection);

    switch (message.type) {
      case 'channel:create':
        return this.createChannel(message.channel ?? message);
      case 'channel:start':
        return this.startChannel(message.channel ?? message);
      case 'channel:pause':
        return this.pauseChannel(message.channelId ?? message.channel_id ?? message.id);
      case 'channel:resume':
        return this.startChannel(
          message.channel ?? { channelId: message.channelId ?? message.channel_id ?? message.id },
          { notifyStatus: true },
        );
      case 'channel:archive':
        return this.archiveChannel(message.channelId ?? message.channel_id ?? message.id);
      case 'channel:event':
        return this.handleEvent(message);
      case 'channel:message.send':
        return this.handleServerMessageSend(message, connection);
      case 'channel:rpc':
        return this.handleServerRpc(message, connection);
      default:
        return false;
    }
  }

  async handleServerRpc(message, connection = this.connection) {
    const requestId = String(message?.requestId ?? '').trim();
    const method = String(message?.method ?? '').trim();
    if (!requestId) {
      throw toRpcError('bad_request', 'requestId is required');
    }

    try {
      if (!['task.list', 'task.show'].includes(method)) {
        throw toRpcError('bad_request', `unsupported channel RPC method: ${method || '(empty)'}`);
      }
      const params = {
        ...(message.params && typeof message.params === 'object' ? message.params : {}),
        channel_id: message.params?.channel_id ?? message.params?.channelId ?? message.channelId ?? message.channel_id,
      };
      const result = await this.rpcCall(method, params);
      connection?.send({
        type: 'channel:rpc.result',
        requestId,
        ok: true,
        result,
      });
      return result;
    } catch (err) {
      const code = err.code ?? 'rpc_error';
      const statusCode = Number.isInteger(err.statusCode) ? err.statusCode : undefined;
      connection?.send({
        type: 'channel:rpc.result',
        requestId,
        ok: false,
        error: {
          code,
          message: err.message,
          ...(statusCode ? { statusCode } : {}),
        },
        code,
        ...(statusCode ? { statusCode } : {}),
      });
      return null;
    }
  }

  async rpcCall(method, params) {
    switch (method) {
      case 'schedule.cron':
        return this.registerSchedule(params, 'cron');
      case 'schedule.at':
        return this.registerSchedule(params, 'at');
      case 'schedule.list':
        return this.listSchedules(params.channel_id ?? params.channelId);
      case 'schedule.cancel':
        return this.cancelSchedule(params.channel_id ?? params.channelId, params.id ?? params.schedule_id);
      case 'channel.info':
        return this.getChannelInfo(params.channel_id ?? params.channelId);
      case 'channel.member.list':
        return this.getChannelMembers(params.channel_id ?? params.channelId);
      case 'channel.capability.list':
        return this.getChannelCapabilities(params.channel_id ?? params.channelId);
      case 'channel.list':
        return this.listChannels();
      case 'channel.start':
        return this.startChannel(params.channel_id ?? params.channelId);
      case 'channel.restart':
        return this.restartChannel(params.channel_id ?? params.channelId);
      case 'channel.stop':
        return this.stopChannel(params.channel_id ?? params.channelId);
      case 'channel.archive':
        return this.archiveChannel(params.channel_id ?? params.channelId);
      case 'message.send':
        return this.sendChannelMessage(params);
      case 'message.emit':
        return this.emitChannelMessage(params);
      case 'message.list':
        return this.listMessages(params.channel_id ?? params.channelId, params.limit ?? 50);
      case 'message.search':
        return this.searchMessages(params.channel_id ?? params.channelId, params.query ?? '', params.limit ?? 20);
      case 'message.query':
        return this.queryChannelMessages(params);
      case 'message.schedule':
        return this.scheduleChannelMessage(params);
      case 'dispatch.start':
        return this.dispatchStart(params);
      case 'dispatch.check':
        return this.dispatchCheck(params);
      case 'dispatch.renew':
        return this.dispatchRenew(params);
      case 'dispatch.list':
        return this.dispatchList(params);
      case 'memo.create':
        return this.createMemo(params);
      case 'memo.recall':
        return this.recallMemo(params);
      case 'task.open':
        return this.openTask(params);
      case 'task.close':
        return this.closeTask(params);
      case 'task.append':
        return this.appendTask(params);
      case 'task.list':
        return this.listTasks(params);
      case 'task.show':
        return this.showTask(params);
      case 'task.tree':
        return this.taskTree(params);
      case 'admin.status':
        return this.getAdminStatus();
      case 'admin.machines':
        return this.listMachines();
      default:
        throw toRpcError('not_implemented', `unsupported RPC method: ${method}`);
    }
  }

  async createChannel(payload) {
    const normalized = normalizeChannelPayload(payload);
    const existing = this.channels.get(normalized.channelId);
    const workdir = existing?.workdir ?? path.join(this.channelsDir, normalized.channelId);

    this._materializeWorkdir(workdir);

    const initialStatus = existing
      ? normalized.status
      : (normalized.status === 'active' ? 'created' : normalized.status);

    const node = {
      channelId: normalized.channelId,
      workspaceId: normalized.workspaceId || existing?.workspaceId || '',
      daemonId: normalized.daemonId || existing?.daemonId || '',
      name: normalized.name || existing?.name || normalized.channelId,
      type: normalized.type || existing?.type || 'xhs-creator',
      channelTypeConfig: normalized.channelTypeConfig ?? existing?.channelTypeConfig ?? null,
      status: initialStatus || existing?.status || 'created',
      capabilitySet: normalized.capabilitySet.cli_binaries.length > 0
        ? normalized.capabilitySet
        : (existing?.capabilitySet ?? { cli_binaries: [] }),
      members: normalized.members.length > 0 ? normalized.members : (existing?.members ?? []),
      agentName: normalized.agentName || existing?.agentName || DEFAULT_AGENT_NAME,
      createdAt: existing?.createdAt ?? normalized.createdAt,
      archivedAt: normalized.archivedAt ?? existing?.archivedAt ?? null,
      workdir,
      proc: existing?.proc ?? null,
      agentPid: existing?.agentPid ?? null,
      sessionId: existing?.sessionId ?? null,
      lastSpawnedAt: existing?.lastSpawnedAt ?? 0,
      crashCount: existing?.crashCount ?? 0,
      intentionalStop: false,
      mountedCliBinaries: existing?.mountedCliBinaries ?? [],
      messageStore: existing?.messageStore ?? null,
      workdirWatcher: existing?.workdirWatcher ?? null,
    };

    this.channels.set(node.channelId, node);
    this._persistNode(node);
    emitJsonEvent('channel.create', { channel_id: node.channelId, status: node.status, project_key: this.projectKey });
    return this._channelInfo(node);
  }

  async startChannel(payload, { restoring = false, notifyStatus = true } = {}) {
    const channelId = typeof payload === 'string'
      ? payload
      : (payload.channelId ?? payload.channel_id ?? payload.id);
    if (!channelId) throw toRpcError('bad_request', 'channel_id is required');

    if (typeof payload === 'object' && payload !== null && !this.channels.has(channelId)) {
      await this.createChannel(payload);
    }

    const node = this._requireNode(channelId);
    if (node.status === 'archived') {
      throw toRpcError('invalid_state', `channel ${channelId} is archived`);
    }
    if (node.proc) {
      return this._channelInfo(node);
    }

    this._openMessageStore(node);

    const sessionIdPath = this._sessionIdPath(node);
    const spawnConfig = buildCoagentSpawn({
      channelId: node.channelId,
      channelName: node.name,
      channelType: node.type,
      workspaceId: node.workspaceId,
      workdir: node.workdir,
      capabilitySet: node.capabilitySet,
      daemonSocketPath: this.daemonSocketPath,
      daemonHttpUrl: this.daemonHttpUrl,
      daemonToken: this.daemonToken,
      sessionIdPath,
      agentName: node.agentName,
    });

    const proc = spawn(spawnConfig.command, spawnConfig.args, {
      cwd: node.workdir,
      env: spawnConfig.env,
      stdio: ['pipe', 'pipe', 'pipe'],
    });

    node.proc = proc;
    node.agentPid = proc.pid ?? null;
    node.sessionId = spawnConfig.sessionId;
    node.mountedCliBinaries = spawnConfig.mountedCliBinaries;
    node.lastSpawnedAt = Date.now();
    node.intentionalStop = false;
    node.status = 'active';
    node.archivedAt = null;
    this._persistNode(node);
    this._loadSchedulesIntoMemory(node);
    this._startWorkdirWatcher(node);
    this._startDueMessageLoop();
    this._wireProcess(node, proc, { restoring });

    if (notifyStatus) {
      this._notifyChannelStatus(node);
    }

    if (this.connection) {
      await this._replayPendingViewSync(node);
    }

    emitJsonEvent('agent.spawn', { channel_id: node.channelId, pid: node.agentPid, session_id: node.sessionId });
    emitJsonEvent('channel.start', { channel_id: node.channelId, status: node.status, pid: node.agentPid });
    console.error(`[ChannelManager] Started ${channelId} pid=${node.agentPid ?? 'n/a'} entry=${spawnConfig.entry}`);
    return this._channelInfo(node);
  }

  async pauseChannel(channelId) {
    const node = this._requireNode(channelId);
    this.cronScheduler.clearChannel(channelId);
    node.status = 'paused';
    this._persistNode(node);
    this._stopWorkdirWatcher(node);
    await this._stopProcess(node, 'SIGTERM');
    this._closeMessageStore(node);
    this._notifyChannelStatus(node);
    emitJsonEvent('channel.stop', { channel_id: node.channelId, status: node.status });
    return this._channelInfo(node);
  }

  async resumeChannel(channelId) {
    const node = this._requireNode(channelId);
    if (node.status === 'archived') {
      throw toRpcError('invalid_state', `channel ${channelId} is archived`);
    }
    return this.startChannel({ channelId }, { notifyStatus: true });
  }

  async stopChannel(channelId) {
    return this.pauseChannel(channelId);
  }

  async restartChannel(channelId) {
    const node = this._requireNode(channelId);
    if (node.status === 'archived') {
      throw toRpcError('invalid_state', `channel ${channelId} is archived`);
    }
    await this._stopProcess(node, 'SIGTERM');
    node.proc = null;
    node.agentPid = null;
    node.status = 'created';
    this._persistNode(node);
    const info = await this.startChannel({ channelId }, { notifyStatus: true });
    emitJsonEvent('channel.restart', { channel_id: node.channelId, status: info.status, pid: info.agent_pid });
    return info;
  }

  async archiveChannel(channelId) {
    const node = this._requireNode(channelId);
    this.cronScheduler.clearChannel(channelId);
    node.status = 'archived';
    node.archivedAt = nowIso();
    this._persistNode(node);
    this._stopWorkdirWatcher(node);
    await this._stopProcess(node, 'SIGTERM');
    this._closeMessageStore(node);

    const archivedWorkdir = path.join(this.archivedDir, `${channelId}-${Date.now()}`);
    renameSync(node.workdir, archivedWorkdir);
    node.workdir = archivedWorkdir;
    this._persistNode(node);
    this.channels.delete(channelId);
    this._notifyChannelStatus(node);
    emitJsonEvent('channel.archive', { channel_id: node.channelId, archived_at: node.archivedAt });
    return this._channelInfo(node);
  }

  async handleEvent(payload) {
    const channelId = String(payload.channelId ?? payload.channel_id ?? payload.id ?? '').trim();
    if (!channelId && !payload.channel) {
      throw toRpcError('bad_request', 'channel_id is required');
    }

    if (payload.channel) {
      await this.createChannel(payload.channel);
    }

    const node = this._requireNode(channelId || payload.channel.channelId || payload.channel.channel_id || payload.channel.id);
    const event = normalizeEvent(payload.event ?? payload);

    if (event.type === 'channel.member.joined') {
      node.members = normalizeMembers([...node.members, event.payload?.member ?? event.payload]);
      this._persistNode(node);
    }

    if (event.type === 'channel.config.updated' && payload.channel) {
      await this.createChannel({ ...payload.channel, status: node.status });
    }

    const outcome = this.triggerGateway.evaluate(event, node);
    const eventMessage = this._messageFromEvent(node, event);

    if (eventMessage && outcome.decision !== 'block') {
      const normalized = await this._appendMessage(node, eventMessage);
      if (event.type === 'user.message.posted') {
        emitJsonEvent('message.receive', {
          channel_id: node.channelId,
          message_id: normalized.messageId,
          sender_type: normalized.senderType,
        });
      }
    }

    return this.triggerGateway.dispatch({ channel: node, event, outcome });
  }

  async registerSchedule(params, kind) {
    const channelId = String(params.channel_id ?? params.channelId ?? '').trim();
    const node = this._requireNode(channelId);
    const scheduleId = normalizeScheduleId(params.id ?? params.schedule_id ?? randomUUID());
    const scheduleDir = this._scheduleDir(node);
    const schedulePath = this._schedulePath(node, scheduleId);
    const upsert = isTruthyOption(params.upsert);
    const schedule = {
      id: scheduleId,
      channel_id: node.channelId,
      kind,
      cron: kind === 'cron' ? String(params.cron ?? params.cron_expr ?? '').trim() : null,
      at: kind === 'at' ? String(params.at ?? params.next_run_at ?? '').trim() : null,
      reason: String(params.reason ?? '').trim(),
      payload: params.payload ?? {},
      created_at: nowIso(),
      created_by: String(params.created_by ?? params.createdBy ?? node.agentName),
    };

    if (kind === 'cron' && !schedule.cron) {
      throw toRpcError('bad_request', 'cron expression is required');
    }
    if (kind === 'at' && !schedule.at) {
      throw toRpcError('bad_request', 'at timestamp is required');
    }
    if (existsSync(schedulePath) && !upsert) {
      throw toRpcError('schedule_exists', `schedule already exists: ${scheduleId}`, 409);
    }

    writeStructuredFile(schedulePath, schedule, { rootDir: scheduleDir });
    if (node.status === 'active') {
      this.cronScheduler.register(schedule);
    }
    return schedule;
  }

  async listSchedules(channelId) {
    const node = this._requireNode(channelId);
    return sortByCreatedAt(this._readSchedules(node));
  }

  async cancelSchedule(channelId, scheduleId, options = {}) {
    if (!scheduleId) throw toRpcError('bad_request', 'schedule id is required');
    const node = this._requireNode(channelId);
    const normalizedScheduleId = normalizeScheduleId(scheduleId);
    const schedulePath = this._schedulePath(node, normalizedScheduleId);

    if (existsSync(schedulePath)) {
      unlinkSync(schedulePath);
    }
    this.cronScheduler.cancel(node.channelId, normalizedScheduleId);

    return options.silent
      ? { canceled: true }
      : { channel_id: node.channelId, schedule_id: normalizedScheduleId, canceled: true };
  }

  async getChannelInfo(channelId) {
    const node = this._requireNode(channelId);
    return this._channelInfo(node);
  }

  async getChannelMembers(channelId) {
    const node = this._requireNode(channelId);
    return { members: node.members };
  }

  async getChannelCapabilities(channelId) {
    const node = this._requireNode(channelId);
    return node.capabilitySet;
  }

  async sendChannelMessage(params, options = {}) {
    const channelId = String(params.channel_id ?? params.channelId ?? '').trim();
    const node = this._requireNode(channelId);
    const message = normalizeMessage(channelId, params, {
      senderType: options.senderType ?? 'channel_agent',
      senderId: options.senderId ?? node.agentName,
      senderName: options.senderName ?? node.agentName,
      source: options.source ?? 'daemon',
    });
    assertDeferredMessageEnvelope(message);
    const event = this._eventFromMessage(message);
    const shouldDefer = message.notBefore != null && message.notBefore > Date.now();
    const outcome = options.trigger === false || shouldDefer
      ? null
      : this.triggerGateway.evaluate(event, node);

    if (outcome?.decision === 'block') {
      await this.triggerGateway.dispatch({ channel: node, event, outcome });
      return {
        ...message,
        blocked: true,
        trigger: { decision: outcome.decision, reason: outcome.reason },
      };
    }

    await this._appendMessage(node, message);
    emitJsonEvent('message.send', {
      channel_id: node.channelId,
      message_id: message.messageId,
      sender_type: message.senderType,
    });
    await this._appendToServerView(node, message, { requestId: options.requestId });

    if (outcome) {
      await this.triggerGateway.dispatch({ channel: node, event, outcome });
    }

    return {
      ...message,
      ...(outcome ? { trigger: { decision: outcome.decision, reason: outcome.reason } } : {}),
    };
  }

  async emitChannelMessage(params) {
    return this.sendChannelMessage(params);
  }

  async handleServerMessageSend(message, connection = this.connection) {
    const requestId = String(message?.requestId ?? '').trim();
    if (!requestId) {
      throw toRpcError('bad_request', 'requestId is required');
    }

    try {
      const sent = await this.sendChannelMessage({
        channelId: message.channelId ?? message.channel_id,
        senderType: message.senderType ?? message.sender_type ?? 'human',
        senderId: message.senderId ?? message.sender_id,
        senderName: message.senderName ?? message.sender_name,
        messageType: message.messageType ?? message.message_type ?? 'chat',
        senderKind: message.senderKind ?? message.sender_kind,
        payloadType: message.payloadType ?? message.payload_type,
        payloadBody: message.payloadBody ?? message.payload_body,
        content: message.content,
        attachments: message.attachments,
        parentId: message.parentId ?? message.parent_id,
        correlationId: message.correlationId ?? message.correlation_id,
        taskId: message.taskId ?? message.task_id,
        threadId: message.threadId ?? message.thread_id,
        audience: message.audience,
        mentions: message.mentions,
        notBefore: message.notBefore ?? message.not_before,
        origin: message.origin,
        expiresAt: message.expiresAt ?? message.expires_at,
      }, {
        requestId,
        senderType: message.senderType ?? message.sender_type ?? 'human',
        senderId: String(message.senderId ?? message.sender_id ?? '').trim(),
        senderName: String(message.senderName ?? message.sender_name ?? '').trim(),
        source: 'server',
      });

      connection?.send({
        type: 'channel:message.send.result',
        requestId,
        ok: true,
        message: sent,
      });
      return sent;
    } catch (err) {
      connection?.send({
        type: 'channel:message.send.result',
        requestId,
        ok: false,
        error: err.message,
        code: err.code ?? 'rpc_error',
      });
      return null;
    }
  }

  async listMessages(channelId, limit = 50) {
    const node = this._requireNode(channelId);
    const messages = queryStoredMessages(this._openMessageStore(node), {
      channel_id: node.channelId,
      limit,
      order: 'desc',
    }).map(legacyMessageOutput);
    return { messages };
  }

  async searchMessages(channelId, query, limit = 20) {
    const node = this._requireNode(channelId);
    const needle = String(query ?? '').trim().toLowerCase();
    if (!needle) throw toRpcError('bad_request', 'query is required');

    const messages = queryStoredMessages(this._openMessageStore(node), {
      channel_id: node.channelId,
      text: needle,
      limit,
      order: 'desc',
    }).map(legacyMessageOutput);
    return { messages };
  }

  async queryChannelMessages(params) {
    const channelId = String(params.channel_id ?? params.channelId ?? '').trim();
    const node = this._requireNode(channelId);
    const filters = {
      ...params,
      channel_id: node.channelId,
    };
    if (params.unread === true || String(params.unread).trim().toLowerCase() === 'true') {
      const cursor = readAgentCursor(node.workdir, node.agentName);
      filters.cursor_seq = cursor.last_seen_seq;
      filters.now_ms = params.now_ms ?? params.nowMs ?? Date.now();
    }
    const messages = queryStoredMessages(this._openMessageStore(node), {
      ...filters,
    });
    if (
      (filters.unread === true || String(filters.unread).trim().toLowerCase() === 'true')
      && isUnreadFullScanQuery(filters)
    ) {
      const maxSeq = messages.reduce((max, message) => Math.max(max, Number(message.seq) || 0), 0);
      if (maxSeq > 0) {
        advanceAgentCursor(node.workdir, node.agentName, maxSeq);
      }
    }
    return { messages };
  }

  async scheduleChannelMessage(params) {
    const channelId = String(params.channel_id ?? params.channelId ?? '').trim();
    const node = this._requireNode(channelId);
    const notBefore = optionalEpochMs(params.not_before ?? params.notBefore);
    if (notBefore == null) throw toRpcError('bad_request', 'not_before is required');
    const { payloadType, payloadBody } = normalizePayloadInput(params, PayloadType.DISPATCH_SELF_CHECK_DUE);
    const content = String(params.content ?? payloadBody?.text ?? JSON.stringify(payloadBody ?? {}));

    return this.sendChannelMessage({
      channelId: node.channelId,
      messageId: params.id ?? params.message_id ?? params.messageId,
      senderType: params.sender_type ?? params.senderType ?? 'channel_agent',
      senderKind: params.sender_kind ?? params.senderKind ?? SenderKind.AGENT,
      senderId: params.sender_id ?? params.senderId ?? node.agentName,
      senderName: params.sender_name ?? params.senderName ?? node.agentName,
      messageType: payloadType,
      content,
      payload: { type: payloadType, body: payloadBody },
      parentId: params.parent_id ?? params.parentId,
      correlationId: params.correlation_id ?? params.correlationId,
      taskId: params.task_id ?? params.taskId,
      threadId: params.thread_id ?? params.threadId,
      audience: params.audience ?? ['self'],
      origin: params.origin ?? 'self',
      notBefore,
      expiresAt: params.expires_at ?? params.expiresAt,
      source: params.source ?? 'schedule',
    }, { trigger: false });
  }

  async dispatchStart(params) {
    const channelId = String(params.channel_id ?? params.channelId ?? '').trim();
    const node = this._requireNode(channelId);
    const target = String(params.target ?? '').trim();
    const type = String(params.type ?? params.dispatch_type ?? '').trim();
    if (!target) throw toRpcError('bad_request', 'target is required');
    if (!type) throw toRpcError('bad_request', 'type is required');

    const correlationId = String(params.correlation_id ?? params.correlationId ?? randomUUID());
    const body = {
      type,
      params: params.params ?? {},
    };
    const mentions = target.startsWith('agent:') ? [target] : null;
    const startMessage = await this.sendChannelMessage({
      channelId: node.channelId,
      senderType: 'channel_agent',
      senderKind: SenderKind.AGENT,
      senderId: node.agentName,
      senderName: node.agentName,
      messageType: PayloadType.DISPATCH_START,
      payload: { type: PayloadType.DISPATCH_START, body },
      content: JSON.stringify(body),
      correlationId,
      taskId: params.in_task ?? params.inTask ?? params.task_id ?? params.taskId,
      audience: [target],
      mentions,
      origin: 'self',
      source: 'dispatch',
    });
    const checkAfterMs = parseDurationMs(params.check_after_ms ?? params.checkAfterMs ?? params.check_after ?? params.checkAfter);
    const checkMessage = await this.scheduleChannelMessage({
      channelId: node.channelId,
      payloadType: PayloadType.DISPATCH_SELF_CHECK_DUE,
      payloadBody: {
        type,
        target,
        correlation_id: correlationId,
        reason: `check ${type} ${correlationId}`,
      },
      correlationId,
      taskId: params.in_task ?? params.inTask ?? params.task_id ?? params.taskId,
      notBefore: Date.now() + checkAfterMs,
      source: 'dispatch',
    });

    return {
      correlation_id: correlationId,
      dispatch: startMessage,
      self_check: checkMessage,
    };
  }

  async dispatchCheck(params) {
    const channelId = String(params.channel_id ?? params.channelId ?? '').trim();
    const correlationId = String(params.correlation_id ?? params.correlationId ?? '').trim();
    if (!correlationId) throw toRpcError('bad_request', 'correlation_id is required');
    const node = this._requireNode(channelId);
    const messages = queryStoredMessages(this._openMessageStore(node), {
      channel_id: node.channelId,
      correlation_id: correlationId,
      order: 'asc',
      limit: 500,
    });
    const dispatchMessages = messages.filter((message) => String(message.payload_type).startsWith('dispatch.'));
    const terminal = [...dispatchMessages].reverse().find((message) => [
      PayloadType.DISPATCH_COMPLETED,
      PayloadType.DISPATCH_FAILED,
      PayloadType.DISPATCH_REJECTED,
    ].includes(message.payload_type));
    const accepted = dispatchMessages.some((message) => message.payload_type === PayloadType.DISPATCH_ACCEPTED);
    const lastSeen = dispatchMessages.at(-1)?.ts_received ?? null;

    if (terminal) {
      const status = terminal.payload_type.replace('dispatch.', '');
      return {
        correlation_id: correlationId,
        status,
        last_seen: terminal.ts_received,
        result: terminal.payload_body?.result ?? terminal.payload_body,
        message: terminal,
      };
    }

    return {
      correlation_id: correlationId,
      status: accepted ? 'accepted_pending' : 'no_response',
      last_seen: lastSeen,
      messages: dispatchMessages,
    };
  }

  async dispatchRenew(params) {
    const channelId = String(params.channel_id ?? params.channelId ?? '').trim();
    const correlationId = String(params.correlation_id ?? params.correlationId ?? '').trim();
    if (!correlationId) throw toRpcError('bad_request', 'correlation_id is required');
    const checkAfterMs = parseDurationMs(params.check_after_ms ?? params.checkAfterMs ?? params.check_after ?? params.checkAfter);
    const message = await this.scheduleChannelMessage({
      channelId,
      payloadType: PayloadType.DISPATCH_SELF_CHECK_DUE,
      payloadBody: {
        correlation_id: correlationId,
        reason: `renew dispatch check ${correlationId}`,
      },
      correlationId,
      taskId: params.task_id ?? params.taskId,
      notBefore: Date.now() + checkAfterMs,
      source: 'dispatch',
    });
    return { correlation_id: correlationId, self_check: message };
  }

  async dispatchList(params) {
    const channelId = String(params.channel_id ?? params.channelId ?? '').trim();
    const node = this._requireNode(channelId);
    const messages = queryStoredMessages(this._openMessageStore(node), {
      channel_id: node.channelId,
      task_id: params.task_id ?? params.taskId,
      order: 'asc',
      limit: 500,
    }).filter((message) => String(message.payload_type).startsWith('dispatch.'));
    const chains = new Map();
    for (const message of messages) {
      const correlationId = message.correlation_id ?? '<none>';
      const chain = chains.get(correlationId) ?? {
        correlation_id: correlationId,
        task_id: message.task_id ?? null,
        type: null,
        status: 'pending',
        last_seen: null,
        messages: [],
      };
      chain.messages.push(message);
      chain.last_seen = message.ts_received;
      if (message.payload_type === PayloadType.DISPATCH_START) {
        chain.type = message.payload_body?.type ?? chain.type;
      }
      if ([
        PayloadType.DISPATCH_COMPLETED,
        PayloadType.DISPATCH_FAILED,
        PayloadType.DISPATCH_REJECTED,
      ].includes(message.payload_type)) {
        chain.status = 'terminal';
        chain.terminal_type = message.payload_type;
      }
      chains.set(correlationId, chain);
    }

    const status = String(params.status ?? '').trim();
    const dispatches = [...chains.values()].filter((chain) => {
      if (!status) return true;
      return status === chain.status;
    });
    return { dispatches };
  }

  async createMemo(params) {
    const channelId = String(params.channel_id ?? params.channelId ?? '').trim();
    const node = this._requireNode(channelId);
    const tag = String(params.tag ?? '').trim();
    const summary = String(params.summary ?? '').trim();
    if (!tag) throw toRpcError('bad_request', 'tag is required');
    if (!summary) throw toRpcError('bad_request', 'summary is required');
    const body = {
      tag,
      scope: String(params.scope ?? 'channel').trim() || 'channel',
      summary,
      ...(params.doc ?? params.doc_ref ?? params.docRef ? { doc_ref: params.doc ?? params.doc_ref ?? params.docRef } : {}),
      status: String(params.status ?? 'active').trim() || 'active',
    };

    return this.sendChannelMessage({
      channelId: node.channelId,
      senderType: 'channel_agent',
      senderKind: SenderKind.AGENT,
      senderId: node.agentName,
      senderName: node.agentName,
      messageType: PayloadType.SELF_MEMO,
      payload: { type: PayloadType.SELF_MEMO, body },
      content: summary,
      correlationId: params.correlation_id ?? params.correlationId,
      taskId: params.task_id ?? params.taskId,
      audience: ['self'],
      origin: 'self',
      source: 'memo',
    });
  }

  async recallMemo(params) {
    const channelId = String(params.channel_id ?? params.channelId ?? '').trim();
    const node = this._requireNode(channelId);
    const tag = String(params.tag ?? '').trim();
    if (!tag) throw toRpcError('bad_request', 'tag is required');
    const messages = queryStoredMessages(this._openMessageStore(node), {
      channel_id: node.channelId,
      payload_type: PayloadType.SELF_MEMO,
      tag,
      status: params.status ?? 'active',
      limit: params.limit ?? 20,
      order: 'desc',
    });
    return {
      memos: messages.map((message) => ({
        id: message.id,
        ts: message.ts,
        summary: message.payload_body?.summary ?? '',
        tag: message.payload_body?.tag ?? tag,
        scope: message.payload_body?.scope ?? 'channel',
        doc_ref: message.payload_body?.doc_ref ?? null,
        status: message.payload_body?.status ?? 'active',
        correlation_id: message.correlation_id ?? null,
      })),
    };
  }

  async openTask(params) {
    const channelId = String(params.channel_id ?? params.channelId ?? '').trim();
    const node = this._requireNode(channelId);
    const taskId = String(params.task_id ?? params.taskId ?? randomUUID()).trim();
    const type = String(params.type ?? 'free').trim() || 'free';
    const title = String(params.title ?? '').trim();
    const docRef = String(params.doc_ref ?? params.docRef ?? params.doc ?? '').trim();
    const parentTaskId = String(params.parent_task_id ?? params.parentTaskId ?? params.parent ?? '').trim();
    if (!taskId) throw toRpcError('bad_request', 'task_id is required');
    if (!title) throw toRpcError('bad_request', 'title is required');
    if (!docRef) throw toRpcError('bad_request', 'doc_ref is required');

    return this._withTaskDocLock(node.channelId, docRef, async () => {
      const db = this._openMessageStore(node);
      if (getStoredTask(db, taskId, node.channelId)) throw toRpcError('conflict', `task already exists: ${taskId}`);
      if (parentTaskId && !getStoredTask(db, parentTaskId, node.channelId)) {
        throw toRpcError('not_found', `parent task not found: ${parentTaskId}`);
      }

      const docWrite = this._writeTaskDocAtomic(node.workdir, docRef, taskDocTemplate({
        taskId,
        type,
        title,
        parentTaskId,
        rationale: params.rationale ? String(params.rationale) : undefined,
      }), { overwrite: false });

      const body = {
        type,
        title,
        doc_ref: docRef,
        ...(parentTaskId ? { parent_task_id: parentTaskId } : {}),
        ...(params.rationale ? { rationale: String(params.rationale) } : {}),
      };

      let message;
      try {
        message = await this.sendChannelMessage({
          channelId: node.channelId,
          messageId: params.message_id ?? params.messageId,
          senderType: 'channel_agent',
          senderKind: SenderKind.AGENT,
          senderId: node.agentName,
          senderName: node.agentName,
          messageType: PayloadType.TASK_OPENED,
          payload: { type: PayloadType.TASK_OPENED, body },
          content: title,
          correlationId: params.correlation_id ?? params.correlationId,
          taskId,
          audience: ['self'],
          origin: 'self',
          source: 'task',
        });
      } catch (err) {
        removeCreatedTaskDoc(docWrite);
        throw err;
      }

      return {
        task_id: taskId,
        doc_ref: docRef,
        message,
        task: getStoredTask(db, taskId, node.channelId),
      };
    });
  }

  async closeTask(params) {
    const channelId = String(params.channel_id ?? params.channelId ?? '').trim();
    const node = this._requireNode(channelId);
    const taskId = String(params.task_id ?? params.taskId ?? params.id ?? '').trim();
    const status = String(params.status ?? '').trim();
    if (!taskId) throw toRpcError('bad_request', 'task_id is required');
    if (!['completed', 'failed', 'abandoned'].includes(status)) {
      throw toRpcError('bad_request', 'status must be completed, failed, or abandoned');
    }
    const db = this._openMessageStore(node);
    const existingTask = getStoredTask(db, taskId, node.channelId);
    if (!existingTask) throw toRpcError('not_found', `task not found: ${taskId}`);
    const summary = params.summary ? String(params.summary) : undefined;
    const resultRefValue = params.result_ref ?? params.resultRef;
    const resultRef = resultRefValue ? String(resultRefValue) : undefined;
    const docRef = existingTask.doc_ref;

    return this._withTaskDocLock(node.channelId, docRef, async () => {
      const task = getStoredTask(db, taskId, node.channelId);
      if (!task) throw toRpcError('not_found', `task not found: ${taskId}`);
      assertTaskMutable(task, taskId);
      const lockedDocRef = task.doc_ref;

      const docWrite = this._writeTaskDocAtomic(
        node.workdir,
        lockedDocRef,
        appendClosedStatus(this._readTaskDocIfPresent(node.workdir, lockedDocRef) ?? '', status, summary, resultRef),
        { overwrite: true },
      );

      const body = {
        status,
        ...(summary ? { summary } : {}),
        ...(resultRef ? { result_ref: resultRef } : {}),
      };

      let message;
      try {
        message = await this.sendChannelMessage({
          channelId: node.channelId,
          senderType: 'channel_agent',
          senderKind: SenderKind.AGENT,
          senderId: node.agentName,
          senderName: node.agentName,
          messageType: PayloadType.TASK_CLOSED,
          payload: { type: PayloadType.TASK_CLOSED, body },
          content: String(summary ?? `task ${taskId} ${status}`),
          correlationId: params.correlation_id ?? params.correlationId,
          taskId,
          audience: ['self'],
          origin: 'self',
          source: 'task',
        });
      } catch (err) {
        this._restoreTaskDoc(node.workdir, lockedDocRef, docWrite.previous);
        throw err;
      }

      return {
        task_id: taskId,
        doc_ref: lockedDocRef,
        status,
        message,
        task: getStoredTask(db, taskId, node.channelId),
      };
    });
  }

  async appendTask(params) {
    const channelId = String(params.channel_id ?? params.channelId ?? '').trim();
    const node = this._requireNode(channelId);
    const taskId = String(params.task_id ?? params.taskId ?? params.id ?? '').trim();
    const summary = String(params.summary ?? params.event_summary ?? params.eventSummary ?? '').trim();
    if (!taskId) throw toRpcError('bad_request', 'task_id is required');
    if (!summary) throw toRpcError('bad_request', 'summary is required');
    const db = this._openMessageStore(node);
    const existingTask = getStoredTask(db, taskId, node.channelId);
    if (!existingTask) throw toRpcError('not_found', `task not found: ${taskId}`);
    const docRef = existingTask.doc_ref;

    return this._withTaskDocLock(node.channelId, docRef, async () => {
      const task = getStoredTask(db, taskId, node.channelId);
      if (!task) throw toRpcError('not_found', `task not found: ${taskId}`);
      assertTaskMutable(task, taskId);
      const lockedDocRef = task.doc_ref;

      const docWrite = this._writeTaskDocAtomic(
        node.workdir,
        lockedDocRef,
        appendTimeline(this._readTaskDocIfPresent(node.workdir, lockedDocRef) ?? '', summary),
        { overwrite: true },
      );

      let message;
      try {
        message = await this.sendChannelMessage({
          channelId: node.channelId,
          senderType: 'channel_agent',
          senderKind: SenderKind.AGENT,
          senderId: node.agentName,
          senderName: node.agentName,
          messageType: PayloadType.TASK_APPENDED,
          payload: { type: PayloadType.TASK_APPENDED, body: { summary } },
          content: summary,
          correlationId: params.correlation_id ?? params.correlationId,
          taskId,
          audience: ['self'],
          origin: 'self',
          source: 'task',
        });
      } catch (err) {
        this._restoreTaskDoc(node.workdir, lockedDocRef, docWrite.previous);
        throw err;
      }

      return {
        task_id: taskId,
        doc_ref: lockedDocRef,
        message,
        task: getStoredTask(db, taskId, node.channelId),
      };
    });
  }

  async listTasks(params) {
    const channelId = String(params.channel_id ?? params.channelId ?? '').trim();
    const node = this._requireNode(channelId);
    const tasks = queryStoredTasks(this._openMessageStore(node), {
      channel_id: node.channelId,
      status: params.status,
      parent_task_id: params.parent_task_id ?? params.parentTaskId ?? params.parent,
      mine: params.mine === true || params.mine === 'true',
      initiator_id: node.agentName,
    });
    return { tasks };
  }

  async showTask(params) {
    const channelId = String(params.channel_id ?? params.channelId ?? '').trim();
    const taskId = String(params.task_id ?? params.taskId ?? params.id ?? '').trim();
    if (!taskId) throw toRpcError('bad_request', 'task_id is required');
    const node = this._requireNode(channelId);
    const db = this._openMessageStore(node);
    const task = getStoredTask(db, taskId, node.channelId);
    if (!task) throw toRpcError('not_found', `task not found: ${taskId}`);
    const messages = queryStoredMessages(db, {
      channel_id: node.channelId,
      task_id: taskId,
      order: 'asc',
      limit: 500,
    });
    return {
      task,
      doc: {
        ref: task.doc_ref,
        content: readDocIfPresent(node.workdir, task.doc_ref),
      },
      messages,
      children: queryStoredTasks(db, {
        channel_id: node.channelId,
        parent_task_id: taskId,
        order: 'opened_asc',
        limit: 500,
      }),
      dispatches: messages.filter((message) => String(message.payload_type).startsWith('dispatch.')),
    };
  }

  async taskTree(params) {
    const channelId = String(params.channel_id ?? params.channelId ?? '').trim();
    const node = this._requireNode(channelId);
    const rootTaskId = String(params.root ?? params.root_task_id ?? params.rootTaskId ?? '').trim();
    const tasks = queryStoredTasks(this._openMessageStore(node), {
      channel_id: node.channelId,
      order: 'opened_asc',
      limit: 500,
    });
    return {
      tasks: buildTaskTree(tasks, rootTaskId || null),
    };
  }

  async listChannels() {
    return { channels: [...this.channels.values()].map((node) => this._channelInfo(node)) };
  }

  async getAdminStatus() {
    const channels = [...this.channels.values()];
    return {
      ok: true,
      project_key: this.projectKey,
      server_url: this.serverUrl,
      daemon_socket: this.daemonSocketPath,
      daemon_http: this.daemonHttpUrl || null,
      connected_to_server: Boolean(this.connection?.ws?.readyState === 1),
      channels_count: channels.length,
      active_channels_count: channels.filter((node) => node.status === 'active').length,
      active_agent_pids: channels.filter((node) => node.agentPid).map((node) => node.agentPid),
    };
  }

  async listMachines() {
    return {
      machines: [{
        id: 'local',
        project_key: this.projectKey,
        server_url: this.serverUrl,
        api_key_prefix: this.machineApiKey ? this.machineApiKey.slice(0, 18) : null,
        status: this.connection?.ws?.readyState === 1 ? 'online' : 'local',
        channels_count: this.channels.size,
      }],
    };
  }

  _channelInfo(node) {
    return {
      channel_id: node.channelId,
      name: node.name,
      workspace_id: node.workspaceId,
      daemon_id: node.daemonId,
      type: node.type,
      channel_type_config: node.channelTypeConfig,
      status: node.status,
      capability_set: node.capabilitySet,
      workdir: node.workdir,
      agent_pid: node.agentPid,
      session_id_path: this._sessionIdPath(node),
      session_id: node.sessionId,
      mounted_cli_binaries: node.mountedCliBinaries,
      members: node.members,
      members_count: node.members.length,
      created_at: node.createdAt,
      archived_at: node.archivedAt,
    };
  }

  _requireNode(channelId) {
    const node = this.channels.get(String(channelId ?? '').trim());
    if (!node) {
      throw toRpcError('not_found', `channel not found locally: ${channelId}`);
    }
    return node;
  }

  _openMessageStore(node) {
    if (node.messageStore?.open) return node.messageStore;
    node.messageStore = openMessageStore(node.workdir);
    return node.messageStore;
  }

  _closeMessageStore(node) {
    if (!node.messageStore?.open) {
      node.messageStore = null;
      return;
    }
    node.messageStore.close();
    node.messageStore = null;
  }

  _materializeWorkdir(workdir) {
    mkdirSync(workdir, { recursive: true });
    if (existsSync(this.workspaceTemplateDir)) {
      cpSync(this.workspaceTemplateDir, workdir, { recursive: true, force: false, errorOnExist: false });
    }
    ensureDirectory(path.join(workdir, 'messages'));
    ensureDirectory(path.join(workdir, 'artifacts'));
    ensureDirectory(path.join(workdir, 'notes'));
    ensureDirectory(path.join(workdir, 'schedules'));
    ensureDirectory(path.join(workdir, 'pending-view-sync'));
    ensureDirectory(path.join(workdir, 'agents', DEFAULT_AGENT_NAME, 'trace'));
  }

  _channelMetaPath(workdir) {
    return path.join(workdir, 'channel.yaml');
  }

  _sessionIdPath(node) {
    return path.join(node.workdir, 'agents', node.agentName, 'session.id');
  }

  _traceDir(node) {
    return path.join(node.workdir, 'agents', node.agentName, 'trace');
  }

  _scheduleDir(node) {
    return path.join(node.workdir, 'schedules');
  }

  _schedulePath(node, scheduleId) {
    return path.join(this._scheduleDir(node), `${scheduleId}.yaml`);
  }

  _persistNode(node) {
    try {
      ensureDirectory(path.dirname(this._sessionIdPath(node)));
      writeStructuredFile(this._channelMetaPath(node.workdir), {
        channel_id: node.channelId,
        name: node.name,
        workspace_id: node.workspaceId,
        daemon_id: node.daemonId,
        type: node.type,
        ...(node.channelTypeConfig ? { channel_type_config: node.channelTypeConfig } : {}),
        status: node.status,
        capability_set: node.capabilitySet,
        members: node.members,
        agent_name: node.agentName,
        created_at: node.createdAt,
        archived_at: node.archivedAt,
      });
      return true;
    } catch (err) {
      console.error(`[ChannelManager] Failed to persist channel ${node.channelId}:`, err.message);
      return false;
    }
  }

  _loadNodeFromDisk(workdir) {
    const meta = parseStructuredFile(this._channelMetaPath(workdir));
    const payload = normalizeChannelPayload(meta);

    return {
      channelId: payload.channelId,
      workspaceId: payload.workspaceId,
      daemonId: payload.daemonId,
      name: payload.name || payload.channelId,
      type: payload.type,
      channelTypeConfig: payload.channelTypeConfig,
      status: payload.status,
      capabilitySet: payload.capabilitySet,
      members: payload.members,
      agentName: payload.agentName,
      createdAt: payload.createdAt,
      archivedAt: payload.archivedAt,
      workdir,
      proc: null,
      agentPid: null,
      sessionId: existsSync(path.join(workdir, 'agents', payload.agentName, 'session.id'))
        ? readFileSync(path.join(workdir, 'agents', payload.agentName, 'session.id'), 'utf8').trim()
        : null,
      lastSpawnedAt: 0,
      crashCount: 0,
      intentionalStop: false,
      mountedCliBinaries: [],
      messageStore: null,
      workdirWatcher: null,
    };
  }

  _messageFromEvent(node, event) {
    if (event.type === 'user.message.posted') {
      return {
        ...(event.payload?.message ?? event.payload),
        source: event.source ?? 'server',
      };
    }

    const payloadTypeByEventType = new Map([
      ['cron.tick', PayloadType.CRON_TICK],
      ['channel.config.updated', PayloadType.CHANNEL_CONFIG_UPDATED],
      ['channel.presence_changed', PayloadType.CHANNEL_PRESENCE_CHANGED],
      ['channel.member.joined', PayloadType.CHANNEL_PRESENCE_CHANGED],
      ['workdir.changed', PayloadType.WORKDIR_CHANGED],
      ['dispatch.self_check_due', PayloadType.DISPATCH_SELF_CHECK_DUE],
    ]);
    const payloadType = payloadTypeByEventType.get(event.type) ?? (String(event.type).includes('.') ? event.type : null);
    if (!payloadType) return null;

    const body = event.payload ?? {};
    return {
      messageId: randomUUID(),
      channelId: node.channelId,
      senderKind: SenderKind.SYSTEM,
      senderType: 'system',
      senderId: 'system:daemon',
      senderName: 'daemon',
      messageType: payloadType,
      content: typeof body?.text === 'string' ? body.text : JSON.stringify(body),
      payload: { type: payloadType, body },
      createdAt: event.createdAt ?? event.created_at ?? nowIso(),
      source: event.source ?? 'daemon',
      origin: 'system',
    };
  }

  _eventFromMessage(message) {
    return {
      type: message.payloadType ?? message.payload?.type ?? message.messageType ?? 'message',
      source: message.source ?? 'daemon',
      createdAt: message.createdAt ?? nowIso(),
      payload: { message },
    };
  }

  _messageFromStoredMessage(node, stored) {
    const legacy = stored.legacy ?? {};
    return {
      ...legacy,
      messageId: stored.id,
      channelId: stored.channel_id ?? node.channelId,
      senderKind: stored.sender_kind,
      senderType: legacy.senderType ?? legacy.sender_type ?? stored.sender_kind,
      senderId: stored.sender_id,
      senderName: stored.sender_name,
      messageType: legacy.messageType ?? legacy.message_type ?? stored.payload_type,
      payloadType: stored.payload_type,
      payloadBody: stored.payload_body,
      payload: stored.payload,
      content: legacy.content ?? stored.payload_body?.text ?? JSON.stringify(stored.payload_body ?? {}),
      parentId: stored.parent_id,
      correlationId: stored.correlation_id,
      taskId: stored.task_id,
      threadId: stored.thread_id,
      audience: stored.audience ? [stored.audience] : null,
      mentions: stored.mentions,
      origin: stored.origin,
      notBefore: stored.not_before,
      expiresAt: stored.expires_at,
      tsReceived: stored.ts_received,
      envelope: stored.envelope,
      createdAt: legacy.createdAt ?? legacy.created_at ?? new Date(stored.ts).toISOString(),
      source: 'sqlite-scheduler',
    };
  }

  _startDueMessageLoop() {
    if (this.dueMessageTimer || this.dueMessagePollMs <= 0) return;
    this.dueMessageTimer = setInterval(() => {
      this.processDueMessages().catch((err) => {
        console.error('[ChannelManager] Due message loop failed:', err.message);
      });
    }, this.dueMessagePollMs);
    this.dueMessageTimer.unref?.();
  }

  _stopDueMessageLoop() {
    if (!this.dueMessageTimer) return;
    clearInterval(this.dueMessageTimer);
    this.dueMessageTimer = null;
  }

  async processDueMessages(channelId = null, nowMs = Date.now()) {
    const nodes = [...this.channels.values()]
      .filter((node) => (!channelId || node.channelId === channelId) && (channelId || node.status === 'active'));

    const delivered = [];
    const failed = [];
    for (const node of nodes) {
      const db = this._openMessageStore(node);
      const dueMessages = readDueMessages(db, nowMs, 100);
      for (const stored of dueMessages) {
        const message = this._messageFromStoredMessage(node, stored);
        const event = this._eventFromMessage(message);
        const outcome = this.triggerGateway.evaluate(event, node);
        const delivery = await this._dispatchDueMessage(node, event, outcome);
        if (delivery.ok && outcome.decision !== 'block') {
          const deliveredMessage = markMessageDelivered(db, stored.id, nowMs);
          if (deliveredMessage?.seq != null) {
            advanceAgentCursor(node.workdir, node.agentName, deliveredMessage.seq);
          }
          delivered.push({
            message_id: stored.id,
            channel_id: node.channelId,
            decision: outcome.decision,
            reason: outcome.reason,
            ok: true,
          });
          continue;
        }

        const lastError = delivery.reason ?? outcome.reason ?? 'delivery failed';
        const failedMessage = markMessageDeliveryAttempt(db, stored.id, {
          attemptedAt: nowMs,
          error: lastError,
        });
        const attempts = failedMessage?.delivery_attempts ?? ((stored.delivery_attempts ?? 0) + 1);
        let failure = {
          message_id: stored.id,
          channel_id: node.channelId,
          decision: outcome.decision,
          reason: lastError,
          last_error: failedMessage?.last_error ?? lastError,
          attempts,
          ok: false,
        };
        if (attempts >= DEFAULT_DELIVERY_FAILURE_LIMIT) {
          const deadLetteredMessage = markMessageDeliveryFailed(db, stored.id, nowMs);
          failure = {
            ...failure,
            delivery_failed_at: deadLetteredMessage?.delivery_failed_at ?? toEpochMs(nowMs),
            last_error: deadLetteredMessage?.last_error ?? failure.last_error,
          };
          if (deadLetteredMessage?.deliveryFailedChanged === true) {
            await this._recordDeliveryDeadLetter(node, message, failure);
          }
        }
        failed.push(failure);
      }
    }
    return { delivered, failed };
  }

  async _dispatchDueMessage(node, event, outcome) {
    if (outcome.decision === 'react') {
      await this._recordTrace(node, {
        kind: 'trigger',
        decision: 'react',
        reason: outcome.reason,
        event,
      });
      return this._deliverEvent(node, event);
    }
    if (outcome.decision === 'log_only') {
      await this._recordTrace(node, {
        kind: 'trigger',
        decision: 'log_only',
        reason: outcome.reason,
        event,
      });
      return { ok: true };
    }
    await this._recordTrace(node, {
      kind: 'trigger',
      decision: 'block',
      reason: outcome.reason,
      event,
    });
    return { ok: false, reason: outcome.reason ?? 'blocked' };
  }

  async _recordDeliveryDeadLetter(node, message, failure) {
    const lastError = failure.last_error ?? failure.reason ?? 'delivery failed';
    await this._recordTrace(node, {
      kind: 'delivery_dead_letter',
      type: 'delivery_dead_letter',
      event: 'delivery_dead_letter',
      messageId: message.messageId,
      message_id: message.messageId,
      channel_id: node.channelId,
      decision: failure.decision,
      reason: failure.reason,
      last_error: lastError,
      attempts: failure.attempts,
      delivery_failed_at: failure.delivery_failed_at,
    });
    emitJsonEvent('inbox.created', {
      channel_id: node.channelId,
      severity: 'blocker',
      reason: 'incident',
      ticket_id: null,
      body: {
        message_id: message.messageId,
        channel_id: node.channelId,
        attempts: failure.attempts,
        last_error: lastError,
      },
    });
    console.error(
      `[ChannelManager] Message delivery dead-lettered channel=${node.channelId} message=${message.messageId} attempts=${failure.attempts}: ${lastError}`,
    );
  }

  _startWorkdirWatcher(node) {
    if (node.workdirWatcher) return;
    node.workdirWatcher = new WorkdirWatcher({
      workdir: node.workdir,
      onEvent: (event) => {
        this.handleEvent({ channelId: node.channelId, event }).catch((err) => {
          console.error(`[ChannelManager] Workdir watcher failed for ${node.channelId}:`, err.message);
        });
      },
    });
    node.workdirWatcher.start();
  }

  _stopWorkdirWatcher(node) {
    if (!node.workdirWatcher) return;
    node.workdirWatcher.stop();
    node.workdirWatcher = null;
  }

  _wireProcess(node, proc, { restoring }) {
    let stdoutBuffer = '';

    proc.stdout.on('data', (chunk) => {
      stdoutBuffer += chunk.toString();
      const lines = stdoutBuffer.split('\n');
      stdoutBuffer = lines.pop() ?? '';
      for (const line of lines) {
        process.stdout.write(`${line}\n`);
      }
    });

    proc.stderr.on('data', (chunk) => {
      const text = chunk.toString().trim();
      if (text) {
        console.error(`[ChannelManager][${node.channelId}] stderr: ${text.slice(0, 500)}`);
      }
    });

    proc.on('exit', async (code, signal) => {
      if (stdoutBuffer) {
        process.stdout.write(`${stdoutBuffer}\n`);
        stdoutBuffer = '';
      }

      if (this.channels.get(node.channelId)?.proc !== proc) return;

      node.proc = null;
      node.agentPid = null;

      emitJsonEvent('agent.exit', { channel_id: node.channelId, code, signal });
      console.error(`[ChannelManager] Channel ${node.channelId} process exited code=${code ?? 'n/a'} signal=${signal ?? 'n/a'}`);

      if (node.intentionalStop) {
        node.intentionalStop = false;
        return;
      }

      if (node.status !== 'active') {
        return;
      }

      const uptimeMs = Date.now() - (node.lastSpawnedAt ?? 0);
      if (uptimeMs > HEALTHY_UPTIME_MS) {
        node.crashCount = 0;
      }
      node.crashCount += 1;

      if (node.crashCount <= 1) {
        console.warn(`[ChannelManager] Respawning ${node.channelId} after unexpected exit`);
        try {
          await this.startChannel({ channelId: node.channelId }, { restoring, notifyStatus: false });
        } catch (err) {
          console.error(`[ChannelManager] Respawn failed for ${node.channelId}:`, err.message);
          emitJsonEvent('agent.error', { channel_id: node.channelId, message: err.message });
          node.status = 'failed';
          this._persistNode(node);
          this._notifyChannelStatus(node);
        }
        return;
      }

      node.status = 'failed';
      this._persistNode(node);
      this._notifyChannelStatus(node);
      emitJsonEvent('agent.error', { channel_id: node.channelId, reason: 'unexpected_exit_twice', code, signal });
      await this._recordTrace(node, {
        kind: 'agent.exit',
        decision: 'failed',
        reason: 'unexpected_exit_twice',
        code,
        signal,
      });
    });
  }

  async _stopProcess(node, signal) {
    if (!node.proc) return;

    const proc = node.proc;
    node.intentionalStop = true;

    await new Promise((resolve) => {
      let settled = false;
      const finish = () => {
        if (settled) return;
        settled = true;
        resolve();
      };

      proc.once('exit', finish);
      try {
        proc.kill(signal);
      } catch {
        finish();
        return;
      }

      setTimeout(() => {
        try {
          proc.kill('SIGKILL');
        } catch {}
        finish();
      }, SHUTDOWN_GRACE_MS);
    });
  }

  async _deliverEvent(node, event) {
    if (node.status !== 'active' || !node.proc?.stdin) {
      return {
        ok: false,
        reason: node.status !== 'active' ? 'channel_inactive' : 'agent_stdin_unavailable',
      };
    }

    if (node.proc.stdin.destroyed || node.proc.stdin.writableEnded) {
      return { ok: false, reason: 'agent_stdin_closed' };
    }

    try {
      node.proc.stdin.write(`${JSON.stringify({ type: 'event', event })}\n`);
      return { ok: true };
    } catch (err) {
      console.error(`[ChannelManager] Failed to deliver event to ${node.channelId}:`, err.message);
      return { ok: false, reason: err.message };
    }
  }

  async _handleCronTick(tick) {
    const node = this.channels.get(tick.channelId);
    if (!node || node.status !== 'active') return;

    if (tick.kind === 'at') {
      await this.cancelSchedule(tick.channelId, tick.scheduleId, { silent: true });
    }

    await this.handleEvent({
      channelId: tick.channelId,
      event: {
        type: 'cron.tick',
        source: 'cron-scheduler',
        created_at: nowIso(),
        payload: {
          schedule_id: tick.scheduleId,
          reason: tick.reason,
          original_payload: tick.payload,
        },
      },
    });
  }

  _loadSchedulesIntoMemory(node) {
    this.cronScheduler.loadChannel(node.channelId, this._readSchedules(node));
  }

  _readSchedules(node) {
    const dir = this._scheduleDir(node);
    if (!existsSync(dir)) return [];

    const schedules = [];
    for (const fileName of readdirSync(dir)) {
      if (!fileName.endsWith('.yaml')) continue;
      const schedulePath = path.join(dir, fileName);
      try {
        schedules.push(parseStructuredFile(schedulePath));
      } catch (err) {
        console.error(`[ChannelManager] Failed to parse schedule ${schedulePath}:`, err.message);
      }
    }
    return schedules;
  }

  async _appendMessage(node, message) {
    const normalized = message?.envelope && message?.payload
      ? message
      : normalizeMessage(node.channelId, message);
    assertKnownPayloadType(normalized.payload?.type ?? normalized.payloadType);
    const createdAt = new Date(normalized.createdAt);
    const bucket = Number.isNaN(createdAt.getTime())
      ? nowIso().slice(0, 10)
      : createdAt.toISOString().slice(0, 10);
    const filePath = path.join(node.workdir, 'messages', `${bucket}.jsonl`);
    const stored = appendMessageToStore(this._openMessageStore(node), normalized);
    if (stored?.inserted === false) return normalized;
    try {
      appendFileSync(filePath, `${JSON.stringify(normalized)}\n`, 'utf8');
    } catch (err) {
      await this._recordTrace(node, {
        kind: 'message_log_write_failed',
        type: 'message_log_write_failed',
        messageId: normalized.messageId,
        filePath,
        reason: err.message,
      });
      console.error(`[ChannelManager] Failed to append JSONL message log for ${node.channelId}:`, err.message);
    }
    return normalized;
  }

  _readMessages(node) {
    const messagesDir = path.join(node.workdir, 'messages');
    if (!existsSync(messagesDir)) return [];

    const messages = [];
    for (const fileName of readdirSync(messagesDir).filter((entry) => entry.endsWith('.jsonl')).sort()) {
      const filePath = path.join(messagesDir, fileName);
      const lines = readFileSync(filePath, 'utf8')
        .split('\n')
        .map((line) => line.trim())
        .filter(Boolean);

      for (const line of lines) {
        try {
          messages.push(JSON.parse(line));
        } catch {}
      }
    }
    return sortByCreatedAt(messages);
  }

  async _appendToServerView(node, message, { requestId = randomUUID() } = {}) {
    if (message.audience?.includes?.('self')) {
      return;
    }

    const payload = {
      type: 'message.append',
      requestId,
      message_id: message.messageId,
      channel_id: node.channelId,
      sender_type: message.senderType,
      sender_id: message.senderId,
      sender_name: message.senderName,
      message_type: message.messageType,
      content: message.content,
      attachments: message.attachments,
      sender_kind: message.senderKind,
      payload_type: message.payloadType,
      payload_body: message.payloadBody,
      parent_id: message.parentId,
      correlation_id: message.correlationId,
      task_id: message.taskId,
      thread_id: message.threadId,
      audience: message.audience,
      mentions: message.mentions,
      not_before: message.notBefore,
      origin: message.origin,
      expires_at: message.expiresAt,
      ts_received: message.tsReceived,
      envelope: message.envelope,
      payload: message.payload,
    };

    const result = await this._sendViewSyncPayload(payload);
    if (!result.ok) {
      this._enqueuePendingViewSync(node, message, payload, result.reason);
      return;
    }
    emitJsonEvent('message.deliver', { channel_id: node.channelId, message_id: message.messageId, request_id: requestId });
  }

  async _sendViewSyncPayload(payload) {
    const requestId = String(payload?.requestId ?? '').trim();
    if (!this.connection) {
      return { ok: false, reason: 'daemon connection is not ready' };
    }
    if (!requestId) {
      return { ok: false, reason: 'message.append requestId is missing' };
    }

    try {
      const response = await this.connection.request({
        message: payload,
        expect: { type: 'message.append.ack', requestId },
        timeoutMs: 10_000,
      });
      if (!response?.ok) {
        return { ok: false, reason: response?.error ?? 'message.append ack failed' };
      }
      return { ok: true, response };
    } catch (err) {
      return { ok: false, reason: err?.message ?? String(err) };
    }
  }

  async _replayPendingViewSyncForAll() {
    for (const node of this.channels.values()) {
      await this._replayPendingViewSync(node);
    }
  }

  async _replayPendingViewSync(node) {
    const pendingDir = path.join(node.workdir, 'pending-view-sync');
    if (!existsSync(pendingDir)) return { replayed: 0, failed: 0 };

    const records = [];
    for (const fileName of readdirSync(pendingDir).filter((entry) => entry.endsWith('.json'))) {
      const filePath = path.join(pendingDir, fileName);
      try {
        const record = JSON.parse(readFileSync(filePath, 'utf8'));
        if (!record?.payload) {
          throw new Error('pending view sync payload is missing');
        }
        records.push({
          fileName,
          filePath,
          enqueuedAt: record.enqueuedAt ?? record.enqueued_at ?? '',
          attempts: Number.parseInt(record.attempts, 10) || 0,
          record,
        });
      } catch (err) {
        await this._recordTrace(node, {
          kind: 'view_sync_replay',
          type: 'view_sync_replay_invalid',
          fileName,
          reason: err.message,
        });
      }
    }

    records.sort((left, right) => String(left.enqueuedAt).localeCompare(String(right.enqueuedAt)));
    let replayed = 0;
    let failed = 0;
    for (const item of records) {
      if (item.attempts >= VIEW_SYNC_RETRY_LIMIT) {
        failed += 1;
        continue;
      }

      const result = await this._sendViewSyncPayload(item.record.payload);
      if (result.ok) {
        try {
          unlinkSync(item.filePath);
          replayed += 1;
          emitJsonEvent('message.deliver', {
            channel_id: item.record.payload.channel_id,
            message_id: item.record.payload.message_id,
            request_id: item.record.payload.requestId,
            replayed: true,
          });
        } catch (err) {
          failed += 1;
          console.error(`[ChannelManager] Failed to remove pending view sync ${item.filePath}:`, err.message);
        }
        continue;
      }

      failed += 1;
      const attempts = item.attempts + 1;
      this._writePendingViewSyncFailure(item.filePath, item.record, result.reason, attempts);
      if (attempts >= VIEW_SYNC_RETRY_LIMIT) {
        await this._recordTrace(node, {
          kind: 'view_sync_replay',
          type: 'view_sync_replay_give_up',
          messageId: item.record.payload.message_id,
          requestId: item.record.payload.requestId,
          attempts,
          reason: result.reason,
        });
        console.error(
          `[ChannelManager] Pending view sync reached retry limit channel=${node.channelId} message=${item.record.payload.message_id}: ${result.reason}`,
        );
      }
    }

    return { replayed, failed };
  }

  _writePendingViewSyncFailure(filePath, record, reason, attempts) {
    try {
      writeFileSync(
        filePath,
        `${JSON.stringify({
          ...record,
          enqueuedAt: record.enqueuedAt ?? nowIso(),
          attempts,
          lastAttemptAt: nowIso(),
          reason,
        }, null, 2)}\n`,
        'utf8',
      );
    } catch (err) {
      console.error(`[ChannelManager] Failed to update pending view sync ${filePath}:`, err.message);
    }
  }

  _enqueuePendingViewSync(node, message, payload, reason) {
    try {
      const pendingDir = ensureDirectory(path.join(node.workdir, 'pending-view-sync'));
      const filePath = path.join(pendingDir, `${message.messageId}.json`);
      let existing = null;
      try {
        existing = existsSync(filePath) ? JSON.parse(readFileSync(filePath, 'utf8')) : null;
      } catch {}
      writeFileSync(
        filePath,
        `${JSON.stringify({
          enqueuedAt: existing?.enqueuedAt ?? nowIso(),
          attempts: existing?.attempts ?? 0,
          reason,
          payload,
        }, null, 2)}\n`,
        'utf8',
      );
      this._recordTrace(node, {
        type: 'view_sync_failed',
        messageId: message.messageId,
        requestId: payload.requestId,
        reason,
      });
    } catch (err) {
      console.error(`[ChannelManager] Failed to enqueue pending view sync for ${node.channelId}:`, err.message);
    }
  }

  async _recordTrace(node, record) {
    try {
      const sessionId = node.sessionId
        || (existsSync(this._sessionIdPath(node)) ? readFileSync(this._sessionIdPath(node), 'utf8').trim() : '')
        || 'pending';
      const traceDir = ensureDirectory(this._traceDir(node));
      appendFileSync(
        path.join(traceDir, `${sessionId}.jsonl`),
        `${JSON.stringify({ ts: nowIso(), ...record })}\n`,
        'utf8',
      );
    } catch (err) {
      console.error(`[ChannelManager] Failed to record trace for ${node?.channelId ?? 'unknown'}:`, err.message);
    }
  }

  _notifyChannelStatus(node) {
    if (!this.connection) return;
    this.connection.send({
      type: 'channel.status',
      channelId: node.channelId,
      status: node.status,
      archivedAt: node.archivedAt ?? null,
    });
  }
}
