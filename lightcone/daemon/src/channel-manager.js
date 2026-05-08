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
  getMaxStoredMessageSeq,
  getStoredMessage,
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
import { formatLocalIso, nowIso } from './time.js';
import { TriggerGateway } from './trigger-gateway.js';
import { WorkdirWatcher } from './workdir-watcher.js';
import { DispatchRouter, DEFAULT_RECOVERY_WINDOW_MS } from './devices/dispatch-router.js';
import { SessionManager } from './devices/session-manager.js';

const DEFAULT_AGENT_NAME = 'channel-agent';
const SHUTDOWN_GRACE_MS = 5_000;
const DEFAULT_DUE_MESSAGE_POLL_MS = 30_000;
const DEFAULT_DELIVERY_FAILURE_LIMIT = 5;
const TURN_FAILURE_DEAD_LETTER_LIMIT = 3;
const SCHEDULE_ID_PATTERN = /^[A-Za-z0-9_-]{1,128}$/;
const TERMINAL_TASK_STATUSES = new Set(['completed', 'failed', 'abandoned', 'archived']);
const PAYLOAD_TYPES_BY_SENDER_KIND = new Map([
  [SenderKind.HUMAN, new Set([
    PayloadType.USER_TEXT,
  ])],
  [SenderKind.AGENT, new Set([
    PayloadType.AGENT_TEXT,
    PayloadType.AGENT_PROGRESS,
    PayloadType.DISPATCH_START,
    PayloadType.DISPATCH_SELF_CHECK_DUE,
    PayloadType.TASK_OPENED,
    PayloadType.TASK_CLOSED,
    PayloadType.TASK_APPENDED,
    PayloadType.SELF_MEMO,
  ])],
  [SenderKind.SYSTEM, new Set([
    PayloadType.SYSTEM_NOTICE,
    PayloadType.SYSTEM_HEARTBEAT,
    PayloadType.CHANNEL_PRESENCE_CHANGED,
    PayloadType.CHANNEL_CONFIG_UPDATED,
    PayloadType.CRON_TICK,
    PayloadType.WORKDIR_CHANGED,
    PayloadType.DISPATCH_SELF_CHECK_DUE,
  ])],
  [SenderKind.EXTERNAL, new Set([
    PayloadType.DISPATCH_ACCEPTED,
    PayloadType.DISPATCH_REJECTED,
    PayloadType.DISPATCH_COMPLETED,
    PayloadType.DISPATCH_FAILED,
  ])],
]);

function toRpcError(code, message, statusCode = 400) {
  const err = new Error(message);
  err.code = code;
  err.statusCode = statusCode;
  return err;
}

// device.command.send payload allowlist + per-type schema (Fix-T2 §3 /
// round-1 review codex#12). Keep validators minimal — they reject unknown
// types and surface obvious shape mismatches without locking down every
// optional XHS field. Exported for unit tests.
export const DEVICE_COMMAND_TYPES = Object.freeze([
  'xhs.publish',
  'xhs.search',
  'xhs.get-my-recent',
  'xhs.get-note',
  'xhs.publish-status',
]);

function _isPlainObject(v) {
  return v != null && typeof v === 'object' && !Array.isArray(v);
}

function _requireNonEmptyString(params, field, type) {
  const value = params?.[field];
  if (typeof value !== 'string' || value.trim() === '') {
    throw toRpcError('bad_request', `${type} requires ${field} (non-empty string)`);
  }
}

export function validateDeviceCommand(type, params) {
  if (typeof type !== 'string' || !DEVICE_COMMAND_TYPES.includes(type)) {
    throw toRpcError(
      'bad_request',
      `unsupported device command type: ${type ?? '(empty)'} — allowed: ${DEVICE_COMMAND_TYPES.join('|')}`,
    );
  }
  if (!_isPlainObject(params)) {
    throw toRpcError('bad_request', `${type} params must be an object`);
  }
  switch (type) {
    case 'xhs.publish': {
      _requireNonEmptyString(params, 'title', type);
      const hasInline = typeof params.content === 'string' && params.content.length > 0;
      const hasPath = typeof params.content_path === 'string' && params.content_path.length > 0;
      if (!hasInline && !hasPath) {
        throw toRpcError('bad_request', `${type} requires content or content_path`);
      }
      // FX1 (round-2 codex#t56.1): real-mode CLI 只发 absolute content_path，
      // 拒绝相对路径以避免 daemon cwd 漂移导致的歧义。
      if (hasPath && !path.isAbsolute(params.content_path)) {
        throw toRpcError('bad_request', `${type} content_path must be an absolute path (got ${params.content_path})`);
      }
      if ('images' in params && params.images != null && !Array.isArray(params.images)) {
        throw toRpcError('bad_request', `${type} images must be an array`);
      }
      if ('tags' in params && params.tags != null && !Array.isArray(params.tags)) {
        throw toRpcError('bad_request', `${type} tags must be an array`);
      }
      break;
    }
    case 'xhs.search': {
      _requireNonEmptyString(params, 'keyword', type);
      if ('limit' in params && params.limit != null) {
        const n = Number(params.limit);
        if (!Number.isFinite(n) || n <= 0) throw toRpcError('bad_request', `${type} limit must be a positive number`);
      }
      break;
    }
    case 'xhs.get-my-recent': {
      if ('limit' in params && params.limit != null) {
        const n = Number(params.limit);
        if (!Number.isFinite(n) || n <= 0) throw toRpcError('bad_request', `${type} limit must be a positive number`);
      }
      break;
    }
    case 'xhs.get-note': {
      const hasUrl = typeof params.url === 'string' && params.url.length > 0;
      const hasToken = typeof params.xsec_token === 'string' && params.xsec_token.length > 0;
      if (!hasUrl && !hasToken) {
        throw toRpcError('bad_request', `${type} requires url or xsec_token`);
      }
      break;
    }
    case 'xhs.publish-status': {
      // FX3 (round-2 codex#t57.1): publish-status device-command params 是
      // {note_id}，与 CLI real_provider.go 和 extension publish-status.ts 对齐。
      // correlation_id 是 dispatch envelope 字段（外层 sendChannelMessage），
      // 不是 device command params 字段。
      _requireNonEmptyString(params, 'note_id', type);
      break;
    }
    default: {
      throw toRpcError('bad_request', `unsupported device command type: ${type}`);
    }
  }
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

function parsePositiveSeq(value, fieldName = 'seq') {
  const text = String(value ?? '').trim();
  if (!/^[1-9]\d*$/.test(text)) {
    throw toRpcError('bad_request', `${fieldName} must be a positive integer`);
  }
  const parsed = Number(text);
  if (!Number.isSafeInteger(parsed)) {
    throw toRpcError('bad_request', `${fieldName} must be a positive integer`);
  }
  return parsed;
}

function hasAckValue(value) {
  return value != null && String(value).trim() !== '';
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

function normalizeSenderKind(rawKind) {
  const kind = String(rawKind ?? '').trim();
  if (Object.values(SenderKind).includes(kind)) return kind;
  throw toRpcError('bad_request', 'sender.kind is required');
}

function assertAgentRpcIdentity(context, node) {
  const agentName = String(context?.agentName ?? '').trim();
  const channelId = String(context?.channelId ?? '').trim();
  if (!agentName || agentName !== node.agentName) {
    throw toRpcError('forbidden', 'message.emit requires the current channel agent identity', 403);
  }
  if (channelId && channelId !== node.channelId) {
    throw toRpcError('forbidden', 'message.emit channel identity mismatch', 403);
  }
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

function assertKnownPayloadType(payloadType) {
  if (!isPayloadType(payloadType)) {
    throw toRpcError('bad_request', `unsupported payload_type: ${payloadType}`);
  }
}

function assertSenderPayloadPair(senderKind, payloadType) {
  assertKnownPayloadType(payloadType);
  const allowedPayloadTypes = PAYLOAD_TYPES_BY_SENDER_KIND.get(senderKind);
  if (!allowedPayloadTypes) {
    throw toRpcError('bad_request', `unsupported sender.kind: ${senderKind}`);
  }
  if (!allowedPayloadTypes.has(payloadType)) {
    throw toRpcError('bad_request', `sender.kind=${senderKind} cannot send payload.type=${payloadType}`);
  }
}

function explicitSenderKind(input) {
  return input?.envelope?.sender?.kind ?? input?.senderKind ?? input?.sender_kind;
}

function explicitSenderId(input) {
  return input?.envelope?.sender?.id ?? input?.senderId ?? input?.sender_id;
}

function explicitPayloadType(input) {
  return input?.payload?.type ?? input?.payloadType ?? input?.payload_type;
}

function assertExplicitMessagePair(input, expectedSenderKind, expectedPayloadType, operation) {
  const rawSenderKind = explicitSenderKind(input);
  if (rawSenderKind != null) {
    const senderKind = normalizeSenderKind(rawSenderKind);
    if (senderKind !== expectedSenderKind) {
      throw toRpcError('bad_request', `${operation} sender.kind must be ${expectedSenderKind}`);
    }
  }

  const rawPayloadType = explicitPayloadType(input);
  if (rawPayloadType != null) {
    const payloadType = String(rawPayloadType).trim();
    assertKnownPayloadType(payloadType);
    if (payloadType !== expectedPayloadType) {
      throw toRpcError('bad_request', `${operation} payload.type must be ${expectedPayloadType}`);
    }
  }

  assertSenderPayloadPair(expectedSenderKind, expectedPayloadType);
}

function assertRpcCallerMatchesMessage(context, node, message, operation) {
  const agentName = String(context?.agentName ?? '').trim();
  if (!agentName) return;
  const channelId = String(context?.channelId ?? '').trim();
  if (agentName !== node.agentName) {
    throw toRpcError('forbidden', `${operation} requires the current channel agent identity`, 403);
  }
  if (channelId && channelId !== node.channelId) {
    throw toRpcError('forbidden', `${operation} channel identity mismatch`, 403);
  }
  if (message.senderKind !== SenderKind.AGENT) {
    throw toRpcError('bad_request', `${operation} sender.kind must be agent for agent RPC caller`);
  }
  if (String(message.senderId ?? '').trim() !== node.agentName) {
    throw toRpcError('bad_request', `${operation} sender.id must match the current channel agent`);
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
  );
  const senderId = String(sender.id ?? message?.senderId ?? message?.sender_id ?? defaults.senderId ?? DEFAULT_AGENT_NAME);
  const senderName = String(sender.name ?? message?.senderName ?? message?.sender_name ?? defaults.senderName ?? defaults.senderId ?? senderId);
  const attachments = Array.isArray(message?.attachments) ? message.attachments : [];
  const payloadType = String(
    message?.payload?.type
      ?? message?.payloadType
      ?? message?.payload_type
      ?? defaults.payloadType
      ?? '',
  ).trim();
  if (!payloadType) throw toRpcError('bad_request', 'payload.type is required');
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
  const createdAt = message?.createdAt ?? message?.created_at ?? formatLocalIso(ts);
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
  assertSenderPayloadPair(senderKind, payloadType);

  return {
    messageId,
    channelId,
    senderId,
    senderName,
    content,
    attachments,
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
    deviceWsServer = null,
    sessionManager = null,
    dispatchRouter = null,
    defaultDeviceId = process.env.COAGENT_DEFAULT_DEVICE_ID || '',
    defaultUserId = process.env.COAGENT_DEFAULT_USER_ID || '',
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
    this.deviceWsServer = deviceWsServer;
    // Default session base dir = <ChannelManager baseDir>/users so test
    // harnesses with isolated tempHome dirs don't leak across tests through
    // ~/.coagent/{projectKey}/users.
    this.sessionManager = sessionManager ?? new SessionManager({
      baseDir: path.join(this.baseDir, 'users'),
    });
    this.dispatchRouter = dispatchRouter ?? new DispatchRouter();
    this.defaultDeviceId = String(defaultDeviceId ?? '').trim();
    this.defaultUserId = String(defaultUserId ?? '').trim();
    this.workspaceTemplateDir = path.join(repoRoot(), 'workspace-template');
    this.dueMessagePollMs = dueMessagePollMs;
    this.dueMessageTimer = null;
    this._taskDocMutex = new Map();
    this.turnFailureCounts = new Map();
    this.deadLetteredEventTypes = new Map();
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
        const eventType = String(event?.type ?? '').trim();
        if (eventType && this._isEventTypeDeadLettered(channel.channelId, eventType)) {
          await this._recordTrace(channel, {
            kind: 'event.dropped_dead_letter',
            event_type: eventType,
            event,
          });
          return { ok: false, reason: 'event_type_dead_lettered' };
        }
        const delivery = await this._deliverEvent(channel, event);
        this._recordSuccessfulDelivery({
          node: channel,
          messageId: this._messageIdFromEvent(event),
          deliveryAck: delivery,
        });
        return delivery;
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
        // M1.1 Fix-T3: rebuild dispatch-router from messages.sqlite so any
        // in-flight callback (extension already POSTing) is routed back to the
        // right channel after a daemon restart. Failure here must not abort
        // channel restore — log and continue.
        try {
          this._recoverDispatchRouter(node);
        } catch (err) {
          emitJsonEvent('dispatch.router.recover_failed', {
            channel_id: node.channelId,
            error: err?.message ?? String(err),
          });
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

  async rpcCall(method, params, context = {}) {
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
        return this.sendChannelMessage(params, { callerIdentity: context, source: 'rpc' });
      case 'message.emit':
        return this.emitChannelMessage(params, context);
      case 'message.list':
        return this.listMessages(params.channel_id ?? params.channelId, params.limit ?? 50);
      case 'message.search':
        return this.searchMessages(params.channel_id ?? params.channelId, params.query ?? '', params.limit ?? 20);
      case 'message.query':
        return this.queryChannelMessages(params);
      case 'message.ack':
        return this.ackChannelMessages(params);
      case 'message.schedule':
        return this.scheduleChannelMessage(params, context);
      case 'dispatch.start':
        return this.dispatchStart(params);
      case 'dispatch.check':
        return this.dispatchCheck(params);
      case 'dispatch.renew':
        return this.dispatchRenew(params);
      case 'dispatch.list':
        return this.dispatchList(params);
      case 'device.command.send':
        return this.deviceCommandSend(params);
      case 'device.session.get':
        return this.deviceSessionGet(params);
      case 'device.session.update':
        return this.deviceSessionUpdate(params);
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
    let appendedMessage = null;

    if (eventMessage && outcome.decision !== 'block') {
      const normalized = await this._appendMessage(node, eventMessage);
      appendedMessage = normalized;
      if (event.type === 'user.message.posted') {
        emitJsonEvent('message.receive', {
          channel_id: node.channelId,
          message_id: normalized.messageId,
          sender_kind: normalized.senderKind,
        });
      }
    }

    const dispatchResult = await this.triggerGateway.dispatch({ channel: node, event, outcome });
    if (appendedMessage && dispatchResult.outcome.decision === 'react') {
      this._recordSuccessfulDelivery({
        node,
        messageId: appendedMessage.messageId,
        deliveryAck: dispatchResult.delivery,
      });
    }
    return dispatchResult.outcome;
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
      senderId: options.senderId ?? node.agentName,
      senderName: options.senderName ?? node.agentName,
      payloadType: options.payloadType,
      source: options.source ?? 'daemon',
    });
    assertRpcCallerMatchesMessage(options.callerIdentity, node, message, 'message.send');
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
      sender_kind: message.senderKind,
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

  async emitChannelMessage(params, context = {}) {
    const channelId = String(params.channel_id ?? params.channelId ?? context.channelId ?? '').trim();
    const node = this._requireNode(channelId);
    assertAgentRpcIdentity(context, node);
    const requestedPayloadType = String(explicitPayloadType(params) ?? PayloadType.AGENT_TEXT).trim();
    if (
      requestedPayloadType !== PayloadType.AGENT_TEXT
      && requestedPayloadType !== PayloadType.AGENT_PROGRESS
    ) {
      throw toRpcError(
        'bad_request',
        `message.emit payload.type must be ${PayloadType.AGENT_TEXT} or ${PayloadType.AGENT_PROGRESS}`,
      );
    }
    assertExplicitMessagePair(params, SenderKind.AGENT, requestedPayloadType, 'message.emit');
    const senderId = explicitSenderId(params);
    if (senderId != null && String(senderId).trim() !== node.agentName) {
      throw toRpcError('bad_request', 'message.emit sender.id must match the current channel agent');
    }
    const parsedBody = parseJsonString(params.payload_body ?? params.payloadBody ?? params.payload ?? {}, {});
    const payloadBody = parsedBody && typeof parsedBody === 'object' && !Array.isArray(parsedBody)
      ? parsedBody
      : { value: parsedBody };
    const text = String(params.text ?? params.content ?? payloadBody.text ?? '').trim();
    if (!text) throw toRpcError('bad_request', 'reply text is required');
    const envelope = params.envelope && typeof params.envelope === 'object' ? { ...params.envelope } : {};
    return this.sendChannelMessage({
      channelId: node.channelId,
      messageId: params.message_id ?? params.messageId,
      senderKind: SenderKind.AGENT,
      senderId: node.agentName,
      senderName: node.agentName,
      content: text,
      payload: {
        type: requestedPayloadType,
        body: { ...payloadBody, text },
      },
      envelope: {
        ...envelope,
        sender: { kind: SenderKind.AGENT, id: node.agentName, name: node.agentName },
      },
      attachments: params.attachments,
      parentId: params.parent_id ?? params.parentId,
      correlationId: params.correlation_id ?? params.correlationId,
      taskId: params.task_id ?? params.taskId,
      threadId: params.thread_id ?? params.threadId,
      audience: params.audience ?? ['channel'],
      mentions: params.mentions,
      origin: 'self',
      expiresAt: params.expires_at ?? params.expiresAt,
      source: requestedPayloadType === PayloadType.AGENT_PROGRESS ? 'agent-progress' : 'agent-reply',
    });
  }

  async handleServerMessageSend(message, connection = this.connection) {
    const requestId = String(message?.requestId ?? '').trim();
    if (!requestId) {
      throw toRpcError('bad_request', 'requestId is required');
    }

    try {
      const sent = await this.sendChannelMessage({
        channelId: message.channelId ?? message.channel_id,
        senderId: message.senderId ?? message.sender_id,
        senderName: message.senderName ?? message.sender_name,
        senderKind: message.senderKind ?? message.sender_kind,
        payloadType: message.payloadType ?? message.payload_type,
        payloadBody: message.payloadBody ?? message.payload_body,
        envelope: message.envelope,
        content: message.content,
        attachments: message.attachments,
        parentId: message.parentId ?? message.parent_id,
        correlationId: message.correlationId ?? message.correlation_id,
        taskId: message.taskId ?? message.task_id,
        threadId: message.threadId ?? message.thread_id,
        audience: message.audience,
        notBefore: message.notBefore ?? message.not_before,
        origin: message.origin,
        expiresAt: message.expiresAt ?? message.expires_at,
      }, {
        requestId,
        senderId: String(message.senderId ?? message.sender_id ?? '').trim(),
        senderName: String(message.senderName ?? message.sender_name ?? '').trim(),
        payloadType: message.payloadType ?? message.payload_type,
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
    });
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
    });
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
    return { messages };
  }

  /**
   * Cumulative ack RPC. Exactly one of:
   * - until_seq: ack all messages with seq <= until_seq
   * - through_message_id: ack all messages with seq <= that message's seq
   */
  async ackChannelMessages(params) {
    const channelId = String(params.channel_id ?? params.channelId ?? '').trim();
    const node = this._requireNode(channelId);
    const untilSeqValue = params.until_seq ?? params.untilSeq;
    const throughMessageId = String(params.through_message_id ?? params.throughMessageId ?? '').trim();
    const hasUntilSeq = hasAckValue(untilSeqValue);
    if (hasUntilSeq === Boolean(throughMessageId)) {
      throw toRpcError('bad_request', 'exactly one of until_seq or through_message_id is required');
    }

    const db = this._openMessageStore(node);
    let ackedThroughSeq;
    if (hasUntilSeq) {
      ackedThroughSeq = parsePositiveSeq(untilSeqValue, 'until_seq');
      const maxSeq = getMaxStoredMessageSeq(db);
      if (ackedThroughSeq > maxSeq) {
        throw toRpcError('bad_request', `until_seq must not exceed current max seq (${maxSeq})`);
      }
    } else {
      const stored = getStoredMessage(db, throughMessageId);
      if (!stored) throw toRpcError('not_found', `message not found: ${throughMessageId}`, 404);
      ackedThroughSeq = parsePositiveSeq(stored.seq, 'message seq');
    }

    const agentName = String(params.agent_name ?? params.agentName ?? node.agentName ?? DEFAULT_AGENT_NAME).trim()
      || DEFAULT_AGENT_NAME;
    const previous = readAgentCursor(node.workdir, agentName);
    const cursor = this._recordSuccessfulDelivery({
      node,
      agentName,
      storedMessage: { seq: ackedThroughSeq },
      deliveryAck: { ok: true },
    });
    return {
      channel_id: node.channelId,
      agent_name: agentName,
      ...(throughMessageId ? { through_message_id: throughMessageId } : {}),
      acked_through_seq: ackedThroughSeq,
      previous_last_seen_seq: previous.last_seen_seq,
      last_seen_seq: cursor.last_seen_seq,
      advanced: cursor.last_seen_seq > previous.last_seen_seq,
    };
  }

  async scheduleChannelMessage(params, context = {}) {
    const channelId = String(params.channel_id ?? params.channelId ?? '').trim();
    const node = this._requireNode(channelId);
    const notBefore = optionalEpochMs(params.not_before ?? params.notBefore);
    if (notBefore == null) throw toRpcError('bad_request', 'not_before is required');
    const { payloadType, payloadBody } = normalizePayloadInput(params, PayloadType.DISPATCH_SELF_CHECK_DUE);
    const senderKind = normalizeSenderKind(params.sender_kind ?? params.senderKind ?? SenderKind.AGENT);
    const content = String(params.content ?? payloadBody?.text ?? JSON.stringify(payloadBody ?? {}));

    return this.sendChannelMessage({
      channelId: node.channelId,
      messageId: params.id ?? params.message_id ?? params.messageId,
      senderKind,
      senderId: params.sender_id ?? params.senderId ?? node.agentName,
      senderName: params.sender_name ?? params.senderName ?? node.agentName,
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
    }, { trigger: false, callerIdentity: context });
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
      senderKind: SenderKind.AGENT,
      senderId: node.agentName,
      senderName: node.agentName,
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

  // ── device.* RPC ──────────────────────────────────────────────────────────
  // device.command.send / device.session.get / device.session.update
  // 行为契约见 .dalek/pm/m1.1-xhs-real-onboarding-spec.md §4.1。

  _resolveDeviceId(channelNode, explicitDeviceId) {
    const explicit = String(explicitDeviceId ?? '').trim();
    if (explicit) return explicit;
    const member = channelNode?.members?.find?.((m) => m.memberType === 'device');
    if (member?.memberId) return String(member.memberId).trim();
    if (this.defaultDeviceId) return this.defaultDeviceId;
    throw toRpcError('device_unavailable', 'no device_id resolvable for channel; configure channel members or COAGENT_DEFAULT_DEVICE_ID');
  }

  _resolveUserId(channelNode, explicitUserId) {
    const explicit = String(explicitUserId ?? '').trim();
    if (explicit) return explicit;
    const member = channelNode?.members?.find?.((m) => m.memberType === 'human');
    if (member?.memberId) return String(member.memberId).trim();
    if (this.defaultUserId) return this.defaultUserId;
    throw toRpcError('user_unavailable', 'no user_id resolvable for channel; configure channel members or COAGENT_DEFAULT_USER_ID');
  }

  async deviceCommandSend(params) {
    const channelId = String(params.channel_id ?? params.channelId ?? '').trim();
    const node = this._requireNode(channelId);
    const type = String(params.type ?? params.command_type ?? '').trim();
    if (!type) throw toRpcError('bad_request', 'type is required');
    const cmdParams = _isPlainObject(params.params) ? params.params : {};
    // Allowlist + per-type schema (Fix-T2 §3 / round-1 codex#12).
    validateDeviceCommand(type, cmdParams);
    const deviceId = this._resolveDeviceId(node, params.device_id ?? params.deviceId);
    const userId = this._resolveUserId(node, params.user_id ?? params.userId);

    const correlationId = String(params.correlation_id ?? params.correlationId ?? randomUUID());
    const targetAudience = `external:device:${deviceId}`;

    const startBody = {
      type,
      params: cmdParams,
      device_id: deviceId,
      user_id: userId,
    };
    const startMessage = await this.sendChannelMessage({
      channelId: node.channelId,
      senderKind: SenderKind.AGENT,
      senderId: node.agentName,
      senderName: node.agentName,
      payload: { type: PayloadType.DISPATCH_START, body: startBody },
      content: JSON.stringify(startBody),
      correlationId,
      taskId: params.in_task ?? params.inTask ?? params.task_id ?? params.taskId,
      audience: [targetAudience],
      origin: 'self',
      source: 'dispatch',
    });

    const checkAfterMs = parseDurationMs(
      params.check_after_ms ?? params.checkAfterMs ?? params.check_after ?? params.checkAfter,
    );
    const checkMessage = await this.scheduleChannelMessage({
      channelId: node.channelId,
      payloadType: PayloadType.DISPATCH_SELF_CHECK_DUE,
      payloadBody: {
        type,
        target: targetAudience,
        correlation_id: correlationId,
        device_id: deviceId,
        user_id: userId,
        reason: `check ${type} ${correlationId}`,
      },
      correlationId,
      taskId: params.in_task ?? params.inTask ?? params.task_id ?? params.taskId,
      notBefore: Date.now() + checkAfterMs,
      source: 'dispatch',
    });

    this.dispatchRouter.register({
      correlationId,
      channelId: node.channelId,
      deviceId,
      userId,
    });

    let session = null;
    try {
      session = this.sessionManager.getSession(userId);
    } catch (err) {
      // session corruption shouldn't block dispatch — log and continue with null.
      emitJsonEvent('device.session.read_failed', {
        user_id: userId,
        device_id: deviceId,
        error: err?.message ?? String(err),
      });
    }

    // FX1 (round-2 codex#t56.1): materialize content_path → content before
    // pushing to device. CLI real mode 只发 absolute content_path（不发 inline
    // content），daemon 在此读盘并把 file body 塞进 params.content；device side
    // 永远拿到 inline content（path 字段被剥离）。读失败走 dispatch.failed 审计 +
    // throw RPC error code='content_read_failed'，永不 push 给 device。
    let pushParams = cmdParams;
    if (type === 'xhs.publish') {
      const hasInlineContent = typeof cmdParams.content === 'string' && cmdParams.content.length > 0;
      const hasContentPath = typeof cmdParams.content_path === 'string' && cmdParams.content_path.length > 0;
      if (!hasInlineContent && hasContentPath) {
        try {
          const body = readFileSync(cmdParams.content_path, 'utf8');
          pushParams = { ...cmdParams, content: body };
          delete pushParams.content_path;
        } catch (readErr) {
          emitJsonEvent('device.command.content_read_failed', {
            device_id: deviceId,
            correlation_id: correlationId,
            content_path: cmdParams.content_path,
            error: readErr?.message ?? String(readErr),
          });
          try {
            await this.sendChannelMessage({
              channelId: node.channelId,
              senderKind: SenderKind.EXTERNAL,
              senderId: `device:${deviceId}`,
              senderName: `device:${deviceId}`,
              payload: {
                type: PayloadType.DISPATCH_FAILED,
                body: {
                  error: {
                    code: 'content_read_failed',
                    reason: readErr?.message ?? String(readErr),
                  },
                  device_id: deviceId,
                  user_id: userId,
                },
              },
              content: JSON.stringify({
                error: {
                  code: 'content_read_failed',
                  reason: readErr?.message ?? String(readErr),
                },
              }),
              correlationId,
              audience: ['channel'],
              origin: 'external',
              source: 'device',
            });
          } catch (recordErr) {
            emitJsonEvent('device.command.failed_record_error', {
              device_id: deviceId,
              correlation_id: correlationId,
              error: recordErr?.message ?? String(recordErr),
            });
          }
          this.dispatchRouter.remove(correlationId);
          throw toRpcError(
            'content_read_failed',
            `failed to read content_path ${cmdParams.content_path}: ${readErr?.message ?? readErr}`,
            400,
          );
        }
      } else if (hasInlineContent && hasContentPath) {
        // Back-compat: 双给时取 inline content，剥离 path（device side 永远 inline）。
        pushParams = { ...cmdParams };
        delete pushParams.content_path;
      }
    }

    const cmd = type.startsWith('xhs.') ? type.slice('xhs.'.length) : type;
    const wsPayload = {
      type: 'command',
      correlation_id: correlationId,
      cmd,
      params: pushParams,
      session,
    };
    let push = { ok: false, reason: 'no_ws_server' };
    if (this.deviceWsServer && typeof this.deviceWsServer.pushCommand === 'function') {
      push = this.deviceWsServer.pushCommand(deviceId, wsPayload);
    }
    if (!push.ok) {
      // Fix-T2 §2 / round-1 codex#4: device push failure is a first-class RPC error.
      // Emit a `dispatch.failed` audit message + drop the router entry, then throw
      // so callers (CLI real_provider) get `{ok:false, error:{code:"device_offline"}}`.
      emitJsonEvent('device.command.push_failed', {
        device_id: deviceId,
        correlation_id: correlationId,
        reason: push.reason ?? 'unknown',
      });
      try {
        await this.sendChannelMessage({
          channelId: node.channelId,
          senderKind: SenderKind.EXTERNAL,
          senderId: `device:${deviceId}`,
          senderName: `device:${deviceId}`,
          payload: {
            type: PayloadType.DISPATCH_FAILED,
            body: {
              error: { code: 'device_offline', reason: push.reason ?? 'unknown' },
              device_id: deviceId,
              user_id: userId,
            },
          },
          content: JSON.stringify({ error: { code: 'device_offline', reason: push.reason ?? 'unknown' } }),
          correlationId,
          audience: ['channel'],
          origin: 'external',
          source: 'device',
        });
      } catch (recordErr) {
        emitJsonEvent('device.command.failed_record_error', {
          device_id: deviceId,
          correlation_id: correlationId,
          error: recordErr?.message ?? String(recordErr),
        });
      }
      this.dispatchRouter.remove(correlationId);
      throw toRpcError(
        'device_offline',
        `device ${deviceId} is offline (${push.reason ?? 'unknown'})`,
        503,
      );
    }

    return {
      correlation_id: correlationId,
      dispatch: startMessage,
      self_check: checkMessage,
      device_id: deviceId,
      user_id: userId,
      push,
    };
  }

  async deviceSessionGet(params) {
    const userId = String(params.user_id ?? params.userId ?? '').trim();
    if (!userId) throw toRpcError('bad_request', 'user_id is required');
    const session = this.sessionManager.getSession(userId);
    // Spec §4.1 / Fix-T2 §4: existing session → flat payload; missing → null
    // (envelope will write `{ok:true, result:null}`, mirrors publish-status).
    if (!session) return null;
    return {
      cookies: Array.isArray(session.cookies) ? session.cookies : [],
      user_id: session.user_id ?? userId,
      login_state: session.login_state ?? 'unknown',
      last_updated_at: session.last_updated_at ?? null,
      expires_at: session.expires_at ?? null,
    };
  }

  async deviceSessionUpdate(params) {
    // 用法 1（RPC 直接调用）：{user_id, cookies, login_state, expires_at}
    // 用法 2（来自 HTTP /api/device/{id}/session 转发）：{deviceId, userId, patch}
    const userId = String(params.user_id ?? params.userId ?? '').trim();
    const patch = params.patch && typeof params.patch === 'object'
      ? params.patch
      : (() => {
          const p = { ...params };
          delete p.user_id;
          delete p.userId;
          delete p.deviceId;
          return p;
        })();
    if (!userId) throw toRpcError('bad_request', 'user_id is required');
    const merged = this.sessionManager.updateSession(userId, patch);
    emitJsonEvent('device.session.updated', {
      user_id: userId,
      device_id: params.deviceId ?? null,
      login_state: merged.login_state ?? null,
    });
    return { user_id: userId, session: merged };
  }

  async deviceCallback({ deviceId, correlationId, status, result, error }) {
    const correlation = String(correlationId ?? '').trim();
    if (!correlation) throw toRpcError('bad_request', 'correlation_id is required');
    const route = this.dispatchRouter.lookup(correlation);
    if (!route) {
      // M1.1 Fix-T3 dedupe: extension may retry / replay the same callback
      // after a transient failure. If a `dispatch.completed | dispatch.failed |
      // dispatch.rejected` row already exists for this correlation_id we treat
      // the second arrival as idempotent → 200 OK with `deduped:true`. Only
      // when the message store has *no* trace of the dispatch do we fall back
      // to the legacy `correlation_unknown` 404.
      const dedupe = this._lookupDispatchTerminalAcrossChannels(correlation, deviceId);
      if (dedupe) {
        emitJsonEvent('device.callback.dedupe', {
          device_id: deviceId ?? null,
          correlation_id: correlation,
          payload_type: dedupe.message.payload_type,
        });
        return {
          correlation_id: correlation,
          payload_type: dedupe.message.payload_type,
          message: dedupe.message,
          deduped: true,
        };
      }
      throw toRpcError('correlation_unknown', `no active dispatch for correlation_id=${correlation}`, 404);
    }
    if (deviceId && route.device_id && deviceId !== route.device_id) {
      throw toRpcError('forbidden', `device_id mismatch for correlation_id=${correlation}`, 403);
    }
    const node = this.channels.get(route.channel_id);
    if (!node) {
      throw toRpcError('channel_not_found', `channel ${route.channel_id} no longer exists`, 404);
    }
    const isOk = status === 'ok';
    const payloadType = isOk ? PayloadType.DISPATCH_COMPLETED : PayloadType.DISPATCH_FAILED;
    const body = isOk
      ? { result: result ?? null, device_id: route.device_id, user_id: route.user_id }
      : { error: error ?? null, device_id: route.device_id, user_id: route.user_id };

    const callbackMessage = await this.sendChannelMessage({
      channelId: node.channelId,
      senderKind: SenderKind.EXTERNAL,
      senderId: `device:${route.device_id}`,
      senderName: `device:${route.device_id}`,
      payload: { type: payloadType, body },
      content: JSON.stringify(body),
      correlationId: correlation,
      audience: ['channel'],
      origin: 'external',
      source: 'device',
    });

    emitJsonEvent('device.callback', {
      device_id: route.device_id,
      correlation_id: correlation,
      status: isOk ? 'completed' : 'failed',
    });

    // Dispatch finalized — drop router entry; idempotent even if extension retries.
    this.dispatchRouter.remove(correlation);

    return {
      correlation_id: correlation,
      payload_type: payloadType,
      message: callbackMessage,
    };
  }

  /**
   * M1.1 Fix-T3 — replay a batch of pending callbacks delivered over the WS
   * `callback_replay` frame. Each payload is dispatched through
   * `deviceCallback` independently; per-payload errors are swallowed (logged
   * via `device.callback.replay_error`) so one bad entry can't block the rest.
   *
   * R3-T3 (FX5) — after dispatching, push a `callback_replay_ack` frame back
   * to the originating device so the extension can decide which outbox
   * entries to drop. dedupe is treated as accepted (daemon already has the
   * row in its message store). per-payload errors → rejected with
   * `{correlation_id, code, message}` so the extension can log + drop them.
   *
   * @param {{ deviceId: string, payloads: Array<object> }} args
   * @returns {Promise<{
   *   accepted: number,
   *   deduped: number,
   *   failed: number,
   *   results: Array<{ correlation_id: string, ok: boolean, deduped?: boolean, error?: string }>,
   *   ack: { accepted: string[], rejected: Array<{ correlation_id: string, code: string, message: string }> },
   * }>}
   */
  async handleCallbackReplay({ deviceId, payloads }) {
    if (!Array.isArray(payloads) || payloads.length === 0) {
      return {
        accepted: 0,
        deduped: 0,
        failed: 0,
        results: [],
        ack: { accepted: [], rejected: [] },
      };
    }
    let accepted = 0;
    let deduped = 0;
    let failed = 0;
    const results = [];
    // R3-T3: build ack lists in lock-step with results so the extension can
    // surgically clear only the entries the daemon has confirmed.
    const ackAccepted = [];
    const ackRejected = [];
    for (const raw of payloads) {
      if (!raw || typeof raw !== 'object') {
        failed += 1;
        results.push({ correlation_id: '', ok: false, error: 'invalid_payload' });
        ackRejected.push({
          correlation_id: '',
          code: 'invalid_payload',
          message: 'payload must be an object',
        });
        continue;
      }
      const correlationId = String(raw.correlation_id ?? raw.correlationId ?? '').trim();
      const status = String(raw.status ?? '').trim().toLowerCase();
      try {
        const out = await this.deviceCallback({
          deviceId,
          correlationId,
          status,
          result: raw.result ?? null,
          error: raw.error ?? null,
        });
        if (out?.deduped) {
          deduped += 1;
          results.push({ correlation_id: correlationId, ok: true, deduped: true });
        } else {
          accepted += 1;
          results.push({ correlation_id: correlationId, ok: true });
        }
        // dedupe → daemon already accepted the callback once; safe for
        // extension to drop the outbox entry.
        ackAccepted.push(correlationId);
      } catch (err) {
        failed += 1;
        const code = err?.code ?? 'replay_error';
        const message = err?.message ?? String(err);
        results.push({ correlation_id: correlationId, ok: false, error: code });
        ackRejected.push({ correlation_id: correlationId, code, message });
        emitJsonEvent('device.callback.replay_error', {
          device_id: deviceId ?? null,
          correlation_id: correlationId,
          error: code,
          message,
        });
      }
    }
    emitJsonEvent('device.callback.replay', {
      device_id: deviceId ?? null,
      total: payloads.length,
      accepted,
      deduped,
      failed,
    });

    // R3-T3 — push callback_replay_ack so extension only drops accepted /
    // rejected entries. Best-effort: pushCommand returns sync {ok|reason} and
    // never throws; if device went offline mid-replay the next reconnect's
    // drain will retry (daemon dedupe path will re-accept).
    const ack = { accepted: ackAccepted, rejected: ackRejected };
    if (deviceId && this.deviceWsServer && typeof this.deviceWsServer.pushCommand === 'function') {
      const push = this.deviceWsServer.pushCommand(deviceId, {
        type: 'callback_replay_ack',
        accepted: ackAccepted,
        rejected: ackRejected,
      });
      if (!push?.ok) {
        emitJsonEvent('device.callback.replay_ack_push_failed', {
          device_id: deviceId,
          reason: push?.reason ?? 'unknown',
          accepted_count: ackAccepted.length,
          rejected_count: ackRejected.length,
        });
      }
    }

    return { accepted, deduped, failed, results, ack };
  }

  /**
   * Find an existing `dispatch.completed | dispatch.failed | dispatch.rejected`
   * row across all loaded channels for `correlationId`. Returns `null` when
   * there is no terminal record yet (caller should 404). Used for dedupe in
   * `deviceCallback` after router miss.
   *
   * If `expectedDeviceId` is provided, the device_id stamped on the existing
   * dispatch.start (or terminal payload body) must match — otherwise we treat
   * the device as untrusted and return null so the legacy `correlation_unknown`
   * path runs.
   */
  _lookupDispatchTerminalAcrossChannels(correlationId, expectedDeviceId = '') {
    const correlation = String(correlationId ?? '').trim();
    if (!correlation) return null;
    const expectDevice = String(expectedDeviceId ?? '').trim();
    for (const node of this.channels.values()) {
      let db;
      try {
        db = this._openMessageStore(node);
      } catch {
        continue;
      }
      // queryStoredMessages requires a single payload_type filter at a time;
      // probe terminal types in priority order.
      for (const terminalType of [
        PayloadType.DISPATCH_COMPLETED,
        PayloadType.DISPATCH_FAILED,
        PayloadType.DISPATCH_REJECTED,
      ]) {
        const rows = queryStoredMessages(db, {
          channel_id: node.channelId,
          correlation_id: correlation,
          payload_type: terminalType,
          order: 'desc',
          limit: 1,
        });
        if (rows.length === 0) continue;
        const message = rows[0];
        if (expectDevice) {
          const recordedDeviceId = String(message.payload_body?.device_id ?? '').trim();
          if (recordedDeviceId && recordedDeviceId !== expectDevice) {
            // Device mismatch — refuse dedupe; caller will throw forbidden later.
            return null;
          }
        }
        return { node, message };
      }
    }
    return null;
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
      senderKind: SenderKind.AGENT,
      senderId: node.agentName,
      senderName: node.agentName,
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
          senderKind: SenderKind.AGENT,
          senderId: node.agentName,
          senderName: node.agentName,
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
          senderKind: SenderKind.AGENT,
          senderId: node.agentName,
          senderName: node.agentName,
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
          senderKind: SenderKind.AGENT,
          senderId: node.agentName,
          senderName: node.agentName,
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

  /**
   * M1.1 Fix-T3 — restart recovery for in-flight dispatches.
   *
   * Scan messages.sqlite for `dispatch.start` rows in the last 30 minutes that
   * have **no** paired `dispatch.completed | dispatch.failed | dispatch.rejected`
   * and re-register them in the in-memory DispatchRouter. Without this, an
   * in-flight extension callback arriving after a daemon restart would be
   * rejected with `correlation_unknown` 404 (the router map is process-local).
   *
   * messages.sqlite remains the source of truth — this method only rebuilds
   * the router cache. It runs at the end of `start()` for each restored
   * channel; failures are non-fatal.
   *
   * @param {object} node — channel node (must already be loaded via
   *   `_loadNodeFromDisk` and registered in `this.channels`).
   * @param {object} [opts]
   * @param {number} [opts.windowMs] — recovery window in ms (default 30min).
   * @param {number} [opts.nowMs]     — clock injection for tests.
   * @returns {{ recovered: number, scanned: number }}
   */
  _recoverDispatchRouter(node, { windowMs = DEFAULT_RECOVERY_WINDOW_MS, nowMs = Date.now() } = {}) {
    const db = this._openMessageStore(node);
    const startRows = queryStoredMessages(db, {
      channel_id: node.channelId,
      payload_type: PayloadType.DISPATCH_START,
      not_before_gte: undefined,
      order: 'desc',
      limit: 500,
    }).filter((row) => Number(row.ts_received) >= nowMs - windowMs);

    if (startRows.length === 0) return { recovered: 0, scanned: 0 };

    const candidates = [];
    for (const row of startRows) {
      const correlationId = String(row.correlation_id ?? '').trim();
      if (!correlationId) continue;
      // Reject if any terminal (completed/failed/rejected) row exists for the
      // same correlation_id — that dispatch has already finalized and the
      // router doesn't need to track it. Probe each terminal type separately
      // since queryStoredMessages filters on a single payload_type.
      const completed = queryStoredMessages(db, {
        channel_id: node.channelId,
        correlation_id: correlationId,
        payload_type: PayloadType.DISPATCH_COMPLETED,
        order: 'desc',
        limit: 1,
      });
      if (completed.length > 0) continue;
      const failed = queryStoredMessages(db, {
        channel_id: node.channelId,
        correlation_id: correlationId,
        payload_type: PayloadType.DISPATCH_FAILED,
        order: 'desc',
        limit: 1,
      });
      if (failed.length > 0) continue;
      const rejected = queryStoredMessages(db, {
        channel_id: node.channelId,
        correlation_id: correlationId,
        payload_type: PayloadType.DISPATCH_REJECTED,
        order: 'desc',
        limit: 1,
      });
      if (rejected.length > 0) continue;

      const body = row.payload_body ?? {};
      candidates.push({
        correlation_id: correlationId,
        channel_id: node.channelId,
        device_id: String(body.device_id ?? '').trim(),
        user_id: String(body.user_id ?? '').trim(),
      });
    }

    const summary = this.dispatchRouter.recoverFromRows(candidates);
    if (summary.recovered > 0) {
      emitJsonEvent('dispatch.router.recovered', {
        channel_id: node.channelId,
        recovered: summary.recovered,
        scanned: startRows.length,
      });
    }
    return { recovered: summary.recovered, scanned: startRows.length };
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
      senderId: 'system:daemon',
      senderName: 'daemon',
      content: typeof body?.text === 'string' ? body.text : JSON.stringify(body),
      payload: { type: payloadType, body },
      createdAt: event.createdAt ?? event.created_at ?? nowIso(),
      source: event.source ?? 'daemon',
      origin: 'system',
    };
  }

  _eventFromMessage(message) {
    return {
      type: message.payloadType ?? message.payload?.type ?? 'message',
      source: message.source ?? 'daemon',
      createdAt: message.createdAt ?? nowIso(),
      payload: { message },
    };
  }

  _messageFromStoredMessage(node, stored) {
    return {
      messageId: stored.id,
      channelId: stored.channel_id ?? node.channelId,
      senderKind: stored.sender_kind,
      senderId: stored.sender_id,
      senderName: stored.sender_name,
      payloadType: stored.payload_type,
      payloadBody: stored.payload_body,
      payload: stored.payload,
      content: stored.payload_body?.text ?? JSON.stringify(stored.payload_body ?? {}),
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
      createdAt: stored.created_at ?? formatLocalIso(stored.ts),
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
        const rawDispatchResult = await this._dispatchDueMessage(node, event, outcome);
        const dispatchResult = rawDispatchResult?.outcome
          ? rawDispatchResult
          : { outcome, delivery: rawDispatchResult ?? null };
        const delivery = dispatchResult.delivery ?? { ok: false, reason: dispatchResult.outcome.reason ?? 'delivery failed' };
        if (delivery.ok && dispatchResult.outcome.decision !== 'block') {
          const deliveredMessage = markMessageDelivered(db, stored.id, nowMs);
          if (deliveredMessage?.seq != null) {
            this._recordSuccessfulDelivery({
              node,
              storedMessage: deliveredMessage,
              deliveryAck: delivery,
            });
          }
          delivered.push({
            message_id: stored.id,
            channel_id: node.channelId,
            decision: dispatchResult.outcome.decision,
            reason: dispatchResult.outcome.reason,
            ok: true,
          });
          continue;
        }

        const lastError = delivery.reason ?? dispatchResult.outcome.reason ?? 'delivery failed';
        const failedMessage = markMessageDeliveryAttempt(db, stored.id, {
          attemptedAt: nowMs,
          error: lastError,
        });
        const attempts = failedMessage?.delivery_attempts ?? ((stored.delivery_attempts ?? 0) + 1);
        let failure = {
          message_id: stored.id,
          channel_id: node.channelId,
          decision: dispatchResult.outcome.decision,
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
      return {
        outcome,
        delivery: await this._deliverEvent(node, event),
      };
    }
    if (outcome.decision === 'log_only') {
      await this._recordTrace(node, {
        kind: 'trigger',
        decision: 'log_only',
        reason: outcome.reason,
        event,
      });
      return {
        outcome,
        delivery: { ok: true },
      };
    }
    await this._recordTrace(node, {
      kind: 'trigger',
      decision: 'block',
      reason: outcome.reason,
      event,
    });
    return {
      outcome,
      delivery: { ok: false, reason: outcome.reason ?? 'blocked' },
    };
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
        this._inspectAgentStdoutLine(node, line).catch((err) => {
          console.error(`[ChannelManager] turn signal inspect failed for ${node.channelId}:`, err.message);
        });
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

  _messageIdFromEvent(event) {
    const message = event?.payload?.message ?? event?.payload;
    return String(
      message?.envelope?.id
        ?? message?.messageId
        ?? message?.message_id
        ?? message?.id
        ?? '',
    ).trim();
  }

  _recordSuccessfulDelivery({
    node,
    storedMessage = null,
    messageId = null,
    deliveryAck = null,
    agentName = null,
  } = {}) {
    if (!node || deliveryAck?.ok !== true) return null;

    let stored = storedMessage;
    if (!stored) {
      const id = String(messageId ?? '').trim();
      if (!id) return null;
      stored = getStoredMessage(this._openMessageStore(node), id);
    }
    if (!stored?.seq) return null;

    const seq = parsePositiveSeq(stored.seq, 'message seq');
    const cursorAgentName = String(agentName ?? node.agentName ?? DEFAULT_AGENT_NAME).trim() || DEFAULT_AGENT_NAME;
    return advanceAgentCursor(node.workdir, cursorAgentName, seq);
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
    assertSenderPayloadPair(
      normalized.senderKind ?? normalized.envelope?.sender?.kind,
      normalized.payload?.type ?? normalized.payloadType,
    );
    const stored = appendMessageToStore(this._openMessageStore(node), normalized);
    if (stored?.inserted === false) return normalized;
    return normalized;
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
      sender_id: message.senderId,
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
      not_before: message.notBefore,
      origin: message.origin,
      expires_at: message.expiresAt,
      ts_received: message.tsReceived,
      envelope: message.envelope,
      payload: message.payload,
    };

    const result = await this._sendViewSyncPayload(payload);
    if (!result.ok) {
      await this._recordTrace(node, {
        type: 'view_sync_failed',
        messageId: message.messageId,
        requestId: payload.requestId,
        reason: result.reason,
      });
      console.error(`[ChannelManager] View sync failed channel=${node.channelId} message=${message.messageId}: ${result.reason}`);
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

  _turnFailureKey(channelId, eventType) {
    return `${channelId} ${eventType}`;
  }

  _isEventTypeDeadLettered(channelId, eventType) {
    return this.deadLetteredEventTypes.has(this._turnFailureKey(channelId, eventType));
  }

  async _inspectAgentStdoutLine(node, line) {
    const trimmed = String(line ?? '').trim();
    if (!trimmed) return;
    if (!trimmed.startsWith('{')) return;
    let entry;
    try {
      entry = JSON.parse(trimmed);
    } catch {
      return;
    }
    if (!entry || typeof entry !== 'object') return;
    if (entry.event !== 'agent.activity') return;
    const eventType = String(entry.event_type ?? '').trim();
    if (!eventType) return;

    if (entry.activity === 'turn.failed') {
      const detail = String(entry.detail ?? '').trim();
      if (detail !== 'claude_turn_failed') return;
      await this._recordTurnFailure(node, eventType, {
        message: String(entry.message ?? '').trim(),
      });
      return;
    }

    if (entry.activity === 'turn.completed') {
      this._recordTurnSuccess(node, eventType);
    }
  }

  async _recordTurnFailure(node, eventType, { message = '' } = {}) {
    const key = this._turnFailureKey(node.channelId, eventType);
    const count = (this.turnFailureCounts.get(key) ?? 0) + 1;
    this.turnFailureCounts.set(key, count);

    await this._recordTrace(node, {
      kind: 'turn.failed.observed',
      event_type: eventType,
      attempts: count,
      message,
    });

    if (count >= TURN_FAILURE_DEAD_LETTER_LIMIT) {
      await this._deadLetterEventType(node, eventType, { attempts: count, message });
    }
  }

  _recordTurnSuccess(node, eventType) {
    const key = this._turnFailureKey(node.channelId, eventType);
    this.turnFailureCounts.delete(key);
  }

  async _deadLetterEventType(node, eventType, { attempts, message }) {
    const key = this._turnFailureKey(node.channelId, eventType);
    if (this.deadLetteredEventTypes.has(key)) return;
    this.deadLetteredEventTypes.set(key, {
      channelId: node.channelId,
      eventType,
      attempts,
      message,
      deadLetteredAt: nowIso(),
    });

    await this._recordTrace(node, {
      kind: 'turn.dead_letter',
      event_type: eventType,
      attempts,
      reason: 'claude_turn_failed_repeated',
      message,
    });

    emitJsonEvent('inbox.created', {
      channel_id: node.channelId,
      severity: 'blocker',
      reason: 'incident',
      ticket_id: null,
      body: {
        channel_id: node.channelId,
        event_type: eventType,
        attempts,
        reason: 'claude_turn_failed_repeated',
        message,
      },
    });

    console.error(
      `[ChannelManager] Dead-lettering channel=${node.channelId} event_type=${eventType} after ${attempts} consecutive claude_turn_failed`,
    );
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
