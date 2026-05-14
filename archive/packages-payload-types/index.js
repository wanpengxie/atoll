export const SenderKind = Object.freeze({
  HUMAN: 'human',
  AGENT: 'agent',
  SYSTEM: 'system',
  EXTERNAL: 'external',
});

export const Audience = Object.freeze({
  CHANNEL: 'channel',
  SELF: 'self',
});

export const PayloadType = Object.freeze({
  USER_TEXT: 'user.text',
  AGENT_TEXT: 'agent.text',
  AGENT_PROGRESS: 'agent.progress',
  SYSTEM_NOTICE: 'system.notice',
  SYSTEM_HEARTBEAT: 'system.heartbeat',
  CHANNEL_PRESENCE_CHANGED: 'channel.presence_changed',
  CHANNEL_CONFIG_UPDATED: 'channel.config.updated',
  DISPATCH_START: 'dispatch.start',
  DISPATCH_ACCEPTED: 'dispatch.accepted',
  DISPATCH_REJECTED: 'dispatch.rejected',
  DISPATCH_COMPLETED: 'dispatch.completed',
  DISPATCH_FAILED: 'dispatch.failed',
  DISPATCH_SELF_CHECK_DUE: 'dispatch.self_check_due',
  TASK_OPENED: 'task.opened',
  TASK_CLOSED: 'task.closed',
  TASK_APPENDED: 'task.appended',
  SELF_MEMO: 'self.memo',
  CRON_TICK: 'cron.tick',
  WORKDIR_CHANGED: 'workdir.changed',
});

export const TriggerDecision = Object.freeze({
  REACT: 'react',
  LOG_ONLY: 'log_only',
  BLOCK: 'block',
});

export const SENDER_KINDS = Object.freeze(Object.values(SenderKind));
export const PAYLOAD_TYPES = Object.freeze(Object.values(PayloadType));
export const TRIGGER_DECISIONS = Object.freeze(Object.values(TriggerDecision));

export const DISPATCH_TERMINAL_PAYLOAD_TYPES = Object.freeze([
  PayloadType.DISPATCH_COMPLETED,
  PayloadType.DISPATCH_FAILED,
  PayloadType.DISPATCH_REJECTED,
]);

export const TASK_PAYLOAD_TYPES = Object.freeze([
  PayloadType.TASK_OPENED,
  PayloadType.TASK_CLOSED,
  PayloadType.TASK_APPENDED,
]);

export const NOISE_PAYLOAD_TYPES = Object.freeze([
  PayloadType.SYSTEM_HEARTBEAT,
]);

export const NOISE_PAYLOAD_PREFIXES = Object.freeze([
  'heartbeat',
  'health.',
  'metric.',
  'sync.',
  'audit.',
  'system.heartbeat',
]);

export function isSenderKind(value) {
  return SENDER_KINDS.includes(value);
}

export function isPayloadType(value) {
  return PAYLOAD_TYPES.includes(value);
}

export function isDispatchTerminalPayloadType(value) {
  return DISPATCH_TERMINAL_PAYLOAD_TYPES.includes(value);
}

export function isTaskPayloadType(value) {
  return TASK_PAYLOAD_TYPES.includes(value);
}

export function isNoisePayloadType(value) {
  const type = String(value ?? '').trim();
  return NOISE_PAYLOAD_TYPES.includes(type) || NOISE_PAYLOAD_PREFIXES.some((prefix) => type.startsWith(prefix));
}
