export type SenderKindValue = 'human' | 'agent' | 'system' | 'external';
export type AudienceValue = 'channel' | 'self' | `external:${string}`;
export type TriggerDecisionValue = 'react' | 'log_only' | 'block';

export type PayloadTypeValue =
  | 'user.text'
  | 'agent.text'
  | 'agent.progress'
  | 'system.notice'
  | 'system.heartbeat'
  | 'channel.presence_changed'
  | 'channel.config.updated'
  | 'dispatch.start'
  | 'dispatch.accepted'
  | 'dispatch.rejected'
  | 'dispatch.completed'
  | 'dispatch.failed'
  | 'dispatch.self_check_due'
  | 'task.opened'
  | 'task.closed'
  | 'task.appended'
  | 'self.memo'
  | 'cron.tick'
  | 'workdir.changed';

export const SenderKind: Readonly<{
  HUMAN: 'human';
  AGENT: 'agent';
  SYSTEM: 'system';
  EXTERNAL: 'external';
}>;

export const Audience: Readonly<{
  CHANNEL: 'channel';
  SELF: 'self';
}>;

export const PayloadType: Readonly<{
  USER_TEXT: 'user.text';
  AGENT_TEXT: 'agent.text';
  AGENT_PROGRESS: 'agent.progress';
  SYSTEM_NOTICE: 'system.notice';
  SYSTEM_HEARTBEAT: 'system.heartbeat';
  CHANNEL_PRESENCE_CHANGED: 'channel.presence_changed';
  CHANNEL_CONFIG_UPDATED: 'channel.config.updated';
  DISPATCH_START: 'dispatch.start';
  DISPATCH_ACCEPTED: 'dispatch.accepted';
  DISPATCH_REJECTED: 'dispatch.rejected';
  DISPATCH_COMPLETED: 'dispatch.completed';
  DISPATCH_FAILED: 'dispatch.failed';
  DISPATCH_SELF_CHECK_DUE: 'dispatch.self_check_due';
  TASK_OPENED: 'task.opened';
  TASK_CLOSED: 'task.closed';
  TASK_APPENDED: 'task.appended';
  SELF_MEMO: 'self.memo';
  CRON_TICK: 'cron.tick';
  WORKDIR_CHANGED: 'workdir.changed';
}>;

export const TriggerDecision: Readonly<{
  REACT: 'react';
  LOG_ONLY: 'log_only';
  BLOCK: 'block';
}>;

export const SENDER_KINDS: readonly SenderKindValue[];
export const PAYLOAD_TYPES: readonly PayloadTypeValue[];
export const TRIGGER_DECISIONS: readonly TriggerDecisionValue[];
export const DISPATCH_TERMINAL_PAYLOAD_TYPES: readonly PayloadTypeValue[];
export const TASK_PAYLOAD_TYPES: readonly PayloadTypeValue[];
export const NOISE_PAYLOAD_TYPES: readonly string[];
export const NOISE_PAYLOAD_PREFIXES: readonly string[];

export function isSenderKind(value: unknown): value is SenderKindValue;
export function isPayloadType(value: unknown): value is PayloadTypeValue;
export function isDispatchTerminalPayloadType(value: unknown): value is PayloadTypeValue;
export function isTaskPayloadType(value: unknown): value is PayloadTypeValue;
export function isNoisePayloadType(value: unknown): boolean;
