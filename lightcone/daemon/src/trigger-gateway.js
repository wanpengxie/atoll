import {
  PayloadType,
  SenderKind,
  TriggerDecision,
  isDispatchTerminalPayloadType,
  isNoisePayloadType,
  isTaskPayloadType,
} from '@coagent/payload-types';

function parseJsonString(value, fallback) {
  if (typeof value !== 'string') return value ?? fallback;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function normalizeMentions(value) {
  const parsed = parseJsonString(value, value);
  return Array.isArray(parsed) ? parsed.map((item) => String(item).trim()).filter(Boolean) : [];
}

function normalizeSenderKind(rawKind, rawSenderType, eventType) {
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
      return String(eventType ?? '').startsWith('user.') ? SenderKind.HUMAN : SenderKind.SYSTEM;
  }
}

function inferPayloadType(event, message) {
  const explicit = message?.payload?.type
    ?? message?.payloadType
    ?? message?.payload_type
    ?? event?.payload?.payload?.type
    ?? event?.payload?.payloadType
    ?? event?.payload?.payload_type;
  if (explicit) return String(explicit).trim();

  switch (String(event?.type ?? '').trim()) {
    case 'user.message.posted':
    case 'user.message.mention':
      return PayloadType.USER_TEXT;
    case 'cron.tick':
      return PayloadType.CRON_TICK;
    case 'channel.config.updated':
      return PayloadType.CHANNEL_CONFIG_UPDATED;
    case 'channel.member.joined':
      return PayloadType.CHANNEL_PRESENCE_CHANGED;
    case 'workdir.changed':
      return PayloadType.WORKDIR_CHANGED;
    default:
      return String(event?.type ?? '').trim();
  }
}

function mentionsSelf(mentions, channel) {
  if (mentions.length === 0) return false;
  const ids = new Set([
    channel?.agentName,
    channel?.agent_id,
    channel?.channelAgentId,
    channel?.channel_agent_id,
    channel?.agentName ? `agent:${channel.agentName}` : null,
  ].filter(Boolean).map(String));
  return mentions.some((mention) => ids.has(mention));
}

function pathFromPayload(payload) {
  return String(payload?.path ?? payload?.file_path ?? payload?.filePath ?? '').trim();
}

function eventContext(event, channel) {
  const message = event?.payload?.message ?? event?.payload;
  const envelope = message?.envelope ?? {};
  const sender = envelope.sender ?? {};
  const senderKind = normalizeSenderKind(
    sender.kind ?? message?.senderKind ?? message?.sender_kind,
    message?.senderType ?? message?.sender_type,
    event?.type,
  );
  const payloadType = inferPayloadType(event, message);
  const mentions = normalizeMentions(envelope.mentions ?? message?.mentions ?? event?.payload?.mentions);
  const payloadBody = message?.payload?.body ?? event?.payload ?? {};

  return {
    senderKind,
    payloadType,
    mentions,
    mentionsSelf: mentionsSelf(mentions, channel),
    path: pathFromPayload(payloadBody),
  };
}

export class TriggerGateway {
  constructor({ onReact, onLogOnly, onBlock, onPass } = {}) {
    this.onReact = onReact ?? onPass;
    this.onLogOnly = onLogOnly;
    this.onBlock = onBlock;
  }

  evaluate(event, channel = null) {
    const type = String(event?.type ?? '').trim();
    if (!type) {
      return { decision: TriggerDecision.BLOCK, reason: 'missing_event_type' };
    }

    const context = eventContext(event, channel);
    const { senderKind, payloadType, mentionsSelf: selfMentioned, path } = context;

    if (isNoisePayloadType(payloadType)) {
      return { decision: TriggerDecision.BLOCK, reason: 'default_ruleset_noise', context };
    }

    if (payloadType === PayloadType.USER_TEXT) {
      return {
        decision: TriggerDecision.REACT,
        reason: selfMentioned ? 'user_text_mentions_self' : 'user_text_default_react',
        context,
      };
    }

    if (isDispatchTerminalPayloadType(payloadType)) {
      return { decision: TriggerDecision.REACT, reason: 'dispatch_terminal_react', context };
    }

    if (payloadType === PayloadType.DISPATCH_ACCEPTED) {
      return { decision: TriggerDecision.LOG_ONLY, reason: 'dispatch_accepted_log_only', context };
    }

    if (payloadType === PayloadType.DISPATCH_SELF_CHECK_DUE) {
      return { decision: TriggerDecision.REACT, reason: 'dispatch_self_check_due_react', context };
    }

    if (senderKind === SenderKind.AGENT && payloadType === PayloadType.DISPATCH_START) {
      return selfMentioned
        ? { decision: TriggerDecision.REACT, reason: 'agent_dispatch_start_mentions_self', context }
        : { decision: TriggerDecision.LOG_ONLY, reason: 'agent_dispatch_start_log_only', context };
    }

    if (senderKind === SenderKind.SYSTEM && payloadType === PayloadType.CRON_TICK) {
      return { decision: TriggerDecision.REACT, reason: 'cron_tick_react', context };
    }

    if (senderKind === SenderKind.SYSTEM && payloadType === PayloadType.CHANNEL_CONFIG_UPDATED) {
      return { decision: TriggerDecision.REACT, reason: 'channel_config_updated_react', context };
    }

    if (senderKind === SenderKind.SYSTEM && payloadType === PayloadType.CHANNEL_PRESENCE_CHANGED) {
      return { decision: TriggerDecision.REACT, reason: 'channel_presence_changed_react', context };
    }

    if (senderKind === SenderKind.SYSTEM && payloadType === PayloadType.WORKDIR_CHANGED) {
      if (path.startsWith('agents/')) {
        return { decision: TriggerDecision.BLOCK, reason: 'workdir_agents_block', context };
      }
      if (path.startsWith('artifacts/') || path.startsWith('notes/')) {
        return { decision: TriggerDecision.LOG_ONLY, reason: 'workdir_silent_log_only', context };
      }
    }

    if (senderKind === SenderKind.AGENT && payloadType === PayloadType.AGENT_TEXT) {
      return { decision: TriggerDecision.LOG_ONLY, reason: 'agent_text_echo_log_only', context };
    }

    if (senderKind === SenderKind.AGENT && isTaskPayloadType(payloadType)) {
      return { decision: TriggerDecision.LOG_ONLY, reason: 'agent_task_signal_log_only', context };
    }

    if (senderKind === SenderKind.AGENT && payloadType === PayloadType.SELF_MEMO) {
      return { decision: TriggerDecision.LOG_ONLY, reason: 'agent_self_memo_log_only', context };
    }

    return { decision: TriggerDecision.BLOCK, reason: 'default_ruleset_unknown', context };
  }

  async dispatch({ channel, event, outcome = null }) {
    const evaluated = outcome ?? this.evaluate(event, channel);

    if (evaluated.decision === TriggerDecision.REACT) {
      const result = await this.onReact?.(channel, event, evaluated);
      return result ?? evaluated;
    }

    if (evaluated.decision === TriggerDecision.LOG_ONLY) {
      const result = await this.onLogOnly?.(channel, event, evaluated);
      return result ?? evaluated;
    }

    const result = await this.onBlock?.(channel, event, evaluated);
    return result ?? evaluated;
  }
}
