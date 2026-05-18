// renderer.js — v4 envelope → chat DOM nodes.
//
// Authoritative spec: .dalek/pm/v4-layer3-spec.md §2 (Message → Chat
// 渲染规则), §3 (中间产出折叠), §6 (多 Actor 视觉), §9 (Inactive actor).
//
// The renderer consumes the output of threading.groupTimeline + a small
// context (viewerActorID, channelID, actor registry snapshot, doc_refs
// proxy) and emits ready-to-mount DOM. Pure-ish: no fetches, no global
// state — caller wires events into the bus.
//
// Rendering tree contract:
//   buildStoryNode(entry, ctx) → <li class="story" data-corr=...>
//     ├── primary message bubble (visibility + kind + sender shape)
//     ├── thinking-fold <details>  (when story has thinkingMessages)
//     ├── threads-fold <details>   (when story has threads)
//     ├── extra events             (kind=event, non-system extras)
//   buildMessageNode(envelope, ctx) → <li class="msg ...">

import { KIND, SENDER_KIND, VISIBILITY, CORE_TYPE, senderColor, senderIsSelf } from './protocol.js';
import { appendInlineMedia } from './media.js';
import { failedTerminalLabel } from './errors.js';

// ───────────────────────────────────────────────────────────────────────
// Top-level helpers
// ───────────────────────────────────────────────────────────────────────

/**
 * Render an entry produced by threading.groupTimeline into a <li> node.
 * @param {Object} entry
 * @param {Object} ctx — { viewerActorID, channelID, actors, mentionedIds }
 *   actors: Map(actorID → { display_name, kind, deregistered_at? })
 *   mentionedIds: Set(envelopeID) — message ids that mention the viewer
 */
export function buildEntryNode(entry, ctx) {
  if (entry.kind === 'system-event') return buildSystemEventNode(entry.primary, ctx);
  if (entry.kind === 'story') return buildStoryNode(entry, ctx);
  return buildMessageNode(entry.primary, ctx);
}

// ───────────────────────────────────────────────────────────────────────
// Story node — wraps primary + thinking fold + threads fold + events
// ───────────────────────────────────────────────────────────────────────

function buildStoryNode(story, ctx) {
  const li = document.createElement('li');
  li.className = 'entry story';
  li.dataset.correlationId = story.correlationID || '';

  if (story.primary) {
    li.appendChild(buildMessageBubble(story.primary, ctx));
  }

  if (story.thinkingMessages.length > 0) {
    li.appendChild(buildThinkingFold(story.thinkingMessages, ctx));
  }

  if (story.threads.length > 0) {
    li.appendChild(buildThreadsFold(story.threads, ctx));
  }

  for (const ev of story.events) {
    if (!story.primary || ev.id !== story.primary.id) {
      li.appendChild(buildEventCard(ev, ctx));
    }
  }
  return li;
}

function buildThinkingFold(messages, ctx) {
  const details = document.createElement('details');
  details.className = 'fold thinking-fold';
  const summary = document.createElement('summary');
  summary.textContent = `▸ 思考过程 (${messages.length} 步)`;
  details.appendChild(summary);
  for (const msg of messages) {
    details.appendChild(buildSystemBubble(msg, ctx));
  }
  return details;
}

function buildThreadsFold(threads, ctx) {
  const details = document.createElement('details');
  details.className = 'fold threads-fold';
  const summary = document.createElement('summary');
  const typeLabel = threads
    .map((t) => t.request?.type || t.response?.type || '?')
    .join(', ');
  summary.textContent = `▸ 工具调用 (${threads.length}: ${typeLabel})`;
  details.appendChild(summary);
  for (const thread of threads) {
    details.appendChild(buildThreadRow(thread, ctx));
  }
  return details;
}

function buildThreadRow(thread, ctx) {
  const row = document.createElement('div');
  row.className = `thread-row thread-status-${thread.status}`;
  if (thread.request) {
    row.appendChild(buildThreadLine('▸ request', thread.request));
  }
  // Status indicator per spec §3.3 — 5-class state.
  const status = document.createElement('div');
  status.className = `thread-status thread-status-${thread.status}`;
  status.textContent = renderThreadStatusLabel(thread, ctx);
  row.appendChild(status);
  if (thread.response) {
    const label = thread.status === 'failed'
      ? '✗ failed'
      : '✓ response';
    row.appendChild(buildThreadLine(label, thread.response));
  }
  return row;
}

function buildThreadLine(label, msg) {
  const line = document.createElement('div');
  line.className = 'thread-line';
  const tag = document.createElement('span');
  tag.className = 'thread-tag';
  tag.textContent = label;
  line.appendChild(tag);
  const body = document.createElement('code');
  body.className = 'thread-payload';
  body.textContent = payloadSummary(msg);
  line.appendChild(body);
  return line;
}

function renderThreadStatusLabel(thread, ctx) {
  switch (thread.status) {
    case 'pending': {
      const req = thread.request;
      if (!req || !Array.isArray(req.audience) || req.audience.length === 0) {
        return '⏳ 待响应';
      }
      const receiverID = req.audience[0];
      const receiver = ctx?.actors?.get?.(receiverID);
      const kind = receiver?.kind || guessReceiverKind(receiverID);
      if (kind === SENDER_KIND.TOOL) return '⏳ 工具处理中';
      if (kind === SENDER_KIND.HUMAN) return `⏳ 等待 @${displayNameOf(receiver, receiverID)} 回应`;
      if (kind === SENDER_KIND.AGENT) return '⏳ agent 处理中';
      return '⏳ 待响应';
    }
    case 'completed':
      return '✓ 已响应';
    case 'failed':
      return failedTerminalLabel(thread.response);
    case 'responded':
      return '✓ 已响应';
    default:
      return '⏳ 待响应';
  }
}

function guessReceiverKind(actorID) {
  if (!actorID) return SENDER_KIND.AGENT;
  if (actorID.startsWith('tool:')) return SENDER_KIND.TOOL;
  if (actorID.startsWith('user:')) return SENDER_KIND.HUMAN;
  if (actorID.startsWith('agent:')) return SENDER_KIND.AGENT;
  if (actorID.startsWith('system')) return SENDER_KIND.SYSTEM;
  return SENDER_KIND.AGENT;
}

function buildEventCard(msg, ctx) {
  const card = document.createElement('div');
  card.className = 'event-card';
  const header = document.createElement('div');
  header.className = 'event-header';
  header.textContent = `📌 ${msg.type}`;
  card.appendChild(header);
  const body = document.createElement('div');
  body.className = 'event-body';
  body.textContent = payloadSummary(msg);
  card.appendChild(body);
  return card;
}

// ───────────────────────────────────────────────────────────────────────
// Standalone message bubble (no correlation_id grouping)
// ───────────────────────────────────────────────────────────────────────

export function buildMessageNode(envelope, ctx) {
  const li = document.createElement('li');
  li.className = 'entry single';
  li.appendChild(buildMessageBubble(envelope, ctx));
  return li;
}

export function buildSystemEventNode(envelope, ctx) {
  const li = document.createElement('li');
  li.className = 'entry system-event';
  li.appendChild(buildSystemBubble(envelope, ctx));
  return li;
}

// ───────────────────────────────────────────────────────────────────────
// Primitive bubbles — public message / system message
// ───────────────────────────────────────────────────────────────────────

function buildMessageBubble(envelope, ctx) {
  const bubble = document.createElement('div');
  const isMe = senderIsSelf(envelope, ctx?.viewerActorID);
  const senderKind = envelope.sender?.kind || SENDER_KIND.SYSTEM;
  bubble.className = [
    'bubble',
    `bubble-${senderKind}`,
    isMe ? 'bubble-self' : 'bubble-other',
    envelope.kind === KIND.REQUEST ? 'bubble-request' : '',
    envelope.kind === KIND.RESPONSE ? 'bubble-response' : '',
    ctx?.mentionedIds?.has?.(envelope.id) ? 'bubble-mention' : '',
  ].filter(Boolean).join(' ');

  // Avatar / sender label — skipped for self (per spec §6 self alignment).
  if (!isMe && senderKind !== SENDER_KIND.SYSTEM) {
    bubble.appendChild(buildSenderHeader(envelope, ctx));
  }

  const body = document.createElement('div');
  body.className = 'bubble-body';
  body.textContent = payloadSummary(envelope);
  bubble.appendChild(body);

  // Inline media for envelopes that carry doc_refs of recognised types.
  if (Array.isArray(envelope.doc_refs) && envelope.doc_refs.length > 0 && ctx?.channelID) {
    appendInlineMedia(bubble, ctx.channelID, envelope.doc_refs);
  }

  // Footer with kind/type/seq — small + muted; helps debugging in demo.
  const meta = document.createElement('small');
  meta.className = 'bubble-meta';
  meta.textContent = `${envelope.kind || '?'} · ${envelope.type} · seq ${envelope.seq}`;
  bubble.appendChild(meta);
  return bubble;
}

function buildSystemBubble(envelope, ctx) {
  const wrap = document.createElement('div');
  wrap.className = 'bubble bubble-system';
  const head = document.createElement('div');
  head.className = 'bubble-system-header';
  head.textContent = `[${envelope.type}] ${envelope.sender?.id || ''}`;
  wrap.appendChild(head);
  const body = document.createElement('pre');
  body.className = 'bubble-system-body';
  body.textContent = payloadSummary(envelope);
  wrap.appendChild(body);
  if (Array.isArray(envelope.doc_refs) && envelope.doc_refs.length > 0 && ctx?.channelID) {
    appendInlineMedia(wrap, ctx.channelID, envelope.doc_refs);
  }
  return wrap;
}

// ───────────────────────────────────────────────────────────────────────
// Sender header — avatar + name + inactive tag + sender.kind shape hint
// ───────────────────────────────────────────────────────────────────────

function buildSenderHeader(envelope, ctx) {
  const header = document.createElement('div');
  header.className = 'sender-header';
  const sender = envelope.sender || {};
  const actor = ctx?.actors?.get?.(sender.id);
  const avatar = document.createElement('span');
  const senderKind = sender.kind || SENDER_KIND.SYSTEM;
  avatar.className = `avatar avatar-${senderKind}`;
  avatar.style.background = senderColor(sender.id || '');
  avatar.textContent = initialsOf(actor, sender);
  header.appendChild(avatar);
  const name = document.createElement('span');
  name.className = 'sender-name';
  name.textContent = displayNameOf(actor, sender.id);
  header.appendChild(name);
  if (actor?.deregistered_at) {
    const tag = document.createElement('span');
    tag.className = 'inactive-tag';
    tag.textContent = '(inactive)';
    header.appendChild(tag);
  }
  return header;
}

function initialsOf(actor, sender) {
  const name = actor?.display_name || actor?.id || sender?.id || '?';
  const parts = name.split(/[\s:.-]+/).filter(Boolean);
  if (parts.length === 0) return '?';
  if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase();
  return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase();
}

function displayNameOf(actor, fallback) {
  return actor?.display_name || actor?.id || fallback || 'unknown';
}

// ───────────────────────────────────────────────────────────────────────
// Payload → short readable string
// ───────────────────────────────────────────────────────────────────────

function payloadSummary(envelope) {
  const p = envelope?.payload;
  if (p == null) return '';
  if (typeof p === 'string') return p;
  if (typeof p === 'object') {
    if (typeof p.text === 'string') return p.text;
    if (typeof p.message === 'string') return p.message;
    try { return JSON.stringify(p); } catch { return String(p); }
  }
  return String(p);
}
