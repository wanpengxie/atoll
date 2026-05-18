// threading.js — correlation_id + parent_id grouping for chat folding.
//
// Authoritative spec: .dalek/pm/v4-layer3-spec.md §3 (中间产出折叠 + Claude
// Code TUI 模式) + §3.2 (折叠分组规则).
//
// Pure functions only — feed normalised envelopes (the renderer-friendly
// shape produced by main.js normalizeStoredMessage) + viewer actor id,
// receive a tree of "story" entries with thread folds attached.
//
// Two-level aggregation per spec §3.2:
//   - Level 1 (story): all envelopes sharing one correlation_id collapse
//     into one logical story block. Orphan envelopes (no correlation_id)
//     become singletons.
//   - Level 2 (thread): inside a story each kind=request pairs to (at
//     most) one terminal kind=response via response.parent_id =
//     request.id (per The One Law, L1 §10.2 / spec §3.2 "Request-Response
//     Thread"). The terminal is decided by payload.status presence — any
//     response is treated as terminal in M1.6 (the framework already
//     enforces single terminal; intermediate progress events are a future
//     extension).

import { KIND, VISIBILITY, CORE_TYPE, shouldDropFromTimeline, isVisibleTo } from './protocol.js';

/**
 * Group a flat ordered list of normalised messages into a story timeline.
 *
 * @param {Array} messages — normalised messages (must have .envelope-like
 *   shape: id, kind, type, parent_id, correlation_id, sender, visibility,
 *   audience, payload, seq, ts).
 * @param {Object} opts
 *   - viewerActorID: required; controls visibility ACL
 *   - nowMs: ms timestamp used for future-message / heartbeat drop
 *   - includeSystem: when true, system-visibility messages produce their
 *     own entries (toggled by the system-events drawer); when false they
 *     are routed to thread folds only.
 * @returns {Array} ordered entries, each shaped as:
 *   {
 *     kind: 'message' | 'story' | 'system-event',
 *     id: string,            // stable key for DOM reconciliation
 *     primary: message,      // the headline envelope
 *     thinkingMessages: [],  // agent.text + visibility=system grouped here
 *     threads: [             // request/response folds
 *       { id, request, response, status: 'pending'|'completed'|'failed' },
 *     ],
 *     events: [],            // arbitrary same-correlation events (kind=event)
 *     seq: number,           // story's max seq, drives sort
 *   }
 */
export function groupTimeline(messages, opts) {
  const viewerActorID = opts?.viewerActorID || '';
  const nowMs = opts?.nowMs ?? Date.now();
  const includeSystem = Boolean(opts?.includeSystem);

  const filtered = messages.filter((m) => {
    if (!m) return false;
    if (shouldDropFromTimeline(m, nowMs)) return false;
    if (!isVisibleTo(m, viewerActorID)) return false;
    return true;
  });

  // Sort by (seq asc) — viewcache already returns ascending but the WS
  // path may interleave with REST; resort to be safe.
  filtered.sort((a, b) => Number(a.seq || 0) - Number(b.seq || 0));

  const stories = new Map(); // correlation_id → story
  const orphans = [];        // entries with no correlation_id

  const storyFor = (corr) => {
    if (!corr) return null;
    let s = stories.get(corr);
    if (!s) {
      s = {
        kind: 'story',
        id: corr,
        correlationID: corr,
        primary: null,
        thinkingMessages: [],
        threads: new Map(), // request_id → thread
        events: [],
        systemEvents: [],
        seq: 0,
      };
      stories.set(corr, s);
    }
    return s;
  };

  for (const msg of filtered) {
    const corr = msg.correlation_id || '';
    const story = storyFor(corr);
    if (!story) {
      // No correlation_id → render as standalone message entry. system
      // visibility messages route to system bucket unless includeSystem.
      if (msg.visibility === VISIBILITY.SYSTEM) {
        orphans.push(systemEventEntry(msg));
      } else {
        orphans.push(messageEntry(msg));
      }
      continue;
    }

    story.seq = Math.max(story.seq, Number(msg.seq || 0));

    // Thread bookkeeping for request/response pairs.
    if (msg.kind === KIND.REQUEST) {
      const t = story.threads.get(msg.id) || newThread(msg.id);
      t.request = msg;
      t.status = computeThreadStatus(t);
      story.threads.set(msg.id, t);
    }
    if (msg.kind === KIND.RESPONSE && msg.parent_id) {
      const t = story.threads.get(msg.parent_id) || newThread(msg.parent_id);
      t.response = msg;
      t.status = computeThreadStatus(t);
      story.threads.set(msg.parent_id, t);
      continue; // response folded into thread — don't add as standalone event
    }

    // Pick the story's "primary" public message: the latest visible,
    // non-system, kind=event agent/human/tool reply. visibility=system
    // messages cluster under the thinking block; system.events bucket.
    if (msg.visibility === VISIBILITY.SYSTEM) {
      if (msg.type === CORE_TYPE.AGENT_TEXT) {
        story.thinkingMessages.push(msg);
      } else if (msg.type === CORE_TYPE.SYSTEM_EVENT) {
        story.systemEvents.push(msg);
      } else {
        // Other visibility=system business events also go to thinking
        // bucket so the public flow stays clean.
        story.thinkingMessages.push(msg);
      }
      continue;
    }

    if (msg.kind === KIND.REQUEST) {
      // Request already filed into thread map above; surface as primary
      // only when no public reply exists yet (so user sees the request
      // happening even before the response lands).
      if (!story.primary) story.primary = msg;
      continue;
    }

    // kind=event, non-system visibility: this is the public reply or a
    // standalone announcement (e.g. xhs.note.archived).
    if (msg.type === CORE_TYPE.AGENT_TEXT ||
      msg.type === CORE_TYPE.HUMAN_TEXT ||
      msg.type === CORE_TYPE.CHAT_TEXT) {
      story.primary = msg;
    } else {
      story.events.push(msg);
    }
  }

  // Assemble final ordered entries: orphans + stories, sorted by seq.
  const out = [];
  for (const s of stories.values()) {
    out.push(finalizeStory(s, includeSystem));
  }
  for (const o of orphans) out.push(o);
  out.sort((a, b) => Number(a.seq || 0) - Number(b.seq || 0));
  return out;
}

function newThread(requestID) {
  return {
    id: requestID,
    request: null,
    response: null,
    status: 'pending',
  };
}

// computeThreadStatus maps a (request, response) pair onto one of the L3
// §3.3 5-state classifier values. We compress the visual variants down to
// 4 string codes the renderer consumes; "agent thinking" is computed
// elsewhere (per-channel agent activity flag, not per-thread).
//
//   pending       → no response yet; renderer shows ⏳ + label inferred
//                   from request audience (tool / human / agent)
//   completed     → response.payload.status === 'completed'
//   failed        → response.payload.status === 'failed' (any reason)
//   responded     → response present but payload.status absent (legacy)
export function computeThreadStatus(thread) {
  if (!thread.response) return 'pending';
  const status = readPayloadStatus(thread.response);
  if (status === 'completed') return 'completed';
  if (status === 'failed') return 'failed';
  return 'responded';
}

function readPayloadStatus(msg) {
  const p = msg && msg.payload;
  if (!p || typeof p !== 'object') return '';
  return typeof p.status === 'string' ? p.status : '';
}

function finalizeStory(story, includeSystem) {
  const threads = Array.from(story.threads.values()).sort(
    (a, b) => Number(a.request?.seq || a.response?.seq || 0) -
      Number(b.request?.seq || b.response?.seq || 0),
  );
  // Fallback primary: if no public reply was seen, pick the first thread's
  // request so the story has a headline (rare; usually agent emits a
  // closing public message).
  let primary = story.primary;
  if (!primary && threads.length > 0) primary = threads[0].request;
  if (!primary) primary = story.events[0] || story.thinkingMessages[0];
  return {
    kind: 'story',
    id: story.id,
    correlationID: story.correlationID,
    primary,
    thinkingMessages: story.thinkingMessages,
    threads,
    events: story.events,
    systemEvents: includeSystem ? story.systemEvents : [],
    seq: story.seq,
  };
}

function messageEntry(msg) {
  return {
    kind: 'message',
    id: msg.id || `seq-${msg.seq}`,
    primary: msg,
    thinkingMessages: [],
    threads: [],
    events: [],
    systemEvents: [],
    seq: Number(msg.seq || 0),
  };
}

function systemEventEntry(msg) {
  return {
    kind: 'system-event',
    id: msg.id || `seq-${msg.seq}`,
    primary: msg,
    thinkingMessages: [],
    threads: [],
    events: [],
    systemEvents: [msg],
    seq: Number(msg.seq || 0),
  };
}
