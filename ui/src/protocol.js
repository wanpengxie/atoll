// protocol.js — v4 envelope closed enums + visibility/kind predicates.
//
// Authoritative spec: .dalek/pm/proto-layer0.md §2 (envelope) +
// .dalek/pm/impl-layer3.md §2-§9 (chat-as-UI render matrix).
//
// All renderer / unread / notify modules import constants and predicates
// from this single file so the spec's closed sets stay in one place. Drift
// here trips the spec audit, not the renderers.

// --- Visibility (L0 §2.4) -----------------------------------------------
export const VISIBILITY = Object.freeze({
  PUBLIC: 'public',
  PRIVATE: 'private',
  SYSTEM: 'system',
});

// --- Kind (L0 §3.1 invariant I7) ----------------------------------------
export const KIND = Object.freeze({
  EVENT: 'event',
  REQUEST: 'request',
  RESPONSE: 'response',
});

// --- SenderKind (L0 §2.3) -----------------------------------------------
export const SENDER_KIND = Object.freeze({
  HUMAN: 'human',
  AGENT: 'agent',
  SYSTEM: 'system',
  TOOL: 'tool',
});

// --- Audience wildcard (L1 §10.2 step 5) ---------------------------------
export const AUDIENCE_WILDCARD = '*';

// --- Response payload.status (Layer 1 final + Layer 2 provisional core) --
//
// Authoritative spec: .dalek/pm/proto-layer0.md §2.5 (semi-closed:
// Layer 1 strict-closed final + Layer 2 strict-closed provisional core
// + Layer 3 half-open business extension `<adapter>.<name>`).
//
// is_terminal derives from (kind, payload.status) — see
// proto-foundation.md §1.6.3:
//
//   is_terminal = (kind == "response" && status ∈ Layer 1)
//
// Layer 3 namespace must equal sender.id's local-name (the substring
// after the last ":" — e.g. "tool:xhs" → "xhs"); UI does not enforce
// (harness does) but the helper exposes the parser so renderers can
// pick the right icon / label.

export const FINAL_STATUS = Object.freeze({
  COMPLETED: 'completed',
  FAILED: 'failed',
});

export const PROVISIONAL_STATUS = Object.freeze({
  RECEIVED: 'received',
  QUEUED: 'queued',
  PROCESSING: 'processing',
  DEFERRED: 'deferred',
  UNAVAILABLE: 'unavailable',
});

const FINAL_SET = new Set(Object.values(FINAL_STATUS));
const PROVISIONAL_CORE_SET = new Set(Object.values(PROVISIONAL_STATUS));
const LAYER3_RE = /^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$/;

/**
 * Whether a payload.status is a Layer-1 final status (completed | failed).
 * Mirrors Go's kernel/message.IsFinalStatus — the harness uses the same
 * predicate to derive is_terminal.
 */
export function isFinalStatus(status) {
  return typeof status === 'string' && FINAL_SET.has(status);
}

/**
 * Whether a payload.status is a Layer-2 provisional core status
 * (received / queued / processing / deferred / unavailable).
 */
export function isProvisionalCoreStatus(status) {
  return typeof status === 'string' && PROVISIONAL_CORE_SET.has(status);
}

/**
 * Whether a payload.status is a Layer-3 business extension
 * (matches `<namespace>.<name>` and the namespace is not a Layer-1/2 name).
 * Note: UI does not enforce sender-namespace ownership — that's harness
 * Step 8 — so a malformed envelope reaching the renderer is logged but
 * still rendered.
 */
export function isLayer3ProvisionalStatus(status) {
  if (typeof status !== 'string' || !LAYER3_RE.test(status)) return false;
  const ns = status.split('.', 1)[0];
  if (FINAL_SET.has(ns) || PROVISIONAL_CORE_SET.has(ns)) return false;
  return true;
}

/**
 * Whether a payload.status is provisional (Layer 2 core OR Layer 3
 * extension). The negation of isFinalStatus on a syntactically valid
 * status — i.e. anything not in {completed, failed} that the harness
 * would still accept.
 */
export function isProvisionalStatus(status) {
  return isProvisionalCoreStatus(status) || isLayer3ProvisionalStatus(status);
}

/**
 * Whether an envelope is a provisional response (kind=response and
 * payload.status is not in Layer 1 final).
 */
export function isProvisionalResponse(envelope) {
  if (!envelope) return false;
  const kind = envelope.kind || envelope.Kind;
  if (kind !== KIND.RESPONSE) return false;
  const status = envelope.payload?.status || envelope.payload?.Status || '';
  return isProvisionalStatus(status);
}

/**
 * Whether an envelope is a final response (kind=response and
 * payload.status ∈ {completed, failed}).
 */
export function isFinalResponse(envelope) {
  if (!envelope) return false;
  const kind = envelope.kind || envelope.Kind;
  if (kind !== KIND.RESPONSE) return false;
  const status = envelope.payload?.status || envelope.payload?.Status || '';
  return isFinalStatus(status);
}

/**
 * Parse a Layer-3 status into { namespace, name }. Returns null for
 * Layer-1 / Layer-2 / malformed status. UI uses the namespace to map
 * provisional updates to a specific adapter (e.g. `xhs.login_queued`
 * pins to the xhs device chip).
 */
export function parseLayer3Status(status) {
  if (!isLayer3ProvisionalStatus(status)) return null;
  const dot = status.indexOf('.');
  return { namespace: status.slice(0, dot), name: status.slice(dot + 1) };
}

/**
 * Extract local-name from an actor.id (the substring after the last
 * ":"). Matches the harness namespace-ownership check (proto-layer0
 * §2.5.3). UI uses this to associate a provisional response with the
 * device chip whose actor sent it.
 */
export function actorLocalName(actorID) {
  if (typeof actorID !== 'string' || actorID.length === 0) return '';
  const colon = actorID.lastIndexOf(':');
  return colon < 0 ? actorID : actorID.slice(colon + 1);
}

// --- Provisional render hints --------------------------------------------
//
// Single source of truth for Layer-2 core status → human label + icon.
// Layer-3 statuses fall through to {icon:'🔄', label:<raw string>} so
// the agent / UI sees exactly what the adapter emitted (B-4 spec
// decision: substrate透传给 agent / UI).

const PROVISIONAL_LABELS = Object.freeze({
  received: { icon: '📥', label: '已收到', tone: 'received' },
  queued: { icon: '⏳', label: '排队中', tone: 'queued' },
  processing: { icon: '🔄', label: '处理中', tone: 'processing' },
  deferred: { icon: '⏰', label: '已推迟', tone: 'deferred' },
  unavailable: { icon: '⚠️', label: '暂不可用', tone: 'unavailable' },
});

/**
 * Resolve a provisional status to a display hint { icon, label, tone }.
 * Layer-2 core gets a curated Chinese label; Layer-3 extension gets the
 * raw status string back (per spec §2.5.3 — UI is allowed to render the
 * namespace badge however it likes, but the string is authoritative).
 */
export function provisionalDisplay(status) {
  const core = PROVISIONAL_LABELS[status];
  if (core) return core;
  if (isLayer3ProvisionalStatus(status)) {
    return { icon: '🔄', label: status, tone: 'extension' };
  }
  // Unknown status (shouldn't happen — harness rejects) — show the
  // raw value so it's debuggable rather than swallowed.
  return { icon: '❔', label: status || '未知状态', tone: 'unknown' };
}

// --- Core types we render specially (L3 §2.2 Core type 渲染) -------------
export const CORE_TYPE = Object.freeze({
  HUMAN_TEXT: 'human.text',
  CHAT_TEXT: 'chat.text',  // legacy / demo alias used by current UI composer
  AGENT_TEXT: 'agent.text',
  SYSTEM_EVENT: 'core.system_event',
  SYSTEM_HEARTBEAT: 'system.heartbeat',
  FILE_CREATED: 'file.created',
  FILE_UPDATED: 'file.updated',
});

// --- Inline-media file-extension table (L3 §4.1) -------------------------
//
// Keep the keys lowercase. Values are the renderer hint enum consumed by
// media.js — kept here so a single table drives both detection and render.
export const INLINE_MEDIA_KIND = Object.freeze({
  IMAGE: 'image',
  VIDEO: 'video',
  MARKDOWN: 'markdown',
  PDF: 'pdf',
  FILE: 'file',
});

const EXTENSION_TABLE = Object.freeze({
  png: INLINE_MEDIA_KIND.IMAGE,
  jpg: INLINE_MEDIA_KIND.IMAGE,
  jpeg: INLINE_MEDIA_KIND.IMAGE,
  webp: INLINE_MEDIA_KIND.IMAGE,
  gif: INLINE_MEDIA_KIND.IMAGE,
  mp4: INLINE_MEDIA_KIND.VIDEO,
  mov: INLINE_MEDIA_KIND.VIDEO,
  md: INLINE_MEDIA_KIND.MARKDOWN,
  pdf: INLINE_MEDIA_KIND.PDF,
});

/**
 * Classify a doc_refs path into an INLINE_MEDIA_KIND value.
 * Returns INLINE_MEDIA_KIND.FILE for unknown extensions (renderer falls
 * back to a generic attachment card per spec §4.1).
 */
export function classifyDocRef(path) {
  if (typeof path !== 'string' || path.length === 0) return INLINE_MEDIA_KIND.FILE;
  const dot = path.lastIndexOf('.');
  if (dot < 0 || dot === path.length - 1) return INLINE_MEDIA_KIND.FILE;
  const ext = path.slice(dot + 1).toLowerCase();
  return EXTENSION_TABLE[ext] || INLINE_MEDIA_KIND.FILE;
}

// --- Visibility ACL (L3 §7.1 visibility guard) ---------------------------
//
// Returns true when the message is visible to the given viewer actor id.
// Precondition: viewerActorID is the current user's member_actor_id
// (never the user UUID — channel.sqlite only knows the channel-local id).
export function isVisibleTo(envelope, viewerActorID) {
  if (!envelope) return false;
  const vis = envelope.visibility || VISIBILITY.PUBLIC;
  switch (vis) {
    case VISIBILITY.PUBLIC:
      return true;
    case VISIBILITY.PRIVATE:
      return envelope.sender && envelope.sender.id === viewerActorID;
    case VISIBILITY.SYSTEM:
      // System messages stay technically visible but the renderer folds
      // them into a "系统事件" section — UI toggles that visibility.
      return true;
    default:
      // Unknown visibility — be conservative; surface so debug can spot.
      return true;
  }
}

// --- Render-drop guard (L3 §2.4) -----------------------------------------
//
// Returns true when the envelope must NOT appear in the chat timeline at
// all — heartbeats, future messages, expired-by-delivery messages.
export function shouldDropFromTimeline(envelope, nowMs) {
  if (!envelope) return true;
  if (envelope.type === CORE_TYPE.SYSTEM_HEARTBEAT) return true;
  // Future message: not_before > now (spec §5.1)
  if (envelope.not_before && Number(envelope.not_before) > nowMs) return true;
  // Expired delivery: delivery_failed_at set + last_error contains 'expired'
  // (best-effort; spec §5.2 leaves the exact predicate to L2 metadata).
  if (envelope.delivery_failed_at && envelope.last_error &&
    /expired/i.test(envelope.last_error)) {
    return true;
  }
  return false;
}

// --- Audience helpers ----------------------------------------------------
export function audienceIncludes(envelope, actorID) {
  if (!envelope || !Array.isArray(envelope.audience)) return false;
  return envelope.audience.includes(actorID);
}

export function audienceIsBroadcast(envelope) {
  if (!envelope || !Array.isArray(envelope.audience)) return false;
  return envelope.audience.length === 1 && envelope.audience[0] === AUDIENCE_WILDCARD;
}

// --- Self check ----------------------------------------------------------
export function senderIsSelf(envelope, viewerActorID) {
  return Boolean(
    envelope && envelope.sender && envelope.sender.id === viewerActorID,
  );
}

// --- Stable color hash (L3 §6 — sender.id 哈希颜色) -----------------------
//
// Deterministic per sender id; saturation/lightness fixed so the palette
// stays consistent across reloads. Returns an HSL string ready to drop
// into CSS.
export function senderColor(senderID) {
  if (!senderID) return 'hsl(0, 0%, 70%)';
  let hash = 0;
  for (let i = 0; i < senderID.length; i++) {
    hash = (hash * 31 + senderID.charCodeAt(i)) >>> 0;
  }
  const hue = hash % 360;
  return `hsl(${hue}, 45%, 55%)`;
}
