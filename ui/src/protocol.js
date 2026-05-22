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
