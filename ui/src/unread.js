// unread.js — sidebar badge + cursor (last_consumed_seq) persistence.
//
// Authoritative spec: .dalek/pm/v4-layer3-spec.md §7.1 (未读 badge) +
// §7.3 (通知规则 — visibility=system never counts).
//
// M1.6 demo persists cursor in localStorage. The spec mentions an
// authoritative server-side actor_cursors row that surfaces across
// devices; persisting that round-trip is a follow-up. Single-device
// browser experience: localStorage is sufficient.
//
// Cursor key shape: `coagent.cursor.<channelID>` → integer seq.

import { VISIBILITY, isVisibleTo, senderIsSelf } from './protocol.js';

const KEY_PREFIX = 'coagent.cursor.';

/**
 * Read the last-consumed seq for a channel from localStorage.
 * Returns 0 when absent or unparsable (UI behaves as "everything unread").
 */
export function readCursor(channelID) {
  if (!channelID) return 0;
  try {
    const v = window.localStorage.getItem(KEY_PREFIX + channelID);
    if (!v) return 0;
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
  } catch {
    return 0;
  }
}

/**
 * Write the last-consumed seq for a channel. Monotonic — refuses to
 * regress (so a stale scroll-to-bottom doesn't reset an advanced cursor).
 */
export function writeCursor(channelID, seq) {
  if (!channelID) return;
  const current = readCursor(channelID);
  const next = Math.max(current, Number(seq) || 0);
  if (next === current) return;
  try {
    window.localStorage.setItem(KEY_PREFIX + channelID, String(next));
  } catch {
    /* storage quota / private mode — silently ignore */
  }
}

/**
 * Compute the unread count for a channel given its known message list
 * and the viewer's actor id. Mirrors the SQL in spec §7.1:
 *
 *   COUNT(*) WHERE seq > cursor
 *            AND (visibility=public
 *                 OR (visibility=private AND sender_id=self))
 *            AND visibility != 'system'
 *
 * @param {Array} messages — normalised messages (envelope-shaped)
 * @param {string} viewerActorID
 * @param {number} cursor — last consumed seq
 * @returns {number}
 */
export function unreadCount(messages, viewerActorID, cursor) {
  if (!Array.isArray(messages)) return 0;
  let n = 0;
  for (const m of messages) {
    if (!m) continue;
    if (Number(m.seq || 0) <= Number(cursor || 0)) continue;
    if (m.visibility === VISIBILITY.SYSTEM) continue; // §7.3 system never counts
    // visibility guard equivalent to isVisibleTo, but the spec carves
    // out private+sender=self specifically — isVisibleTo covers it.
    if (!isVisibleTo(m, viewerActorID)) continue;
    n += 1;
  }
  return n;
}

/**
 * Returns the message ids whose audience targets the viewer specifically
 * (i.e. drives @ highlight + browser notification). visibility=system
 * messages are stripped per §7.3.
 *
 * @returns {string[]} envelope ids that mention the viewer.
 */
export function mentionedIds(messages, viewerActorID, cursor) {
  if (!Array.isArray(messages)) return [];
  const out = [];
  for (const m of messages) {
    if (!m) continue;
    if (Number(m.seq || 0) <= Number(cursor || 0)) continue;
    if (m.visibility === VISIBILITY.SYSTEM) continue;
    if (!Array.isArray(m.audience)) continue;
    if (!m.audience.includes(viewerActorID)) continue;
    // Skip own-authored messages — self-mention is noise.
    if (senderIsSelf(m, viewerActorID)) continue;
    out.push(m.id);
  }
  return out;
}
