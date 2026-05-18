// notify.js — browser Notification API for @ mentions + response landings.
//
// Authoritative spec: .dalek/pm/v4-layer3-spec.md §7.3 (通知规则).
//
// Triggers (after visibility guard, only when document is hidden):
//   - kind=request, audience=[self], visibility=public → "有人问你"
//   - kind=response, audience contains self AND response.parent_id was a
//     request authored by self → "你的请求有响应"
//   - kind=event, audience contains self, visibility=public → 普通通知
//
// Permission flow: requested lazily — when the first mentionable message
// arrives we prompt; user can deny. We don't preemptively prompt at boot
// (intrusive + lowers conversion).

import { KIND, VISIBILITY, audienceIncludes, senderIsSelf } from './protocol.js';

let permissionState = 'default';
let permissionAsked = false;

export function notifySupported() {
  return typeof window !== 'undefined' && 'Notification' in window;
}

export function currentPermission() {
  if (!notifySupported()) return 'denied';
  permissionState = window.Notification.permission;
  return permissionState;
}

/**
 * Request notification permission. Idempotent — only prompts once per
 * session even if called repeatedly.
 *
 * @returns {Promise<NotificationPermission>}
 */
export async function ensurePermission() {
  if (!notifySupported()) return 'denied';
  if (permissionState === 'granted' || permissionState === 'denied') return permissionState;
  if (permissionAsked) return permissionState;
  permissionAsked = true;
  try {
    permissionState = await window.Notification.requestPermission();
  } catch {
    permissionState = 'denied';
  }
  return permissionState;
}

/**
 * Decide whether the envelope should fire a browser notification for the
 * given viewer. Pure function — separable from the side-effecting fire().
 *
 * @param {Object} envelope — incoming v4 envelope
 * @param {Object} ctx
 *   - viewerActorID: required
 *   - requestSentBySelf: predicate (envelopeID) → boolean; the caller
 *     supplies it so notify.js stays free of message-store coupling.
 * @returns {Object|null} notification descriptor or null if not eligible
 */
export function classifyNotification(envelope, ctx) {
  if (!envelope || !ctx?.viewerActorID) return null;
  // §7.3 visibility guard: system never notifies.
  if (envelope.visibility === VISIBILITY.SYSTEM) return null;
  // Don't notify self of own messages.
  if (senderIsSelf(envelope, ctx.viewerActorID)) return null;

  const isAudienceSelf = audienceIncludes(envelope, ctx.viewerActorID);
  if (!isAudienceSelf) return null;

  // Branch on kind.
  if (envelope.kind === KIND.REQUEST && envelope.visibility === VISIBILITY.PUBLIC) {
    return {
      title: '有人问你',
      body: summarise(envelope),
      tag: `mention-${envelope.id}`,
    };
  }
  if (envelope.kind === KIND.RESPONSE && envelope.parent_id &&
    ctx.requestSentBySelf && ctx.requestSentBySelf(envelope.parent_id)) {
    return {
      title: '你的请求有响应',
      body: summarise(envelope),
      tag: `response-${envelope.id}`,
    };
  }
  if (envelope.kind === KIND.EVENT && envelope.visibility === VISIBILITY.PUBLIC) {
    return {
      title: '提到你',
      body: summarise(envelope),
      tag: `event-${envelope.id}`,
    };
  }
  return null;
}

function summarise(envelope) {
  const p = envelope.payload;
  if (p && typeof p === 'object' && typeof p.text === 'string') {
    return clip(p.text, 140);
  }
  if (typeof p === 'string') return clip(p, 140);
  return `${envelope.type || 'message'}`;
}

function clip(s, n) {
  return s.length > n ? `${s.slice(0, n - 1)}…` : s;
}

/**
 * Fire a browser notification if permission allows AND the document is
 * hidden. Document-visible state already shows the message inline; firing
 * a notification then would double-up.
 *
 * @param {Object} descriptor — output of classifyNotification
 * @param {Function} onClick — invoked when user clicks the notification
 */
export function fire(descriptor, onClick) {
  if (!descriptor || !notifySupported()) return;
  if (permissionState !== 'granted') return;
  if (typeof document !== 'undefined' && document.visibilityState === 'visible') return;
  try {
    const n = new window.Notification(descriptor.title, {
      body: descriptor.body,
      tag: descriptor.tag,
    });
    if (onClick) {
      n.onclick = () => {
        try { window.focus(); } catch { /* noop */ }
        onClick(descriptor);
        n.close();
      };
    }
  } catch {
    /* notification fired during page unload, etc — ignore */
  }
}
