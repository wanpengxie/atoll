// api.js — thin fetch wrapper for the Go server.
//
// All requests use `credentials: 'include'` so the `coagent_session`
// cookie (set by POST /api/identity/login) rides along automatically.
//
// Non-2xx responses are converted into APIError so callers can branch
// on .status / .body. The error JSON shape matches the server: `{
// error: "..." }`.

/**
 * APIError carries the HTTP status + decoded body so handlers can
 * inspect both. `message` is suitable for direct UI display.
 */
export class APIError extends Error {
  constructor(method, path, status, body) {
    super(`${method} ${path} → ${status}: ${body?.error ?? body ?? ''}`);
    this.method = method;
    this.path = path;
    this.status = status;
    this.body = body;
  }
}

// newEnvelopeID returns a fresh sender-provided envelope.id for
// /api/channels/:chID/messages. We prefer crypto.randomUUID when
// available (modern browsers + secure contexts) and fall back to a
// hand-rolled random UUID shape built from crypto.getRandomValues so
// callers in older / non-secure contexts still emit a distinct id.
// Caller-supplied ids are what drives L1 §2.3 harness dedupe — see
// proto-layer3.md §1.8.3.
function newEnvelopeID() {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }
  if (typeof crypto !== 'undefined' && typeof crypto.getRandomValues === 'function') {
    const buf = new Uint8Array(16);
    crypto.getRandomValues(buf);
    buf[6] = (buf[6] & 0x0f) | 0x40; // v4
    buf[8] = (buf[8] & 0x3f) | 0x80; // RFC4122 variant
    const hex = Array.from(buf, (b) => b.toString(16).padStart(2, '0')).join('');
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
  }
  // Last-resort non-cryptographic fallback (shouldn't trigger in
  // current target browsers, but keeps the API from throwing).
  return 'msg-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 10);
}

async function request(method, path, body) {
  const init = {
    method,
    credentials: 'include',
    headers: { Accept: 'application/json' },
  };
  if (body !== undefined) {
    init.headers['Content-Type'] = 'application/json';
    init.body = JSON.stringify(body);
  }
  const res = await fetch(path, init);
  const text = await res.text();
  let parsed = null;
  if (text) {
    try { parsed = JSON.parse(text); } catch { parsed = text; }
  }
  if (!res.ok) {
    throw new APIError(method, path, res.status, parsed);
  }
  return parsed;
}

export const api = {
  // Identity
  issueCode:    (email, purpose = 'register') => request('POST', '/api/identity/verification/issue', { email, purpose }),
  register:     (input)                       => request('POST', '/api/identity/register', input),
  login:        (email, password)             => request('POST', '/api/identity/login', { email, password }),
  logout:       ()                            => request('POST', '/api/identity/logout'),
  me:           ()                            => request('GET',  '/api/identity/me'),

  // Workspaces / channels
  listWorkspaces:   ()                          => request('GET',  '/api/workspaces'),
  createWorkspace:  (name)                      => request('POST', '/api/workspaces', { name }),
  listChannels:     (wsID)                      => request('GET',  `/api/workspaces/${wsID}/channels`),
  createChannel:    (wsID, name, type = 'group') => request('POST', `/api/workspaces/${wsID}/channels`, { name, type }),
  // Bind a channel to a daemon — reserves placement and sends the
  // control.create_channel frame; daemon ACKs and placement flips to
  // active. Must be called after createChannel before any message can
  // be written into the channel.
  bindChannel:      (wsID, chID, daemonID) => request('POST', `/api/workspaces/${wsID}/channels/${chID}/bind`, { daemon_id: daemonID }),
  getChannel:       (chID)                      => request('GET',  `/api/channels/${chID}`),
  listMembers:      (chID)                      => request('GET',  `/api/channels/${chID}/members`).then((r) => ({ members: r.members || [] })),
  listActors:       (chID)                      => request('GET',  `/api/channels/${chID}/actors`).then((r) => ({ ...r, actors: r?.actors || [] })),

  // Proxy daemons — channel view (attached only)
  listDaemons:      (chID)                      => request('GET',  `/api/channels/${chID}/daemons`).then((r) => ({ daemons: r?.daemons || [] })),
  // Composite: create new daemon row + attach to current channel.
  createDaemon:     (chID, input)               => request('POST', `/api/channels/${chID}/daemons`, input),
  // Attach existing owner daemons to current channel (no create).
  attachDaemons:    (chID, daemonIDs)           => request('POST', `/api/channels/${chID}/daemons/attach`, { daemon_ids: daemonIDs }),
  // Detach a daemon from this channel; daemon row stays.
  detachDaemon:     (chID, daemonID)            => request('DELETE', `/api/channels/${chID}/daemons/${daemonID}/attach`),
  // Owner-scoped list (all of caller's daemons, attached or not). Used
  // by the attach UI so the picker covers every device the owner has.
  listOwnerDaemons: ()                          => request('GET',  `/api/daemons`).then((r) => ({ daemons: r?.daemons || [] })),
  // Revoke a daemon outright (deletes the row + cascade attachments).
  revokeDaemon:     (daemonID)                  => request('DELETE', `/api/daemons/${daemonID}`),

  // Messages — send accepts an envelope shape so callers can drive
  // kind / audience / visibility explicitly; defaults keep current
  // behaviour ("public event of the given type") for the demo composer.
  //
  // R4-3: caller MUST supply envelope.id (L3 §1.8.1 sender-provided);
  // re-emit the same id on retry to hit L1 §2.3 dedupe. When the caller
  // omits id we generate a fresh per-request uuid so the first attempt
  // succeeds (and any same-payload retry through that path would
  // produce a new id, which is conservative — collapse-on-retry only
  // kicks in for callers that thread their own id through).
  listMessages:     (chID, after = 0, limit = 200) =>
                      request('GET', `/api/channels/${chID}/messages?after=${after}&limit=${limit}`),
  sendMessage:      (chID, payload, type = 'human.text', opts = {}) =>
                      request('POST', `/api/channels/${chID}/messages`, {
                        id: opts.id || newEnvelopeID(),
                        type,
                        payload,
                        visibility: opts.visibility || 'public',
                        kind: opts.kind,
                        audience: opts.audience,
                        parent_id: opts.parent_id,
                      }),
};
