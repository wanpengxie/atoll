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
  getChannel:       (chID)                      => request('GET',  `/api/channels/${chID}`),
  listMembers:      (chID)                      => request('GET',  `/api/channels/${chID}/members`),

  // Messages
  listMessages:     (chID, after = 0, limit = 200) =>
                      request('GET', `/api/channels/${chID}/messages?after=${after}&limit=${limit}`),
  sendMessage:      (chID, payload, type = 'text') =>
                      request('POST', `/api/channels/${chID}/messages`, {
                        type,
                        payload,
                        visibility: 'public',
                      }),

  // T148 (M1.6-T6) — device session lifecycle (issued by server/devicebus).
  //
  // getPlacement → which daemon currently owns this channel. Web UI
  // needs the daemon_id to POST a device session row; placement is
  // populated by the M1.6-T0 control plane after channel bind.
  // Returns 404 if the channel has no active placement yet (user must
  // bind the channel first).
  getPlacement:       (chID) =>
                        request('GET',  `/api/placements/${chID}`),
  // POST a fresh device_session_token. Server (devicebus) returns
  // {device_session_id, token, expires_at, token_fingerprint}; the
  // call also kicks the daemon control.bind_device_session handshake
  // synchronously so the session is in `ready` state on return.
  issueDeviceSession: (chID, body) =>
                        request('POST', `/api/channels/${chID}/devices`, body),
  // Revoke an issued session. Server flips the row to `revoked` and
  // best-effort sends control.unbind_device_session to the daemon;
  // the extension's WS will be closed server-side as well.
  revokeDeviceSession:(sid) =>
                        request('DELETE', `/api/devices/${sid}`),
};
