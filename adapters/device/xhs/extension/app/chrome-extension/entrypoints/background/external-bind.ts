// background/external-bind.ts — T148 §B (M1.6-T6)
//
// Externally-connectable message handler. Web UI hosted on a coagent
// server domain calls `chrome.runtime.sendMessage(EXTENSION_ID, ...)`
// from the page context; Chrome routes the message here when manifest
// `externally_connectable.matches` accepts the sender origin.
//
// This module owns the **protocol surface** (3 actions) + the **origin
// allowlist enforcement**. WS lifecycle / persistence are delegated via
// the `ExternalBindDeps` adapter so the handler stays unit-testable
// without dragging the WS client / chrome storage into vitest.
//
// Actions (web UI → extension):
//   1. getDeviceInfo  → returns persistent device_id (auto-generates on
//                       first call), extension version, and current
//                       channel/session metadata if already bound.
//   2. setDeviceToken → persists the v4 session bundle
//                       (server_ws_url, device_session_id, token,
//                       channel_id, user_id, device_id, expires_at) and
//                       opens the WS. Returns {status:'connected'} on
//                       success or {status:'failed', reason} otherwise.
//   3. unbindDevice   → disconnects the active WS, clears v4 fields
//                       from storage (server-side revoke happens
//                       independently via DELETE /api/devices/:sid).
//
// Security:
//   - Layer 1 (Chrome): manifest `externally_connectable.matches`. Only
//     pages whose origin matches a pattern can even reach this listener.
//   - Layer 2 (this module): `isAllowedSenderOrigin()` re-validates the
//     `sender.origin`/`sender.url` against the same allowlist before
//     touching storage or the WS client. Belt-and-suspenders so a
//     wildcard manifest mistake doesn't immediately leak token writes.
//
// All responses are shaped as `{status:'ok'|'connected'|'unbound'|'failed',
// reason?, ...}` so the web UI can branch on status without parsing
// English error strings.

import type { ConnectionConfig } from './connection-state';

/** Allowed origin patterns (manifest-style match patterns). The
 *  background script reads these at install time from the build-time
 *  env var `COAGENT_WEB_ORIGINS` and passes them in via deps; tests
 *  inject their own list. */
export type OriginMatcher = string;

/** Stable shape returned by every external action. `status` is the
 *  closed discriminator the web UI keys off. */
export type ExternalBindResponse =
  | { status: 'ok'; device_id: string; version: string; bound?: BoundSnapshot }
  | { status: 'connected'; device_session_id: string; channel_id: string; user_id?: string }
  | { status: 'unbound' }
  | { status: 'failed'; reason: ExternalBindFailureReason; detail?: string };

/** Current bind snapshot surfaced by getDeviceInfo so the UI can tell
 *  if this extension is already bound to a channel (e.g. show "rebind"
 *  instead of "bind"). Empty fields mean "not bound to that field". */
export interface BoundSnapshot {
  channel_id?: string;
  user_id?: string;
  device_session_id?: string;
  server_ws_url?: string;
}

/** Closed set of failure reasons. Web UI maps these to friendly text. */
export type ExternalBindFailureReason =
  | 'origin_not_allowed'
  | 'invalid_payload'
  | 'ws_connect_failed'
  | 'ws_connect_timeout'
  | 'internal_error';

/** Three known actions. Anything else surfaces as `invalid_payload`. */
export type ExternalBindAction = 'getDeviceInfo' | 'setDeviceToken' | 'unbindDevice';

export interface ExternalBindMessage {
  action: ExternalBindAction;
  /** setDeviceToken payload — see `validateSetDeviceTokenPayload`. */
  server_ws_url?: string;
  device_session_id?: string;
  token?: string;
  channel_id?: string;
  user_id?: string;
  device_id?: string;
  expires_at?: number;
  device_type?: string;
}

/** Minimal `chrome.runtime.MessageSender` projection — keeps deps small
 *  so tests don't need the full chrome types. `origin` is the canonical
 *  sender field (Chrome 80+); `url` is the documented fallback for older
 *  Chrome versions or where origin is absent (e.g. file:// pages). */
export interface ExternalSender {
  origin?: string;
  url?: string;
  id?: string;
  tab?: { id?: number };
}

/** Adapter the background entrypoint must implement. Splitting these
 *  out keeps `external-bind.ts` pure: the handler never imports chrome
 *  or ../services/* directly. */
export interface ExternalBindDeps {
  /** Read the persistent ConnectionConfig (merged with defaults). */
  getConfig: () => Promise<ConnectionConfig>;
  /** Save a partial ConnectionConfig patch; returns the merged result. */
  saveConfig: (patch: Partial<ConnectionConfig>) => Promise<ConnectionConfig>;
  /** Drop both transport clients (legacy + v4). Idempotent. */
  disconnectAll: () => void;
  /** Push the latest config into both clients (idempotent — only the
   *  active transport actually connects on next `connect()`). */
  applyClients: (cfg: ConnectionConfig) => void;
  /** Trigger the active client's `connect()`. Returns `{success,error?}`
   *  in line with the WS client's existing surface. */
  connect: () => Promise<{ success: boolean; error?: string }>;
  /** Extension version (manifest.version) — surfaced by getDeviceInfo. */
  extensionVersion: string;
  /** Allowlist of match patterns (e.g. `https://*.coagent.dev/*`). */
  allowedOrigins: readonly OriginMatcher[];
  /** UUID generator — tests inject a deterministic counter. Returns a
   *  string suitable as a device_id; production uses `crypto.randomUUID()`. */
  generateDeviceID: () => string;
  /** Optional WS-open wait. setDeviceToken returns once this resolves
   *  (or times out) so the UI sees a real connected/failed terminal.
   *  Default implementation: await connect() result only. */
  waitForOpen?: (timeoutMs: number) => Promise<{ open: boolean; error?: string }>;
}

/** Default WS-open wait used in production. Resolves on the first
 *  `connected:true` push from connection-state.broadcast or on timeout.
 *  Tests inject a deterministic shim. */
export const DEFAULT_OPEN_TIMEOUT_MS = 5_000;

/**
 * Validate a sender against an allowlist of manifest match patterns.
 *
 * Match-pattern grammar (subset, sufficient for `externally_connectable`):
 *   <scheme>://<host>/<path>
 *   scheme : "http" | "https"  (no "*" — manifest forbids cross-scheme
 *                                wildcard for externally_connectable)
 *   host   : exact hostname or "*.<suffix>" or "*"
 *   path   : ignored (always treated as "/*")
 *   port   : implicit "*" (Chrome ignores port in match patterns)
 *
 * Returns true iff the sender's `origin` parses as a URL whose scheme
 * + host matches at least one allowlist pattern. `sender.url` is the
 * documented fallback when `origin` is absent (older Chrome).
 *
 * Defense-in-depth: even though Chrome enforces matches at the manifest
 * level, a future maintainer adding `<all_urls>` would silently weaken
 * the boundary. This second-pass check makes the allowlist explicit in
 * source and unit-testable.
 */
export function isAllowedSenderOrigin(
  sender: ExternalSender | undefined,
  allowedOrigins: readonly OriginMatcher[],
): boolean {
  if (!sender) return false;
  const candidate = (sender.origin && sender.origin.length > 0
    ? sender.origin
    : sender.url) ?? '';
  if (!candidate) return false;
  let parsed: URL;
  try {
    parsed = new URL(candidate);
  } catch {
    return false;
  }
  const scheme = parsed.protocol.replace(/:$/, '');
  const host = parsed.hostname;
  if (!scheme || !host) return false;
  for (const pattern of allowedOrigins) {
    if (matchPattern(pattern, scheme, host)) return true;
  }
  return false;
}

function matchPattern(pattern: OriginMatcher, scheme: string, host: string): boolean {
  // Chrome match pattern: scheme://host[:port]/path. We only validate
  // scheme + host. Chrome docs explicitly say "the port part of the URL
  // is ignored" for match patterns (see
  // https://developer.chrome.com/docs/extensions/develop/concepts/match-patterns),
  // so we strip everything from the first ':' in the pattern host before
  // comparing. The browser URL parser already drops the port from
  // sender.hostname, so the right-hand side never carries a port.
  const m = pattern.match(/^([a-z]+):\/\/([^/]+)(\/.*)?$/i);
  if (!m) return false;
  const patternScheme = m[1].toLowerCase();
  const patternHostRaw = m[2];
  // Strip port from pattern host: 'localhost:*' → 'localhost',
  // '*.coagent.dev:8080' → '*.coagent.dev'. Bare ':' / empty host
  // segments are rejected.
  const colonIdx = patternHostRaw.indexOf(':');
  const patternHost = colonIdx >= 0 ? patternHostRaw.slice(0, colonIdx) : patternHostRaw;
  if (!patternHost) return false;
  if (patternScheme !== '*' && patternScheme !== scheme.toLowerCase()) return false;
  if (patternHost === '*') return true;
  if (patternHost.startsWith('*.')) {
    const suffix = patternHost.slice(2).toLowerCase();
    const h = host.toLowerCase();
    return h === suffix || h.endsWith('.' + suffix);
  }
  return patternHost.toLowerCase() === host.toLowerCase();
}

/**
 * Validate setDeviceToken payload. Returns null on success, a failure
 * reason string on validation error. Keeps each missing-field branch
 * narrow so the UI debug logs pinpoint which side dropped data.
 */
function validateSetDeviceTokenPayload(
  msg: ExternalBindMessage,
): { ok: true } | { ok: false; reason: 'invalid_payload'; detail: string } {
  if (!nonEmpty(msg.server_ws_url)) return fail('server_ws_url required');
  if (!nonEmpty(msg.device_session_id)) return fail('device_session_id required');
  if (!nonEmpty(msg.token)) return fail('token required');
  if (!nonEmpty(msg.channel_id)) return fail('channel_id required');
  if (!nonEmpty(msg.device_id)) return fail('device_id required');
  // Sanity check the WS URL shape — we don't want to write malformed
  // URLs into storage and then have the client log a confusing parse error.
  try {
    const u = new URL(msg.server_ws_url!);
    if (u.protocol !== 'ws:' && u.protocol !== 'wss:') {
      return fail('server_ws_url must be ws[s]://');
    }
  } catch {
    return fail('server_ws_url is not a valid URL');
  }
  return { ok: true };

  function fail(detail: string): { ok: false; reason: 'invalid_payload'; detail: string } {
    return { ok: false, reason: 'invalid_payload', detail };
  }
}

function nonEmpty(v: unknown): boolean {
  return typeof v === 'string' && v.trim().length > 0;
}

/**
 * Main entrypoint. The background script registers a chrome listener
 * that funnels everything through here. Pure function modulo deps —
 * tests pass synthetic `ExternalBindDeps` and `ExternalSender`.
 */
export async function handleExternalMessage(
  message: ExternalBindMessage,
  sender: ExternalSender | undefined,
  deps: ExternalBindDeps,
): Promise<ExternalBindResponse> {
  if (!isAllowedSenderOrigin(sender, deps.allowedOrigins)) {
    return {
      status: 'failed',
      reason: 'origin_not_allowed',
      detail: sender?.origin || sender?.url || '(no origin)',
    };
  }
  if (!message || typeof message !== 'object') {
    return { status: 'failed', reason: 'invalid_payload', detail: 'message must be an object' };
  }
  switch (message.action) {
    case 'getDeviceInfo':
      return handleGetDeviceInfo(deps);
    case 'setDeviceToken':
      return handleSetDeviceToken(message, deps);
    case 'unbindDevice':
      return handleUnbindDevice(deps);
    default:
      return {
        status: 'failed',
        reason: 'invalid_payload',
        detail: `unknown action: ${String((message as { action?: unknown }).action)}`,
      };
  }
}

async function handleGetDeviceInfo(deps: ExternalBindDeps): Promise<ExternalBindResponse> {
  try {
    let cfg = await deps.getConfig();
    let deviceID = (cfg.deviceId ?? '').trim();
    if (!deviceID) {
      deviceID = deps.generateDeviceID();
      cfg = await deps.saveConfig({ deviceId: deviceID });
    }
    const snapshot: BoundSnapshot = {
      channel_id: cfg.channelId || undefined,
      user_id: cfg.userId || undefined,
      device_session_id: cfg.deviceSessionId || undefined,
      server_ws_url: cfg.serverWsEndpoint || undefined,
    };
    return {
      status: 'ok',
      device_id: deviceID,
      version: deps.extensionVersion,
      bound: snapshot,
    };
  } catch (err) {
    return errorResponse(err);
  }
}

async function handleSetDeviceToken(
  message: ExternalBindMessage,
  deps: ExternalBindDeps,
): Promise<ExternalBindResponse> {
  const validation = validateSetDeviceTokenPayload(message);
  if (!validation.ok) {
    return { status: 'failed', reason: validation.reason, detail: validation.detail };
  }
  try {
    const patch: Partial<ConnectionConfig> = {
      serverWsEndpoint: message.server_ws_url!.trim(),
      deviceSessionId: message.device_session_id!.trim(),
      deviceSessionToken: message.token!.trim(),
      channelId: message.channel_id!.trim(),
      deviceId: message.device_id!.trim(),
      autoReconnect: true,
    };
    if (nonEmpty(message.user_id)) patch.userId = message.user_id!.trim();
    // Drop the legacy daemon-direct fields so selectTransport() picks v4.
    patch.serverUrl = '';
    patch.wsUrl = '';
    const cfg = await deps.saveConfig(patch);
    // Tear down both transports first — config may have flipped from
    // legacy to v4, and we don't want a stale legacy socket lingering.
    deps.disconnectAll();
    deps.applyClients(cfg);
    const connectResult = await deps.connect();
    if (!connectResult.success) {
      return {
        status: 'failed',
        reason: connectResult.error?.includes('timeout') ? 'ws_connect_timeout' : 'ws_connect_failed',
        detail: connectResult.error,
      };
    }
    // Optional: wait for first `open` event so the UI feedback reflects
    // a real WS handshake, not just "connect() returned success".
    if (deps.waitForOpen) {
      const opened = await deps.waitForOpen(DEFAULT_OPEN_TIMEOUT_MS);
      if (!opened.open) {
        return {
          status: 'failed',
          reason: 'ws_connect_timeout',
          detail: opened.error || 'no open event within timeout',
        };
      }
    }
    return {
      status: 'connected',
      device_session_id: cfg.deviceSessionId ?? '',
      channel_id: cfg.channelId ?? '',
      user_id: cfg.userId,
    };
  } catch (err) {
    return errorResponse(err);
  }
}

async function handleUnbindDevice(deps: ExternalBindDeps): Promise<ExternalBindResponse> {
  try {
    deps.disconnectAll();
    // Clear v4 fields but keep deviceId persistent — same physical browser
    // can re-bind to a different channel later without re-generating its id.
    await deps.saveConfig({
      serverWsEndpoint: '',
      deviceSessionId: '',
      deviceSessionToken: '',
      channelId: '',
      autoReconnect: false,
    });
    return { status: 'unbound' };
  } catch (err) {
    return errorResponse(err);
  }
}

function errorResponse(err: unknown): ExternalBindResponse {
  const msg = err instanceof Error ? err.message : String(err);
  return { status: 'failed', reason: 'internal_error', detail: msg };
}

/**
 * Parse the comma-separated list of match patterns from the build-time
 * env var. Empty / unset → empty list (handler will reject everything
 * with `origin_not_allowed`, so a misconfigured prod build fails closed).
 */
export function parseAllowedOriginsEnv(raw: string | undefined): OriginMatcher[] {
  if (!raw) return [];
  return raw
    .split(',')
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
}
