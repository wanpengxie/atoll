// background/external-bind.ts — T148 §B (M1.6-T6)
//
// Externally-connectable message handler. Web UI hosted on a coagent
// server domain calls `chrome.runtime.sendMessage(EXTENSION_ID, ...)`
// from the page context; Chrome routes the message here when manifest
// `externally_connectable.matches` accepts the sender origin.
//
// This module owns the **protocol surface** (2 actions) + the **origin
// allowlist enforcement**. WS lifecycle / persistence are delegated via
// the `ExternalBindDeps` adapter so the handler stays unit-testable
// without dragging the WS client / chrome storage into vitest.
//
// Actions (web UI → extension):
//   1. getDeviceInfo  → returns persistent device_id (auto-generates on
//                       first call), extension version, and current
//                       proxy daemon metadata if already configured —
//                       INCLUDING the realtime `connected` flag and
//                       `last_error` from chrome.storage so the web UI
//                       can show live status without the user opening
//                       the popup.
//   2. unbindDevice   → disconnects the active proxy WS and disables
//                       auto-reconnect.
//   3. connectProxy   → set proxy endpoint config + open WS to local
//                       daemon; returns realtime connected snapshot.
//                       This is what surfaces "connect / reconnect"
//                       to the web UI so the popup is optional.
//
// Security:
//   - Layer 1 (Chrome): manifest `externally_connectable.matches`. Only
//     pages whose origin matches a pattern can even reach this listener.
//   - Layer 2 (this module): `isAllowedSenderOrigin()` re-validates the
//     `sender.origin`/`sender.url` against the same allowlist before
//     touching storage or the WS client. Belt-and-suspenders so a
//     wildcard manifest mistake doesn't immediately leak token writes.
//
// All responses are shaped as `{status:'ok'|'unbound'|'failed',
// reason?, ...}` so the web UI can branch on status without parsing
// English error strings.

import {
  type ConnectionConfig,
  type ExtensionConnectionStatus,
} from './connection-state';

/** Allowed origin patterns (manifest-style match patterns). The
 *  background script reads these at install time from the build-time
 *  env var `COAGENT_WEB_ORIGINS` and passes them in via deps; tests
 *  inject their own list. */
export type OriginMatcher = string;

/** Stable shape returned by every external action. `status` is the
 *  closed discriminator the web UI keys off. */
export type ExternalBindResponse =
  | {
      status: 'ok';
      device_id: string;
      version: string;
      bound?: BoundSnapshot;
    }
  | {
      status: 'connected';
      endpoint: string;
      bound: BoundSnapshot;
    }
  | { status: 'unbound' }
  | { status: 'failed'; reason: ExternalBindFailureReason; detail?: string };

/** Current bind snapshot surfaced by getDeviceInfo so the UI can tell
 *  if this extension is already bound to a channel (e.g. show "rebind"
 *  instead of "bind"). Empty fields mean "not bound to that field".
 *  Realtime fields (`connected`, `last_error`) come from the persisted
 *  `ExtensionConnectionStatus`; they tell the web UI whether the WS is
 *  actually up right now, not just whether config was saved. */
export interface BoundSnapshot {
  connection_mode?: ConnectionConfig['connectionMode'];
  proxy_endpoint?: string;
  connected?: boolean;
  reconnecting?: boolean;
  last_error?: string;
  last_updated?: number;
}

/** Closed set of failure reasons. Web UI maps these to friendly text. */
export type ExternalBindFailureReason =
  | 'origin_not_allowed'
  | 'invalid_payload'
  | 'internal_error'
  | 'connect_failed';

/** Known external actions. Anything else surfaces as `invalid_payload`. */
export type ExternalBindAction =
  | 'getDeviceInfo'
  | 'unbindDevice'
  | 'connectProxy';

export interface ExternalBindMessage {
  action: ExternalBindAction;
  /** Optional proxyEndpoint override for connectProxy. Empty / missing
   *  falls back to the extension's default (ws://127.0.0.1:10387). */
  proxy_endpoint?: string;
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
  /** Drop the active transport client. Idempotent. */
  disconnectAll: () => void;
  /** Extension version (manifest.version) — surfaced by getDeviceInfo. */
  extensionVersion: string;
  /** Allowlist of match patterns (e.g. `https://*.coagent.dev/*`). */
  allowedOrigins: readonly OriginMatcher[];
  /** UUID generator — tests inject a deterministic counter. Returns a
   *  string suitable as a device_id; production uses `crypto.randomUUID()`. */
  generateDeviceID: () => string;
  /** Realtime WS status snapshot — drives the `connected` / `last_error`
   *  fields surfaced through getDeviceInfo. Source of truth is the
   *  persisted ExtensionConnectionStatus in chrome.storage.local. */
  getConnectionStatus: () => Promise<ExtensionConnectionStatus>;
  /** Apply a proxy-mode config (endpoint + auto-reconnect) and open the
   *  WS. Returns the post-attempt connected flag so the web UI can
   *  surface success / failure synchronously. */
  connectProxy: (endpoint?: string) => Promise<{ connected: boolean; endpoint: string; error?: string }>;
}

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
    case 'unbindDevice':
      return handleUnbindDevice(deps);
    case 'connectProxy':
      return handleConnectProxy(deps, message.proxy_endpoint);
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
    const status = await deps.getConnectionStatus();
    const snapshot = buildSnapshot(cfg, status);
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

async function handleConnectProxy(
  deps: ExternalBindDeps,
  endpoint: string | undefined,
): Promise<ExternalBindResponse> {
  try {
    const result = await deps.connectProxy(endpoint);
    const cfg = await deps.getConfig();
    const status = await deps.getConnectionStatus();
    const snapshot = buildSnapshot(cfg, status);
    if (!result.connected) {
      return {
        status: 'failed',
        reason: 'connect_failed',
        detail: result.error || 'WS did not reach connected state',
      };
    }
    return {
      status: 'connected',
      endpoint: result.endpoint,
      bound: snapshot,
    };
  } catch (err) {
    return errorResponse(err);
  }
}

function buildSnapshot(
  cfg: ConnectionConfig,
  status: ExtensionConnectionStatus,
): BoundSnapshot {
  const snapshot: BoundSnapshot = {};
  if (cfg.connectionMode) snapshot.connection_mode = cfg.connectionMode;
  if (cfg.proxyEndpoint) snapshot.proxy_endpoint = cfg.proxyEndpoint;
  snapshot.connected = Boolean(status.connected);
  if (status.reconnecting) snapshot.reconnecting = true;
  if (status.lastError) snapshot.last_error = status.lastError;
  if (status.lastUpdated) snapshot.last_updated = status.lastUpdated;
  return snapshot;
}

async function handleUnbindDevice(deps: ExternalBindDeps): Promise<ExternalBindResponse> {
  try {
    deps.disconnectAll();
    await deps.saveConfig({
      autoReconnect: false,
      connectionMode: undefined,
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
