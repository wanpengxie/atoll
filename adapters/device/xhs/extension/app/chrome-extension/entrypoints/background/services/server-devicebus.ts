// services/server-devicebus.ts — coagent **server** devicebus WS client (v4).
//
// Protocol (impl-layer3 §6.5.1 — R5-14):
//   - WS endpoint: wss://{server-host}/devicebus?session_id={sid}
//   - Handshake:   Sec-WebSocket-Protocol: coagent.device.v1, token.{tok}
//                  (token rides in WS subprotocol slot — NOT URL query,
//                   to avoid leaking into proxy/access logs)
//   - Inbound DeviceFrame (daemon → server → device):
//       {
//         direction: "to_device",
//         device_session_id, channel_id,
//         request_id, correlation_id,
//         payload: <Command JSON> { type:"command", correlation_id, cmd, params },
//         expires_at?: ms epoch
//       }
//   - Outbound DeviceFrame (device → server → daemon):
//       {
//         direction: "from_device",
//         device_session_id, channel_id,
//         request_id, correlation_id,
//         payload: <Callback JSON> { correlation_id, status, result?, error?, device_id? },
//       }
//
// Behaviour:
//   - Persistent WS; exponential reconnect (1/2/4/8/16/30s caps).
//   - On open: drain outbox of any callback DeviceFrames that were
//     enqueued while disconnected.
//   - Inbound `to_device` frame: decode embedded Command payload, dispatch
//     via cmd-handlers (same registry as legacy `coagent-device.ts`), then
//     send back a `from_device` DeviceFrame with the Callback payload.
//   - Unknown / non-`command` inner payloads short-circuit to an error
//     Callback so the adapter framework sees a terminal response instead
//     of timing out.
//
// Differences from legacy `coagent-device.ts`:
//   - Single transport (WS) — no HTTP callback retry path, no
//     callback_replay_ack handshake. The outbox only handles the
//     "WS-down-while-response-ready" case; daemon-side framework's orphan
//     filter (correlation_id) protects against duplicate sends on
//     reconnect.
//   - No device api_key on the wire; the server-issued session token is
//     consumed once at WS handshake.
//   - Tests use `setWebSocketImpl` test seam exactly as the legacy module
//     to keep the harness shape comparable.

import {
  COAGENT_SERVER_DEVICEBUS_PROTOCOL,
  ERROR_MESSAGES,
} from 'coagent-xhs-shared';
import {
  type CommandFrame,
  getCommandHandler,
  isKnownCommand,
} from './cmd-handlers';
import { saveConnectionStatus } from '../connection-state';

// ─── Wire shapes ─────────────────────────────────────────────────────────

/**
 * DeviceFrame is the JSON wire envelope exchanged with server/devicebus.
 * Field shape MUST stay aligned with `server/devicebus/connection.go`'s
 * `DeviceFrame` struct — the gateway translates DeviceFrame ↔ kernel/
 * devicetransit.SendFrame across the daemonbus boundary and any drift breaks
 * the `via_server_transit` binding silently.
 */
export interface DeviceFrame {
  direction: 'to_device' | 'from_device';
  device_session_id: string;
  channel_id: string;
  request_id?: string;
  correlation_id?: string;
  /** Embedded JSON object. Server side declares this as json.RawMessage
   *  so any JSON literal flows through opaque to the adapter. */
  payload: unknown;
  expires_at?: number;
}

/** Inner payload for direction="to_device" frames. Same shape as the
 *  legacy daemon-direct command frame so `cmd-handlers.ts` works as-is. */
export interface CommandPayload {
  type: 'command';
  correlation_id: string;
  cmd: string;
  params?: Record<string, unknown>;
  /** Server pass-through: cookie/login_state context for the device. */
  session?: CommandFrame['session'];
}

/** Inner payload for direction="from_device" frames. Matches the
 *  `adapters/device/xhs/proto.go::Callback` struct on the daemon side. */
export interface CallbackPayload {
  correlation_id: string;
  status: 'ok' | 'error';
  result?: Record<string, unknown> | null;
  error?: { code?: string; message?: string } | null;
  device_id?: string;
}

/** R3-T4 FX9 — public surface kept compatible with `coagent-device.ts`
 *  so `recoverPublishWaitStates` can pass `postCallback` opaque to
 *  whichever client is installed. */
export interface CallbackBody {
  correlation_id: string;
  status: 'ok' | 'error';
  result: Record<string, unknown> | null;
  error: { code: string; message: string } | null;
}

export interface ConnectResult {
  success: boolean;
  error?: string;
}

/** Outbox entry — one undelivered Callback DeviceFrame body + timestamps. */
export interface OutboxEntry {
  frame: DeviceFrame;
  enqueued_at: number;
  last_attempt_at?: number;
}

// ─── Outbox storage helpers ───────────────────────────────────────────────

/** Minimal chrome.storage.local shape — kept narrow so tests can inject
 *  an in-memory shim without dragging the full chrome typings in. */
interface StorageLike {
  get(key: string): Promise<Record<string, unknown>>;
  set(items: Record<string, unknown>): Promise<void>;
}

function getStorage(): StorageLike | null {
  const c: any = (globalThis as any).chrome;
  if (!c?.storage?.local) return null;
  return c.storage.local as StorageLike;
}

const OUTBOX_KEY = COAGENT_SERVER_DEVICEBUS_PROTOCOL.OUTBOX_STORAGE_KEY;
const OUTBOX_MAX_SIZE = COAGENT_SERVER_DEVICEBUS_PROTOCOL.OUTBOX_MAX_SIZE;
const OUTBOX_MAX_AGE_MS = COAGENT_SERVER_DEVICEBUS_PROTOCOL.OUTBOX_MAX_AGE_MS;

export async function readOutbox(): Promise<OutboxEntry[]> {
  const storage = getStorage();
  if (!storage) return [];
  try {
    const stored = await storage.get(OUTBOX_KEY);
    const raw = stored[OUTBOX_KEY];
    if (!Array.isArray(raw)) return [];
    return raw.filter(isOutboxEntry);
  } catch (err) {
    console.warn('[ServerDeviceBus] readOutbox failed', err);
    return [];
  }
}

export async function writeOutbox(entries: OutboxEntry[]): Promise<void> {
  const storage = getStorage();
  if (!storage) return;
  try {
    const trimmed = entries.slice(-OUTBOX_MAX_SIZE);
    await storage.set({ [OUTBOX_KEY]: trimmed });
  } catch (err) {
    console.warn('[ServerDeviceBus] writeOutbox failed', err);
  }
}

function isOutboxEntry(value: unknown): value is OutboxEntry {
  if (!value || typeof value !== 'object') return false;
  const v = value as Record<string, unknown>;
  if (!v.frame || typeof v.frame !== 'object') return false;
  const f = v.frame as Record<string, unknown>;
  return (
    f.direction === 'from_device' &&
    typeof f.device_session_id === 'string' &&
    typeof f.channel_id === 'string' &&
    typeof v.enqueued_at === 'number'
  );
}

// ─── Connection config ───────────────────────────────────────────────────

/**
 * ServerDeviceConfig is the input the v4 client consumes. The
 * background entrypoint composes it from `ConnectionConfig` (read from
 * chrome.storage.local) — see `index.ts` for the mapping.
 *
 * Required fields: `wsEndpoint`, `sessionId`, `token`, `channelId`.
 * `deviceSessionId` is the same value as the server-side device_session_id
 * (carried in every outbound DeviceFrame); it equals `sessionId` here —
 * server.devicebus uses one id for both WS handshake and frame routing.
 * `userId` is informational (forensic logging only).
 */
export interface ServerDeviceConfig {
  /** Base WS URL, e.g. `wss://coagent.example.com/devicebus`. */
  wsEndpoint: string;
  /** server-allocated device_session_id (= WS handshake `session_id`). */
  sessionId: string;
  /** server-issued bearer token (HMAC of session+channel+expiry, 24h TTL). */
  token: string;
  /** channel_id this device is bound to — pass-through on every outbound
   *  DeviceFrame so the daemon-side `OnDeviceTransit` routes to the right
   *  per-channel adapter manager. */
  channelId: string;
  /** Auto reconnect on close (default true). */
  autoReconnect?: boolean;
  /** Informational — appears in console logs only. */
  userId?: string;
  /** Informational — appears in console logs only. */
  deviceId?: string;
}

// ─── WS URL helpers ──────────────────────────────────────────────────────

/**
 * Real WebSocket subprotocol negotiated on /devicebus — mirror of
 * `server/devicebus.DeviceWSSubprotocol`. Server selects (and echoes
 * back) this; never echoes the `token.*` slot.
 */
export const DEVICE_WS_SUBPROTOCOL = 'coagent.device.v1';

/** Prefix used on the `token.*` pseudo-subprotocol slot. */
export const DEVICE_WS_TOKEN_PREFIX = 'token.';

/**
 * Build the v4 devicebus WS URL with `session_id` in the query string.
 * Per impl-layer3 §6.5.1 (R5-14) the bearer token does NOT go on the
 * URL — it rides in `Sec-WebSocket-Protocol` (see
 * `buildDeviceBusSubprotocols`). session_id is non-secret (forensics
 * correlation only) and stays in the URL.
 *
 * Returns "" when any required field is missing so callers can short-
 * circuit without trying to construct an invalid URL.
 */
export function buildDeviceBusUrl(cfg: ServerDeviceConfig): string {
  const base = (cfg.wsEndpoint ?? '').trim();
  const sid = (cfg.sessionId ?? '').trim();
  const tok = (cfg.token ?? '').trim();
  // token is required at the config-validity gate, even though it no
  // longer goes on the URL — without it the subprotocol header would
  // be malformed and the upgrade would 400.
  if (!base || !sid || !tok) return '';
  try {
    const u = new URL(base);
    u.searchParams.set('session_id', sid);
    return u.toString();
  } catch {
    return '';
  }
}

/**
 * Build the `Sec-WebSocket-Protocol` subprotocol list the client offers
 * on the /devicebus handshake (impl-layer3 §6.5.1):
 *
 *   `coagent.device.v1` — real subprotocol; server echoes this back
 *   `token.<token>`     — bearer token slot; server consumes + does
 *                         NOT echo (token must not be reflected to
 *                         the client per RFC 6455 + spec)
 *
 * Returns an empty array when token is missing so callers can decide
 * whether to abort the connect attempt. Pass the result as the second
 * arg of `new WebSocket(url, protocols)`.
 */
export function buildDeviceBusSubprotocols(cfg: ServerDeviceConfig): string[] {
  const tok = (cfg.token ?? '').trim();
  if (!tok) return [];
  return [DEVICE_WS_SUBPROTOCOL, `${DEVICE_WS_TOKEN_PREFIX}${tok}`];
}

/**
 * Redact the bearer token if it ever appears in a URL — defence in
 * depth for legacy callers. With R5-14 token no longer goes in the
 * query string, so this is now a no-op for properly built URLs;
 * retained because chrome.storage may still carry legacy entries.
 *
 * Equivalent to legacy `redactKey` for the new shape.
 */
export function redactToken(url: string): string {
  if (!url) return url;
  return url.replace(/([?&])token=[^&#]*/gi, '$1token=***');
}

// ─── Inner-payload (de)serialisation helpers ─────────────────────────────

/**
 * Coerce the inbound DeviceFrame.payload into a CommandPayload. The
 * server's `Payload json.RawMessage` flows through opaque, so on the
 * wire we'll see one of:
 *   - JSON object (already parsed by JSON.parse(text))
 *   - JSON string literal (rare; we never produce this server-side)
 * Anything else is rejected and surfaces as a synthetic error Callback.
 */
function decodeCommandPayload(raw: unknown): CommandPayload | null {
  if (raw == null) return null;
  if (typeof raw === 'string') {
    try {
      return decodeCommandPayload(JSON.parse(raw));
    } catch {
      return null;
    }
  }
  if (typeof raw !== 'object') return null;
  const o = raw as Record<string, unknown>;
  if (o.type !== COAGENT_SERVER_DEVICEBUS_PROTOCOL.COMMAND_TYPE) return null;
  if (typeof o.correlation_id !== 'string') return null;
  if (typeof o.cmd !== 'string') return null;
  const cmd: CommandPayload = {
    type: 'command',
    correlation_id: o.correlation_id,
    cmd: o.cmd,
  };
  if (o.params && typeof o.params === 'object') {
    cmd.params = o.params as Record<string, unknown>;
  }
  if (o.session && typeof o.session === 'object') {
    cmd.session = o.session as CommandPayload['session'];
  }
  return cmd;
}

// ─── Client ──────────────────────────────────────────────────────────────

class CoagentServerDeviceClient {
  private socket: WebSocket | null = null;
  private currentUrl: string | null = null;
  private config: ServerDeviceConfig | null = null;
  private shouldReconnect = false;
  private reconnectAttempt = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private connectInFlight: Promise<ConnectResult> | null = null;
  /**
   * Generation counter that increments on every reset (disconnect /
   * updateConfig with a different token / unbind). All async paths
   * (pending connect promise resolvers, scheduled reconnect callbacks,
   * close-event handlers of stale sockets) capture the generation they
   * were spawned under and self-abort when `this.generation` has moved
   * on. Fixes the stale-session reconnect-loop race documented in
   * https://github.com/wanpengxie/ActOS issue: extension reconnects to a
   * revoked session_id even after the UI hands over a fresh token.
   */
  private generation = 0;
  /**
   * Pending connect-promise resolver. Held in a field (not just inside
   * the connectOnce promise closure) so a hard reset can settle it
   * synchronously as canceled — previously close-event short-circuit
   * via `if (this.socket !== socket) return` left this promise pending
   * forever, and the next `connect()` call's `if (this.connectInFlight)
   * return await it;` would hang.
   */
  private pendingSettle: ((result: ConnectResult) => void) | null = null;
  /** Test seam — overridden via `setWebSocketImpl`. */
  private wsCtor: typeof WebSocket = WebSocket;

  setWebSocketImpl(impl: typeof WebSocket): void {
    this.wsCtor = impl;
  }

  /**
   * Replace the device config. When the WS-handshake-affecting fields
   * (`sessionId` / `token` / `wsEndpoint`) change vs the previous
   * config, this is an atomic identity swap — we bump the generation
   * counter so any in-flight connect/reconnect attached to the previous
   * identity self-cancels, close the active socket, clear the reconnect
   * timer, reset the backoff attempt counter, and settle any hung
   * pending promise. Same-config updates (e.g. flipping
   * `autoReconnect`) keep the generation stable so an open WS isn't
   * needlessly cycled.
   */
  updateConfig(config: ServerDeviceConfig): void {
    const prev = this.config;
    const next: ServerDeviceConfig = { ...config };
    const identityChanged =
      !prev ||
      (prev.sessionId ?? '') !== (next.sessionId ?? '') ||
      (prev.token ?? '') !== (next.token ?? '') ||
      (prev.wsEndpoint ?? '') !== (next.wsEndpoint ?? '');
    this.config = next;
    if (identityChanged) {
      this.hardResetForIdentitySwap('updateConfig identity changed');
    }
  }

  isConnected(): boolean {
    return this.socket != null && this.socket.readyState === WebSocket.OPEN;
  }

  // ── Connect / disconnect / reconnect ──────────────────────────────────

  async connect(): Promise<ConnectResult> {
    if (!this.config) {
      return { success: false, error: ERROR_MESSAGES.DEVICE_NOT_CONFIGURED };
    }
    const wsUrl = buildDeviceBusUrl(this.config);
    if (!wsUrl) {
      return { success: false, error: ERROR_MESSAGES.DEVICE_NOT_CONFIGURED };
    }
    if (this.isConnected() && this.currentUrl === wsUrl) {
      return { success: true };
    }
    if (this.connectInFlight) {
      return await this.connectInFlight;
    }
    this.shouldReconnect = this.config.autoReconnect !== false;
    const gen = this.generation;
    this.connectInFlight = this.connectOnce(wsUrl, gen).finally(() => {
      // Only clear the in-flight slot if it still belongs to our gen.
      // hardResetForIdentitySwap already nulls it for prior gens.
      if (this.generation === gen) this.connectInFlight = null;
    });
    return await this.connectInFlight;
  }

  private async connectOnce(url: string, gen: number): Promise<ConnectResult> {
    if (this.generation !== gen) {
      return { success: false, error: 'canceled: generation changed' };
    }
    this.currentUrl = url;
    this.clearReconnectTimer();

    // Close any previous socket cleanly first.
    if (this.socket) {
      try { this.socket.close(1000, 'reconnect'); } catch { /* ignore */ }
      this.socket = null;
    }

    const safeUrl = redactToken(url);
    await saveConnectionStatus({
      connected: false,
      reconnecting: this.shouldReconnect,
      serverUrl: safeUrl,
      lastError: undefined,
    });

    if (this.generation !== gen) {
      return { success: false, error: 'canceled: generation changed' };
    }

    return await new Promise<ConnectResult>((resolve) => {
      let resolved = false;
      const settle = (result: ConnectResult) => {
        if (resolved) return;
        resolved = true;
        if (this.pendingSettle === settle) this.pendingSettle = null;
        resolve(result);
      };
      this.pendingSettle = settle;

      let socket: WebSocket;
      try {
        // R5-14: token rides in Sec-WebSocket-Protocol (subprotocol
        // slot `token.<tok>`), NOT URL query. `this.config` is
        // guaranteed non-null here because `connect()` short-circuits
        // earlier on missing config; buildDeviceBusSubprotocols also
        // returns [] when token is empty so the constructor will throw
        // and the catch below surfaces a clear error.
        const protocols = buildDeviceBusSubprotocols(this.config!);
        socket = new this.wsCtor(url, protocols);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        void saveConnectionStatus({
          connected: false,
          reconnecting: false,
          serverUrl: safeUrl,
          lastError: message,
        });
        settle({ success: false, error: message });
        return;
      }

      this.socket = socket;
      console.info('[ServerDeviceBus] connecting', { url: safeUrl });

      socket.addEventListener('open', () => {
        if (this.generation !== gen) return;
        if (this.socket !== socket) return;
        console.info('[ServerDeviceBus] WS opened', { url: safeUrl });
        this.reconnectAttempt = 0;
        void saveConnectionStatus({
          connected: true,
          reconnecting: false,
          serverUrl: safeUrl,
          lastError: undefined,
        });
        // Drain any callback frames that piled up while we were down.
        void this.drainOutbox(socket);
        settle({ success: true });
      });

      socket.addEventListener('message', (event) => {
        if (this.generation !== gen) return;
        if (this.socket !== socket) return;
        void this.onMessage(event.data);
      });

      socket.addEventListener('close', (event) => {
        // Same-generation close on the currently-active socket: normal
        // close path → settle + maybe schedule reconnect.
        // Different-generation OR replaced-socket close: stale socket
        // from a previous identity that's being shut down — drop it
        // quietly. Critically we DO NOT settle()/scheduleReconnect()
        // here, otherwise a revoked-session close lingering after
        // updateConfig() would respawn a stale-token reconnect.
        if (this.generation !== gen) return;
        if (this.socket !== socket) return;
        const reason = event.reason || 'WebSocket closed';
        console.warn('[ServerDeviceBus] WS closed', {
          url: safeUrl,
          code: event.code,
          wasClean: event.wasClean,
          reason,
        });
        this.socket = null;
        // Auth-failure close codes from server (token revoked / expired):
        // stop the reconnect loop, clear the dead URL, and surface
        // lastError. The next bindFlow's updateConfig() will reset
        // shouldReconnect anyway. Without this guard a 4401/4403 from
        // server would spam reconnects with the same dead token.
        const isAuthFail =
          event.code === COAGENT_SERVER_DEVICEBUS_PROTOCOL.WS_CLOSE_CODE_AUTH_FAILED ||
          event.code === COAGENT_SERVER_DEVICEBUS_PROTOCOL.WS_CLOSE_CODE_SESSION_REVOKED;
        if (isAuthFail) {
          console.warn('[ServerDeviceBus] auth-failure close — abandoning reconnect', {
            code: event.code,
            reason,
          });
          this.shouldReconnect = false;
          this.currentUrl = null;
          this.reconnectAttempt = 0;
        }
        // Bound the dirty-close (1006-style) reconnect loop. After N
        // consecutive non-clean closes without a successful handshake,
        // we give up rather than spam the server with a revoked token.
        // `reconnectAttempt` is reset on a successful `open`, so a
        // legitimately bouncing network does not exhaust the budget.
        const isCleanClose =
          event.code === COAGENT_SERVER_DEVICEBUS_PROTOCOL.WS_CLOSE_CODE_NORMAL ||
          event.code === COAGENT_SERVER_DEVICEBUS_PROTOCOL.WS_CLOSE_CODE_GOING_AWAY;
        const exhausted =
          !isCleanClose &&
          this.reconnectAttempt >=
            COAGENT_SERVER_DEVICEBUS_PROTOCOL.RECONNECT_MAX_ATTEMPTS_AFTER_DIRTY_CLOSE;
        if (exhausted) {
          console.warn(
            '[ServerDeviceBus] reconnect budget exhausted — abandoning loop',
            { attempts: this.reconnectAttempt, code: event.code },
          );
          this.shouldReconnect = false;
          this.currentUrl = null;
        }
        void saveConnectionStatus({
          connected: false,
          reconnecting: this.shouldReconnect,
          serverUrl: safeUrl,
          lastError: event.wasClean ? undefined : reason,
        });
        if (!resolved) settle({ success: false, error: reason });
        if (this.shouldReconnect) this.scheduleReconnect(gen);
      });

      socket.addEventListener('error', (event) => {
        if (this.generation !== gen) return;
        if (this.socket !== socket) return;
        const message =
          typeof ErrorEvent !== 'undefined' && event instanceof ErrorEvent
            ? event.message
            : (event && (event as any).message) || 'WebSocket error';
        console.error('[ServerDeviceBus] WS error', { url: safeUrl, message });
        void saveConnectionStatus({
          connected: false,
          reconnecting: this.shouldReconnect,
          serverUrl: safeUrl,
          lastError: message,
        });
        if (!resolved) settle({ success: false, error: message });
      });
    });
  }

  /**
   * Hard reset triggered when the device identity changes (different
   * sessionId / token / endpoint) or when the user explicitly unbinds.
   *
   * Atomically:
   *   - bumps the generation so all in-flight callbacks self-cancel
   *   - settles the pending connect-promise (if any) as canceled
   *   - closes the active socket
   *   - clears the reconnect timer + resets backoff
   *   - clears `currentUrl` and `connectInFlight` so subsequent
   *     `connect()` starts from a clean slate
   *   - leaves `this.config` untouched (caller is responsible for
   *     setting the new config either before or after this reset)
   */
  private hardResetForIdentitySwap(reason: string): void {
    this.generation += 1;
    console.info('[ServerDeviceBus] hard reset', {
      reason,
      generation: this.generation,
    });
    if (this.pendingSettle) {
      const s = this.pendingSettle;
      this.pendingSettle = null;
      try {
        s({ success: false, error: `canceled: ${reason}` });
      } catch { /* ignore */ }
    }
    this.connectInFlight = null;
    this.clearReconnectTimer();
    this.reconnectAttempt = 0;
    this.currentUrl = null;
    this.shouldReconnect = false;
    if (this.socket) {
      try { this.socket.close(1000, 'reset'); } catch { /* ignore */ }
      this.socket = null;
    }
  }

  disconnect(): void {
    // disconnect is the explicit "stop and forget" — full hard reset
    // so a stray reconnect timer / pending promise from a prior
    // session_id can't fire after the user unbinds. Previously this
    // only cleared shouldReconnect+timer+socket, leaving currentUrl
    // populated and connectInFlight hung; if the WS close handler ran
    // BEFORE disconnect (e.g. server revoked mid-flight), the close
    // path scheduled a reconnect that would respawn under the old
    // token.
    this.hardResetForIdentitySwap('disconnect');
    void saveConnectionStatus({ connected: false, reconnecting: false });
  }

  private scheduleReconnect(gen: number): void {
    if (this.generation !== gen) return;
    if (!this.shouldReconnect || !this.currentUrl) return;
    this.clearReconnectTimer();
    const base = COAGENT_SERVER_DEVICEBUS_PROTOCOL.RECONNECT_BASE_MS;
    const cap = COAGENT_SERVER_DEVICEBUS_PROTOCOL.RECONNECT_MAX_MS;
    const delay = Math.min(cap, base * Math.pow(2, this.reconnectAttempt));
    this.reconnectAttempt += 1;
    console.info('[ServerDeviceBus] schedule reconnect', {
      attempt: this.reconnectAttempt,
      delay,
      generation: gen,
    });
    this.reconnectTimer = setTimeout(() => {
      // Re-check generation at fire time — a hardReset between
      // schedule and fire must abort the reconnect.
      if (this.generation !== gen) return;
      void this.connect();
    }, delay);
  }

  private clearReconnectTimer(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }

  // ── Inbound frame dispatch ────────────────────────────────────────────

  private async onMessage(raw: unknown): Promise<void> {
    const text = await normalizeWsMessage(raw);
    if (!text) return;
    let parsed: unknown;
    try {
      parsed = JSON.parse(text);
    } catch (err) {
      console.warn('[ServerDeviceBus] non-JSON frame ignored', err);
      return;
    }
    if (!parsed || typeof parsed !== 'object') return;
    const frame = parsed as DeviceFrame;
    // Only `to_device` frames are addressed at us; anything else is
    // probably an echo of our own outbound — drop quietly.
    if (frame.direction !== COAGENT_SERVER_DEVICEBUS_PROTOCOL.DIRECTION_TO_DEVICE) {
      console.debug('[ServerDeviceBus] unhandled direction', frame.direction);
      return;
    }
    await this.handleToDeviceFrame(frame);
  }

  /**
   * Dispatch one DeviceFrame{direction:to_device} through the cmd
   * registry. Always emits exactly one outbound DeviceFrame{from_device}
   * — the adapter framework's correlation tracker pairs request →
   * response by `request_id` / inner `correlation_id`, so we MUST reply
   * even on bad / unknown payloads.
   */
  private async handleToDeviceFrame(frame: DeviceFrame): Promise<void> {
    const cmd = decodeCommandPayload(frame.payload);
    if (!cmd) {
      console.warn('[ServerDeviceBus] inbound frame missing command payload', {
        request_id: frame.request_id,
        correlation_id: frame.correlation_id,
      });
      await this.replyCallback(frame, {
        correlation_id: pickCorrelationId(frame, ''),
        status: 'error',
        error: {
          code: 'invalid_payload',
          message: 'inbound device_transit.send payload not a command frame',
        },
        result: null,
      });
      return;
    }

    const correlationId = cmd.correlation_id.trim();
    if (!correlationId) {
      console.warn('[ServerDeviceBus] command frame missing correlation_id', cmd);
      return;
    }

    console.info('[ServerDeviceBus] dispatch command', {
      correlationId,
      cmd: cmd.cmd,
      hasSession: Boolean(cmd.session?.cookies?.length),
      paramKeys: Object.keys(cmd.params ?? {}),
    });

    if (!isKnownCommand(cmd.cmd)) {
      await this.replyCallback(frame, {
        correlation_id: correlationId,
        status: 'error',
        error: { code: 'unknown_cmd', message: `unknown cmd: ${cmd.cmd}` },
        result: null,
      });
      return;
    }

    const handler = getCommandHandler(cmd.cmd);
    if (!handler) {
      await this.replyCallback(frame, {
        correlation_id: correlationId,
        status: 'error',
        error: {
          code: 'not_implemented',
          message: `cmd ${cmd.cmd} has no registered handler`,
        },
        result: null,
      });
      return;
    }

    try {
      const envelope = await handler(cmd.params ?? {}, cmd.session ?? null, {
        correlationId,
      });
      await this.replyCallback(frame, {
        correlation_id: correlationId,
        status: 'ok',
        result: envelope.result ?? {},
        error: null,
      });
    } catch (err: any) {
      const code = (err && (err.code as string)) || 'handler_error';
      const message = err instanceof Error ? err.message : String(err);
      console.error('[ServerDeviceBus] handler error', {
        cmd: cmd.cmd,
        correlationId,
        code,
        message,
      });
      await this.replyCallback(frame, {
        correlation_id: correlationId,
        status: 'error',
        error: { code, message },
        result: null,
      });
    }
  }

  /** Build + send a `from_device` DeviceFrame echoing the inbound IDs. */
  private async replyCallback(
    inbound: DeviceFrame,
    body: CallbackPayload,
  ): Promise<void> {
    const cfg = this.config;
    const out: DeviceFrame = {
      direction: 'from_device',
      device_session_id: inbound.device_session_id || (cfg?.sessionId ?? ''),
      channel_id: inbound.channel_id || (cfg?.channelId ?? ''),
      request_id: inbound.request_id,
      correlation_id: inbound.correlation_id || body.correlation_id,
      payload: body,
    };
    await this.sendOrEnqueue(out);
  }

  /**
   * Send one DeviceFrame on the WS, or persist to the outbox if the
   * socket is not OPEN. The daemon-side adapter framework's orphan
   * filter (correlation_id) makes duplicate sends safe — extension MAY
   * resend a frame on reconnect without breaking The One Law.
   */
  private async sendOrEnqueue(frame: DeviceFrame): Promise<void> {
    const text = JSON.stringify(frame);
    if (this.socket && this.socket.readyState === WebSocket.OPEN) {
      try {
        this.socket.send(text);
        return;
      } catch (err) {
        console.warn('[ServerDeviceBus] socket.send failed; enqueuing', err);
      }
    }
    await this.enqueueOutbox(frame);
  }

  /**
   * Public API kept compatible with legacy `coagent-device.ts` so the
   * existing `recoverPublishWaitStates` helper can drive either client
   * via the same signature.
   *
   * Recovery path (SW evict → cold start → state replay): the inbound
   * DeviceFrame metadata is no longer in memory, so the v4 client
   * synthesises the outbound frame from `ConnectionConfig` (session_id +
   * channel_id) + the persisted correlationId — the daemon's framework
   * routes the recovered callback by `payload.correlation_id` (= inbound
   * envelope.id), and the outer `correlation_id` slot also carries the
   * same value as the device-wire correlation key.
   */
  async postCallback(
    correlationId: string,
    payload: Omit<CallbackBody, 'correlation_id'>,
  ): Promise<void> {
    const cfg = this.config;
    if (!cfg) {
      console.warn('[ServerDeviceBus] postCallback: no config');
      return;
    }
    const cb: CallbackPayload = {
      correlation_id: correlationId,
      status: payload.status,
      result: payload.result ?? null,
      error: payload.error ?? null,
    };
    const frame: DeviceFrame = {
      direction: 'from_device',
      device_session_id: cfg.sessionId,
      channel_id: cfg.channelId,
      request_id: correlationId,
      correlation_id: correlationId,
      payload: cb,
    };
    await this.sendOrEnqueue(frame);
  }

  // ── Outbox primitives ─────────────────────────────────────────────────

  private async enqueueOutbox(frame: DeviceFrame): Promise<void> {
    const existing = await readOutbox();
    const now = Date.now();
    const corr = String(frame.correlation_id ?? '').trim();
    const filtered = corr
      ? existing.filter((e) => String(e.frame.correlation_id ?? '') !== corr)
      : existing.slice();
    filtered.push({ frame, enqueued_at: now, last_attempt_at: now });
    await writeOutbox(filtered);
    console.warn('[ServerDeviceBus] callback enqueued (WS down)', {
      correlation_id: corr,
      outbox_size: filtered.length,
    });
  }

  /**
   * Drain the outbox over the WS that just opened. Sends each callback
   * frame individually so the daemon can ack / reject per-correlation.
   * GCs entries older than OUTBOX_MAX_AGE_MS before sending — bounds
   * unbounded growth when the daemon never replies.
   */
  private async drainOutbox(socket: WebSocket): Promise<void> {
    const pending = await readOutbox();
    if (pending.length === 0) return;

    const now = Date.now();
    const live: OutboxEntry[] = [];
    for (const entry of pending) {
      const lastAttempt = entry.last_attempt_at ?? entry.enqueued_at;
      if (Number.isFinite(lastAttempt) && now - lastAttempt > OUTBOX_MAX_AGE_MS) {
        console.warn('[ServerDeviceBus] outbox.dropped', {
          correlation_id: entry.frame.correlation_id,
          age_ms: now - lastAttempt,
          reason: 'gc_max_age_exceeded',
        });
        continue;
      }
      live.push(entry);
    }

    if (live.length === 0) {
      if (live.length !== pending.length) await writeOutbox(live);
      return;
    }
    if (socket.readyState !== WebSocket.OPEN) {
      if (live.length !== pending.length) await writeOutbox(live);
      return;
    }

    // Sent successfully → clear from outbox. Failed → keep & try next
    // reconnect. We refresh last_attempt_at on the failure path so GC
    // measures from the most recent attempt, not the original enqueue.
    const survivors: OutboxEntry[] = [];
    let sent = 0;
    for (const entry of live) {
      const text = JSON.stringify(entry.frame);
      try {
        socket.send(text);
        sent += 1;
      } catch (err) {
        console.warn('[ServerDeviceBus] drain send failed; keeping entry', {
          correlation_id: entry.frame.correlation_id,
          err: err instanceof Error ? err.message : String(err),
        });
        survivors.push({ ...entry, last_attempt_at: now });
      }
    }
    await writeOutbox(survivors);
    if (sent > 0) {
      console.info('[ServerDeviceBus] drained outbox callbacks', {
        sent,
        kept: survivors.length,
      });
    }
  }
}

async function normalizeWsMessage(raw: unknown): Promise<string | null> {
  if (typeof raw === 'string') return raw;
  if (raw instanceof ArrayBuffer) return new TextDecoder().decode(raw);
  if (typeof Blob !== 'undefined' && raw instanceof Blob) return await raw.text();
  return null;
}

function pickCorrelationId(frame: DeviceFrame, fallback: string): string {
  if (typeof frame.correlation_id === 'string' && frame.correlation_id.trim()) {
    return frame.correlation_id.trim();
  }
  if (typeof frame.request_id === 'string' && frame.request_id.trim()) {
    return frame.request_id.trim();
  }
  return fallback;
}

export const coagentServerDeviceClient = new CoagentServerDeviceClient();
/** Test-only export — lets vitest exercise dispatch + outbox in isolation. */
export { CoagentServerDeviceClient as CoagentServerDeviceClientForTest };
