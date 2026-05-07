// services/coagent-device.ts — coagent daemon device WS client。
//
// 协议（spec §4.2 / §4.3 + M1.1 Fix-T3）：
//   - WS:  ws://{daemon-host}/device/{deviceId}?key={device_api_key}
//   - 入站 frame：{type:"command", correlation_id, cmd, params, session}
//   - 可选回 ack frame：{type:"ack", correlation_id}
//   - 完成回调（HTTP）：POST {daemonHttpBase}/api/device/{deviceId}/callback
//        Authorization: Bearer {device_api_key}
//        body: {correlation_id, status:"ok"|"error", result|null, error|null}
//
// 行为：
//   - 长连 WS，指数重连 1/2/4/8/16/30s 上限
//   - 收 command frame → registerCommandHandler 路由 → 完成发 callback
//   - command handler 异常 → callback `{status:"error", error:{code,message}}`
//   - daemon 心跳：daemon 端发 ping，浏览器 WebSocket 自动 pong（不需手动）
//
// 韧性（M1.1 Fix-T3）：
//   - postCallback 每次 fetch 加 AbortController 10s 超时
//   - 失败指数退避 3 次（1s / 2s / 4s）
//   - 仍失败 → 序列化进 chrome.storage.local 暂存表 pendingCallbacks
//   - WS 重连 open 时 drain outbox 并通过 {type:"callback_replay", payloads:[...]} frame 重发
//
// 此模块不直接连 chrome.cookies；cookie sync 由 tools/sync-cookies.ts 单独处理。

import {
  COAGENT_DEVICE_PROTOCOL,
  ERROR_MESSAGES,
} from 'xiaohongshu-mcp-shared';
import type { ConnectionConfig } from '../connection-state';
import { saveConnectionStatus } from '../connection-state';
import {
  type CommandFrame,
  type CommandResultEnvelope,
  getCommandHandler,
  isKnownCommand,
} from './cmd-handlers';

export interface ConnectResult {
  success: boolean;
  error?: string;
}

export interface CallbackBody {
  correlation_id: string;
  status: 'ok' | 'error';
  result: Record<string, unknown> | null;
  error: { code: string; message: string } | null;
}

/** Storage entry shape for the pending-callbacks outbox. */
export interface PendingCallbackEntry {
  body: CallbackBody;
  /** Timestamp of the original failure (epoch ms); helpful for log forensics. */
  enqueued_at: number;
}

/** Minimal chrome.storage.local typing — keeps unit tests / mocks simple. */
interface StorageLike {
  get(key: string): Promise<Record<string, unknown>>;
  set(items: Record<string, unknown>): Promise<void>;
}

function getStorage(): StorageLike | null {
  // `chrome` is only present in the SW context. Tests inject a mock via
  // (globalThis as any).chrome.
  const c: any = (globalThis as any).chrome;
  if (!c?.storage?.local) return null;
  return c.storage.local as StorageLike;
}

/** Read outbox; return [] when storage is unavailable or value is malformed. */
export async function readPendingCallbacks(): Promise<PendingCallbackEntry[]> {
  const storage = getStorage();
  if (!storage) return [];
  try {
    const stored = await storage.get(COAGENT_DEVICE_PROTOCOL.CALLBACK_OUTBOX_STORAGE_KEY);
    const raw = stored[COAGENT_DEVICE_PROTOCOL.CALLBACK_OUTBOX_STORAGE_KEY];
    if (!Array.isArray(raw)) return [];
    return raw.filter(isPendingCallbackEntry);
  } catch (err) {
    console.warn('[CoagentDevice] readPendingCallbacks failed', err);
    return [];
  }
}

export async function writePendingCallbacks(entries: PendingCallbackEntry[]): Promise<void> {
  const storage = getStorage();
  if (!storage) return;
  try {
    const trimmed = entries.slice(-COAGENT_DEVICE_PROTOCOL.CALLBACK_OUTBOX_MAX_SIZE);
    await storage.set({
      [COAGENT_DEVICE_PROTOCOL.CALLBACK_OUTBOX_STORAGE_KEY]: trimmed,
    });
  } catch (err) {
    console.warn('[CoagentDevice] writePendingCallbacks failed', err);
  }
}

function isPendingCallbackEntry(value: unknown): value is PendingCallbackEntry {
  if (!value || typeof value !== 'object') return false;
  const v = value as Record<string, unknown>;
  if (!v.body || typeof v.body !== 'object') return false;
  const body = v.body as Record<string, unknown>;
  return typeof body.correlation_id === 'string' && (body.status === 'ok' || body.status === 'error');
}

class CoagentDeviceClient {
  private socket: WebSocket | null = null;
  private currentUrl: string | null = null;
  private config: ConnectionConfig | null = null;
  private shouldReconnect = false;
  private reconnectAttempt = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private connectInFlight: Promise<ConnectResult> | null = null;
  /** Test seam — overridden in unit tests via setFetchImpl. */
  private fetchImpl: typeof fetch = (...args) => fetch(...args);

  /** Test-only: replace the fetch implementation used by postCallback. */
  setFetchImpl(impl: typeof fetch): void {
    this.fetchImpl = impl;
  }

  /** Test-only: replace the WebSocket constructor used by connect(). */
  private wsCtor: typeof WebSocket = WebSocket;
  setWebSocketImpl(impl: typeof WebSocket): void {
    this.wsCtor = impl;
  }

  updateConfig(config: ConnectionConfig): void {
    this.config = { ...config };
  }

  isConnected(): boolean {
    return this.socket != null && this.socket.readyState === WebSocket.OPEN;
  }

  /** Resolve daemon WS URL from config. URL must end with `/device/{deviceId}` and carry `?key=`. */
  private resolveWsUrl(): string {
    const cfg = this.config;
    if (!cfg) return '';
    let url = (cfg.serverUrl ?? '').trim();
    if (!url) return '';
    // If user provided only `ws://host:port` (no /device/...), auto-append.
    const deviceId = (cfg.deviceId ?? '').trim();
    const apiKey = (cfg.apiKey ?? '').trim();
    if (!deviceId || !apiKey) return '';
    try {
      const u = new URL(url);
      if (!/\/device\/[^/]+/.test(u.pathname)) {
        u.pathname = `/device/${encodeURIComponent(deviceId)}`;
      }
      // Always set/refresh key.
      u.searchParams.set('key', apiKey);
      return u.toString();
    } catch {
      // serverUrl 不是合法 URL：让 connect() 直接失败。
      return '';
    }
  }

  async connect(): Promise<ConnectResult> {
    if (!this.config) {
      return { success: false, error: ERROR_MESSAGES.DEVICE_NOT_CONFIGURED };
    }
    const wsUrl = this.resolveWsUrl();
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
    this.connectInFlight = this.connectOnce(wsUrl).finally(() => {
      this.connectInFlight = null;
    });
    return await this.connectInFlight;
  }

  private async connectOnce(url: string): Promise<ConnectResult> {
    this.currentUrl = url;
    this.clearReconnectTimer();

    // Close any previous socket cleanly first.
    if (this.socket) {
      try { this.socket.close(1000, 'reconnect'); } catch { /* ignore */ }
      this.socket = null;
    }

    await saveConnectionStatus({
      connected: false,
      reconnecting: this.shouldReconnect,
      serverUrl: url,
      lastError: undefined,
    });

    return await new Promise<ConnectResult>((resolve) => {
      let resolved = false;
      const settle = (result: ConnectResult) => {
        if (resolved) return;
        resolved = true;
        resolve(result);
      };

      let socket: WebSocket;
      try {
        socket = new this.wsCtor(url);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        void saveConnectionStatus({
          connected: false,
          reconnecting: false,
          serverUrl: url,
          lastError: message,
        });
        settle({ success: false, error: message });
        return;
      }

      this.socket = socket;
      console.info('[CoagentDevice] connecting', { url });

      socket.addEventListener('open', () => {
        if (this.socket !== socket) return;
        console.info('[CoagentDevice] WS opened', { url });
        this.reconnectAttempt = 0;
        void saveConnectionStatus({
          connected: true,
          reconnecting: false,
          serverUrl: url,
          lastError: undefined,
        });
        // M1.1 Fix-T3: drain pending-callbacks outbox via callback_replay frame.
        void this.drainPendingCallbacks(socket);
        settle({ success: true });
      });

      socket.addEventListener('message', (event) => {
        if (this.socket !== socket) return;
        void this.onMessage(event.data);
      });

      socket.addEventListener('close', (event) => {
        if (this.socket !== socket) return;
        const reason = event.reason || 'WebSocket closed';
        console.warn('[CoagentDevice] WS closed', {
          url,
          code: event.code,
          wasClean: event.wasClean,
          reason,
        });
        this.socket = null;
        void saveConnectionStatus({
          connected: false,
          reconnecting: this.shouldReconnect,
          serverUrl: url,
          lastError: event.wasClean ? undefined : reason,
        });
        if (!resolved) settle({ success: false, error: reason });
        if (this.shouldReconnect) this.scheduleReconnect();
      });

      socket.addEventListener('error', (event) => {
        if (this.socket !== socket) return;
        const message = event instanceof ErrorEvent ? event.message : 'WebSocket error';
        console.error('[CoagentDevice] WS error', { url, message });
        void saveConnectionStatus({
          connected: false,
          reconnecting: this.shouldReconnect,
          serverUrl: url,
          lastError: message,
        });
      });
    });
  }

  disconnect(): void {
    this.shouldReconnect = false;
    this.clearReconnectTimer();
    if (this.socket) {
      try { this.socket.close(1000, 'disconnect'); } catch { /* ignore */ }
      this.socket = null;
    }
    void saveConnectionStatus({ connected: false, reconnecting: false });
  }

  private scheduleReconnect(): void {
    if (!this.shouldReconnect || !this.currentUrl) return;
    this.clearReconnectTimer();
    const base = COAGENT_DEVICE_PROTOCOL.RECONNECT_BASE_MS;
    const cap = COAGENT_DEVICE_PROTOCOL.RECONNECT_MAX_MS;
    const delay = Math.min(cap, base * Math.pow(2, this.reconnectAttempt));
    this.reconnectAttempt += 1;
    console.info('[CoagentDevice] schedule reconnect', { attempt: this.reconnectAttempt, delay });
    this.reconnectTimer = setTimeout(() => {
      void this.connect();
    }, delay);
  }

  private clearReconnectTimer(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }

  // ─── Inbound frame dispatch ────────────────────────────────────────────────

  private async onMessage(raw: unknown): Promise<void> {
    const text = await normalizeWsMessage(raw);
    if (!text) return;
    let frame: any;
    try {
      frame = JSON.parse(text);
    } catch (err) {
      console.warn('[CoagentDevice] non-JSON frame ignored', err);
      return;
    }
    if (!frame || typeof frame !== 'object') return;
    if (frame.type !== COAGENT_DEVICE_PROTOCOL.COMMAND_FRAME_TYPE) {
      console.debug('[CoagentDevice] unhandled frame type', frame.type);
      return;
    }
    await this.handleCommand(frame as CommandFrame);
  }

  private async handleCommand(frame: CommandFrame): Promise<void> {
    const correlationId = String(frame.correlation_id ?? '').trim();
    const cmd = String(frame.cmd ?? '').trim();
    if (!correlationId || !cmd) {
      console.warn('[CoagentDevice] command frame missing correlation_id or cmd', frame);
      return;
    }

    // Optional ack frame back to daemon (heartbeat-style; daemon side log only).
    this.sendAck(correlationId);

    const params = (frame.params ?? {}) as Record<string, unknown>;
    const session = frame.session ?? null;

    console.info('[CoagentDevice] dispatch command', {
      correlationId,
      cmd,
      hasSession: Boolean(session?.cookies?.length),
      paramKeys: Object.keys(params),
    });

    if (!isKnownCommand(cmd)) {
      await this.postCallback(correlationId, {
        status: 'error',
        error: { code: 'unknown_cmd', message: `unknown cmd: ${cmd}` },
        result: null,
      });
      return;
    }

    const handler = getCommandHandler(cmd);
    if (!handler) {
      await this.postCallback(correlationId, {
        status: 'error',
        error: { code: 'not_implemented', message: `cmd ${cmd} has no registered handler` },
        result: null,
      });
      return;
    }

    try {
      const envelope: CommandResultEnvelope = await handler(params, session);
      await this.postCallback(correlationId, {
        status: 'ok',
        result: envelope.result ?? {},
        error: null,
      });
    } catch (err: any) {
      const code = (err && (err.code as string)) || 'handler_error';
      const message = err instanceof Error ? err.message : String(err);
      console.error('[CoagentDevice] handler error', { cmd, correlationId, code, message });
      await this.postCallback(correlationId, {
        status: 'error',
        error: { code, message },
        result: null,
      });
    }
  }

  private sendAck(correlationId: string): void {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) return;
    try {
      this.socket.send(
        JSON.stringify({
          type: COAGENT_DEVICE_PROTOCOL.ACK_FRAME_TYPE,
          correlation_id: correlationId,
        })
      );
    } catch (err) {
      console.warn('[CoagentDevice] sendAck failed', err);
    }
  }

  // ─── Callback HTTP ────────────────────────────────────────────────────────

  /**
   * POST a callback to the daemon. M1.1 Fix-T3:
   *   - per-attempt AbortController 10s timeout
   *   - exponential backoff 1s / 2s / 4s, 3 retries on transport / 5xx errors
   *   - 4xx (except 429) is terminal — body shape is wrong, retrying won't help
   *   - if all retries fail, push entry into chrome.storage.local outbox to be
   *     replayed via WS callback_replay frame on next reconnect
   */
  async postCallback(
    correlationId: string,
    payload: Omit<CallbackBody, 'correlation_id'>
  ): Promise<void> {
    const cfg = this.config;
    if (!cfg) {
      console.warn('[CoagentDevice] postCallback: no config');
      return;
    }
    const httpBase = (cfg.daemonHttpBase ?? '').trim() || deriveHttpBaseFromWsUrl(cfg.serverUrl);
    const deviceId = (cfg.deviceId ?? '').trim();
    const apiKey = (cfg.apiKey ?? '').trim();
    if (!httpBase || !deviceId || !apiKey) {
      console.warn('[CoagentDevice] postCallback: incomplete config', {
        hasHttpBase: Boolean(httpBase),
        hasDeviceId: Boolean(deviceId),
        hasApiKey: Boolean(apiKey),
      });
      return;
    }

    const url =
      `${stripTrailingSlash(httpBase)}` +
      `${COAGENT_DEVICE_PROTOCOL.CALLBACK_PATH_PREFIX}` +
      `${encodeURIComponent(deviceId)}` +
      `${COAGENT_DEVICE_PROTOCOL.CALLBACK_PATH_SUFFIX_CALLBACK}`;

    const body: CallbackBody = {
      correlation_id: correlationId,
      ...payload,
    };

    const result = await this.sendCallbackWithRetry(url, apiKey, body);
    if (result === 'ok' || result === 'terminal') return;
    // 'exhausted' — transport / 5xx kept failing; queue for WS replay.
    console.error('[CoagentDevice] callback exhausted retries — queuing for WS replay', {
      correlationId,
    });
    await this.enqueuePendingCallback(body);
  }

  /**
   * Inner retry loop.
   *  - 'ok'        → first 2xx response (no further action).
   *  - 'terminal'  → 4xx (non-429) — daemon refused this body; replaying won't
   *                  help, drop the callback (caller must NOT queue).
   *  - 'exhausted' → transport / 5xx / 429 / abort timed out across all retries.
   *
   * Total attempts: `1 + CALLBACK_RETRY_BACKOFF_MS_LIST.length`.
   */
  private async sendCallbackWithRetry(
    url: string,
    apiKey: string,
    body: CallbackBody,
  ): Promise<'ok' | 'terminal' | 'exhausted'> {
    const backoff = COAGENT_DEVICE_PROTOCOL.CALLBACK_RETRY_BACKOFF_MS_LIST;
    const totalAttempts = 1 + backoff.length;
    for (let attempt = 0; attempt < totalAttempts; attempt += 1) {
      const outcome = await this.attemptCallbackOnce(url, apiKey, body);
      if (outcome === 'ok') return 'ok';
      if (outcome === 'terminal') return 'terminal';
      // 'retry' branch — schedule next attempt unless we've exhausted.
      const nextDelay = backoff[attempt];
      if (nextDelay == null) break;
      console.warn('[CoagentDevice] callback attempt failed, retrying', {
        attempt: attempt + 1,
        nextDelayMs: nextDelay,
        correlationId: body.correlation_id,
      });
      await sleep(nextDelay);
    }
    return 'exhausted';
  }

  /** One HTTP attempt. Returns 'ok' / 'retry' / 'terminal'. */
  private async attemptCallbackOnce(
    url: string,
    apiKey: string,
    body: CallbackBody,
  ): Promise<'ok' | 'retry' | 'terminal'> {
    const controller = new AbortController();
    const timer = setTimeout(
      () => controller.abort(),
      COAGENT_DEVICE_PROTOCOL.CALLBACK_RETRY_TIMEOUT_MS,
    );
    try {
      const resp = await this.fetchImpl(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${apiKey}`,
        },
        body: JSON.stringify(body),
        signal: controller.signal,
      });
      if (resp.ok) return 'ok';
      // 429 / 5xx → retry; everything else (4xx auth/validation) → terminal.
      if (resp.status === 429 || resp.status >= 500) {
        const text = await resp.text().catch(() => '');
        console.warn('[CoagentDevice] callback non-2xx (retryable)', {
          status: resp.status,
          text,
          correlationId: body.correlation_id,
        });
        return 'retry';
      }
      const text = await resp.text().catch(() => '');
      console.error('[CoagentDevice] callback non-2xx (terminal)', {
        status: resp.status,
        text,
        correlationId: body.correlation_id,
      });
      return 'terminal';
    } catch (err: any) {
      // AbortError (timeout) and network errors are retryable.
      const message = err instanceof Error ? err.message : String(err);
      console.warn('[CoagentDevice] callback fetch failed (will retry)', {
        correlationId: body.correlation_id,
        error: message,
      });
      return 'retry';
    } finally {
      clearTimeout(timer);
    }
  }

  /**
   * Append a callback body to the chrome.storage.local outbox. Bounded by
   * COAGENT_DEVICE_PROTOCOL.CALLBACK_OUTBOX_MAX_SIZE — older entries are
   * dropped FIFO-style when the cap is hit.
   */
  private async enqueuePendingCallback(body: CallbackBody): Promise<void> {
    const existing = await readPendingCallbacks();
    const entry: PendingCallbackEntry = { body, enqueued_at: Date.now() };
    // Replace any earlier entry for the same correlation_id (extension may
    // re-attempt the same callback after a SW restart) — last-writer-wins.
    const filtered = existing.filter((e) => e.body.correlation_id !== body.correlation_id);
    filtered.push(entry);
    await writePendingCallbacks(filtered);
  }

  /**
   * Drain the outbox by sending one `callback_replay` WS frame containing all
   * pending payloads. On send failure (socket closed mid-call etc.) the entries
   * remain in storage so the next reconnect retries.
   */
  private async drainPendingCallbacks(socket: WebSocket): Promise<void> {
    const pending = await readPendingCallbacks();
    if (pending.length === 0) return;
    if (socket.readyState !== WebSocket.OPEN) return;
    const payloads = pending.map((entry) => entry.body);
    const frame = JSON.stringify({
      type: COAGENT_DEVICE_PROTOCOL.CALLBACK_REPLAY_FRAME_TYPE,
      payloads,
    });
    try {
      socket.send(frame);
      console.info('[CoagentDevice] drained pending callbacks via WS replay', {
        count: payloads.length,
      });
      await writePendingCallbacks([]);
    } catch (err) {
      console.warn('[CoagentDevice] callback_replay send failed; keeping outbox', err);
    }
  }
}

async function normalizeWsMessage(raw: unknown): Promise<string | null> {
  if (typeof raw === 'string') return raw;
  if (raw instanceof ArrayBuffer) return new TextDecoder().decode(raw);
  if (typeof Blob !== 'undefined' && raw instanceof Blob) return await raw.text();
  return null;
}

function stripTrailingSlash(s: string): string {
  return s.endsWith('/') ? s.slice(0, -1) : s;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Fallback: ws://host:port/device/{id}?key=... → http://host:port. */
export function deriveHttpBaseFromWsUrl(wsUrl: string): string {
  if (!wsUrl) return '';
  try {
    const u = new URL(wsUrl);
    const proto = u.protocol === 'wss:' ? 'https:' : 'http:';
    return `${proto}//${u.host}`;
  } catch {
    return '';
  }
}

export const coagentDeviceClient = new CoagentDeviceClient();
/** Test-only export — lets vitest exercise retry + outbox without singleton state. */
export { CoagentDeviceClient as CoagentDeviceClientForTest };
