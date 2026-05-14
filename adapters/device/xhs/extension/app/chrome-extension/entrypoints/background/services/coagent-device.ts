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
// R3-T3 (FX5) — callback_replay_ack 闭环：
//   - drain 后 *不清* outbox；只刷新 last_attempt_at
//   - 收到 daemon `{type:'callback_replay_ack', accepted, rejected}` frame 才
//     按 correlation_id 删 outbox entry
//   - rejected 项也删除（最简实现）+ console.warn `device.callback.outbox.dropped`
//   - 兜底：超过 7 天未 ack 的 entry 在下次 drain 时 GC，防 daemon 永不 ack 时无限增长
//
// 此模块不直接连 chrome.cookies；cookie sync 由 tools/sync-cookies.ts 单独处理。

import {
  COAGENT_DEVICE_PROTOCOL,
  ERROR_MESSAGES,
} from 'coagent-xhs-shared';
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
  /**
   * R3-T3 (FX5) — epoch ms of the most recent `callback_replay` send attempt.
   * Used by the GC pass to drop entries the daemon has never ack'd within
   * `CALLBACK_OUTBOX_MAX_AGE_MS`. Optional so older stored entries (pre-FX5)
   * remain readable; readers fall back to `enqueued_at`.
   */
  last_attempt_at?: number;
}

/** Shape of the daemon → extension ack frame for a callback_replay batch. */
interface CallbackReplayAckFrame {
  type: 'callback_replay_ack';
  accepted?: unknown;
  rejected?: unknown;
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

/**
 * M1.1 Fix-T4 §4: 把 WS URL 中的 `?key=...` / `&key=...` 替换为 `key=***`。
 *
 * WebSocket 构造仍需要原始 URL 走握手鉴权；脱敏版本只用于：
 *   - `saveConnectionStatus({serverUrl})` 持久化进 chrome.storage.local
 *   - `console.info / warn / error` 日志输出
 *
 * 不依赖 URL 解析失败时回退原串（保守处理：解析不通过仍然 regex redact）。
 */
export function redactKey(url: string): string {
  if (!url) return url;
  // 同时覆盖 `?key=…` / `&key=…` 与 URL 编码后的形态。
  return url.replace(/([?&])key=[^&#]*/gi, '$1key=***');
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

    // M1.1 Fix-T4 §4: 持久化 / 日志只用脱敏 URL；socket 构造仍用带 key 的原始 url。
    const safeUrl = redactKey(url);

    await saveConnectionStatus({
      connected: false,
      reconnecting: this.shouldReconnect,
      serverUrl: safeUrl,
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
          serverUrl: safeUrl,
          lastError: message,
        });
        settle({ success: false, error: message });
        return;
      }

      this.socket = socket;
      console.info('[CoagentDevice] connecting', { url: safeUrl });

      socket.addEventListener('open', () => {
        if (this.socket !== socket) return;
        console.info('[CoagentDevice] WS opened', { url: safeUrl });
        this.reconnectAttempt = 0;
        void saveConnectionStatus({
          connected: true,
          reconnecting: false,
          serverUrl: safeUrl,
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
          url: safeUrl,
          code: event.code,
          wasClean: event.wasClean,
          reason,
        });
        this.socket = null;
        void saveConnectionStatus({
          connected: false,
          reconnecting: this.shouldReconnect,
          serverUrl: safeUrl,
          lastError: event.wasClean ? undefined : reason,
        });
        if (!resolved) settle({ success: false, error: reason });
        if (this.shouldReconnect) this.scheduleReconnect();
      });

      socket.addEventListener('error', (event) => {
        if (this.socket !== socket) return;
        // ErrorEvent 仅在浏览器环境下存在；vitest 跑在 node 环境时直接读 message 字段。
        const message =
          typeof ErrorEvent !== 'undefined' && event instanceof ErrorEvent
            ? event.message
            : (event && (event as any).message) || 'WebSocket error';
        console.error('[CoagentDevice] WS error', { url: safeUrl, message });
        void saveConnectionStatus({
          connected: false,
          reconnecting: this.shouldReconnect,
          serverUrl: safeUrl,
          lastError: message,
        });
        // Fix-T4 §7 covers connectOnce open+error race: error 之前未 settle 时
        // 应明确告知调用方失败，避免长 pending。
        if (!resolved) settle({ success: false, error: message });
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
    if (frame.type === COAGENT_DEVICE_PROTOCOL.CALLBACK_REPLAY_ACK_FRAME_TYPE) {
      // R3-T3 (FX5) — daemon confirmed which replay entries it accepted /
      // rejected; clear those outbox rows (and only those).
      await this.handleCallbackReplayAck(frame as CallbackReplayAckFrame);
      return;
    }
    if (frame.type !== COAGENT_DEVICE_PROTOCOL.COMMAND_FRAME_TYPE) {
      console.debug('[CoagentDevice] unhandled frame type', frame.type);
      return;
    }
    await this.handleCommand(frame as CommandFrame);
  }

  /**
   * R3-T3 (FX5) — handle daemon's `callback_replay_ack` reply. accepted +
   * rejected together form the "daemon has decided this entry's fate" set
   * — both are removed from the outbox. rejected entries also get a
   * `device.callback.outbox.dropped` log line carrying the daemon's
   * `{code, message}` for forensics.
   */
  private async handleCallbackReplayAck(frame: CallbackReplayAckFrame): Promise<void> {
    const accepted = Array.isArray(frame.accepted)
      ? frame.accepted.filter((v): v is string => typeof v === 'string' && v.length > 0)
      : [];
    const rejected = Array.isArray(frame.rejected)
      ? frame.rejected.filter(
          (v): v is { correlation_id: string; code?: string; message?: string } =>
            !!v && typeof v === 'object' && typeof (v as any).correlation_id === 'string',
        )
      : [];
    if (accepted.length === 0 && rejected.length === 0) return;

    // Surface rejected entries so an operator can see them after the fact.
    for (const r of rejected) {
      console.warn('[CoagentDevice] device.callback.outbox.dropped', {
        correlation_id: r.correlation_id,
        code: r.code ?? 'unknown',
        message: r.message ?? '',
      });
    }

    const toRemove = new Set<string>([
      ...accepted,
      ...rejected.map((r) => r.correlation_id).filter((id) => typeof id === 'string' && id.length > 0),
    ]);
    if (toRemove.size === 0) return;
    const existing = await readPendingCallbacks();
    const survivors = existing.filter((entry) => !toRemove.has(entry.body.correlation_id));
    if (survivors.length !== existing.length) {
      await writePendingCallbacks(survivors);
      console.info('[CoagentDevice] callback_replay_ack consumed outbox entries', {
        accepted: accepted.length,
        rejected: rejected.length,
        outbox_remaining: survivors.length,
      });
    }
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
      // R3-T4 FX9：把 correlationId 作为 context 传给 handler，PublishContentTool
      // 据此在 chrome.storage.local 持久化 publish-wait state，使 SW evict 后
      // background recoverPublishWaitStates 能恢复发送 callback。
      const envelope: CommandResultEnvelope = await handler(params, session, {
        correlationId,
      });
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
    const now = Date.now();
    const entry: PendingCallbackEntry = { body, enqueued_at: now, last_attempt_at: now };
    // Replace any earlier entry for the same correlation_id (extension may
    // re-attempt the same callback after a SW restart) — last-writer-wins.
    const filtered = existing.filter((e) => e.body.correlation_id !== body.correlation_id);
    filtered.push(entry);
    await writePendingCallbacks(filtered);
  }

  /**
   * Drain the outbox by sending one `callback_replay` WS frame containing all
   * pending payloads.
   *
   * R3-T3 (FX5): outbox is **not** cleared after a successful socket.send —
   * `socket.send` only writes into the browser's local WS buffer and does not
   * imply the daemon processed the entries. The daemon will respond with a
   * `callback_replay_ack` frame; `handleCallbackReplayAck` is the only place
   * that removes entries from storage. We do however refresh `last_attempt_at`
   * so the GC pass below has a stable signal.
   *
   * GC: entries whose newest attempt timestamp is older than
   * `CALLBACK_OUTBOX_MAX_AGE_MS` are dropped here (with a `device.callback.
   * outbox.dropped` log line) — bounds unbounded outbox growth when the daemon
   * never ack's (e.g. wedged service / lost device id).
   */
  private async drainPendingCallbacks(socket: WebSocket): Promise<void> {
    const pending = await readPendingCallbacks();
    if (pending.length === 0) return;

    const now = Date.now();
    const maxAge = COAGENT_DEVICE_PROTOCOL.CALLBACK_OUTBOX_MAX_AGE_MS;
    const live: PendingCallbackEntry[] = [];
    for (const entry of pending) {
      const lastAttempt = entry.last_attempt_at ?? entry.enqueued_at;
      if (Number.isFinite(lastAttempt) && now - lastAttempt > maxAge) {
        console.warn('[CoagentDevice] device.callback.outbox.dropped', {
          correlation_id: entry.body.correlation_id,
          age_ms: now - lastAttempt,
          reason: 'gc_max_age_exceeded',
        });
        continue;
      }
      live.push(entry);
    }

    if (live.length === 0) {
      // All entries aged out — persist the empty outbox before short-circuiting.
      if (live.length !== pending.length) await writePendingCallbacks(live);
      return;
    }

    if (socket.readyState !== WebSocket.OPEN) {
      // Connection raced shut after open fired; persist the GC outcome but
      // skip the send — next reconnect's drain will pick up.
      if (live.length !== pending.length) await writePendingCallbacks(live);
      return;
    }

    const payloads = live.map((entry) => entry.body);
    const frame = JSON.stringify({
      type: COAGENT_DEVICE_PROTOCOL.CALLBACK_REPLAY_FRAME_TYPE,
      payloads,
    });
    try {
      socket.send(frame);
      console.info('[CoagentDevice] drained pending callbacks via WS replay', {
        count: payloads.length,
      });
      // Refresh last_attempt_at so the next GC tick measures from this send,
      // not from the original failed POST. Crucially we *do not* clear the
      // outbox — `handleCallbackReplayAck` is responsible for removal.
      const stamped = live.map((entry) => ({ ...entry, last_attempt_at: now }));
      await writePendingCallbacks(stamped);
    } catch (err) {
      console.warn('[CoagentDevice] callback_replay send failed; keeping outbox', err);
      // Still persist any GC pruning we did before the send attempt.
      if (live.length !== pending.length) await writePendingCallbacks(live);
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
