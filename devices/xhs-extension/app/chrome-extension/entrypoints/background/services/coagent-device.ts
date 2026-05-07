// services/coagent-device.ts — coagent daemon device WS client。
//
// 协议（spec §4.2 / §4.3）：
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

interface CallbackBody {
  correlation_id: string;
  status: 'ok' | 'error';
  result: Record<string, unknown> | null;
  error: { code: string; message: string } | null;
}

class CoagentDeviceClient {
  private socket: WebSocket | null = null;
  private currentUrl: string | null = null;
  private config: ConnectionConfig | null = null;
  private shouldReconnect = false;
  private reconnectAttempt = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private connectInFlight: Promise<ConnectResult> | null = null;

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
        socket = new WebSocket(url);
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

  private async postCallback(
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

    try {
      const resp = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${apiKey}`,
        },
        body: JSON.stringify(body),
      });
      if (!resp.ok) {
        const text = await resp.text().catch(() => '');
        console.error('[CoagentDevice] callback non-2xx', {
          status: resp.status,
          text,
          correlationId,
        });
      }
    } catch (err) {
      console.error('[CoagentDevice] callback fetch failed', {
        correlationId,
        error: err instanceof Error ? err.message : String(err),
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

function stripTrailingSlash(s: string): string {
  return s.endsWith('/') ? s.slice(0, -1) : s;
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
