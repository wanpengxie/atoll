import { SocketMessage, SocketMessageType } from 'xiaohongshu-mcp-shared';
import type { DumasAsyncEvent } from 'xiaohongshu-mcp-shared';
import { saveConnectionStatus, getConnectionConfig, ConnectionConfig } from './connection-state';

interface ConnectResult {
  success: boolean;
  error?: string;
}

type ReconnectTimer = ReturnType<typeof setTimeout> | null;

class WebSocketClient {
  private socket: WebSocket | null = null;
  private shouldReconnect = false;
  private reconnectTimer: ReconnectTimer = null;
  private currentUrl: string | null = null;
  private config: ConnectionConfig | null = null;
  private connectInFlight: Promise<ConnectResult> | null = null;
  private connectInFlightUrl: string | null = null;

  // V2 Event protocol callbacks
  private onEventAckCallback: ((eventId: string, taskId: string) => void) | null = null;
  private onTaskControlCallback:
    | ((taskId: string, action: string, payload?: Record<string, unknown>) => void)
    | null = null;
  private onReconnectCallback: (() => void) | null = null;

  async initialize(): Promise<void> {
    this.config = await getConnectionConfig();

    if (this.config.autoReconnect) {
      const url = this.config.serverUrl;
      if (url) {
        await this.connect(url);
      }
    }
  }

  async connect(url: string): Promise<ConnectResult> {
    url = (url || '').trim();
    console.info('[WebSocketClient] connect requested', { url });

    if (!url) {
      return { success: false, error: 'WebSocket 地址不能为空' };
    }

    // Idempotent connect: already connected to the same url.
    if (this.isConnected() && this.currentUrl === url) {
      return { success: true };
    }

    // Prevent concurrent connects which can create multiple sockets and break ping/pong + request correlation.
    if (this.connectInFlight) {
      if (this.connectInFlightUrl === url) {
        return await this.connectInFlight;
      }

      try {
        await this.connectInFlight;
      } catch {
        // ignore
      }
    }

    this.connectInFlightUrl = url;
    this.connectInFlight = this.connectInternal(url).finally(() => {
      this.connectInFlight = null;
      this.connectInFlightUrl = null;
    });
    return await this.connectInFlight;
  }

  private async connectInternal(url: string): Promise<ConnectResult> {
    if (!this.config) {
      this.config = await getConnectionConfig();
      console.info('[WebSocketClient] loaded config from storage', this.config);
    }

    if (this.config) {
      this.config = {
        ...this.config,
        serverUrl: url,
      };
      this.shouldReconnect = this.config.autoReconnect;
    } else {
      this.shouldReconnect = true;
    }

    console.info('[WebSocketClient] effective config before connect', this.config);

    this.currentUrl = url;
    this.clearReconnectTimer();

    await saveConnectionStatus({
      connected: false,
      reconnecting: this.shouldReconnect,
      serverUrl: url,
      lastError: undefined,
    });

    return await new Promise<ConnectResult>((resolve) => {
      let resolved = false;
      const resolveOnce = (result: ConnectResult) => {
        if (!resolved) {
          resolved = true;
          resolve(result);
        }
      };

      try {
        // Ensure there is only one active websocket connection.
        // If we leave old sockets open, PING/PONG and tool_result may be sent on the wrong socket,
        // causing backend request correlation to break.
        if (this.socket) {
          try {
            console.info('[WebSocketClient] closing previous WebSocket before reconnect', {
              url: this.currentUrl,
            });
            this.socket.close();
          } catch {
            // ignore
          } finally {
            this.socket = null;
          }
        }

        const socket = new WebSocket(url);
        this.socket = socket;

        console.info('[WebSocketClient] opening WebSocket', { url });

        socket.addEventListener('open', () => {
          // Ignore events from stale sockets.
          if (this.socket !== socket) return;

          console.info('[WebSocketClient] WebSocket opened', { url });
          void saveConnectionStatus({
            connected: true,
            reconnecting: false,
            serverUrl: url,
            lastError: undefined,
          });

          // Send HELLO message with API Key for authentication
          this.sendMessage({
            type: SocketMessageType.HELLO,
            payload: {
              extensionVersion: chrome.runtime.getManifest().version,
              apiKey: this.config?.apiKey, // API Key for authentication
            },
          });

          console.info('[WebSocketClient] HELLO message sent with API Key');
          resolveOnce({ success: true });

          // V2: Notify TaskRuntimeManager to retry pending events after reconnect
          if (this.onReconnectCallback) {
            this.onReconnectCallback();
          }
        });

        socket.addEventListener('message', (event) => {
          if (this.socket !== socket) return;
          console.debug('[WebSocketClient] message received', event.data);
          void this.handleIncomingMessage(event.data);
        });

        socket.addEventListener('close', (event) => {
          if (this.socket !== socket) return;

          console.warn('[WebSocketClient] WebSocket closed', {
            url,
            code: event.code,
            reason: event.reason,
            wasClean: event.wasClean,
          });
          this.socket = null;

          const reason = event.reason || 'WebSocket connection closed';
          void saveConnectionStatus({
            connected: false,
            reconnecting: this.shouldReconnect,
            serverUrl: url,
            lastError: event.wasClean ? undefined : reason,
          });

          // If close before open, resolve with error
          if (!resolved) {
            resolveOnce({ success: false, error: reason });
          }

          if (this.shouldReconnect) {
            this.scheduleReconnect();
          }
        });

        socket.addEventListener('error', (event) => {
          if (this.socket !== socket) return;

          const errorMessage = event instanceof ErrorEvent ? event.message : 'WebSocket 连接失败';
          console.error('[WebSocketClient] WebSocket error', {
            url,
            event,
            errorMessage,
          });
          void saveConnectionStatus({
            connected: false,
            reconnecting: this.shouldReconnect,
            serverUrl: url,
            lastError: errorMessage,
          });
        });
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        console.error('[WebSocketClient] Failed to create WebSocket', {
          url,
          error,
        });
        void saveConnectionStatus({
          connected: false,
          reconnecting: false,
          serverUrl: url,
          lastError: message,
        });
        resolveOnce({ success: false, error: message });
      }
    });
  }

  disconnect(): void {
    this.shouldReconnect = false;
    this.clearReconnectTimer();

    if (this.socket) {
      console.info('[WebSocketClient] closing WebSocket', { url: this.currentUrl });
      this.socket.close();
      this.socket = null;
    }

    // 只更新连接状态，不修改 serverUrl（避免覆盖用户刚保存的新地址）
    void saveConnectionStatus({
      connected: false,
      reconnecting: false,
    });
  }

  isConnected(): boolean {
    return this.socket !== null && this.socket.readyState === WebSocket.OPEN;
  }

  getCurrentUrl(): string | null {
    return this.currentUrl;
  }

  updateConfig(config: Partial<ConnectionConfig>): void {
    console.info('[WebSocketClient] updateConfig', config);
    this.config = {
      serverUrl: config.serverUrl ?? this.config?.serverUrl ?? this.currentUrl ?? '',
      autoReconnect: config.autoReconnect ?? this.config?.autoReconnect ?? true,
      apiKey: config.apiKey ?? this.config?.apiKey,
    };
    this.shouldReconnect = this.config.autoReconnect;
    console.info('[WebSocketClient] effective config after update', this.config);
  }

  private scheduleReconnect(): void {
    if (!this.shouldReconnect || !this.currentUrl) {
      return;
    }

    this.clearReconnectTimer();
    this.reconnectTimer = setTimeout(() => {
      void this.connect(this.currentUrl!);
    }, 5000);
  }

  private clearReconnectTimer(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }

  private async handleIncomingMessage(raw: unknown): Promise<void> {
    const rawString = await this.normalizeMessageRaw(raw);
    if (!rawString) {
      return;
    }

    let message: SocketMessage;

    try {
      message = JSON.parse(rawString);
    } catch (error) {
      console.error('Invalid WebSocket message:', rawString, error);
      return;
    }

    console.info('[WebSocketClient] parsed incoming message', {
      type: message.type,
      requestId: message.requestId,
      responseToRequestId: message.responseToRequestId,
    });

    if (message.type === SocketMessageType.PING) {
      this.sendMessage({ type: SocketMessageType.PONG });
      return;
    }

    if (message.type === SocketMessageType.TOOL_CALL && message.requestId) {
      await this.handleToolCall(message);
      return;
    }

    // V2: Handle EVENT_ACK from backend
    if (message.type === SocketMessageType.EVENT_ACK) {
      const payload = message.payload as any;
      const eventId = (
        payload?.eventId ||
        payload?.event_id ||
        (message as any).eventId ||
        (message as any).event_id ||
        ''
      ) as string;
      const taskId = (
        payload?.taskId ||
        payload?.task_id ||
        (message as any).taskId ||
        (message as any).task_id ||
        ''
      ) as string;
      if (eventId && this.onEventAckCallback) {
        console.info('[WebSocketClient] EVENT_ACK received', { eventId, taskId });
        this.onEventAckCallback(eventId, taskId);
      }
      return;
    }

    // V2: Handle TASK_CONTROL from backend
    if (message.type === SocketMessageType.TASK_CONTROL) {
      const payload = message.payload as any;
      const taskId = (payload?.taskId || payload?.task_id || (message as any).task_id || '') as string;
      const action = (payload?.action || (message as any).action || '') as string;
      const controlPayload =
        payload && typeof payload.payload === 'object' && payload.payload !== null
          ? (payload.payload as Record<string, unknown>)
          : undefined;
      if (taskId && action && this.onTaskControlCallback) {
        console.info('[WebSocketClient] TASK_CONTROL received', {
          taskId,
          action,
        });
        this.onTaskControlCallback(taskId, action, controlPayload);
      }
      return;
    }

    console.debug('Unhandled WebSocket message:', message);
  }

  private async handleToolCall(message: SocketMessage): Promise<void> {
    if (!message.requestId) {
      return;
    }

    try {
      const payload = (message.payload || {}) as any;
      const args = (payload?.args || {}) as any;
      console.info('[WebSocketClient] received TOOL_CALL', {
        requestId: message.requestId,
        toolName: payload?.name,
        argKeys: Object.keys(args || {}),
      });
      this.sendStatus(message.requestId, {
        stage: 'received',
        tool: payload?.name || '',
      });

      // Use dynamic import to avoid circular dependency:
      // websocket-client -> tools -> tool implementations -> websocket-client (for status reporting).
      const { handleCallTool } = await import('./tools');
      this.sendStatus(message.requestId, {
        stage: 'executing',
        tool: payload?.name || '',
      });

      // Inject requestId into args for long-running tools to report progress.
      const injectedPayload = {
        ...payload,
        args: {
          ...args,
          __toolCallRequestId: message.requestId,
        },
      };

      const result = await handleCallTool(injectedPayload);
      this.sendStatus(message.requestId, {
        stage: 'result_ready',
        tool: payload?.name || '',
      });
      this.sendMessage({
        type: SocketMessageType.TOOL_RESULT,
        responseToRequestId: message.requestId,
        payload: {
          status: 'success',
          data: result,
        },
      });
    } catch (error) {
      this.sendMessage({
        type: SocketMessageType.TOOL_RESULT,
        responseToRequestId: message.requestId,
        payload: {
          status: 'error',
          error: error instanceof Error ? error.message : String(error),
        },
      });
      this.sendStatus(message.requestId, {
        stage: 'error',
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  // Send periodic status/progress updates for long-running tool calls.
  // Backend can map requestId to the pending tool call.
  sendStatus(requestId: string, payload: any): void {
    if (!requestId) {
      return;
    }
    this.sendMessage({
      type: SocketMessageType.STATUS,
      requestId,
      payload,
    });
  }

  // ── V2 Event Protocol ────────────────────────────────────

  /**
   * Set callback for EVENT_ACK received from backend.
   */
  setOnEventAck(fn: (eventId: string, taskId: string) => void): void {
    this.onEventAckCallback = fn;
  }

  /**
   * Set callback for TASK_CONTROL received from backend.
   */
  setOnTaskControl(fn: (taskId: string, action: string, payload?: Record<string, unknown>) => void): void {
    this.onTaskControlCallback = fn;
  }

  /**
   * Set callback for WebSocket reconnect (to retry pending events).
   */
  setOnReconnect(fn: () => void): void {
    this.onReconnectCallback = fn;
  }

  /**
   * Send a V2 EVENT to backend.
   * Returns true if the message was sent, false if WS not connected.
   */
  sendEvent(event: DumasAsyncEvent): boolean {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
      return false;
    }

    try {
      // Must match backend protocol.Message + protocol.EventPayload wire shape.
      const wireMessage: SocketMessage = {
        type: SocketMessageType.EVENT,
        payload: {
          taskId: event.taskId,
          eventId: event.eventId,
          seq: event.seq,
          eventType: event.eventType,
          payload: event.payload,
          timestamp: event.timestamp,
          needAck: event.needAck !== false,
        },
      };

      console.info('[WebSocketClient] sendEvent', {
        eventId: event.eventId,
        taskId: event.taskId,
        eventType: event.eventType,
      });

      this.socket.send(JSON.stringify(wireMessage));
      return true;
    } catch (error) {
      console.error('[WebSocketClient] Failed to send event', error);
      return false;
    }
  }

  private sendMessage(message: SocketMessage): void {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
      return;
    }

    try {
      if (
        message.type === SocketMessageType.TOOL_RESULT ||
        message.type === SocketMessageType.STATUS ||
        message.type === SocketMessageType.TOOL_CALL
      ) {
        console.info('[WebSocketClient] send websocket message', {
          type: message.type,
          requestId: message.requestId,
          responseToRequestId: message.responseToRequestId,
        });
      }
      this.socket.send(JSON.stringify(message));
    } catch (error) {
      console.error('Failed to send WebSocket message:', message, error);
    }
  }

  private async normalizeMessageRaw(raw: unknown): Promise<string | null> {
    if (typeof raw === 'string') {
      return raw;
    }

    if (raw instanceof ArrayBuffer) {
      return new TextDecoder().decode(raw);
    }

    if (raw instanceof Blob) {
      return await raw.text();
    }

    console.warn('Unsupported WebSocket message payload:', raw);
    return null;
  }
}

export const websocketClient = new WebSocketClient();
