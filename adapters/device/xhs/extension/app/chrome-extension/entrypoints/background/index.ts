// Background entrypoint — coagent xhs-extension service worker.
//
// M1.1-T2 起：
//   - 启动 tools registry（沿用 fork 母本的 5 大类工具实现，复用）
//   - 启动 device WS 客户端（services/coagent-device.ts 或 server-devicebus.ts）
//   - chrome.storage.local 保存 device 配置
//   - chrome.runtime.onMessage 仍提供 popup ↔ background 桥（连接 / 断开 / 状态查询
//     / 配置存取 / 手动 cookie sync / 直接 EXECUTE_TOOL 调试）。
//
// T147 §A-E：根据 ConnectionConfig 选择 v4 server-devicebus client 或 legacy
// daemon-direct client：当 `serverWsEndpoint + deviceActorId +
// deviceActorToken` 三件套齐时走 v4；否则保留 legacy 路径兼容老配置。
// 两条客户端都实现 `connect / disconnect / updateConfig / postCallback`
// — `activeDeviceClient()` 在每次 dispatch 时按当前 config 选择，确保 popup
// 切换配置后立刻生效，无需 SW 重启。

import { initToolsRegistry, handleCallTool } from './tools';
import {
  ConnectionConfig,
  getConnectionConfig,
  saveConnectionConfig,
  getStoredConnectionStatus,
} from './connection-state';
import { cookieSyncService } from './tools/sync-cookies';
import { coagentDeviceClient } from './services/coagent-device';
import { coagentServerDeviceClient, type ServerDeviceConfig } from './services/server-devicebus';
import { resolveDeviceConfig } from './services/resolve';
import { recoverPublishWaitStates } from './tools/publish-content';
import { registerKeepaliveAlarm } from './keepalive';
import {
  handleExternalMessage,
  type ExternalBindMessage,
  type ExternalSender,
  type ExternalBindDeps,
} from './external-bind';

interface ExecuteToolPayload {
  name: string;
  args: any;
}

type BackgroundRequest =
  | { type: 'EXECUTE_TOOL'; payload: ExecuteToolPayload }
  | { type: 'CHECK_CONNECTION' }
  | { type: 'GET_STATUS' }
  | { type: 'TOOL_RESULT'; payload: any }
  | { type: 'CONNECT_DEVICE'; payload?: Partial<ConnectionConfig> }
  | { type: 'DISCONNECT_DEVICE' }
  | { type: 'GET_CONNECTION_CONFIG' }
  | { type: 'SAVE_CONNECTION_CONFIG'; payload: Partial<ConnectionConfig> }
  | {
      // M1.2-T3: popup 主入口 1-key 流程。Background 调 coagent resolve API
      // 拿全套连接信息，落 storage，再触发 coagent device WS connect。
      type: 'RESOLVE_AND_CONNECT';
      payload: { coagentServerUrl: string; apiKey: string };
    }
  | { type: 'SYNC_COOKIES' };

let connectionConfig: ConnectionConfig | null = null;
let connectionConfigLoad: Promise<ConnectionConfig> | null = null;

/**
 * Tag identifying which device transport binding is in use for the
 * currently-loaded ConnectionConfig:
 *   - 'server' → T147 §A-E v4 path via `coagentServerDeviceClient` to
 *     `wss://{server}/devicebus?actor_id=...`
 *   - 'daemon' → legacy M1.1/M1.2 daemon-direct path via
 *     `coagentDeviceClient` to `ws://{daemon}/device/{id}?key=...`
 *   - 'none'   → config incomplete; no client should connect.
 */
type DeviceTransport = 'server' | 'daemon' | 'none';

function selectTransport(cfg: ConnectionConfig | null | undefined): DeviceTransport {
  if (!cfg) return 'none';
  if (hasServerDeviceConfig(cfg)) return 'server';
  if (hasLegacyDeviceConfig(cfg)) return 'daemon';
  return 'none';
}

/** Translate the persistent ConnectionConfig into the v4 client's input. */
function toServerDeviceConfig(cfg: ConnectionConfig): ServerDeviceConfig {
  return {
    wsEndpoint: (cfg.serverWsEndpoint ?? '').trim(),
    actorId: (cfg.deviceActorId ?? '').trim(),
    token: (cfg.deviceActorToken ?? '').trim(),
    channelId: (cfg.channelId ?? '').trim(),
    autoReconnect: cfg.autoReconnect !== false,
    userId: cfg.userId,
    deviceId: cfg.deviceId,
  };
}

/**
 * applyClients pushes the latest ConnectionConfig into both transport
 * clients (only one will actually be used by `activeDeviceClient()` —
 * the inactive one stays idle). Calling this is idempotent.
 */
function applyClients(cfg: ConnectionConfig): void {
  coagentDeviceClient.updateConfig(cfg);
  coagentServerDeviceClient.updateConfig(toServerDeviceConfig(cfg));
}

function loadConnectionConfig(): Promise<ConnectionConfig> {
  if (connectionConfig) return Promise.resolve(connectionConfig);
  if (!connectionConfigLoad) {
    connectionConfigLoad = getConnectionConfig()
      .then((cfg) => {
        connectionConfig = cfg;
        applyClients(cfg);
        return cfg;
      })
      .finally(() => {
        connectionConfigLoad = null;
      });
  }
  return connectionConfigLoad;
}

/**
 * activeDeviceClient returns the client paired with the currently
 * configured transport. Both share the `connect / disconnect / postCallback`
 * surface used by the rest of background. Callers MUST NOT cache the
 * returned reference across config saves — `applyClients()` may flip
 * the active transport on the next dispatch.
 */
function activeDeviceClient(): {
  transport: DeviceTransport;
  connect: () => Promise<{ success: boolean; error?: string }>;
  disconnect: () => void;
  postCallback: (
    correlationId: string,
    payload: {
      status: 'ok' | 'error';
      result: Record<string, unknown> | null;
      error: { code: string; message: string } | null;
    }
  ) => Promise<void>;
} {
  const transport = selectTransport(connectionConfig);
  if (transport === 'server') {
    return {
      transport,
      connect: () => coagentServerDeviceClient.connect(),
      disconnect: () => coagentServerDeviceClient.disconnect(),
      postCallback: (id, p) => coagentServerDeviceClient.postCallback(id, p),
    };
  }
  return {
    transport,
    connect: () => coagentDeviceClient.connect(),
    disconnect: () => coagentDeviceClient.disconnect(),
    postCallback: (id, p) => coagentDeviceClient.postCallback(id, p),
  };
}

function disconnectAll(): void {
  coagentDeviceClient.disconnect();
  coagentServerDeviceClient.disconnect();
}

export default defineBackground(() => {
  console.warn('[Boot] service worker started', {
    at: Date.now(),
    version: chrome.runtime.getManifest().version ?? '0.0.0',
  });

  registerKeepaliveAlarm({
    alarms: chrome.alarms,
    loadConnectionConfig,
    selectTransport,
    activeDeviceClient,
  });

  initToolsRegistry();

  // 异步初始化，不阻塞 Service Worker 启动。
  (async () => {
    connectionConfig = await loadConnectionConfig();
    const transport = selectTransport(connectionConfig);
    if (connectionConfig.autoReconnect && transport !== 'none') {
      console.warn('[Background] auto-connecting device transport', {
        at: Date.now(),
        transport,
      });
      void activeDeviceClient().connect();
    } else {
      console.warn('[Background] device config incomplete, skip auto-connect', {
        at: Date.now(),
        transport,
        hasServerWs: Boolean(connectionConfig.serverWsEndpoint),
        hasActorId: Boolean(connectionConfig.deviceActorId),
        hasActorToken: Boolean(connectionConfig.deviceActorToken),
        hasLegacyUrl: Boolean(connectionConfig.serverUrl),
        hasLegacyKey: Boolean(connectionConfig.apiKey),
        hasDeviceId: Boolean(connectionConfig.deviceId),
      });
    }
    // R3-T4 FX9：MV3 SW evict 后，由 publish_wait:* storage 条目恢复
    // publish-wait 收尾。recovery 走 activeDeviceClient().postCallback —
    // 当 v4 transport 启用时由 server-devicebus 直接走 WS 回 callback；
    // legacy transport 走 HTTP 兜底 + outbox。两者底层都有 outbox，断网
    // 期间入队，下次连上自动 replay。
    void recoverPublishWaitStates({
      postCallback: (correlationId, payload) =>
        activeDeviceClient().postCallback(correlationId, payload),
    })
      .then((summaries) => {
        if (summaries.length > 0) {
          console.warn('[Background] publish-wait recovery summaries', {
            at: Date.now(),
            summaries,
          });
        }
      })
      .catch((err) => {
        console.warn('[Background] publish-wait recovery failed', err);
      });
  })();

  chrome.runtime.onMessage.addListener((request: BackgroundRequest, sender, sendResponse) => {
    console.log('[Background] Received message', request, sender);

    void (async () => {
      try {
        switch (request.type) {
          case 'EXECUTE_TOOL': {
            const result = await handleCallTool(request.payload);
            sendResponse({ success: !result.isError, result });
            break;
          }
          case 'CHECK_CONNECTION':
          case 'GET_STATUS': {
            const status = await getStoredConnectionStatus();
            sendResponse({ success: true, status });
            break;
          }
          case 'TOOL_RESULT': {
            handleToolResult(request.payload);
            sendResponse({ success: true });
            break;
          }
          case 'CONNECT_DEVICE': {
            const patch = request.payload ?? {};
            connectionConfig = await saveConnectionConfig(patch);
            applyClients(connectionConfig);
            const transport = selectTransport(connectionConfig);
            if (transport === 'none') {
              sendResponse({
                success: false,
                error:
                  'Device 配置不完整：需要 server WS + actor_id + token（v4），或 daemon WS + api key + device id（legacy）。',
              });
              break;
            }
            const result = await activeDeviceClient().connect();
            sendResponse(result);
            break;
          }
          case 'DISCONNECT_DEVICE': {
            disconnectAll();
            sendResponse({ success: true });
            break;
          }
          case 'GET_CONNECTION_CONFIG': {
            connectionConfig = await getConnectionConfig();
            sendResponse({ success: true, config: connectionConfig });
            break;
          }
          case 'SAVE_CONNECTION_CONFIG': {
            connectionConfig = await saveConnectionConfig(request.payload);
            // 配置变更：先断开两条 transport，再统一 update（不自动重连，由 popup 触发）。
            disconnectAll();
            applyClients(connectionConfig);
            const transport = selectTransport(connectionConfig);
            console.log('[Background] device config saved', {
              transport,
              hasServerWs: Boolean(connectionConfig.serverWsEndpoint),
              hasActorId: Boolean(connectionConfig.deviceActorId),
              hasLegacyUrl: Boolean(connectionConfig.serverUrl),
              hasLegacyKey: Boolean(connectionConfig.apiKey),
              deviceId: connectionConfig.deviceId,
              userId: connectionConfig.userId,
            });
            sendResponse({ success: true, config: connectionConfig });
            break;
          }
          case 'RESOLVE_AND_CONNECT': {
            // M1.2-T3 popup 主入口 1-key 流程。
            const { coagentServerUrl, apiKey } = request.payload ?? ({} as any);
            const result = await resolveDeviceConfig({
              serverUrl: String(coagentServerUrl ?? ''),
              apiKey: String(apiKey ?? ''),
            });
            if (!result.ok) {
              // resolve 失败 → 不写 storage，把错误透给 popup 显示。
              console.warn('[Background] device.resolve failed', {
                kind: result.error.kind,
                status: result.error.status,
                message: result.error.message,
              });
              sendResponse({
                success: false,
                error: result.error.message,
                errorKind: result.error.kind,
              });
              break;
            }
            // 写完整 device config（含 wsUrl/httpBase 别名）。
            const patch: Partial<ConnectionConfig> = {
              coagentServerUrl: String(coagentServerUrl ?? '').trim(),
              apiKey: String(apiKey ?? '').trim(),
              serverUrl: result.data.ws_url, // canonical（legacy daemon WS）
              wsUrl: result.data.ws_url, // 别名
              daemonHttpBase: result.data.http_url, // canonical（callback / sync-cookies）
              httpBase: result.data.http_url, // 别名
              deviceId: result.data.device_id,
              userId: result.data.user_id,
              channelId: result.data.channel_id,
              daemonId: result.data.daemon_id,
            };
            connectionConfig = await saveConnectionConfig(patch);
            // 切换连接：先断旧再 update，再 connect。
            disconnectAll();
            applyClients(connectionConfig);
            console.warn('[Background] device.resolve ok', {
              at: Date.now(),
              transport: selectTransport(connectionConfig),
              deviceId: connectionConfig.deviceId,
              channelId: connectionConfig.channelId,
              daemonId: connectionConfig.daemonId,
            });
            const connectResult = await activeDeviceClient().connect();
            sendResponse({
              success: connectResult.success,
              error: connectResult.error,
              config: connectionConfig,
            });
            break;
          }
          case 'SYNC_COOKIES': {
            console.log('[Background] Manual cookie sync requested');
            const result = await cookieSyncService.syncNow();
            sendResponse({ success: !result.isError, result });
            break;
          }
          default:
            sendResponse({ success: false, error: 'Unknown message type' });
        }
      } catch (error) {
        console.error('Background message handling failed:', error);
        sendResponse({
          success: false,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    })();

    return true;
  });

  // T148 (M1.6-T6): externally-connectable handshake. Web UI hosted on
  // an allowed origin (see wxt.config.ts externally_connectable.matches)
  // calls `chrome.runtime.sendMessage(EXTENSION_ID, {action, ...})` to
  // hand off a fresh device actor token, bypassing the popup
  // RESOLVE_AND_CONNECT manual flow. Three actions:
  //   - getDeviceInfo  → returns persistent device_id (auto-generated)
  //   - setDeviceToken → writes v4 actor token bundle + opens WS
  //   - unbindDevice   → clears v4 fields + disconnects
  //
  // All policy / origin validation lives in external-bind.ts so the
  // surface is unit-testable; here we only wire chrome APIs to the
  // pure handler.
  //
  // SECURITY: the runtime allowlist is derived from
  // `chrome.runtime.getManifest().externally_connectable.matches` —
  // single source of truth with the manifest. This means a future maintainer
  // who tightens / loosens the manifest does NOT need to remember to also
  // edit a second list here; the handler's defense-in-depth check will
  // simply mirror whatever Chrome already enforced upstream.
  const manifestMatches =
    (
      chrome.runtime.getManifest() as chrome.runtime.Manifest & {
        externally_connectable?: { matches?: string[] };
      }
    ).externally_connectable?.matches ?? [];
  const runtimeAllowedOrigins =
    manifestMatches.length > 0
      ? manifestMatches
      : // Defensive: if the manifest somehow lost its allowlist, default
        // to localhost-only so we never fail-open onto arbitrary origins.
        ['http://localhost:*/*', 'http://127.0.0.1:*/*'];

  const externalDeps: ExternalBindDeps = {
    getConfig: () => getConnectionConfig(),
    saveConfig: (patch) => {
      // saveConnectionConfig is the canonical write path — mirrors
      // serverUrl ↔ wsUrl aliases and persists to chrome.storage.local.
      return saveConnectionConfig(patch).then(async (cfg) => {
        connectionConfig = cfg;
        return cfg;
      });
    },
    disconnectAll: () => disconnectAll(),
    applyClients: (cfg) => applyClients(cfg),
    connect: () => activeDeviceClient().connect(),
    extensionVersion: chrome.runtime.getManifest().version ?? '0.0.0',
    allowedOrigins: runtimeAllowedOrigins,
    generateDeviceID: () => {
      // crypto.randomUUID is available in MV3 service workers (Chrome 92+).
      const c: any = (globalThis as any).crypto;
      if (c && typeof c.randomUUID === 'function') return `xhs-${c.randomUUID()}`;
      // Defensive fallback for unexpected runtimes; not security-sensitive
      // because device_id is a logical identifier, not a secret.
      return `xhs-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
    },
  };

  chrome.runtime.onMessageExternal.addListener(
    (message: ExternalBindMessage, sender, sendResponse) => {
      // The sender shape we care about is `{origin, url, id}` — narrow
      // to ExternalSender so the handler doesn't depend on chrome types.
      const narrowSender: ExternalSender = {
        origin: (sender as any)?.origin,
        url: sender?.url,
        id: sender?.id,
        tab: sender?.tab ? { id: sender.tab.id } : undefined,
      };
      console.log('[Background] Received external message', {
        action: message?.action,
        sender: narrowSender,
      });
      void handleExternalMessage(message, narrowSender, externalDeps)
        .then((response) => {
          console.log('[Background] External message response', {
            action: message?.action,
            status: response.status,
          });
          sendResponse(response);
        })
        .catch((err) => {
          console.error('[Background] External message handler crashed', err);
          sendResponse({
            status: 'failed',
            reason: 'internal_error',
            detail: err instanceof Error ? err.message : String(err),
          });
        });
      return true; // async sendResponse — Chrome keeps the channel open.
    }
  );

  chrome.runtime.onInstalled.addListener((details) => {
    if (details.reason === 'install') {
      console.log('Coagent xhs-extension installed');
    } else if (details.reason === 'update') {
      console.log('Coagent xhs-extension updated');
    }
  });
});

function handleToolResult(payload: any) {
  console.log('Tool result:', payload);
  chrome.runtime.sendMessage({ type: 'TOOL_RESULT_UPDATE', payload }).catch(() => {
    // No listeners registered
  });
}

/** v4 server-devicebus transport readiness: server endpoint + actor_id + token. */
function hasServerDeviceConfig(cfg: ConnectionConfig): boolean {
  return Boolean(
    (cfg.serverWsEndpoint ?? '').trim() &&
    (cfg.deviceActorId ?? '').trim() &&
    (cfg.deviceActorToken ?? '').trim()
  );
}

/** Legacy daemon-direct transport readiness: daemon WS + api key + device id. */
function hasLegacyDeviceConfig(cfg: ConnectionConfig): boolean {
  return Boolean(
    (cfg.serverUrl ?? '').trim() && (cfg.apiKey ?? '').trim() && (cfg.deviceId ?? '').trim()
  );
}
