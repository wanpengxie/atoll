// Background entrypoint — coagent xhs-extension service worker.
//
// M1.1-T2 起：
//   - 启动 tools registry（沿用 fork 母本的 5 大类工具实现，复用）
//   - 启动本机 proxy daemon WS 客户端（services/server-devicebus.ts proxy mode）
//   - chrome.storage.local 保存 device 配置
//   - chrome.runtime.onMessage 仍提供 popup ↔ background 桥（连接 / 断开 / 状态查询
//     / 配置存取 / 手动 cookie sync / 直接 EXECUTE_TOOL 调试）。
//
// T178/T5：extension 只保留 proxy_endpoint mode。旧 direct-to-server
// actor-token path 和 daemon-direct / resolve fallback 已下线。

import { initToolsRegistry, handleCallTool } from './tools';
import {
	ConnectionConfig,
	DEFAULT_PROXY_ACTOR_ID,
	getConnectionConfig,
	saveConnectionConfig,
	getStoredConnectionStatus,
	getDefaultProxyEndpoint,
} from './connection-state';
import { cookieSyncService } from './tools/sync-cookies';
import { coagentServerDeviceClient, type ServerDeviceConfig } from './services/server-devicebus';
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
	| { type: 'CONNECT_PROXY_DAEMON'; payload?: { proxyEndpoint?: string } }
	| { type: 'DISCONNECT_DEVICE' }
	| { type: 'GET_CONNECTION_CONFIG' }
	| { type: 'SYNC_COOKIES' };

let connectionConfig: ConnectionConfig | null = null;
let connectionConfigLoad: Promise<ConnectionConfig> | null = null;

/**
 * Tag identifying whether the proxy endpoint config is connectable.
 */
type DeviceTransport = 'proxy' | 'none';

function selectTransport(cfg: ConnectionConfig | null | undefined): DeviceTransport {
	if (!cfg) return 'none';
	if (cfg.connectionMode === 'proxy') return hasProxyDeviceConfig(cfg) ? 'proxy' : 'none';
	return 'none';
}

/** Translate the persistent ConnectionConfig into the proxy client input. */
function toServerDeviceConfig(cfg: ConnectionConfig): ServerDeviceConfig {
	return {
		mode: 'proxy',
		wsEndpoint: (cfg.proxyEndpoint ?? getDefaultProxyEndpoint()).trim(),
		actorId: ((cfg.deviceActorId ?? '').trim() || DEFAULT_PROXY_ACTOR_ID),
		channelId: '',
		autoReconnect: cfg.autoReconnect !== false,
	};
}

/**
 * applyClients pushes the latest ConnectionConfig into both transport
 * clients (only one will actually be used by `activeDeviceClient()` —
 * the inactive one stays idle). Calling this is idempotent.
 */
function applyClients(cfg: ConnectionConfig): void {
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
 * activeDeviceClient returns the proxy transport client. Callers MUST NOT
 * cache the returned reference across config saves.
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
	return {
		transport,
		connect: () => coagentServerDeviceClient.connect(),
		disconnect: () => coagentServerDeviceClient.disconnect(),
		postCallback: (id, p) => coagentServerDeviceClient.postCallback(id, p),
	};
}

function disconnectAll(): void {
	coagentServerDeviceClient.disconnect();
}

/**
 * connectToLocalProxy — single source of truth for "connect this
 * extension to the local proxy daemon". Both the internal popup message
 * handler and the external-bind action (called from the web UI) funnel
 * through here so the user gets identical behaviour regardless of which
 * surface they triggered it from.
 *
 * Steps:
 *   1. Normalize endpoint (fall back to default ws://127.0.0.1:10387).
 *   2. Persist a proxy-mode ConnectionConfig (clears legacy fields so
 *      selectTransport() picks 'proxy' deterministically).
 *   3. Drop the current transport client and re-apply with the new cfg.
 *   4. Trigger an explicit connect() so a fresh WS attempt fires
 *      immediately (autoReconnect alone would race the caller).
 */
async function connectToLocalProxy(rawEndpoint: string | undefined): Promise<{
	connected: boolean;
	endpoint: string;
	error?: string;
}> {
	const endpoint = (rawEndpoint ?? '').trim() || getDefaultProxyEndpoint();
	connectionConfig = await saveConnectionConfig({
		connectionMode: 'proxy',
		proxyEndpoint: endpoint,
		deviceActorId: DEFAULT_PROXY_ACTOR_ID,
		channelId: '',
		autoReconnect: true,
		serverUrl: '',
		wsUrl: '',
		apiKey: '',
		daemonHttpBase: '',
		httpBase: '',
	});
	disconnectAll();
	applyClients(connectionConfig);
	const result = await activeDeviceClient().connect();
	return {
		connected: Boolean(result.success),
		endpoint,
		error: result.error,
	};
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
				proxyEndpoint: connectionConfig.proxyEndpoint,
			});
		}
    // R3-T4 FX9：MV3 SW evict 后，由 publish_wait:* storage 条目恢复
    // publish-wait 收尾。recovery 走 activeDeviceClient().postCallback —
			// proxy transport 由 server-devicebus 直接走 WS 回 callback；
			// 断网期间入队，下次连上自动 replay。
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
				case 'CONNECT_PROXY_DAEMON': {
					const result = await connectToLocalProxy(request.payload?.proxyEndpoint);
					sendResponse({
						success: result.connected,
						error: result.error,
						config: connectionConfig,
					});
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
  // calls `chrome.runtime.sendMessage(EXTENSION_ID, {action, ...})` for
  // lightweight diagnostics. T178/T5 removed the old token bind action:
  //   - getDeviceInfo  → returns persistent device_id (auto-generated)
  //   - unbindDevice   → disconnects proxy transport
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
	    extensionVersion: chrome.runtime.getManifest().version ?? '0.0.0',
	    allowedOrigins: runtimeAllowedOrigins,
	    getConnectionStatus: () => getStoredConnectionStatus(),
	    connectProxy: (endpoint) => connectToLocalProxy(endpoint),
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

/** Local proxy daemon transport readiness: endpoint + actor id, no token. */
function hasProxyDeviceConfig(cfg: ConnectionConfig): boolean {
  return Boolean(
    (cfg.proxyEndpoint ?? getDefaultProxyEndpoint()).trim() &&
    ((cfg.deviceActorId ?? '').trim() || DEFAULT_PROXY_ACTOR_ID)
  );
}
