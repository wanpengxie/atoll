// Background entrypoint — coagent xhs-extension service worker。
//
// M1.1-T2 起：
//   - 启动 tools registry（沿用 fork 母本的 5 大类工具实现，复用）
//   - 启动 coagent device WS 客户端（services/coagent-device.ts）
//   - chrome.storage.local 保存 device 配置（serverUrl/apiKey/daemonHttpBase/deviceId/userId）
//   - chrome.runtime.onMessage 仍提供 popup ↔ background 桥（连接 / 断开 / 状态查询 / 配置存取
//     / 手动 cookie sync / 直接 EXECUTE_TOOL 调试）。旧 1studio backend 派发协议（HELLO/TOOL_CALL/
//     V2 EVENT/TASK_CONTROL/PENDING_ACK 等）已切断。

import { initToolsRegistry, handleCallTool } from './tools';
import {
  ConnectionConfig,
  getConnectionConfig,
  saveConnectionConfig,
  getStoredConnectionStatus,
} from './connection-state';
import { cookieSyncService } from './tools/sync-cookies';
import { coagentDeviceClient } from './services/coagent-device';
import { recoverPublishWaitStates } from './tools/publish-content';

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
  | { type: 'SYNC_COOKIES' };

let connectionConfig: ConnectionConfig;

export default defineBackground(() => {
  console.log('🚀 Coagent xhs-extension Background script initialized');

  initToolsRegistry();

  // 异步初始化，不阻塞 Service Worker 启动。
  (async () => {
    connectionConfig = await getConnectionConfig();
    coagentDeviceClient.updateConfig(connectionConfig);
    if (connectionConfig.autoReconnect && hasMinimalDeviceConfig(connectionConfig)) {
      void coagentDeviceClient.connect();
    } else {
      console.log('[Background] device config incomplete, skip auto-connect', {
        hasUrl: Boolean(connectionConfig.serverUrl),
        hasKey: Boolean(connectionConfig.apiKey),
        hasDeviceId: Boolean(connectionConfig.deviceId),
      });
    }
    // R3-T4 FX9：MV3 SW evict 后，由 publish_wait:* storage 条目恢复
    // publish-wait 收尾。recovery 走的 callback 复用 coagentDeviceClient
    // 已有的 retry / outbox 兜底（无网时入队，下次连上自动 replay）。
    void recoverPublishWaitStates({
      postCallback: (correlationId, payload) =>
        coagentDeviceClient.postCallback(correlationId, payload),
    })
      .then((summaries) => {
        if (summaries.length > 0) {
          console.info('[Background] publish-wait recovery summaries', summaries);
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
            coagentDeviceClient.updateConfig(connectionConfig);
            if (!hasMinimalDeviceConfig(connectionConfig)) {
              sendResponse({
                success: false,
                error:
                  'Device 配置不完整：需要 daemon WS URL、device api key、device id。',
              });
              break;
            }
            const result = await coagentDeviceClient.connect();
            sendResponse(result);
            break;
          }
          case 'DISCONNECT_DEVICE': {
            coagentDeviceClient.disconnect();
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
            // 配置变更：先断开旧连接，再用新配置（不自动重连，由 popup 触发）。
            coagentDeviceClient.disconnect();
            coagentDeviceClient.updateConfig(connectionConfig);
            console.log('[Background] device config saved', {
              serverUrl: connectionConfig.serverUrl,
              hasKey: Boolean(connectionConfig.apiKey),
              deviceId: connectionConfig.deviceId,
              userId: connectionConfig.userId,
            });
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

function hasMinimalDeviceConfig(cfg: ConnectionConfig): boolean {
  return Boolean(
    (cfg.serverUrl ?? '').trim() &&
      (cfg.apiKey ?? '').trim() &&
      (cfg.deviceId ?? '').trim()
  );
}
