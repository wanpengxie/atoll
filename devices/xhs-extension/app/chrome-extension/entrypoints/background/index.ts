import { initToolsRegistry, handleCallTool } from './tools';
import { websocketClient } from './websocket-client';
import {
  ConnectionConfig,
  getConnectionConfig,
  saveConnectionConfig,
  getStoredConnectionStatus,
} from './connection-state';
import { cookieSyncService } from './tools/sync-cookies';
import { metricsCollectorService } from './services/metrics-collector';
import { TaskRuntimeManager } from './task-runtime';
import type { DumasAsyncEvent, TaskEventMessage } from 'xiaohongshu-mcp-shared';
import { TASK_EVENT_MESSAGE_TYPE } from 'xiaohongshu-mcp-shared';

interface ExecuteToolPayload {
  name: string;
  args: any;
}

interface MetricsCapturedPayload {
  dataType: 'list' | 'detail';
  data: any;
  timestamp: number;
  sourceUrl: string;
}

type BackgroundRequest =
  | { type: 'EXECUTE_TOOL'; payload: ExecuteToolPayload }
  | { type: 'CHECK_CONNECTION' }
  | { type: 'GET_STATUS' }
  | { type: 'TOOL_RESULT'; payload: any }
  | { type: 'CONNECT_WEBSOCKET'; payload?: { url?: string; apiKey?: string } }
  | { type: 'DISCONNECT_WEBSOCKET' }
  | { type: 'GET_CONNECTION_CONFIG' }
  | { type: 'SAVE_CONNECTION_CONFIG'; payload: Partial<ConnectionConfig> }
  | { type: 'SYNC_COOKIES' }
  | { type: 'METRICS_CAPTURED'; payload: MetricsCapturedPayload }
  | { type: typeof TASK_EVENT_MESSAGE_TYPE; event: DumasAsyncEvent };

let connectionConfig: ConnectionConfig;
const taskRuntimeManager = new TaskRuntimeManager();

export default defineBackground(() => {
  console.log('🚀 XiaoHongShu MCP Background script initialized');

  initToolsRegistry();

  // Bind TaskRuntimeManager to WebSocketClient
  taskRuntimeManager.bindSendEvent((event) => websocketClient.sendEvent(event));
  websocketClient.setOnEventAck((eventId, _taskId) => taskRuntimeManager.handleEventAck(eventId));
  websocketClient.setOnTaskControl((taskId, action, payload) =>
    taskRuntimeManager.handleTaskControl(taskId, action, payload)
  );
  websocketClient.setOnReconnect(() => taskRuntimeManager.onReconnect());

  // 异步初始化，不阻塞 Service Worker 启动
  (async () => {
    connectionConfig = await getConnectionConfig();
    websocketClient.updateConfig(connectionConfig);
    // Restore pending ACK queue from storage before connecting
    await taskRuntimeManager.initialize();
    await websocketClient.initialize();
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
          case 'CONNECT_WEBSOCKET': {
            // 如果前端没传地址或 API Key，从最新保存的配置中读取
            let url = request.payload?.url;
            let apiKey = request.payload?.apiKey;

            if (!url || !apiKey) {
              connectionConfig = await getConnectionConfig();
              url = url || connectionConfig.serverUrl;
              apiKey = apiKey || connectionConfig.apiKey;
            }

            console.log('[Background] CONNECT_WEBSOCKET triggered', {
              url,
              hasApiKey: !!apiKey,
              fromPayload: !!request.payload?.url,
            });

            // 验证必需参数
            if (!apiKey) {
              console.error('[Background] Missing API Key');
              sendResponse({
                success: false,
                error: 'API Key 未设置，请先在设置中保存 API Key',
              });
              break;
            }

            // 连接时自动保存配置，这样 metrics-collector 也能读取到
            connectionConfig = await saveConnectionConfig({
              serverUrl: url,
              apiKey: apiKey,
            });

            // 更新 WebSocket 客户端配置并连接
            websocketClient.updateConfig({
              serverUrl: url,
              apiKey: apiKey,
            });

            const result = await websocketClient.connect(url);
            console.log('[Background] CONNECT_WEBSOCKET result', result);
            sendResponse(result);
            break;
          }
          case 'DISCONNECT_WEBSOCKET': {
            console.log('[Background] DISCONNECT_WEBSOCKET');
            websocketClient.disconnect();
            sendResponse({ success: true });
            break;
          }
          case 'GET_CONNECTION_CONFIG': {
            connectionConfig = await getConnectionConfig();
            sendResponse({ success: true, config: connectionConfig });
            break;
          }
          case 'SAVE_CONNECTION_CONFIG': {
            // 保存配置到 storage
            connectionConfig = await saveConnectionConfig(request.payload);

            // 断开当前连接（包括停止自动重连定时器）
            websocketClient.disconnect();

            // 更新 websocketClient 的配置（不触发连接）
            websocketClient.updateConfig(connectionConfig);

            console.log('[Background] Configuration saved and updated', connectionConfig);
            sendResponse({ success: true, config: connectionConfig });
            break;
          }
          case 'SYNC_COOKIES': {
            console.log('[Background] Manual cookie sync requested');
            const result = await cookieSyncService.syncNow();
            sendResponse({ success: !result.isError, result });
            break;
          }
          case 'METRICS_CAPTURED': {
            console.log('[Background] Metrics captured from content script');
            const metricsResult = await metricsCollectorService.handleCapturedMetrics(request.payload);
            sendResponse(metricsResult);
            break;
          }
          case TASK_EVENT_MESSAGE_TYPE: {
            // V2: Generic event entry point from content_script relay
            const taskEvent = (request as TaskEventMessage).event;
            if (taskEvent) {
              taskRuntimeManager.handleTaskEvent(taskEvent, sender.tab?.id);
            }
            sendResponse({ success: true });
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
      console.log('Extension installed');
    } else if (details.reason === 'update') {
      console.log('Extension updated');
    }
  });
});

function handleToolResult(payload: any) {
  console.log('Tool result:', payload);
  chrome.runtime.sendMessage({ type: 'TOOL_RESULT_UPDATE', payload }).catch(() => {
    // No listeners registered
  });
}
