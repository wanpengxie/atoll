// Coagent device 连接配置 + 状态快照（chrome.storage.local 抽象层）。
//
// M1.1-T2 起 extension 直连 coagent daemon `/device/{deviceId}` WS，
// 不再连 1studio backend；本模块持有 device 配置：
//   - serverUrl       (= daemon WS URL，例 ws://127.0.0.1:9501/device/{deviceId})
//   - apiKey          (= device api key，header 与 WS 鉴权共用)
//   - daemonHttpBase  (= daemon HTTP base，例 http://127.0.0.1:9501)
//   - deviceId        (= 设备唯一 ID；与 WS path 中的 deviceId 一致)
//   - userId          (= 当前主人 user_id；session sync / callback 上报使用)
//
// `serverUrl` / `apiKey` 命名沿用，方便最少改动 tools/sync-cookies.ts 等下游；
// 在 phase-5 cookie sync 完成后这两个字段语义统一为 device 形态。
//
// 默认 serverUrl 留空：未配置时 service 不自动连接。

import { EXTENSION_CONSTANTS } from 'xiaohongshu-mcp-shared';

const DEFAULT_DAEMON_HTTP_BASE = 'http://127.0.0.1:9501';

/** Device 连接状态快照（持久化到 chrome.storage.local；popup 也读它）。 */
export interface ExtensionConnectionStatus {
  connected: boolean;
  serverUrl?: string;
  reconnecting?: boolean;
  lastError?: string;
  lastUpdated: number;
}

/** Device 配置；popup 写、background coagent-device.ts 读。 */
export interface ConnectionConfig {
  /** Daemon WS URL，必填（未配置时 service 不连接）。 */
  serverUrl: string;
  /** WS 自动重连开关（默认 true）。 */
  autoReconnect: boolean;
  /** Device api key（WS query `?key=` + HTTP `Authorization: Bearer`）。 */
  apiKey?: string;
  /** Daemon HTTP base，例 http://127.0.0.1:9501（callback / session / 心跳上报）。 */
  daemonHttpBase?: string;
  /** Device 唯一 ID；用于 callback path 与 WS path。 */
  deviceId?: string;
  /** 主人 user_id，session sync / callback 携带；空则后端取默认。 */
  userId?: string;
}

const DEFAULT_STATUS: ExtensionConnectionStatus = {
  connected: false,
  serverUrl: '',
  lastUpdated: 0,
};

const DEFAULT_CONFIG: ConnectionConfig = {
  serverUrl: '',
  autoReconnect: true,
  apiKey: '',
  daemonHttpBase: DEFAULT_DAEMON_HTTP_BASE,
  deviceId: '',
  userId: '',
};

export async function getStoredConnectionStatus(): Promise<ExtensionConnectionStatus> {
  const stored = await chrome.storage.local.get(EXTENSION_CONSTANTS.STORAGE_KEYS.SERVER_STATUS);
  const status = stored[EXTENSION_CONSTANTS.STORAGE_KEYS.SERVER_STATUS] as
    | ExtensionConnectionStatus
    | undefined;

  if (!status) {
    const initialStatus: ExtensionConnectionStatus = {
      ...DEFAULT_STATUS,
      lastUpdated: Date.now(),
    };
    await chrome.storage.local.set({
      [EXTENSION_CONSTANTS.STORAGE_KEYS.SERVER_STATUS]: initialStatus,
    });
    return initialStatus;
  }

  return { ...DEFAULT_STATUS, ...status };
}

export async function saveConnectionStatus(
  partial: Partial<ExtensionConnectionStatus>
): Promise<ExtensionConnectionStatus> {
  const current = await getStoredConnectionStatus();
  const updated: ExtensionConnectionStatus = {
    ...current,
    ...partial,
    lastUpdated: Date.now(),
  };
  await chrome.storage.local.set({
    [EXTENSION_CONSTANTS.STORAGE_KEYS.SERVER_STATUS]: updated,
  });
  broadcastConnectionStatus(updated);
  return updated;
}

export async function getConnectionConfig(): Promise<ConnectionConfig> {
  const stored = await chrome.storage.local.get(EXTENSION_CONSTANTS.STORAGE_KEYS.CONNECTION_CONFIG);
  const config = stored[EXTENSION_CONSTANTS.STORAGE_KEYS.CONNECTION_CONFIG] as
    | ConnectionConfig
    | undefined;

  if (!config) {
    const initialConfig: ConnectionConfig = { ...DEFAULT_CONFIG };
    await chrome.storage.local.set({
      [EXTENSION_CONSTANTS.STORAGE_KEYS.CONNECTION_CONFIG]: initialConfig,
    });
    return initialConfig;
  }

  return { ...DEFAULT_CONFIG, ...config };
}

export async function saveConnectionConfig(
  partial: Partial<ConnectionConfig>
): Promise<ConnectionConfig> {
  const current = await getConnectionConfig();
  const updated: ConnectionConfig = { ...current, ...partial };
  await chrome.storage.local.set({
    [EXTENSION_CONSTANTS.STORAGE_KEYS.CONNECTION_CONFIG]: updated,
  });
  return updated;
}

export function broadcastConnectionStatus(status: ExtensionConnectionStatus): void {
  chrome.runtime
    .sendMessage({ type: 'SERVER_STATUS_CHANGED', payload: status })
    .catch(() => {
      // ignore when no listeners are registered
    });
}

/** 默认 daemon WS URL placeholder（popup 用以 hint 占位符）。 */
export function getDefaultWebSocketUrl(): string {
  return '';
}

/** 默认 daemon HTTP base（popup 用以 hint 占位符）。 */
export function getDefaultDaemonHttpBase(): string {
  return DEFAULT_DAEMON_HTTP_BASE;
}
