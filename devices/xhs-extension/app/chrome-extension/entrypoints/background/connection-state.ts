import { EXTENSION_CONSTANTS } from 'xiaohongshu-mcp-shared';

// 根据环境设置默认 WebSocket 地址
// const DEFAULT_WS_URL = import.meta.env.MODE === 'production'
//   ? 'ws://154.8.160.25:18040/ws'
//   : 'ws://127.0.0.1:18040/ws';

const DEFAULT_WS_URL = 'ws://127.0.0.1:18040/ws';

export interface ExtensionConnectionStatus {
  connected: boolean;
  serverUrl?: string;
  reconnecting?: boolean;
  lastError?: string;
  lastUpdated: number;
}

export interface ConnectionConfig {
  serverUrl: string;
  autoReconnect: boolean;
  apiKey?: string; // API Key for multi-tenant authentication
}

const DEFAULT_STATUS: ExtensionConnectionStatus = {
  connected: false,
  serverUrl: DEFAULT_WS_URL,
  lastUpdated: 0,
};

const DEFAULT_CONFIG: ConnectionConfig = {
  serverUrl: DEFAULT_WS_URL,
  autoReconnect: true,
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

  return {
    ...DEFAULT_STATUS,
    ...status,
  };
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

  return {
    ...DEFAULT_CONFIG,
    ...config,
  };
}

export async function saveConnectionConfig(
  partial: Partial<ConnectionConfig>
): Promise<ConnectionConfig> {
  const current = await getConnectionConfig();
  const updated: ConnectionConfig = {
    ...current,
    ...partial,
  };

  await chrome.storage.local.set({
    [EXTENSION_CONSTANTS.STORAGE_KEYS.CONNECTION_CONFIG]: updated,
  });

  return updated;
}

export function broadcastConnectionStatus(status: ExtensionConnectionStatus): void {
  chrome.runtime
    .sendMessage({
      type: 'SERVER_STATUS_CHANGED',
      payload: status,
    })
    .catch(() => {
      // ignore when no listeners are registered
    });
}

export function getDefaultWebSocketUrl(): string {
  return DEFAULT_WS_URL;
}
