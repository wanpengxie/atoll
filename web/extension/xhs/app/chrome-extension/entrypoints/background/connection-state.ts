// Coagent xhs extension 连接配置 + 状态快照（chrome.storage.local 抽象层）。
//
// T178/T5 起 extension 只连接本机 coagent-proxy daemon：
//   - connectionMode = "proxy"
//   - proxyEndpoint  = ws://127.0.0.1:10387
//   - deviceActorId  = 本机 proxy module actor id，默认 tool:xhs
//
// serverUrl / apiKey / daemonHttpBase / wsUrl / httpBase 是 cookie-sync
// 兼容字段，不能驱动 device transport 选择。

import { EXTENSION_CONSTANTS } from 'coagent-xhs-shared';

const DEFAULT_DAEMON_HTTP_BASE = 'http://127.0.0.1:9501';
const DEFAULT_PROXY_ENDPOINT = 'ws://127.0.0.1:10387';
export const DEFAULT_PROXY_ACTOR_ID = 'tool:xhs';

export type ConnectionMode = 'proxy';

/** Device 连接状态快照（持久化到 chrome.storage.local；popup 也读它）。 */
export interface ExtensionConnectionStatus {
  connected: boolean;
  serverUrl?: string;
  reconnecting?: boolean;
  lastError?: string;
  lastUpdated: number;
}

/** Device 配置；popup 写、background proxy client 读。 */
export interface ConnectionConfig {
  /** Cookie-sync compatibility field; not used for transport selection. */
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

  /** Retired resolve-flow compatibility field; ignored by transport selection. */
  coagentServerUrl?: string;
  /**
   * `serverUrl` 的别名（与 ticket 描述里给的 storage shape 对齐）。
   * 写入时双写以便未来 reader 直接读 `wsUrl`。
   */
  wsUrl?: string;
  /**
   * `daemonHttpBase` 的别名（与 ticket 描述里给的 storage shape 对齐）。
   * 写入时双写；sync-cookies 仍然读 `daemonHttpBase` 作为 canonical 源。
   */
  httpBase?: string;
  /** Resolve 返回的 channel_id 元数据（暂时不被运行时消费，方便 debug）。 */
  channelId?: string;
  /** Resolve 返回的 daemon_id 元数据（暂时不被运行时消费，方便 debug）。 */
  daemonId?: string;

  /** Local proxy actor id selected by popup/user action. */
  deviceActorId?: string;
  /** Explicit proxy transport mode selected by popup/user action. */
  connectionMode?: ConnectionMode;
  /** Local proxy daemon endpoint. Defaults to ws://127.0.0.1:10387. */
  proxyEndpoint?: string;
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
  coagentServerUrl: '',
  wsUrl: '',
  httpBase: DEFAULT_DAEMON_HTTP_BASE,
  channelId: '',
  daemonId: '',
  // T7 UX: ext defaults to proxy mode so a freshly-loaded extension
  // auto-connects to the local proxy daemon on background boot — no
  // popup / web-UI button press required. The user only sees a button
  // when they need to override (different port / dev work).
  connectionMode: 'proxy',
  deviceActorId: DEFAULT_PROXY_ACTOR_ID,
  proxyEndpoint: DEFAULT_PROXY_ENDPOINT,
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
  // M1.2-T3：双向镜像 serverUrl/wsUrl 与 daemonHttpBase/httpBase，保证存储里
  // 两套字段始终一致，无论 patch 用哪一边写入。
  syncFieldAliases(updated, partial);
  await chrome.storage.local.set({
    [EXTENSION_CONSTANTS.STORAGE_KEYS.CONNECTION_CONFIG]: updated,
  });
  return updated;
}

/**
 * 把 `serverUrl ↔ wsUrl`、`daemonHttpBase ↔ httpBase` 双写镜像。
 *
 * 规则：
 *   - 如果 patch 里只显式写了别名（wsUrl / httpBase），把它复制到 canonical 字段。
 *   - 如果 patch 里只显式写了 canonical（serverUrl / daemonHttpBase），把它复制到别名。
 *   - 两边都写：以 patch 显式提供为准，canonical 优先（避免 popup 同时填两份不一致）。
 *
 * 仅在 partial 里的字段触发同步；纯加载场景由 getConnectionConfig 的 fallback 处理。
 */
function syncFieldAliases(
  updated: ConnectionConfig,
  partial: Partial<ConnectionConfig>,
): void {
  const hasServerUrl = Object.prototype.hasOwnProperty.call(partial, 'serverUrl');
  const hasWsUrl = Object.prototype.hasOwnProperty.call(partial, 'wsUrl');
  if (hasServerUrl && !hasWsUrl) {
    updated.wsUrl = updated.serverUrl;
  } else if (hasWsUrl && !hasServerUrl) {
    updated.serverUrl = String(updated.wsUrl ?? '');
  }
  const hasHttpBase = Object.prototype.hasOwnProperty.call(partial, 'daemonHttpBase');
  const hasHttpBaseAlias = Object.prototype.hasOwnProperty.call(partial, 'httpBase');
  if (hasHttpBase && !hasHttpBaseAlias) {
    updated.httpBase = updated.daemonHttpBase;
  } else if (hasHttpBaseAlias && !hasHttpBase) {
    updated.daemonHttpBase = updated.httpBase;
  }
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

/**
/** Retired resolve-flow placeholder. Kept for compatibility with older tests/tools. */
export function getDefaultCoagentServerUrl(): string {
  return '';
}

export function getDefaultProxyEndpoint(): string {
  return DEFAULT_PROXY_ENDPOINT;
}
