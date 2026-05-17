// Coagent device 连接配置 + 状态快照（chrome.storage.local 抽象层）。
//
// M1.1-T2 起 extension 直连 coagent daemon `/device/{deviceId}` WS，
// 不再连旧 upstream backend；本模块持有 device 配置：
//   - serverUrl          (= daemon WS URL，例 ws://127.0.0.1:9501/device/{deviceId})
//   - apiKey             (= device api key，header 与 WS 鉴权共用)
//   - daemonHttpBase     (= daemon HTTP base，例 http://127.0.0.1:9501)
//   - deviceId           (= 设备唯一 ID；与 WS path 中的 deviceId 一致)
//   - userId             (= 当前主人 user_id；session sync / callback 上报使用)
//   - coagentServerUrl   (M1.2-T3) coagent server URL，popup 主入口调
//                        `/api/device/resolve` 反查 device 全套配置时使用。
//   - channelId/daemonId (M1.2-T3) resolve 返回的元数据，备用。
//   - wsUrl/httpBase     (M1.2-T3) 描述 storage 形态对齐用的别名字段；运行时
//                        coagent-device.ts/sync-cookies.ts 仍然只读 serverUrl/
//                        daemonHttpBase 这两个 canonical 字段；写入侧把同样的
//                        值再以 wsUrl/httpBase 写一份方便外部读取与未来 rename。
//
// `serverUrl` / `apiKey` 命名沿用，方便最少改动 tools/sync-cookies.ts 等下游；
// 在 phase-5 cookie sync 完成后这两个字段语义统一为 device 形态。
//
// 默认 serverUrl 留空：未配置时 service 不自动连接。

import { EXTENSION_CONSTANTS } from 'coagent-xhs-shared';

const DEFAULT_DAEMON_HTTP_BASE = 'http://127.0.0.1:9501';
/**
 * M1.2-T3 — popup 主入口默认 coagent server URL。`https://coagent-server`
 * 作为 placeholder；用户必须改成真实部署域名才能让 resolve 调用走通。
 */
const DEFAULT_COAGENT_SERVER_URL = 'https://coagent-server';

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

  // ── M1.2-T3 新增字段 ────────────────────────────────────────────────────
  /**
   * Coagent server URL（popup 主入口走 1-key 流程时使用）。
   * 例 `https://coagent.example.com`。POST `${coagentServerUrl}/api/device/resolve`
   * 反查 device 全套配置（ws_url/http_url/device_id/user_id/channel_id/daemon_id）。
   *
   * 注意：与 `serverUrl`（= daemon WS URL）不是同一个东西。
   *   - coagentServerUrl: coagent backend HTTP，每次 popup connect 时被请求。
   *   - serverUrl:        daemon WS，service worker 长连保持。
   */
  coagentServerUrl?: string;
  /**
   * `serverUrl` 的别名（与 ticket 描述里给的 storage shape 对齐）。
   * 写入时双写以便未来 reader 直接读 `wsUrl`；运行时 coagent-device.ts 仍然
   * 读 `serverUrl` 作为 canonical 源。
   */
  wsUrl?: string;
  /**
   * `daemonHttpBase` 的别名（与 ticket 描述里给的 storage shape 对齐）。
   * 写入时双写；运行时 coagent-device.ts/sync-cookies.ts 仍然读
   * `daemonHttpBase` 作为 canonical 源。
   */
  httpBase?: string;
  /** Resolve 返回的 channel_id 元数据（暂时不被运行时消费，方便 debug）。 */
  channelId?: string;
  /** Resolve 返回的 daemon_id 元数据（暂时不被运行时消费，方便 debug）。 */
  daemonId?: string;

  // ── T147 §A-E v4 server-devicebus 协议字段 ─────────────────────────────
  /**
   * Coagent server `wss://{server}/devicebus` 基础 URL（不带 query；
   * `session_id` / `token` 由客户端追加）。当此字段存在时，background
   * 启用 v4 client（coagentServerDeviceClient）；缺失时仍走 legacy
   * daemon-direct client 兼容旧部署。
   */
  serverWsEndpoint?: string;
  /** server.devicebus 分配的 device_session_id。 */
  deviceSessionId?: string;
  /** server.devicebus 签发的 bearer token（24h TTL）。 */
  deviceSessionToken?: string;
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
  // M1.2-T3：主入口 coagent server URL 默认值（用户必须改成真实域名）。
  coagentServerUrl: DEFAULT_COAGENT_SERVER_URL,
  wsUrl: '',
  httpBase: DEFAULT_DAEMON_HTTP_BASE,
  channelId: '',
  daemonId: '',
  // T147 §A-E：v4 字段默认空；popup / resolve API 在 issueSession 后写入。
  serverWsEndpoint: '',
  deviceSessionId: '',
  deviceSessionToken: '',
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
 * M1.2-T3 — 默认 coagent server URL（popup 主入口的 placeholder）。
 *
 * `https://coagent-server` 是 placeholder，用户必须改成真实部署域名
 * （例 `https://coagent.example.com`）才能让 `/api/device/resolve` 调用走通。
 */
export function getDefaultCoagentServerUrl(): string {
  return DEFAULT_COAGENT_SERVER_URL;
}
