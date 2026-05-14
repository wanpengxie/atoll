/**
 * 小红书相关常量
 */
export const XIAOHONGSHU_URLS = {
  HOME: 'https://www.xiaohongshu.com',
  CREATOR: 'https://creator.xiaohongshu.com',
  PUBLISH: 'https://creator.xiaohongshu.com/publish/publish',
  PROFILE: 'https://www.xiaohongshu.com/user/profile/',
  EXPLORE: 'https://www.xiaohongshu.com/explore',
} as const;

/**
 * 工具名称常量（chrome.runtime.onMessage 内部 EXECUTE_TOOL 仍使用，
 * 仅作 background 内部 dispatch；不再对外暴露旧 upstream backend 派发协议）。
 */
export const XIAOHONGSHU_TOOL_NAMES = {
  CHECK_LOGIN_STATUS: 'xhs_check_login_status',
  PUBLISH_CONTENT: 'xhs_publish_content',
  PUBLISH_LONG_CONTENT: 'xhs_publish_long_content',
  SEARCH_FEEDS: 'xhs_search_feeds',
  INJECT_SCRIPT: 'xhs_inject_script',
  READ_PAGE_DATA: 'xhs_read_page_data',
  SYNC_COOKIES: 'xhs_sync_cookies',
  GET_NOTE: 'xhs_get_note',
  GET_NOTE_COMMENTS: 'xhs_get_note_comments',
  GET_TRENDING_TOPICS: 'xhs_get_trending_topics',
  ANALYZE_MY_PROFILE: 'xhs_analyze_my_profile',
  ANALYZE_PROFILE: 'xhs_analyze_profile',
  PUBLISH_STATUS: 'xhs_publish_status',
  GET_MY_RECENT: 'xhs_get_my_recent',
} as const;

/**
 * Chrome 插件常量。
 *
 * M1.1-T2 起 extension 直接连 coagent daemon 的 device 通道，不再走旧 upstream backend 链路。
 * STORAGE_KEYS / 默认值同步为 device 形态。
 */
export const EXTENSION_CONSTANTS = {
  NATIVE_HOST_NAME: 'ai.coagent.xhs.device',
  STORAGE_KEYS: {
    /** 上一次连接快照（serverUrl/connected/lastError）。 */
    SERVER_STATUS: 'coagent_device_status',
    /** Device 配置：daemon WS / HTTP base / api key / device id / userId。 */
    CONNECTION_CONFIG: 'coagent_device_config',
  },
  /** Default coagent daemon HTTP port（WS 与 HTTP 单端口共享，via /device/{id} upgrade）。 */
  DEFAULT_DAEMON_HTTP_PORT: 9501,
  /** Default coagent daemon WS path prefix；deviceId 由 storage 读取拼接。 */
  DEFAULT_DEVICE_WS_PATH_PREFIX: '/device/',
} as const;

/**
 * Coagent device 协议常量（与 spec §四 align）。
 */
export const COAGENT_DEVICE_PROTOCOL = {
  /** WS frame from daemon → extension. */
  COMMAND_FRAME_TYPE: 'command',
  /** Optional WS frame from extension → daemon (heartbeat-style). */
  ACK_FRAME_TYPE: 'ack',
  /**
   * WS frame from extension → daemon, after a reconnect, carrying the entries
   * accumulated in `chrome.storage.local[CALLBACK_OUTBOX_STORAGE_KEY]` while
   * the connection was down. Daemon dispatches each payload through
   * `deviceCallback` with built-in dedupe (M1.1 Fix-T3).
   * Payload shape: { type: 'callback_replay', payloads: CallbackBody[] }.
   */
  CALLBACK_REPLAY_FRAME_TYPE: 'callback_replay',
  /**
   * R3-T3 (FX5) — daemon → extension ack for a `callback_replay` batch.
   * Daemon pushes `{type:'callback_replay_ack', accepted:string[],
   * rejected:Array<{correlation_id,code,message}>}`. Extension only deletes
   * outbox entries listed in `accepted` ∪ `rejected.map(r=>r.correlation_id)`.
   */
  CALLBACK_REPLAY_ACK_FRAME_TYPE: 'callback_replay_ack',
  /** Final result is delivered via HTTP callback, not WS. */
  CALLBACK_PATH_PREFIX: '/api/device/',
  /** Cookie / login_state sync endpoint suffix. */
  CALLBACK_PATH_SUFFIX_CALLBACK: '/callback',
  CALLBACK_PATH_SUFFIX_SESSION: '/session',
  /** Reconnect backoff: 1s, 2s, 4s, 8s, 16s, ... up to MAX. */
  RECONNECT_BASE_MS: 1_000,
  RECONNECT_MAX_MS: 30_000,
  /** Default heartbeat from daemon side is 30s; client doesn't ping. */
  COMMAND_DISPATCH_TIMEOUT_MS: 180_000,
  /** Per-attempt callback HTTP timeout (AbortController). */
  CALLBACK_RETRY_TIMEOUT_MS: 10_000,
  /** Backoff schedule between retry attempts (3 retries: 1s/2s/4s). */
  CALLBACK_RETRY_BACKOFF_MS_LIST: [1_000, 2_000, 4_000] as readonly number[],
  /** chrome.storage.local key used to stash callbacks pending replay. */
  CALLBACK_OUTBOX_STORAGE_KEY: 'coagent_device_pending_callbacks',
  /** Hard cap on outbox size — drops oldest entries beyond this. */
  CALLBACK_OUTBOX_MAX_SIZE: 200,
  /**
   * R3-T3 (FX5) — outbox entries older than this without ever receiving a
   * daemon ack are GC'd (logged via `device.callback.outbox.dropped`). Bounds
   * unbounded growth when the daemon never replies (e.g. wedged service).
   */
  CALLBACK_OUTBOX_MAX_AGE_MS: 7 * 24 * 60 * 60 * 1000, // 7 days
} as const;

/**
 * 5 个 device cmd 名称（spec §6.2.3）。
 */
export const COAGENT_DEVICE_COMMANDS = {
  PUBLISH: 'publish',
  SEARCH: 'search',
  GET_MY_RECENT: 'get-my-recent',
  GET_NOTE: 'get-note',
  PUBLISH_STATUS: 'publish-status',
} as const;

export type CoagentDeviceCommand =
  (typeof COAGENT_DEVICE_COMMANDS)[keyof typeof COAGENT_DEVICE_COMMANDS];

/**
 * WebSocket 通信常量
 */
export const SOCKET_MESSAGE_TIMEOUT_MS = 180_000; // 3分钟，足够发布任务完成

/**
 * 错误消息
 */
export const ERROR_MESSAGES = {
  NATIVE_HOST_NOT_CONNECTED: 'Native host not connected',
  TOOL_NOT_FOUND: 'Tool not found',
  TOOL_EXECUTION_FAILED: 'Tool execution failed',
  NOT_LOGGED_IN: 'User not logged in to XiaoHongShu',
  PAGE_LOAD_TIMEOUT: 'Page load timeout',
  ELEMENT_NOT_FOUND: 'Required element not found on page',
  INVALID_ARGUMENTS: 'Invalid arguments provided',
  NETWORK_ERROR: 'Network error occurred',
  DEVICE_NOT_CONFIGURED: 'Device not configured (daemon URL / api key / device id missing)',
  DEVICE_CALLBACK_FAILED: 'Device callback POST to daemon failed',
} as const;

/**
 * 成功消息
 */
export const SUCCESS_MESSAGES = {
  TOOL_EXECUTED: 'Tool executed successfully',
  CONTENT_PUBLISHED: 'Content published successfully',
  COMMENT_POSTED: 'Comment posted successfully',
} as const;

/**
 * 超时设置(毫秒)
 */
export const TIMEOUTS = {
  PAGE_LOAD: 30000,
  ELEMENT_WAIT: 10000,
  NETWORK_REQUEST: 30000,
  TOOL_EXECUTION: 60000,
  NAVIGATION_WAIT: 30000,
  SCRIPT_INJECTION: 30000, // 30 秒，支持多标签逐字符输入
} as const;
