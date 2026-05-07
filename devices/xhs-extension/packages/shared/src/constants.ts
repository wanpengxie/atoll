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
 * 工具名称常量
 */
export const XIAOHONGSHU_TOOL_NAMES = {
  CHECK_LOGIN_STATUS: 'xhs_check_login_status',
  PUBLISH_CONTENT: 'xhs_publish_content',
  PUBLISH_LONG_CONTENT: 'xhs_publish_long_content',
  SEARCH_FEEDS: 'xhs_search_feeds',
  INJECT_SCRIPT: 'xhs_inject_script',
  READ_PAGE_DATA: 'xhs_read_page_data',
  SYNC_COOKIES: 'xhs_sync_cookies',
  GET_NOTE_COMMENTS: 'xhs_get_note_comments',
  GET_TRENDING_TOPICS: 'xhs_get_trending_topics',
  ANALYZE_MY_PROFILE: 'xhs_analyze_my_profile',
  ANALYZE_PROFILE: 'xhs_analyze_profile',
} as const;

/**
 * Chrome插件常量
 */
export const EXTENSION_CONSTANTS = {
  NATIVE_HOST_NAME: 'com.xiaohongshu.mcp',
  STORAGE_KEYS: {
    SERVER_STATUS: 'server_status',
    USER_CONFIG: 'user_config',
    AUTH_TOKEN: 'auth_token',
    CONNECTION_CONFIG: 'connection_config',
  },
  DEFAULT_PORT: 18040, // Go Backend 端口
  DEFAULT_WS_PATH: '/ws',
} as const;

/**
 * WebSocket通信常量
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
} as const;

/**
 * 成功消息
 */
export const SUCCESS_MESSAGES = {
  TOOL_EXECUTED: 'Tool executed successfully',
  SERVER_STARTED: 'MCP server started',
  SERVER_STOPPED: 'MCP server stopped',
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
  SCRIPT_INJECTION: 30000, // 增加到 30 秒，支持多标签逐字符输入
} as const;

// ─── V2 Event Protocol Constants ────────────────────────────────────────────

/**
 * ACK 超时时间（毫秒）。超过此时间未收到 ACK 则触发重试。
 */
export const ACK_TIMEOUT_MS = 5_000;

/**
 * 最大 ACK 重试次数。超过后事件标记为 dropped。
 */
export const MAX_ACK_RETRIES = 3;

/**
 * chrome.storage.local 中 pendingAck 队列的存储 key。
 */
export const PENDING_ACK_STORAGE_KEY = 'dumas_pending_ack_queue';

/**
 * pendingAck 队列最大容量。超过后新事件按 FIFO 丢弃最旧条目。
 */
export const MAX_PENDING_ACK_SIZE = 100;

/**
 * ACK 重试退避基数（毫秒）。实际间隔 = base * 2^retryCount，上限 MAX_ACK_RETRY_INTERVAL_MS。
 */
export const ACK_RETRY_BASE_MS = 5_000;

/**
 * ACK 重试退避上限（毫秒）。
 */
export const MAX_ACK_RETRY_INTERVAL_MS = 60_000;

/**
 * 页面 postMessage 事件类型标识。content_script 据此过滤。
 */
export const DUMAS_ASYNC_EVENT_TYPE = 'DUMAS_ASYNC_EVENT' as const;

/**
 * Background -> content_script -> 页面 runtime 的控制消息类型。
 */
export const DUMAS_TASK_CONTROL_TYPE = 'DUMAS_TASK_CONTROL' as const;

/**
 * content_script → Background 的内部消息类型。
 */
export const TASK_EVENT_MESSAGE_TYPE = 'TASK_EVENT' as const;
