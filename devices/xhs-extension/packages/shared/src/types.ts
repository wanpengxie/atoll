/**
 * Native Messaging消息类型
 */
export enum NativeMessageType {
  CALL_TOOL = 'call_tool',
  PROCESS_DATA = 'process_data',
  SERVER_STARTED = 'server_started',
  SERVER_STOPPED = 'server_stopped',
  ERROR_FROM_NATIVE_HOST = 'error_from_native_host',
}

/**
 * WebSocket消息类型
 */
export enum SocketMessageType {
  HELLO = 'hello',
  TOOL_CALL = 'tool_call',
  TOOL_RESULT = 'tool_result',
  STATUS = 'status',
  PING = 'ping',
  PONG = 'pong',
  ERROR = 'error',
  // V2 Event protocol
  EVENT = 'event',
  EVENT_ACK = 'event_ack',
  TASK_CONTROL = 'task_control',
}

/**
 * 连接模式
 */
export enum ConnectionMode {
  WEBSOCKET = 'websocket',
  NATIVE = 'native',
}

/**
 * 工具执行结果
 */
export interface ToolResult {
  content: Array<{
    type: 'text' | 'image' | 'error';
    text?: string;
    data?: string;
  }>;
  isError?: boolean;
}

/**
 * 小红书内容发布参数
 */
export interface PublishContentArgs {
  title: string;
  content: string;
  images: Array<{
    type: 'url' | 'data';
    value: string;
    fileName?: string;
  }>;
  tags?: string[];
  publish_at?: string; // 定时发布时间（可选），如 2026-03-01 20:30 或 RFC3339
  auto_publish?: boolean; // 是否自动点击发布（默认 true）
}

/**
 * 小红书长文发布参数（固定走“写长文”模式）
 */
export interface PublishLongContentArgs {
  title: string;
  content: string;
  description?: string;
  publish_at?: string; // 定时发布时间（可选），如 2026-03-01 20:30 或 RFC3339
  auto_publish?: boolean; // 是否自动点击发布（默认 true）
}

/**
 * Feed数据结构
 */
export interface Feed {
  id: string;
  title: string;
  content: string;
  author: string;
  likes: number;
  comments: number;
  xsecToken: string;
  createTime: string;
  coverImage?: string;
  url?: string;
}

/**
 * 登录状态响应
 */
export interface LoginStatusResponse {
  isLoggedIn: boolean;
  userName?: string;
  userId?: string;
}

/**
 * Feed详情
 */
export interface FeedDetail extends Feed {
  images: string[];
  commentList: Comment[];
  shareCount: number;
  collectCount: number;
}

/**
 * 评论数据结构
 */
export interface Comment {
  id: string;
  content: string;
  author: string;
  authorAvatar?: string;
  createTime: string;
  likes: number;
  subComments?: Comment[];
}

/**
 * Native消息请求
 */
export interface NativeRequest {
  type: NativeMessageType;
  requestId: string;
  payload: any;
}

/**
 * Native消息响应
 */
export interface NativeResponse {
  responseToRequestId: string;
  payload: {
    status: 'success' | 'error';
    message?: string;
    data?: any;
    error?: string;
  };
}

/**
 * WebSocket消息结构
 */
export interface SocketMessage {
  type: SocketMessageType;
  requestId?: string;
  responseToRequestId?: string;
  payload?: any;
}

/**
 * Chrome Extension 执行环境
 */
export enum ExecutionWorld {
  ISOLATED = 'ISOLATED', // Content Script 隔离环境
  MAIN = 'MAIN', // 页面 JavaScript 环境
}

// ─── V2 Event Protocol Types ────────────────────────────────────────────────

/**
 * 页面脚本发出的统一异步事件 envelope。
 * 页面通过 window.postMessage({ type: 'DUMAS_ASYNC_EVENT', ... }) 发出，
 * 常驻 content_script 中继到 Background。
 */
export interface DumasAsyncEvent {
  /** 全局唯一事件 ID（页面脚本生成，用于 ACK 幂等） */
  eventId: string;
  /** 关联的任务 ID */
  taskId: string;
  /** 事件序号（同一 taskId 内递增，用于排序） */
  seq: number;
  /** 业务事件类型（如 "publish_clicked", "monitor_detected" 等） */
  eventType: string;
  /** 业务载荷，不透明 JSON */
  payload: Record<string, unknown>;
  /** 事件产生的 Unix 毫秒时间戳 */
  timestamp: number;
  /** 是否需要后端 ACK（默认 true） */
  needAck?: boolean;
}

/**
 * 页面 postMessage 的完整格式。
 * content_script 过滤 event.data.type === 'DUMAS_ASYNC_EVENT'。
 */
export interface DumasAsyncEventEnvelope {
  type: 'DUMAS_ASYNC_EVENT';
  /** 事件内容 */
  event: DumasAsyncEvent;
}

/**
 * 页面 runtime 控制消息（由 Background 下发，经 content_script 桥接）。
 */
export interface DumasTaskControlEnvelope {
  type: 'DUMAS_TASK_CONTROL';
  control: {
    taskId: string;
    action: 'cancel' | 'pause' | 'resume' | 'retry';
    payload?: Record<string, unknown>;
  };
}

/**
 * content_script → Background 的内部消息格式。
 */
export interface TaskEventMessage {
  type: 'TASK_EVENT';
  /** 透传的异步事件 */
  event: DumasAsyncEvent;
}

/**
 * 后端下发的 EVENT_ACK 消息（通过 WebSocket）。
 * 对应后端 wsEnvelope { type: "event_ack", event_id, task_id }
 */
export interface EventAckMessage {
  type: SocketMessageType.EVENT_ACK;
  eventId: string;
  taskId: string;
}

/**
 * 后端下发的 TASK_CONTROL 消息。
 * 对应后端 wsEnvelope { type: "task_control", task_id, action, payload }
 */
export interface TaskControlMessage {
  type: SocketMessageType.TASK_CONTROL;
  taskId: string;
  action: 'cancel' | 'pause' | 'resume' | 'retry';
  payload?: Record<string, unknown>;
}

/**
 * Extension → 后端的 event_ack 上报格式（确认收到后端下发的 EVENT）。
 */
export interface EventAckOutbound {
  type: 'event_ack';
  eventId: string;
  taskId: string;
}

/**
 * pendingAck 队列中的条目。
 * 包含原始事件和重试元数据。
 */
export interface PendingAckEntry {
  /** 原始事件 */
  event: DumasAsyncEvent;
  /** 已重试次数 */
  retryCount: number;
  /** 下次重试的 Unix 毫秒时间戳 */
  nextRetryAt: number;
  /** 首次入队的 Unix 毫秒时间戳 */
  enqueuedAt: number;
}
