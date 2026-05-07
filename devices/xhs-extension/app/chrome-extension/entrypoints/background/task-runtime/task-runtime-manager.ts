/**
 * TaskRuntimeManager
 *
 * Background 通用事件处理入口。接收 content_script 中继的 TASK_EVENT，
 * 通过 WebSocket sendEvent 发往后端，管理 pendingAck 队列。
 *
 * 职责：
 * 1. 接收 TASK_EVENT 消息并路由
 * 2. 通过 sendEvent 发送到后端
 * 3. 管理 pendingAck 队列（ACK 确认、重试、持久化）
 * 4. SW 重启后恢复 pendingAck 并继续重发
 */

import type { DumasAsyncEvent } from 'xiaohongshu-mcp-shared';
import { PendingAckQueue } from './pending-ack-queue';
import { savePendingAckQueue, restorePendingAckQueue } from './ack-persistence';

type TaskControlAction = 'pause' | 'resume' | 'cancel' | 'retry';
type TaskRuntimeStatus = 'running' | 'paused' | 'cancelled';

interface TaskRuntimeState {
  taskId: string;
  tabId?: number;
  taskType?: string;
  mode?: string;
  status: TaskRuntimeStatus;
  lastSeq: number;
  lastEventType: string;
  lastEventAt: number;
  lastHeartbeatAt?: number;
}

export class TaskRuntimeManager {
  private pendingAck: PendingAckQueue;
  private runtimes: Map<string, TaskRuntimeState>;
  private sendEventWs: ((event: DumasAsyncEvent) => boolean) | null = null;
  private persistDebounceTimer: ReturnType<typeof setTimeout> | null = null;

  constructor() {
    this.pendingAck = new PendingAckQueue();
    this.runtimes = new Map<string, TaskRuntimeState>();

    this.pendingAck.setSendFn((event) => this.doSendEvent(event));
    this.pendingAck.setOnDropped((event, reason) => {
      console.warn('[TaskRuntimeManager] Event dropped', {
        eventId: event.eventId,
        taskId: event.taskId,
        reason,
      });
      this.emitDroppedEvent(event, reason);
      this.debouncedPersist();
    });
  }

  /**
   * Bind the WebSocket send function.
   * Call this after WebSocketClient is initialized, and again on reconnect.
   */
  bindSendEvent(fn: (event: DumasAsyncEvent) => boolean): void {
    this.sendEventWs = fn;
    this.pendingAck.setSendFn((event) => this.doSendEvent(event));
  }

  /**
   * Initialize: restore pending queue from storage.
   * Call this on SW startup.
   */
  async initialize(): Promise<void> {
    const entries = await restorePendingAckQueue();
    if (entries.length > 0) {
      this.pendingAck.restore(entries);
    }
  }

  /**
   * Handle incoming TASK_EVENT from content_script relay.
   * This is the single entry point for all async events.
   */
  handleTaskEvent(event: DumasAsyncEvent, tabId?: number): void {
    if (!event.eventId || !event.taskId) {
      console.warn('[TaskRuntimeManager] Invalid event, missing eventId/taskId', event);
      return;
    }

    const runtime = this.ensureRuntime(event.taskId, tabId);
    this.updateRuntimeFromEvent(runtime, event, tabId);

    if (runtime.status === 'cancelled') {
      console.info('[TaskRuntimeManager] Ignore event from cancelled runtime', {
        taskId: event.taskId,
        eventId: event.eventId,
      });
      return;
    }

    if (runtime.status === 'paused') {
      console.info('[TaskRuntimeManager] Runtime paused, skip sending event', {
        taskId: event.taskId,
        eventId: event.eventId,
        eventType: event.eventType,
      });
      return;
    }

    console.info('[TaskRuntimeManager] Processing TASK_EVENT', {
      eventId: event.eventId,
      taskId: event.taskId,
      eventType: event.eventType,
      seq: event.seq,
      tabId: runtime.tabId,
    });

    // Send immediately
    const sent = this.doSendEvent(event);

    // Add to pending queue for ACK tracking (whether sent or not)
    const needAck = event.needAck !== false; // default true
    if (needAck) {
      this.pendingAck.add(event);
      this.debouncedPersist();
    }

    if (!sent) {
      console.warn('[TaskRuntimeManager] Event queued (WS not connected)', {
        eventId: event.eventId,
      });
    }
  }

  /**
   * Handle TASK_CONTROL from backend.
   * This updates local runtime state and forwards control to page runtime via content_script relay.
   */
  handleTaskControl(taskId: string, action: string, payload?: Record<string, unknown>): void {
    const normalizedTaskId = (taskId || '').trim();
    const normalizedAction = (action || '').trim().toLowerCase() as TaskControlAction;
    if (!normalizedTaskId || !normalizedAction) {
      return;
    }
    if (
      normalizedAction !== 'pause' &&
      normalizedAction !== 'resume' &&
      normalizedAction !== 'cancel' &&
      normalizedAction !== 'retry'
    ) {
      console.warn('[TaskRuntimeManager] Ignore unknown TASK_CONTROL action', {
        taskId: normalizedTaskId,
        action,
      });
      return;
    }

    const runtime = this.ensureRuntime(normalizedTaskId);

    switch (normalizedAction) {
      case 'pause':
        runtime.status = 'paused';
        break;
      case 'resume':
        runtime.status = 'running';
        break;
      case 'cancel': {
        runtime.status = 'cancelled';
        const removed = this.pendingAck.removeByTaskId(normalizedTaskId);
        if (removed > 0) {
          this.debouncedPersist();
        }
        break;
      }
      case 'retry':
        runtime.status = 'running';
        this.pendingAck.retryAllNow();
        break;
      default:
        break;
    }

    this.forwardTaskControlToPage(runtime, normalizedTaskId, normalizedAction, payload);

    console.info('[TaskRuntimeManager] TASK_CONTROL handled', {
      taskId: normalizedTaskId,
      action: normalizedAction,
      tabId: runtime.tabId,
      status: runtime.status,
    });
  }

  /**
   * Handle EVENT_ACK from backend (via WebSocket).
   */
  handleEventAck(eventId: string): void {
    const removed = this.pendingAck.ack(eventId);
    if (removed) {
      this.debouncedPersist();
    }
  }

  /**
   * Called when WebSocket reconnects. Retry all pending events.
   */
  onReconnect(): void {
    console.info('[TaskRuntimeManager] WebSocket reconnected, retrying pending events', {
      count: this.pendingAck.size,
    });
    this.pendingAck.retryAllNow();
  }

  /**
   * Get current pending queue size (for diagnostics).
   */
  get pendingCount(): number {
    return this.pendingAck.size;
  }

  getRuntimeSnapshot(taskId: string): TaskRuntimeState | null {
    return this.runtimes.get(taskId) || null;
  }

  // ── Internal ──────────────────────────────────────────────

  private doSendEvent(event: DumasAsyncEvent): boolean {
    if (!this.sendEventWs) return false;
    return this.sendEventWs(event);
  }

  /**
   * Debounced persistence: batch saves within 500ms window.
   */
  private debouncedPersist(): void {
    if (this.persistDebounceTimer !== null) {
      clearTimeout(this.persistDebounceTimer);
    }
    this.persistDebounceTimer = setTimeout(() => {
      this.persistDebounceTimer = null;
      void savePendingAckQueue(this.pendingAck.getAll());
    }, 500);
  }

  private ensureRuntime(taskId: string, tabId?: number): TaskRuntimeState {
    const existing = this.runtimes.get(taskId);
    if (existing) {
      if (typeof tabId === 'number' && tabId > 0) {
        existing.tabId = tabId;
      }
      return existing;
    }

    const created: TaskRuntimeState = {
      taskId,
      tabId,
      status: 'running',
      lastSeq: 0,
      lastEventType: '',
      lastEventAt: Date.now(),
    };
    this.runtimes.set(taskId, created);
    return created;
  }

  private updateRuntimeFromEvent(runtime: TaskRuntimeState, event: DumasAsyncEvent, tabId?: number): void {
    runtime.lastSeq = event.seq;
    runtime.lastEventType = event.eventType;
    runtime.lastEventAt = Date.now();
    if (typeof tabId === 'number' && tabId > 0) {
      runtime.tabId = tabId;
    }
    if (event.eventType === 'heartbeat') {
      runtime.lastHeartbeatAt = event.timestamp || Date.now();
    }

    const payload = event.payload || {};
    const rawTaskType = payload['taskType'];
    const rawMode = payload['mode'];
    const payloadTaskType = typeof rawTaskType === 'string' ? rawTaskType.trim() : '';
    const payloadMode = typeof rawMode === 'string' ? rawMode.trim() : '';
    if (payloadTaskType) {
      runtime.taskType = payloadTaskType;
    }
    if (payloadMode) {
      runtime.mode = payloadMode;
    }
  }

  private forwardTaskControlToPage(
    runtime: TaskRuntimeState,
    taskId: string,
    action: TaskControlAction,
    payload?: Record<string, unknown>
  ): void {
    if (typeof runtime.tabId !== 'number' || runtime.tabId <= 0) {
      return;
    }

    chrome.tabs
      .sendMessage(runtime.tabId, {
        type: 'TASK_CONTROL',
        taskId,
        action,
        payload: payload || {},
      })
      .catch((error) => {
        console.warn('[TaskRuntimeManager] Failed to forward TASK_CONTROL to page', {
          taskId,
          action,
          tabId: runtime.tabId,
          error: error instanceof Error ? error.message : String(error),
        });
      });
  }

  private emitDroppedEvent(event: DumasAsyncEvent, reason: 'max_retries' | 'overflow'): void {
    const now = Date.now();
    const droppedEvent: DumasAsyncEvent = {
      eventId: `drop_${event.eventId}_${now}`,
      taskId: event.taskId,
      seq: event.seq + 1,
      eventType: reason === 'overflow' ? 'event.dropped_overflow' : 'event.dropped',
      payload: {
        reason,
        originalEventId: event.eventId,
        originalEventType: event.eventType,
      },
      timestamp: now,
      needAck: false,
    };

    const sent = this.doSendEvent(droppedEvent);
    if (!sent) {
      console.warn('[TaskRuntimeManager] Failed to emit dropped-event telemetry (WS disconnected)', {
        taskId: event.taskId,
        eventId: event.eventId,
        reason,
      });
    }
  }
}
