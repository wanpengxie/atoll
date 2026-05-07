/**
 * PendingAckQueue
 *
 * 管理已发送但尚未收到 ACK 的事件。
 * 支持指数退避重试和容量上限。
 */

import type { DumasAsyncEvent, PendingAckEntry } from 'xiaohongshu-mcp-shared';
import {
  ACK_RETRY_BASE_MS,
  MAX_ACK_RETRY_INTERVAL_MS,
  MAX_ACK_RETRIES,
  MAX_PENDING_ACK_SIZE,
} from 'xiaohongshu-mcp-shared';

export type SendEventFn = (event: DumasAsyncEvent) => boolean;
export type OnEventDropped = (event: DumasAsyncEvent, reason: 'max_retries' | 'overflow') => void;

const RETRY_TICK_MS = 200;
const RETRY_BATCH_SIZE = 5;

export class PendingAckQueue {
  private queue: Map<string, PendingAckEntry> = new Map();
  private retryTimer: ReturnType<typeof setInterval> | null = null;
  private processingRetries = false;
  private sendEventFn: SendEventFn | null = null;
  private onDropped: OnEventDropped | null = null;

  /**
   * Bind the send function used for retries.
   */
  setSendFn(fn: SendEventFn): void {
    this.sendEventFn = fn;
  }

  /**
   * Set callback for dropped events (metrics/logging).
   */
  setOnDropped(fn: OnEventDropped): void {
    this.onDropped = fn;
  }

  /**
   * Add an event to the pending queue after initial send.
   */
  add(event: DumasAsyncEvent): void {
    // Overflow: remove oldest entry if at capacity
    if (this.queue.size >= MAX_PENDING_ACK_SIZE && !this.queue.has(event.eventId)) {
      const oldestKey = this.queue.keys().next().value;
      if (oldestKey) {
        const oldestEntry = this.queue.get(oldestKey);
        this.queue.delete(oldestKey);
        if (oldestEntry && this.onDropped) {
          this.onDropped(oldestEntry.event, 'overflow');
        }
        console.warn('[PendingAckQueue] Overflow, dropped oldest event', { eventId: oldestKey });
      }
    }

    const now = Date.now();
    this.queue.set(event.eventId, {
      event,
      retryCount: 0,
      nextRetryAt: now + this.computeDelay(0),
      enqueuedAt: now,
    });

    this.ensureRetryLoop();
  }

  /**
   * Acknowledge an event (remove from queue).
   * Returns true if the event was in the queue.
   */
  ack(eventId: string): boolean {
    const existed = this.queue.delete(eventId);
    if (existed) {
      console.info('[PendingAckQueue] ACK received', { eventId, remaining: this.queue.size });
      if (this.queue.size === 0) {
        this.stopRetryLoop();
      }
    }
    return existed;
  }

  /**
   * Get current queue size.
   */
  get size(): number {
    return this.queue.size;
  }

  /**
   * Get all entries (for persistence).
   */
  getAll(): PendingAckEntry[] {
    return Array.from(this.queue.values());
  }

  /**
   * Restore entries (from persistence). Does NOT trigger immediate retry.
   */
  restore(entries: PendingAckEntry[]): void {
    for (const entry of entries) {
      if (!this.queue.has(entry.event.eventId)) {
        this.queue.set(entry.event.eventId, entry);
      }
    }
    if (this.queue.size > 0) {
      console.info('[PendingAckQueue] Restored entries', { count: this.queue.size });
      this.ensureRetryLoop();
    }
  }

  /**
   * Retry all eligible events now (e.g. after reconnect).
   */
  retryAllNow(): void {
    const now = Date.now();
    for (const entry of this.queue.values()) {
      entry.nextRetryAt = now; // make them all eligible immediately
    }
    this.ensureRetryLoop();
    this.processRetries();
  }

  /**
   * Clear the queue and cancel timers.
   */
  clear(): void {
    this.queue.clear();
    this.stopRetryLoop();
  }

  /**
   * Stop the retry timer (for cleanup).
   */
  destroy(): void {
    this.stopRetryLoop();
  }

  /**
   * Remove all pending events for a task.
   * Returns number of removed entries.
   */
  removeByTaskId(taskId: string): number {
    if (!taskId) return 0;
    let removed = 0;
    for (const [eventId, entry] of this.queue.entries()) {
      if (entry.event.taskId === taskId) {
        this.queue.delete(eventId);
        removed++;
      }
    }
    if (removed > 0) {
      console.info('[PendingAckQueue] Removed pending events by taskId', { taskId, removed });
      if (this.queue.size === 0) {
        this.stopRetryLoop();
      }
    }
    return removed;
  }

  // ── Internal ──────────────────────────────────────────────

  private computeDelay(retryCount: number): number {
    const delay = ACK_RETRY_BASE_MS * Math.pow(2, retryCount);
    return Math.min(delay, MAX_ACK_RETRY_INTERVAL_MS);
  }

  private ensureRetryLoop(): void {
    if (this.retryTimer !== null) return;
    if (this.queue.size === 0) return;
    this.retryTimer = setInterval(() => {
      this.processRetries();
    }, RETRY_TICK_MS);
  }

  private stopRetryLoop(): void {
    if (this.retryTimer !== null) {
      clearInterval(this.retryTimer);
      this.retryTimer = null;
    }
  }

  private processRetries(): void {
    if (!this.sendEventFn) return;
    if (this.processingRetries) return;
    this.processingRetries = true;
    try {
      const now = Date.now();
      const toDrop: string[] = [];
      let processed = 0;

      for (const [eventId, entry] of this.queue.entries()) {
        if (processed >= RETRY_BATCH_SIZE) {
          break;
        }
        if (entry.nextRetryAt > now) continue;

        if (entry.retryCount >= MAX_ACK_RETRIES) {
          toDrop.push(eventId);
          processed++;
          continue;
        }

        // Attempt re-send
        const sent = this.sendEventFn(entry.event);
        processed++;
        if (sent) {
          entry.retryCount++;
          entry.nextRetryAt = now + this.computeDelay(entry.retryCount);
          console.info('[PendingAckQueue] Retried event', {
            eventId,
            retryCount: entry.retryCount,
            nextRetryAt: entry.nextRetryAt,
          });
        } else {
          // WebSocket not connected; push retry to future
          entry.nextRetryAt = now + this.computeDelay(entry.retryCount);
        }
      }

      // Drop events that exceeded max retries
      for (const eventId of toDrop) {
        const entry = this.queue.get(eventId);
        this.queue.delete(eventId);
        if (entry && this.onDropped) {
          this.onDropped(entry.event, 'max_retries');
        }
        console.warn('[PendingAckQueue] Dropped event (max retries)', { eventId });
      }

      if (this.queue.size === 0) {
        this.stopRetryLoop();
      }
    } finally {
      this.processingRetries = false;
    }
  }
}
