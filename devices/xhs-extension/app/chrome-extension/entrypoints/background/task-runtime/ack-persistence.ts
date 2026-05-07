/**
 * ACK Persistence
 *
 * 将 pendingAck 队列持久化到 chrome.storage.local，
 * 以便 Service Worker 重启后恢复未 ACK 的事件。
 */

import type { PendingAckEntry } from 'xiaohongshu-mcp-shared';
import { PENDING_ACK_STORAGE_KEY } from 'xiaohongshu-mcp-shared';

/**
 * Save pending entries to chrome.storage.local.
 * Call this after each queue mutation (add/ack/retry).
 */
export async function savePendingAckQueue(entries: PendingAckEntry[]): Promise<void> {
  try {
    await chrome.storage.local.set({
      [PENDING_ACK_STORAGE_KEY]: entries,
    });
  } catch (err) {
    console.error('[AckPersistence] Failed to save pending queue', err);
  }
}

/**
 * Restore pending entries from chrome.storage.local.
 * Call this on SW startup.
 */
export async function restorePendingAckQueue(): Promise<PendingAckEntry[]> {
  try {
    const result = await chrome.storage.local.get(PENDING_ACK_STORAGE_KEY);
    const entries = result[PENDING_ACK_STORAGE_KEY];
    if (Array.isArray(entries)) {
      console.info('[AckPersistence] Restored pending queue', { count: entries.length });
      return entries as PendingAckEntry[];
    }
  } catch (err) {
    console.error('[AckPersistence] Failed to restore pending queue', err);
  }
  return [];
}

/**
 * Clear persisted pending queue.
 */
export async function clearPendingAckQueue(): Promise<void> {
  try {
    await chrome.storage.local.remove(PENDING_ACK_STORAGE_KEY);
  } catch (err) {
    console.error('[AckPersistence] Failed to clear pending queue', err);
  }
}
