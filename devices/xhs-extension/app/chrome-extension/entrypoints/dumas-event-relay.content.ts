/**
 * DUMAS Async Event Relay Content Script
 *
 * 常驻 content_script，监听页面脚本通过 window.postMessage 发出的
 * DUMAS_ASYNC_EVENT，中继到 Background 的 TASK_EVENT 入口。
 *
 * 设计要点：
 * - 纯中继，不解析业务字段
 * - 运行在 ISOLATED world，通过 message 事件桥接 MAIN world
 * - 匹配所有小红书域名（www + creator）
 */

import {
  DUMAS_ASYNC_EVENT_TYPE,
  DUMAS_TASK_CONTROL_TYPE,
  TASK_EVENT_MESSAGE_TYPE,
} from 'xiaohongshu-mcp-shared';
import type { DumasAsyncEventEnvelope, DumasTaskControlEnvelope } from 'xiaohongshu-mcp-shared';

export default defineContentScript({
  matches: [
    'https://*.xiaohongshu.com/*',
    'https://creator.xiaohongshu.com/*',
  ],
  runAt: 'document_start',

  main() {
    console.log('[DUMAS EventRelay] Content script loaded');

    window.addEventListener('message', (event: MessageEvent) => {
      // Only accept messages from the same frame
      if (event.source !== window) return;

      const data = event.data as DumasAsyncEventEnvelope | undefined;
      if (!data || data.type !== DUMAS_ASYNC_EVENT_TYPE) return;

      const asyncEvent = data.event;
      if (!asyncEvent || !asyncEvent.eventId || !asyncEvent.taskId) {
        console.warn('[DUMAS EventRelay] Malformed event, missing eventId or taskId', data);
        return;
      }

      console.info('[DUMAS EventRelay] Relaying event', {
        eventId: asyncEvent.eventId,
        taskId: asyncEvent.taskId,
        eventType: asyncEvent.eventType,
        seq: asyncEvent.seq,
      });

      // Relay to Background as TASK_EVENT
      chrome.runtime.sendMessage({
        type: TASK_EVENT_MESSAGE_TYPE,
        event: asyncEvent,
      }).catch((err) => {
        // Background may not be ready yet (e.g. during SW restart)
        console.warn('[DUMAS EventRelay] Failed to relay event to background', err);
      });
    });

    // Background -> content_script -> page runtime control bridge
    chrome.runtime.onMessage.addListener((message: any) => {
      if (!message || message.type !== 'TASK_CONTROL') return;
      const taskId = typeof message.taskId === 'string' ? message.taskId : '';
      const action = typeof message.action === 'string' ? message.action : '';
      if (!taskId || !action) return;

      const controlEnvelope: DumasTaskControlEnvelope = {
        type: DUMAS_TASK_CONTROL_TYPE,
        control: {
          taskId,
          action: action as DumasTaskControlEnvelope['control']['action'],
          payload: (message.payload || {}) as Record<string, unknown>,
        },
      };
      window.postMessage(controlEnvelope, '*');
    });

    console.log('[DUMAS EventRelay] Event listener installed');
  },
});
