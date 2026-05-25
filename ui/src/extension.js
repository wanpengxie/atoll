// extension.js — T148 (M1.6-T6).
//
// Thin wrapper around `chrome.runtime.sendMessage(EXTENSION_ID, ...)` so
// the rest of the UI can call `extension.bind(...)` / `extension.unbind(...)`
// without sprinkling chrome guards across the codebase.
//
// EXTENSION_ID is build-time injected via Vite's `import.meta.env`:
//   .env  /  .env.local:   VITE_COAGENT_EXTENSION_ID=ngghjmpccpgmfgblbifmlmjnnpfknhka
// Dev mode: copy the value from chrome://extensions after loading the
//           unpacked artifact (it shows the runtime-allocated 32-char id).
// Prod mode: use the Chrome Web Store-assigned id.
//
// If EXTENSION_ID is missing or chrome.runtime is unavailable (e.g. the
// user opened the UI in Firefox), all calls return
// `{available:false, reason:'extension_unavailable'}` so the UI can
// disable the bind button + show a hint.

const EXTENSION_ID =
  // Vite replaces import.meta.env.* at build time. Missing keys are
  // returned as undefined; we never want to send messages to "undefined".
  (import.meta.env.VITE_COAGENT_EXTENSION_ID || '').trim();

const DEFAULT_TIMEOUT_MS = 8_000;

/**
 * isExtensionAvailable — synchronous capability check.
 * Returns true iff:
 *   1. chrome.runtime.sendMessage exists (i.e. we're in a chromium
 *      browser AND we're on a page whose origin is on the extension's
 *      `externally_connectable.matches` list — Chrome injects the API
 *      only for allowed origins);
 *   2. VITE_COAGENT_EXTENSION_ID is configured.
 */
export function isExtensionAvailable() {
  if (!EXTENSION_ID) return false;
  if (typeof chrome === 'undefined') return false;
  if (!chrome.runtime) return false;
  if (typeof chrome.runtime.sendMessage !== 'function') return false;
  return true;
}

/**
 * extensionUnavailableReason — diagnostic helper for the UI status row.
 * Returns a short tag the UI can render (`'no_chrome' | 'no_extension_id'
 * | 'no_runtime' | null`).
 */
export function extensionUnavailableReason() {
  if (typeof chrome === 'undefined') return 'no_chrome';
  if (!chrome.runtime || typeof chrome.runtime.sendMessage !== 'function') return 'no_runtime';
  if (!EXTENSION_ID) return 'no_extension_id';
  return null;
}

/**
 * Internal: send one message + wait for sendResponse.
 * Wraps the Chrome callback API into a promise, with a hard timeout
 * fallback in case the extension never calls sendResponse (e.g. it
 * crashed mid-handle).
 */
function sendMessage(message, { timeoutMs = DEFAULT_TIMEOUT_MS } = {}) {
  if (!isExtensionAvailable()) {
    return Promise.resolve({ available: false, reason: 'extension_unavailable' });
  }
  return new Promise((resolve) => {
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      resolve({ available: true, status: 'failed', reason: 'extension_timeout' });
    }, timeoutMs);

    try {
      chrome.runtime.sendMessage(EXTENSION_ID, message, (response) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        const lastError = chrome.runtime.lastError;
        if (lastError) {
          // Common lastError texts: "Could not establish connection" =
          // extension not installed / disabled, "Specified native messaging
          // host not found" = wrong id. Map to extension_not_installed for
          // UI clarity.
          const text = String(lastError.message || lastError);
          resolve({
            available: true,
            status: 'failed',
            reason: text.includes('Could not establish')
              ? 'extension_not_installed'
              : 'sendMessage_failed',
            detail: text,
          });
          return;
        }
        if (!response) {
          // Listener returned `false` from `return true` contract; treat
          // as a malformed handler.
          resolve({
            available: true,
            status: 'failed',
            reason: 'empty_response',
          });
          return;
        }
        resolve({ available: true, ...response });
      });
    } catch (err) {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve({
        available: true,
        status: 'failed',
        reason: 'sendMessage_threw',
        detail: err?.message || String(err),
      });
    }
  });
}

/**
 * Resolve the server's WebSocket /devicebus URL given the current
 * window.location. The web UI is served from the same origin as the
 * coagent server (vite proxy in dev; bundled static in prod), so we
 * can derive the WS endpoint without configuration.
 */
export function defaultServerWsUrl() {
  if (typeof window === 'undefined' || !window.location) return '';
  const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  return `${proto}//${window.location.host}/devicebus`;
}

/**
 * getDeviceInfo — read the extension's persistent device_id (and the
 * current bound snapshot if it's already wired to a channel).
 */
export function getDeviceInfo() {
  return sendMessage({ action: 'getDeviceInfo' });
}

/**
 * setDeviceToken — hand off a fresh actor token bundle. `expires_at` is
 * informational (extension client doesn't auto-refresh in M1.6).
 */
export function setDeviceToken({
  server_ws_url,
  actor_id,
  token,
  channel_id,
  user_id,
  device_id,
  expires_at,
}) {
  return sendMessage({
    action: 'setDeviceToken',
    server_ws_url,
    actor_id,
    token,
    channel_id,
    user_id,
    device_id,
    expires_at,
  });
}

/**
 * unbindDevice — tell the extension to drop its current binding.
 * Pairs with the server-side device actor revoke call.
 */
export function unbindDevice() {
  return sendMessage({ action: 'unbindDevice' });
}

/**
 * Friendly text for a failure reason. Centralised so the UI strings
 * stay in one place. Falls through to a generic "unknown" message.
 */
export function describeReason(reason, detail) {
  switch (reason) {
    case 'extension_unavailable':
    case 'no_chrome':
      return '当前浏览器无法访问 Chrome extension（请用 Chromium 内核 + 装扩展）';
    case 'no_extension_id':
      return '前端构建未配置 VITE_COAGENT_EXTENSION_ID — 请在 .env.local 设置 extension id';
    case 'no_runtime':
      return 'chrome.runtime API 不可用（externally_connectable 未匹配当前页面 origin？）';
    case 'extension_not_installed':
      return '未检测到 Chrome extension（请先安装 coagent xhs-device 扩展）';
    case 'extension_timeout':
      return 'Extension 未在 8 秒内响应';
    case 'origin_not_allowed':
      return 'Extension 拒绝了来源（页面 origin 不在白名单内）';
    case 'invalid_payload':
      return `Extension 拒绝了请求 payload：${detail || '字段缺失'}`;
    case 'ws_connect_failed':
      return `WebSocket 连接失败：${detail || '原因未知'}`;
    case 'ws_connect_timeout':
      return 'WebSocket 握手超时';
    case 'internal_error':
      return `Extension 内部错误：${detail || ''}`;
    case 'sendMessage_failed':
    case 'sendMessage_threw':
    case 'empty_response':
      return `与 extension 通信失败：${detail || reason}`;
    default:
      return detail ? `${reason}: ${detail}` : reason || 'unknown error';
  }
}
