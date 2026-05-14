/**
 * CDP 网络拦截器 - 使用 Chrome DevTools Protocol 获取网络响应体
 *
 * 工作原理：
 * 1. 使用 chrome.debugger.attach 连接到标签页
 * 2. 发送 Network.enable 启用网络监控
 * 3. 监听 Network.responseReceived 事件（记录匹配的请求）
 * 4. 监听 Network.loadingFinished 事件（获取响应体）
 * 5. 使用 Network.getResponseBody 获取响应体
 * 6. 完成后 chrome.debugger.detach 断开连接
 *
 * 注意：用户会看到黄色调试提示条，但网站无法检测
 */

import { getXsecToken, isXhsHost, isXhsNotePath, isXhsProfilePath, safeParseUrl } from './xiaohongshu/xhs-url';

export interface InterceptedResponse {
  url: string;
  status: number;
  data: any;
  requestId: string;
}

interface PendingRequest {
  url: string;
  requestId: string;
  status: number;
  matchedPattern: string;
}

/**
 * CDP 网络拦截器类
 */
export class CDPInterceptor {
  private tabId: number;
  private urlPatterns: string[];
  private responses: Map<string, InterceptedResponse> = new Map();
  private pendingRequests: Map<string, PendingRequest> = new Map();
  private attached = false;
  private eventHandler: ((
    source: chrome.debugger.Debuggee,
    method: string,
    params?: any
  ) => void) | null = null;

  constructor(tabId: number, urlPatterns: string[]) {
    this.tabId = tabId;
    this.urlPatterns = urlPatterns;
  }

  /**
   * 连接调试器并启用网络监控
   */
  async attach(): Promise<void> {
    if (this.attached) {
      return;
    }

    return new Promise((resolve, reject) => {
      chrome.debugger.attach({ tabId: this.tabId }, '1.3', () => {
        if (chrome.runtime.lastError) {
          const errorMsg = chrome.runtime.lastError.message || '';
          // 提供更友好的错误信息
          if (errorMsg.includes('Another debugger')) {
            reject(new Error('调试器已被占用（可能 DevTools 已打开），请关闭 DevTools 后重试'));
          } else {
            reject(new Error(`连接调试器失败: ${errorMsg}`));
          }
          return;
        }

        this.attached = true;

        // 启用网络监控
        chrome.debugger.sendCommand(
          { tabId: this.tabId },
          'Network.enable',
          {},
          () => {
            if (chrome.runtime.lastError) {
              reject(new Error(`启用网络监控失败: ${chrome.runtime.lastError.message}`));
              return;
            }

            // 设置事件监听器
            this.eventHandler = this.handleDebuggerEvent.bind(this);
            chrome.debugger.onEvent.addListener(this.eventHandler);

            console.log('[CDPInterceptor] Attached and Network.enable sent');
            resolve();
          }
        );
      });
    });
  }

  /**
   * 处理调试器事件
   */
  private handleDebuggerEvent(
    source: chrome.debugger.Debuggee,
    method: string,
    params?: any
  ): void {
    if (source.tabId !== this.tabId) {
      return;
    }

    // 响应头已接收（记录匹配的请求）
    if (method === 'Network.responseReceived') {
      const { requestId, response } = params;
      const url = response.url;
      const status = response.status;

      // 检查 URL 是否匹配
      const matchedPattern = this.urlPatterns.find((pattern) =>
        url.includes(pattern)
      );

      if (matchedPattern) {
        console.log('[CDPInterceptor] Response received:', url, 'status:', status);

        // 保存请求信息，等待 loadingFinished 后获取 body
        this.pendingRequests.set(requestId, {
          url,
          requestId,
          status,
          matchedPattern,
        });
      }
    }

    // 响应体加载完成（获取响应体）
    if (method === 'Network.loadingFinished') {
      const { requestId } = params;
      const pending = this.pendingRequests.get(requestId);

      if (pending) {
        this.fetchResponseBody(pending);
      }
    }

    // 请求失败（清理 pending）
    if (method === 'Network.loadingFailed') {
      const { requestId, errorText } = params;
      const pending = this.pendingRequests.get(requestId);

      if (pending) {
        console.warn('[CDPInterceptor] Request failed:', pending.url, errorText);
        this.pendingRequests.delete(requestId);
      }
    }
  }

  /**
   * 获取响应体
   */
  private fetchResponseBody(pending: PendingRequest): void {
    chrome.debugger.sendCommand(
      { tabId: this.tabId },
      'Network.getResponseBody',
      { requestId: pending.requestId },
      (result: any) => {
        // 检查是否已经 detach
        if (!this.attached) {
          return;
        }

        if (chrome.runtime.lastError) {
          console.warn(
            '[CDPInterceptor] Failed to get response body:',
            chrome.runtime.lastError.message
          );
          this.pendingRequests.delete(pending.requestId);
          return;
        }

        if (result && result.body) {
          try {
            // 处理 base64 编码的响应
            let bodyStr = result.body;
            if (result.base64Encoded) {
              bodyStr = atob(result.body);
            }

            const data = JSON.parse(bodyStr);

            this.responses.set(pending.matchedPattern, {
              url: pending.url,
              status: pending.status,
              data,
              requestId: pending.requestId,
            });

            console.log(
              '[CDPInterceptor] Response body captured:',
              pending.matchedPattern,
              'status:',
              pending.status
            );
          } catch (e) {
            console.warn('[CDPInterceptor] Failed to parse JSON:', e);
          }
        }

        this.pendingRequests.delete(pending.requestId);
      }
    );
  }

  /**
   * 等待指定 URL 模式的响应
   */
  async waitForResponse(
    urlPattern: string,
    timeout: number = 15000
  ): Promise<any | null> {
    const startTime = Date.now();

    return new Promise((resolve) => {
      const check = () => {
        const response = this.responses.get(urlPattern);
        if (response) {
          this.responses.delete(urlPattern);
          resolve(response.data);
          return;
        }

        if (Date.now() - startTime > timeout) {
          console.warn('[CDPInterceptor] Timeout waiting for:', urlPattern);
          resolve(null);
          return;
        }

        setTimeout(check, 100);
      };

      check();
    });
  }

  /**
   * 等待多个响应
   */
  async waitForMultipleResponses(
    timeout: number = 15000
  ): Promise<Record<string, any>> {
    const startTime = Date.now();
    const results: Record<string, any> = {};
    const pendingPatterns = new Set(this.urlPatterns);

    return new Promise((resolve) => {
      const check = () => {
        for (const pattern of pendingPatterns) {
          const response = this.responses.get(pattern);
          if (response) {
            results[pattern] = response.data;
            this.responses.delete(pattern);
            pendingPatterns.delete(pattern);
          }
        }

        // 所有都获取到了
        if (pendingPatterns.size === 0) {
          console.log('[CDPInterceptor] All responses captured');
          resolve(results);
          return;
        }

        // 超时
        if (Date.now() - startTime > timeout) {
          console.warn(
            '[CDPInterceptor] Timeout, missing patterns:',
            Array.from(pendingPatterns)
          );
          resolve(results);
          return;
        }

        setTimeout(check, 100);
      };

      check();
    });
  }

  /**
   * 获取已拦截的响应
   */
  getResponse(urlPattern: string): any | null {
    const response = this.responses.get(urlPattern);
    return response?.data || null;
  }

  /**
   * 获取所有已拦截的响应
   */
  getAllResponses(): Record<string, any> {
    const results: Record<string, any> = {};
    for (const [pattern, response] of this.responses) {
      results[pattern] = response.data;
    }
    return results;
  }

  /**
   * 断开调试器连接
   */
  async detach(): Promise<void> {
    if (!this.attached) {
      return;
    }

    // 先标记为未连接，防止回调中继续处理
    this.attached = false;

    // 移除事件监听器
    if (this.eventHandler) {
      chrome.debugger.onEvent.removeListener(this.eventHandler);
      this.eventHandler = null;
    }

    return new Promise((resolve) => {
      chrome.debugger.detach({ tabId: this.tabId }, () => {
        if (chrome.runtime.lastError) {
          console.warn(
            '[CDPInterceptor] Detach warning:',
            chrome.runtime.lastError.message
          );
        }
        this.responses.clear();
        this.pendingRequests.clear();
        console.log('[CDPInterceptor] Detached');
        resolve();
      });
    });
  }
}

/**
 * 便捷函数：创建拦截器、导航、等待响应、断开
 */
export async function interceptNetworkResponses(
  tabId: number,
  targetUrl: string,
  urlPatterns: string[],
  timeout: number = 20000
): Promise<Record<string, any>> {
  // 小红书用户/笔记直达链接必须带 xsec_token，否则通常会被屏蔽
  const u = safeParseUrl(targetUrl);
  if (u && isXhsHost(u.hostname) && (isXhsNotePath(u.pathname) || isXhsProfilePath(u.pathname)) && !getXsecToken(u)) {
    throw new Error('小红书用户/笔记链接必须包含 xsec_token（请从小红书内“分享-复制链接”获取完整链接）');
  }

  const interceptor = new CDPInterceptor(tabId, urlPatterns);

  try {
    // 1. 连接调试器
    await interceptor.attach();

    // 2. 导航到目标页面
    await chrome.tabs.update(tabId, { url: targetUrl });

    // 3. 等待响应
    const responses = await interceptor.waitForMultipleResponses(timeout);

    return responses;
  } finally {
    // 4. 断开连接
    await interceptor.detach();
  }
}
