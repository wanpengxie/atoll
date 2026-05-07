/**
 * 通用的等待工具集，参考Playwright和Puppeteer的设计
 */

import { runInMainWorld } from './inject-script';

export interface WaitOptions {
  /** 超时时间（毫秒） */
  timeout?: number;
  /** 轮询间隔（毫秒） */
  interval?: number;
  /** 是否在隐藏元素上等待 */
  visible?: boolean;
}

export interface WaitForSelectorOptions extends WaitOptions {
  /** 等待元素的状态 */
  state?: 'attached' | 'detached' | 'visible' | 'hidden';
}

export interface WaitForFunctionOptions extends WaitOptions {
  /** 传递给函数的参数 */
  args?: any[];
}

/**
 * 等待选择器出现在页面上
 * @param tabId 标签页ID
 * @param selector CSS选择器
 * @param options 等待选项
 * @returns 是否找到元素
 */
export async function waitForSelector(
  tabId: number,
  selector: string,
  options: WaitForSelectorOptions = {}
): Promise<boolean> {
  const { timeout = 30000, interval = 100, visible = false, state = 'attached' } = options;

  return runInMainWorld<boolean>(
    tabId,
    (args: {
      selector: string;
      timeout: number;
      interval: number;
      visible: boolean;
      state: string;
    }) => {
      return new Promise((resolve) => {
        const startTime = Date.now();

        const check = () => {
          const element = document.querySelector(args.selector);

          switch (args.state) {
            case 'attached':
              if (element) {
                resolve(true);
                return;
              }
              break;

            case 'detached':
              if (!element) {
                resolve(true);
                return;
              }
              break;

            case 'visible':
              if (element && (element as HTMLElement).offsetParent !== null) {
                resolve(true);
                return;
              }
              break;

            case 'hidden':
              if (!element || (element as HTMLElement).offsetParent === null) {
                resolve(true);
                return;
              }
              break;
          }

          if (Date.now() - startTime > args.timeout) {
            resolve(false);
            return;
          }

          setTimeout(check, args.interval);
        };

        check();
      });
    },
    { selector, timeout, interval, visible, state }
  );
}

/**
 * 等待函数返回真值
 * @param tabId 标签页ID
 * @param fn 要执行的函数
 * @param options 等待选项
 * @returns 函数返回的值
 */
export async function waitForFunction<T = any>(
  tabId: number,
  fn: string | ((...args: any[]) => T | Promise<T>),
  options: WaitForFunctionOptions = {}
): Promise<T | null> {
  const { timeout = 30000, interval = 100, args = [] } = options;

  return runInMainWorld<T | null>(
    tabId,
    (injectedArgs: { fnString: string; timeout: number; interval: number; args: any[] }) => {
      return new Promise((resolve) => {
        const startTime = Date.now();
        const fn = new Function('args', `return (${injectedArgs.fnString})(...args)`);

        const check = async () => {
          try {
            const result = await fn(injectedArgs.args);
            if (result) {
              resolve(result);
              return;
            }
          } catch (error) {
            // 忽略执行错误，继续等待
          }

          if (Date.now() - startTime > injectedArgs.timeout) {
            resolve(null);
            return;
          }

          setTimeout(check, injectedArgs.interval);
        };

        check();
      });
    },
    {
      fnString: typeof fn === 'function' ? fn.toString() : fn,
      timeout,
      interval,
      args,
    }
  );
}

/**
 * 等待页面加载到特定状态
 * @param tabId 标签页ID
 * @param state 要等待的状态
 * @param timeout 超时时间
 */
export async function waitForLoadState(
  tabId: number,
  state: 'load' | 'domcontentloaded' | 'networkidle' = 'load',
  timeout: number = 30000
): Promise<boolean> {
  if (state === 'networkidle') {
    return waitForNetworkIdle(tabId, { timeout });
  }

  return runInMainWorld<boolean>(
    tabId,
    (args: { state: string; timeout: number }) => {
      return new Promise((resolve) => {
        const startTime = Date.now();
        let checkInterval: any;
        let resolved = false;
        let eventListener: (() => void) | null = null;

        const cleanup = () => {
          resolved = true;
          if (checkInterval) {
            clearInterval(checkInterval);
          }
          // 移除事件监听器
          if (eventListener) {
            if (args.state === 'domcontentloaded') {
              document.removeEventListener('DOMContentLoaded', eventListener);
            } else {
              window.removeEventListener('load', eventListener);
            }
          }
        };

        const resolveAndCleanup = (value: boolean) => {
          if (!resolved) {
            cleanup();
            resolve(value);
          }
        };

        // 事件监听器
        eventListener = () => resolveAndCleanup(true);

        // 立即检查当前状态
        const checkState = () => {
          if (resolved) return;

          // 超时检查
          if (Date.now() - startTime > args.timeout) {
            resolveAndCleanup(false);
            return;
          }

          const currentState = document.readyState;

          if (args.state === 'domcontentloaded') {
            if (currentState === 'interactive' || currentState === 'complete') {
              resolveAndCleanup(true);
              return;
            }
          } else if (args.state === 'load') {
            if (currentState === 'complete') {
              resolveAndCleanup(true);
              return;
            }
          }
        };

        // 设置事件监听器
        if (args.state === 'domcontentloaded') {
          document.addEventListener('DOMContentLoaded', eventListener, { once: true });
        } else {
          window.addEventListener('load', eventListener, { once: true });
        }

        // 立即检查一次
        checkState();

        // 如果还没有 resolve，设置定期检查
        if (!resolved) {
          checkInterval = setInterval(checkState, 100);
        }

        // 设置超时
        setTimeout(() => {
          resolveAndCleanup(false);
        }, args.timeout);
      });
    },
    { state, timeout }
  );
}

/**
 * 等待网络空闲
 * @param tabId 标签页ID
 * @param options 选项
 */
export async function waitForNetworkIdle(
  tabId: number,
  options: { timeout?: number; maxInflightRequests?: number } = {}
): Promise<boolean> {
  const { timeout = 30000, maxInflightRequests = 0 } = options;

  return new Promise((resolve) => {
    const startTime = Date.now();
    let inflightRequests = 0;
    let idleTimer: NodeJS.Timeout | null = null;
    let timeoutTimer: NodeJS.Timeout | null = null;
    let resolved = false;

    const cleanup = () => {
      resolved = true;
      if (idleTimer) {
        clearTimeout(idleTimer);
        idleTimer = null;
      }
      if (timeoutTimer) {
        clearTimeout(timeoutTimer);
        timeoutTimer = null;
      }
      chrome.webRequest.onBeforeRequest.removeListener(onRequest);
      chrome.webRequest.onCompleted.removeListener(onComplete);
      chrome.webRequest.onErrorOccurred.removeListener(onError);
    };

    const resolveAndCleanup = (value: boolean) => {
      if (!resolved) {
        cleanup();
        resolve(value);
      }
    };

    const checkIdle = () => {
      if (resolved) return;

      if (inflightRequests <= maxInflightRequests) {
        if (idleTimer) {
          clearTimeout(idleTimer);
        }
        idleTimer = setTimeout(() => {
          resolveAndCleanup(true);
        }, 500); // 500ms没有新请求认为网络空闲
      }
    };

    const onRequest = (details: chrome.webRequest.WebRequestBodyDetails) => {
      if (details.tabId === tabId) {
        inflightRequests++;
        if (idleTimer) {
          clearTimeout(idleTimer);
          idleTimer = null;
        }
      }
    };

    const onComplete = (details: chrome.webRequest.WebResponseCacheDetails) => {
      if (details.tabId === tabId) {
        inflightRequests = Math.max(0, inflightRequests - 1);
        checkIdle();
      }
    };

    const onError = (details: chrome.webRequest.WebResponseErrorDetails) => {
      if (details.tabId === tabId) {
        inflightRequests = Math.max(0, inflightRequests - 1);
        checkIdle();
      }
    };

    // 设置超时
    timeoutTimer = setTimeout(() => {
      resolveAndCleanup(false);
    }, timeout);

    // 监听网络请求
    chrome.webRequest.onBeforeRequest.addListener(onRequest, { urls: ['<all_urls>'], tabId }, [
      'requestBody',
    ]);

    chrome.webRequest.onCompleted.addListener(onComplete, { urls: ['<all_urls>'], tabId });

    chrome.webRequest.onErrorOccurred.addListener(onError, { urls: ['<all_urls>'], tabId });

    // 立即检查一次初始状态
    checkIdle();
  });
}

/**
 * 等待元素包含特定文本
 * @param tabId 标签页ID
 * @param selector 选择器
 * @param text 要包含的文本
 * @param options 选项
 */
export async function waitForText(
  tabId: number,
  selector: string,
  text: string,
  options: WaitOptions = {}
): Promise<boolean> {
  const { timeout = 30000, interval = 100 } = options;

  return runInMainWorld<boolean>(
    tabId,
    (args: { selector: string; text: string; timeout: number; interval: number }) => {
      return new Promise((resolve) => {
        const startTime = Date.now();

        const check = () => {
          const element = document.querySelector(args.selector);
          if (element && element.textContent?.includes(args.text)) {
            resolve(true);
            return;
          }

          if (Date.now() - startTime > args.timeout) {
            resolve(false);
            return;
          }

          setTimeout(check, args.interval);
        };

        check();
      });
    },
    { selector, text, timeout, interval }
  );
}

/**
 * 等待URL变化
 * @param tabId 标签页ID
 * @param urlPattern 要匹配的URL模式（字符串或正则）
 * @param timeout 超时时间
 */
export async function waitForURL(
  tabId: number,
  urlPattern: string | RegExp,
  timeout: number = 30000
): Promise<boolean> {
  return new Promise((resolve) => {
    const startTime = Date.now();
    let intervalTimer: NodeJS.Timeout | null = null;
    let timeoutTimer: NodeJS.Timeout | null = null;
    let resolved = false;

    const cleanup = () => {
      resolved = true;
      if (intervalTimer) {
        clearInterval(intervalTimer);
        intervalTimer = null;
      }
      if (timeoutTimer) {
        clearTimeout(timeoutTimer);
        timeoutTimer = null;
      }
      chrome.tabs.onUpdated.removeListener(onUpdated);
    };

    const resolveAndCleanup = (value: boolean) => {
      if (!resolved) {
        cleanup();
        resolve(value);
      }
    };

    const checkURL = async () => {
      if (resolved) return;

      try {
        const tab = await chrome.tabs.get(tabId);
        const url = tab.url || '';

        const matches =
          typeof urlPattern === 'string' ? url.includes(urlPattern) : urlPattern.test(url);

        if (matches) {
          resolveAndCleanup(true);
          return;
        }
      } catch (error) {
        // Tab might be closed
        resolveAndCleanup(false);
        return;
      }
    };

    const onUpdated = (id: number, info: chrome.tabs.TabChangeInfo) => {
      if (id === tabId && info.url) {
        checkURL();
      }
    };

    // 设置超时
    timeoutTimer = setTimeout(() => {
      resolveAndCleanup(false);
    }, timeout);

    // 监听URL变化
    chrome.tabs.onUpdated.addListener(onUpdated);

    // 定期检查
    intervalTimer = setInterval(checkURL, 100);

    // 立即检查一次
    checkURL();
  });
}

/**
 * 等待XHR/Fetch请求完成
 * @param tabId 标签页ID
 * @param urlPattern 要匹配的请求URL模式
 * @param options 选项
 */
export async function waitForRequest(
  tabId: number,
  urlPattern: string | RegExp,
  options: { timeout?: number; method?: string } = {}
): Promise<chrome.webRequest.WebResponseCacheDetails | null> {
  const { timeout = 30000, method } = options;

  return new Promise((resolve) => {
    let timer: NodeJS.Timeout;

    const onCompleted = (details: chrome.webRequest.WebResponseCacheDetails) => {
      if (details.tabId !== tabId) return;

      const matches =
        typeof urlPattern === 'string'
          ? details.url.includes(urlPattern)
          : urlPattern.test(details.url);

      if (matches && (!method || details.method === method)) {
        cleanup();
        resolve(details);
      }
    };

    const cleanup = () => {
      clearTimeout(timer);
      chrome.webRequest.onCompleted.removeListener(onCompleted);
    };

    timer = setTimeout(() => {
      cleanup();
      resolve(null);
    }, timeout);

    chrome.webRequest.onCompleted.addListener(onCompleted, { urls: ['<all_urls>'], tabId }, [
      'responseHeaders',
    ]);
  });
}

/**
 * 组合多个等待条件（类似Promise.all）
 * @param conditions 等待条件数组
 */
export async function waitForAll(...conditions: Array<() => Promise<any>>): Promise<any[]> {
  return Promise.all(conditions.map((fn) => fn()));
}

/**
 * 组合多个等待条件（类似Promise.race）
 * @param conditions 等待条件数组
 */
export async function waitForAny(...conditions: Array<() => Promise<any>>): Promise<any> {
  return Promise.race(conditions.map((fn) => fn()));
}
