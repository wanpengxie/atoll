import { BaseTool } from '../base-tool';
import { ToolResult } from 'coagent-xhs-shared';
import { getXsecToken, isXhsHost, isXhsNotePath, isXhsProfilePath, safeParseUrl } from '../xiaohongshu/xhs-url';

interface NavigateArgs {
  url: string; // 目标 URL
  waitUntil?: 'load' | 'domcontentloaded' | 'networkidle'; // 等待条件
  timeout?: number; // 超时时间(毫秒)
}

/**
 * chrome_navigate - 导航到指定 URL
 */
export class ChromeNavigateTool extends BaseTool {
  name = 'chrome_navigate';

  async execute(args: NavigateArgs): Promise<ToolResult> {
    const { url, waitUntil = 'load', timeout = 30000 } = args;

    if (!url) {
      return this.createErrorResult('URL 参数必填');
    }

    const u = safeParseUrl(url);
    if (!u) {
      return this.createErrorResult('URL 格式不正确');
    }

    // 小红书用户/笔记直达链接必须带 xsec_token，否则通常会被屏蔽
    if (isXhsHost(u.hostname) && (isXhsNotePath(u.pathname) || isXhsProfilePath(u.pathname)) && !getXsecToken(u)) {
      return this.createErrorResult('小红书用户/笔记链接必须包含 xsec_token（请从小红书内“分享-复制链接”获取完整链接）');
    }

    try {
      // 创建新标签页并激活到前台
      const tab = await chrome.tabs.create({
        url,
        active: true  // 激活到前台
      });

      if (!tab.id) {
        return this.createErrorResult('创建标签页失败');
      }

      const tabId = tab.id;

      // 根据 waitUntil 参数等待页面加载
      switch (waitUntil) {
        case 'load':
          await this.waitForTabLoad(tabId, timeout);
          break;

        case 'domcontentloaded':
          // Chrome Extension API 不直接支持 DOMContentLoaded
          // 使用脚本检测 readyState
          await this.waitForReadyState(tabId, 'interactive', timeout);
          break;

        case 'networkidle':
          // 等待页面加载完成 + 额外等待网络空闲
          await this.waitForTabLoad(tabId, timeout);
          await this.waitForNetworkIdle(tabId, 2000, timeout);
          break;
      }

      // 重新获取 tab 对象以获得最新的 URL（导航完成后）
      const updatedTab = await chrome.tabs.get(tabId);

      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              success: true,
              url: updatedTab.url,
              tabId: updatedTab.id,
            }),
          },
        ],
        isError: false,
      };
    } catch (error) {
      return this.createErrorResult(
        error instanceof Error ? error.message : '导航失败'
      );
    }
  }

  /**
   * 等待页面 readyState 达到指定状态
   */
  private async waitForReadyState(
    tabId: number,
    targetState: 'interactive' | 'complete',
    timeout: number
  ): Promise<void> {
    const startTime = Date.now();

    while (Date.now() - startTime < timeout) {
      try {
        const readyState = await this.executeInTab<string>(tabId, () => {
          return document.readyState;
        });

        if (readyState === targetState || readyState === 'complete') {
          return;
        }
      } catch (error) {
        // 页面可能还在加载，继续等待
      }

      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    throw new Error(`等待 readyState=${targetState} 超时`);
  }

  /**
   * 等待网络空闲 (简化版)
   * 检测一段时间内没有新的网络请求
   */
  private async waitForNetworkIdle(
    tabId: number,
    idleTime: number,
    timeout: number
  ): Promise<void> {
    const startTime = Date.now();
    let lastActivityTime = Date.now();

    // 监听网络请求
    const listener = (details: chrome.webRequest.WebRequestDetails) => {
      if (details.tabId === tabId) {
        lastActivityTime = Date.now();
      }
    };

    chrome.webRequest.onCompleted.addListener(listener, { urls: ['<all_urls>'] });

    try {
      while (Date.now() - startTime < timeout) {
        // 如果超过 idleTime 没有活动，认为网络空闲
        if (Date.now() - lastActivityTime >= idleTime) {
          return;
        }

        await new Promise((resolve) => setTimeout(resolve, 200));
      }

      throw new Error('等待网络空闲超时');
    } finally {
      chrome.webRequest.onCompleted.removeListener(listener);
    }
  }
}
