import { ToolResult, ERROR_MESSAGES, TIMEOUTS } from 'coagent-xhs-shared';
import {
  getXsecToken,
  isXhsHost,
  isXhsNotePath,
  isXhsProfilePath,
  safeParseUrl,
} from './xiaohongshu/xhs-url';

export type TabSessionSnapshot = {
  previousActiveTabId?: number;
  existingTabIds: Set<number>;
};

type PublishPageProbeResult = {
  currentUrl: string;
  hasPublishPath: boolean;
  hasRequiredCreatorTab: boolean;
  matchedCreatorTab: string;
  visibleCreatorTabs: string[];
  ready: boolean;
};

export type PublishedNoteProbeResult = {
  title: string;
  noteId: string;
  url: string;
};

/**
 * 基础工具类
 */
export abstract class BaseTool {
  abstract name: string;
  abstract execute(args: any): Promise<ToolResult>;

  /**
   * 等待标签页加载完成
   */
  protected async waitForTabLoad(tabId: number, timeout: number = 30000): Promise<void> {
    const tab = await chrome.tabs.get(tabId).catch(() => null);
    if (!tab) {
      throw new Error('Tab not found');
    }

    if (tab.status === 'complete') {
      return;
    }

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        chrome.tabs.onUpdated.removeListener(listener);
        chrome.tabs.onRemoved.removeListener(onRemoved);
        reject(new Error(ERROR_MESSAGES.PAGE_LOAD_TIMEOUT));
      }, timeout);

      const listener = (id: number, info: chrome.tabs.TabChangeInfo) => {
        if (id === tabId && info.status === 'complete') {
          chrome.tabs.onUpdated.removeListener(listener);
          chrome.tabs.onRemoved.removeListener(onRemoved);
          clearTimeout(timer);
          resolve();
        }
      };

      const onRemoved = (removedId: number) => {
        if (removedId === tabId) {
          chrome.tabs.onUpdated.removeListener(listener);
          chrome.tabs.onRemoved.removeListener(onRemoved);
          clearTimeout(timer);
          reject(new Error('Tab closed before load finished'));
        }
      };

      chrome.tabs.onUpdated.addListener(listener);
      chrome.tabs.onRemoved.addListener(onRemoved);
    });
  }

  /**
   * 在标签页中执行脚本
   */
  protected async executeInTab<T>(tabId: number, func: Function, args?: any[]): Promise<T> {
    const results = await chrome.scripting
      .executeScript({
        target: { tabId },
        func: func as any,
        args: args || [],
      })
      .catch((error) => {
        throw new Error(error && error.message ? error.message : 'Script execution failed');
      });

    if (!results || results.length === 0) {
      throw new Error('Script execution failed');
    }

    return results[0].result;
  }

  protected async waitForXHSPublishPageReady(
    tabId: number,
    options: {
      requiredCreatorTabs?: string[];
      retryIntervalsMs?: number[];
    } = {}
  ): Promise<PublishPageProbeResult> {
    const requiredCreatorTabs = (options.requiredCreatorTabs || [])
      .map((tab) => tab.trim())
      .filter(Boolean);
    const retryIntervalsMs =
      options.retryIntervalsMs && options.retryIntervalsMs.length > 0
        ? options.retryIntervalsMs
        : [2000, 8000, 16000];

    let lastProbe: PublishPageProbeResult | null = null;
    let lastProbeError: string | null = null;

    for (let attempt = 0; attempt <= retryIntervalsMs.length; attempt++) {
      try {
        lastProbe = await this.probeXHSPublishPage(tabId, requiredCreatorTabs);
        if (lastProbe.ready) {
          return lastProbe;
        }
      } catch (error) {
        lastProbeError = error instanceof Error ? error.message : String(error);
      }

      if (attempt === retryIntervalsMs.length) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, retryIntervalsMs[attempt]));
    }

    const requiredTabHint =
      requiredCreatorTabs.length > 0 ? `，且出现 Tab: ${requiredCreatorTabs.join(' / ')}` : '';
    const probeHint = lastProbe
      ? ` 当前URL: ${lastProbe.currentUrl || 'unknown'}；可见Tab: ${lastProbe.visibleCreatorTabs.join(', ') || 'none'}`
      : '';
    const errorHint = lastProbeError ? ` 最近错误: ${lastProbeError}` : '';

    throw new Error(
      `未成功进入小红书发布页（要求路径包含 /publish/publish${requiredTabHint}）。${probeHint}${errorHint}`
    );
  }

  protected async resolveLatestPublishedNoteFromManager(
    tabId: number,
    expectedTitle: string,
    options: {
      retryIntervalsMs?: number[];
      noteManagerUrl?: string;
    } = {}
  ): Promise<PublishedNoteProbeResult> {
    const retryIntervalsMs =
      options.retryIntervalsMs && options.retryIntervalsMs.length > 0
        ? options.retryIntervalsMs
        : [2000, 8000, 16000];
    const noteManagerUrl =
      options.noteManagerUrl || 'https://creator.xiaohongshu.com/new/note-manager?source=official';

    const normalize = (text: string): string => text.replace(/\s+/g, '').trim();
    const expectedTitleNormalized = normalize(expectedTitle || '');

    let lastProbe: {
      latestTitle: string;
      latestNoteId: string;
      latestNoteUrl: string;
      matched: boolean;
      noteCount: number;
      latestPublishTimeText: string;
      latestPublishTimeMs: number;
    } | null = null;
    let lastProbeError = '';

    for (let attempt = 0; attempt <= retryIntervalsMs.length; attempt++) {
      try {
        await chrome.tabs.update(tabId, { url: noteManagerUrl, active: true });
        await this.waitForTabLoad(tabId, 30000);
        await new Promise((resolve) => setTimeout(resolve, 500));

        const probe = await this.executeInTab<{
          latestTitle: string;
          latestNoteId: string;
          latestNoteUrl: string;
          matched: boolean;
          noteCount: number;
          latestPublishTimeText: string;
          latestPublishTimeMs: number;
        }>(
          tabId,
          (expectedNorm: string) => {
            const normalizeText = (text: string): string => text.replace(/\s+/g, '').trim();
            const isVisible = (el: Element): boolean => {
              const style = window.getComputedStyle(el);
              const rect = el.getBoundingClientRect();
              if (style.display === 'none' || style.visibility === 'hidden') return false;
              if (rect.width <= 0 || rect.height <= 0) return false;
              if (rect.left < -9000 || rect.top < -9000) return false;
              return true;
            };

            const parseNoteId = (noteEl: Element): string => {
              const candidates = [
                noteEl.getAttribute('data-note-id') || '',
                noteEl.getAttribute('data-noteid') || '',
                (noteEl as HTMLElement).dataset?.noteId || '',
                (noteEl as HTMLElement).dataset?.noteid || '',
              ].map((value) => value.trim());

              const direct = candidates.find((value) => Boolean(value));
              if (direct) return direct;

              const imp = noteEl.getAttribute('data-impression') || '';
              if (!imp) return '';
              try {
                const parsed = JSON.parse(imp);
                const byPath =
                  parsed?.noteTarget?.value?.noteId ||
                  parsed?.note_target?.value?.noteId ||
                  parsed?.note_id;
                if (typeof byPath === 'string' && byPath.trim()) {
                  return byPath.trim();
                }
              } catch {
                // ignore JSON parse failures, fallback to regex below
              }

              const match = imp.match(/"noteId"\s*:\s*"([^"]+)"/);
              return match ? match[1].trim() : '';
            };

            const extractTitle = (noteEl: Element): string => {
              const titleByClass =
                noteEl.querySelector('.title') || noteEl.querySelector('.note-title');
              if (titleByClass) {
                const value = (titleByClass.textContent || '').trim();
                if (value) return value;
              }

              const elements = Array.from(noteEl.querySelectorAll('*'));
              const titleFallback = elements.find((el) => {
                const text = (el.textContent || '').trim();
                if (!text) return false;
                if (text.startsWith('发布于')) return false;
                if (/权限设置|置顶|编辑|删除|浏览|点赞|收藏|评论/.test(text)) return false;
                return text.length <= 100;
              });
              return (titleFallback?.textContent || '').trim();
            };

            const matchesExpectedTitle = (actualTitle: string): boolean => {
              if (!expectedNorm) return true;
              const actual = normalizeText(actualTitle);
              if (!actual) return false;
              return (
                actual === expectedNorm ||
                actual.includes(expectedNorm) ||
                expectedNorm.includes(actual)
              );
            };

            const parsePublishTime = (noteEl: Element): { text: string; timestamp: number } => {
              const timeEl =
                noteEl.querySelector('.time_status') ||
                Array.from(noteEl.querySelectorAll('*')).find((el) =>
                  /^(发布于|定时发布)/.test((el.textContent || '').trim())
                );
              const rawText = (timeEl?.textContent || '').trim();
              if (!rawText) {
                return { text: '', timestamp: 0 };
              }

              const match = rawText.match(
                /(?:发布于|定时发布)\s*(\d{4})年(\d{2})月(\d{2})日\s+(\d{2}):(\d{2})/
              );
              if (!match) {
                return { text: rawText, timestamp: 0 };
              }

              const year = Number(match[1]);
              const month = Number(match[2]);
              const day = Number(match[3]);
              const hour = Number(match[4]);
              const minute = Number(match[5]);
              const value = new Date(year, month - 1, day, hour, minute, 0, 0).getTime();
              return { text: rawText, timestamp: Number.isFinite(value) ? value : 0 };
            };

            const notes = Array.from(
              document.querySelectorAll('.notes-container .content .note, .notes-container .note')
            ).filter((el) => isVisible(el));
            const latest = notes[0] || null;

            if (!latest) {
              return {
                latestTitle: '',
                latestNoteId: '',
                latestNoteUrl: '',
                matched: false,
                noteCount: 0,
                latestPublishTimeText: '',
                latestPublishTimeMs: 0,
              };
            }

            const latestTitle = extractTitle(latest);
            const latestNoteId = parseNoteId(latest);
            const publishTime = parsePublishTime(latest);
            const latestNoteUrl = latestNoteId
              ? `https://www.xiaohongshu.com/explore/${latestNoteId}?xsec_source=pc_creatormng`
              : '';

            return {
              latestTitle,
              latestNoteId,
              latestNoteUrl,
              matched:
                Boolean(latestTitle) &&
                Boolean(latestNoteId) &&
                matchesExpectedTitle(latestTitle),
              noteCount: notes.length,
              latestPublishTimeText: publishTime.text,
              latestPublishTimeMs: publishTime.timestamp,
            };
          },
          [expectedTitleNormalized]
        );

        lastProbe = probe;

        if (probe.matched && probe.latestTitle && probe.latestNoteId) {
          return {
            title: probe.latestTitle,
            noteId: probe.latestNoteId,
            url: probe.latestNoteUrl,
          };
        }
      } catch (error) {
        lastProbeError = error instanceof Error ? error.message : String(error);
      }

      if (attempt === retryIntervalsMs.length) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, retryIntervalsMs[attempt]));
    }

    const probeHint = lastProbe
      ? ` latestTitle="${lastProbe.latestTitle || 'unknown'}" latestNoteId="${lastProbe.latestNoteId || 'unknown'}" publishTime="${lastProbe.latestPublishTimeText || 'unknown'}" noteCount=${lastProbe.noteCount}`
      : '';
    const errorHint = lastProbeError ? ` 最近错误: ${lastProbeError}` : '';
    throw new Error(
      `发布成功后进入笔记管理页未获取到匹配笔记（需要 title + noteId）。${probeHint}${errorHint}`
    );
  }

  private async probeXHSPublishPage(
    tabId: number,
    requiredCreatorTabs: string[]
  ): Promise<PublishPageProbeResult> {
    return await this.executeInTab<PublishPageProbeResult>(
      tabId,
      (expectedTabs: string[]): PublishPageProbeResult => {
        const normalizeText = (text: string): string => text.replace(/\s+/g, '').trim();
        const expected = (expectedTabs || []).map(normalizeText).filter(Boolean);

        const isVisible = (el: Element): boolean => {
          const style = window.getComputedStyle(el);
          const rect = el.getBoundingClientRect();
          if (style.display === 'none' || style.visibility === 'hidden') return false;
          if (rect.width <= 0 || rect.height <= 0) return false;
          if (rect.left < -9000 || rect.top < -9000) return false;
          return true;
        };

        const visibleCreatorTabs = Array.from(document.querySelectorAll('div.creator-tab'))
          .filter((node) => isVisible(node))
          .map((node) => (node.textContent || '').trim())
          .filter(Boolean);

        const normalizedTabs = visibleCreatorTabs.map(normalizeText);
        const matchedCreatorTab =
          expected.find((target) =>
            normalizedTabs.some((tabText) => tabText === target || tabText.includes(target))
          ) || '';

        const currentUrl = window.location.href || '';
        const hasPublishPath =
          window.location.pathname.includes('/publish/publish') ||
          currentUrl.includes('/publish/publish');
        const hasRequiredCreatorTab = expected.length === 0 ? true : Boolean(matchedCreatorTab);
        const ready = hasPublishPath && hasRequiredCreatorTab;

        return {
          currentUrl,
          hasPublishPath,
          hasRequiredCreatorTab,
          matchedCreatorTab,
          visibleCreatorTabs,
          ready,
        };
      },
      [requiredCreatorTabs]
    );
  }

  /**
   * 创建隔离标签页（不复用已有标签页）。
   */
  protected async createIsolatedTab(url?: string): Promise<chrome.tabs.Tab> {
    const newTab = await chrome.tabs.create({
      url: url || 'about:blank',
      active: true,
    });

    if (newTab.id && url) {
      await this.waitForTabLoad(newTab.id);
    }

    return newTab;
  }

  /**
   * 创建小红书隔离标签页（不复用已有标签页）。
   * @param url 可选，如果提供则导航到该 URL 并等待加载
   */
  protected async createIsolatedXHSTab(url?: string): Promise<chrome.tabs.Tab> {
    // 小红书用户/笔记直达链接必须带 xsec_token，否则通常会被屏蔽
    if (url) {
      const u = safeParseUrl(url);
      if (
        u &&
        isXhsHost(u.hostname) &&
        (isXhsNotePath(u.pathname) || isXhsProfilePath(u.pathname)) &&
        !getXsecToken(u)
      ) {
        throw new Error(
          '小红书用户/笔记链接必须包含 xsec_token（请从小红书内“分享-复制链接”获取完整链接）'
        );
      }
    }

    return this.createIsolatedTab(url);
  }

  /**
   * 查找或创建小红书标签页
   * - 优先复用已存在的小红书标签页
   * - 如果没有，则创建新标签页
   *
   * @param url 可选，如果提供则导航到该 URL 并等待加载
   *            如果不提供，则只返回标签页（用于需要先 attach 调试器再导航的场景）
   * @deprecated 批量发布场景请使用 createIsolatedXHSTab，避免 Tab 复用冲突
   */
  protected async findOrCreateXHSTab(url?: string): Promise<chrome.tabs.Tab> {
    // 小红书用户/笔记直达链接必须带 xsec_token，否则通常会被屏蔽
    if (url) {
      const u = safeParseUrl(url);
      if (
        u &&
        isXhsHost(u.hostname) &&
        (isXhsNotePath(u.pathname) || isXhsProfilePath(u.pathname)) &&
        !getXsecToken(u)
      ) {
        throw new Error(
          '小红书用户/笔记链接必须包含 xsec_token（请从小红书内“分享-复制链接”获取完整链接）'
        );
      }
    }

    // 查找已存在的小红书标签页（不包括当前活动标签页）
    const [activeTab] = await chrome.tabs.query({ active: true, currentWindow: true });
    const tabs = await chrome.tabs.query({
      url: 'https://*.xiaohongshu.com/*',
    });

    // 过滤掉当前活动标签页，避免影响用户正在使用的页面
    const xhsTabs = tabs.filter((tab) => tab.id !== activeTab?.id);

    if (xhsTabs.length > 0) {
      const tab = xhsTabs[0];
      if (tab.id) {
        // 激活标签页到前台
        await chrome.tabs.update(tab.id, { active: true });
        // 如果提供了 URL，则导航并等待加载
        if (url) {
          await chrome.tabs.update(tab.id, { url });
          await this.waitForTabLoad(tab.id);
        }
      }
      return tab;
    }

    // 创建新标签页（激活到前台）
    // 如果不提供 URL，创建空白页
    const newTab = await chrome.tabs.create({
      url: url || 'about:blank',
      active: true,
    });
    if (newTab.id && url) {
      await this.waitForTabLoad(newTab.id);
    }
    return newTab;
  }

  /**
   * 获取当前活动标签页
   */
  protected async getActiveTab(): Promise<chrome.tabs.Tab | undefined> {
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    return tab;
  }

  /**
   * 查找或创建抖音标签页
   * - 优先复用已存在的抖音创作者平台标签页
   * - 如果没有，则创建新标签页并激活到前台
   */
  protected async findOrCreateDouyinTab(url: string): Promise<chrome.tabs.Tab> {
    // 查找已存在的抖音创作者平台标签页（不包括当前活动标签页）
    const [activeTab] = await chrome.tabs.query({ active: true, currentWindow: true });
    const tabs = await chrome.tabs.query({
      url: 'https://creator.douyin.com/*',
    });

    // 过滤掉当前活动标签页，避免影响用户正在使用的页面
    const douyinTabs = tabs.filter((tab) => tab.id !== activeTab?.id);

    if (douyinTabs.length > 0) {
      const tab = douyinTabs[0];
      if (tab.id) {
        await chrome.tabs.update(tab.id, { url, active: true }); // 激活标签页到前台
        await this.waitForTabLoad(tab.id);
      }
      return tab;
    }

    // 创建新标签页（激活到前台）
    const newTab = await chrome.tabs.create({
      url,
      active: true, // 激活到前台
    });
    if (newTab.id) {
      await this.waitForTabLoad(newTab.id);
    }
    return newTab;
  }

  /**
   * 创建错误结果
   */
  protected createErrorResult(message: string): ToolResult {
    return {
      content: [
        {
          type: 'error',
          text: message,
        },
      ],
      isError: true,
    };
  }

  /**
   * 等待页面导航完成
   * @param tabId 标签页ID
   * @param timeout 超时时间(毫秒)
   * @returns Promise<boolean> 是否发生了导航
   */
  protected async waitForNavigation(
    tabId: number,
    timeout: number = TIMEOUTS.NAVIGATION_WAIT
  ): Promise<boolean> {
    return new Promise((resolve) => {
      let navigationOccurred = false;
      const startTime = Date.now();

      const timer = setTimeout(() => {
        chrome.webNavigation.onCompleted.removeListener(onCompleted);
        chrome.webNavigation.onErrorOccurred.removeListener(onError);
        chrome.tabs.onRemoved.removeListener(onRemoved);
        resolve(navigationOccurred);
      }, timeout);

      const onCompleted = (details: chrome.webNavigation.WebNavigationFramedCallbackDetails) => {
        if (details.tabId === tabId && details.frameId === 0) {
          navigationOccurred = true;
          cleanup();
          resolve(true);
        }
      };

      const onError = (details: chrome.webNavigation.WebNavigationFramedErrorCallbackDetails) => {
        if (details.tabId === tabId && details.frameId === 0) {
          cleanup();
          resolve(false);
        }
      };

      const onRemoved = (removedTabId: number) => {
        if (removedTabId === tabId) {
          cleanup();
          resolve(false);
        }
      };

      const cleanup = () => {
        clearTimeout(timer);
        chrome.webNavigation.onCompleted.removeListener(onCompleted);
        chrome.webNavigation.onErrorOccurred.removeListener(onError);
        chrome.tabs.onRemoved.removeListener(onRemoved);
      };

      // 添加监听器
      chrome.webNavigation.onCompleted.addListener(onCompleted);
      chrome.webNavigation.onErrorOccurred.addListener(onError);
      chrome.tabs.onRemoved.addListener(onRemoved);

      // 检查是否立即发生导航
      chrome.tabs.get(tabId, (tab) => {
        if (chrome.runtime.lastError || !tab) {
          cleanup();
          resolve(false);
        }
      });
    });
  }

  /**
   * 点击元素并可选等待导航
   */
  protected async clickAndWaitForNavigation(
    tabId: number,
    clickFunction: () => Promise<void>,
    waitForNav: boolean = true,
    timeout: number = TIMEOUTS.NAVIGATION_WAIT
  ): Promise<{ clicked: boolean; navigationOccurred: boolean }> {
    if (!waitForNav) {
      await clickFunction();
      return { clicked: true, navigationOccurred: false };
    }

    const navigationPromise = this.waitForNavigation(tabId, timeout);

    try {
      await clickFunction();
    } catch (error) {
      navigationPromise.catch(() => undefined);
      throw error;
    }

    const navigationOccurred = await navigationPromise;

    return { clicked: true, navigationOccurred };
  }

  /**
   * 记录工具执行前的标签页状态，用于执行后回收与还原。
   */
  protected async captureTabSessionSnapshot(): Promise<TabSessionSnapshot> {
    const [activeTab] = await chrome.tabs.query({ active: true, currentWindow: true });
    const allTabs = await chrome.tabs.query({});
    const existingTabIds = new Set<number>();
    for (const tab of allTabs) {
      if (typeof tab.id === 'number') {
        existingTabIds.add(tab.id);
      }
    }
    return {
      previousActiveTabId: activeTab?.id,
      existingTabIds,
    };
  }

  /**
   * 判断标签页是否由本次工具执行新创建。
   */
  protected wasTabCreatedInSession(
    tabId: number | undefined,
    snapshot: TabSessionSnapshot | null
  ): boolean {
    if (typeof tabId !== 'number' || !snapshot) return false;
    return !snapshot.existingTabIds.has(tabId);
  }

  /**
   * 安全关闭标签页（忽略页面已关闭等异常）。
   */
  protected async closeTabSafely(tabId: number | undefined): Promise<void> {
    if (typeof tabId !== 'number') return;
    try {
      await chrome.tabs.remove(tabId);
    } catch {
      // ignore
    }
  }

  /**
   * 安全恢复到执行前的活动标签页（忽略页面不存在等异常）。
   */
  protected async restoreActiveTabSafely(tabId: number | undefined): Promise<void> {
    if (typeof tabId !== 'number') return;
    try {
      await chrome.tabs.update(tabId, { active: true });
    } catch {
      // ignore
    }
  }

  /**
   * 工具执行后的通用清理：
   * 1) 可选关闭本次新创建标签页
   * 2) 还原到执行前活动标签页
   */
  protected async cleanupToolTabSession(
    currentTabId: number | undefined,
    snapshot: TabSessionSnapshot | null,
    options: {
      closeIfCreated?: boolean;
      restoreActiveTab?: boolean;
    } = {}
  ): Promise<void> {
    const closeIfCreated = options.closeIfCreated !== false;
    const restoreActiveTab = options.restoreActiveTab !== false;

    if (closeIfCreated && this.wasTabCreatedInSession(currentTabId, snapshot)) {
      await this.closeTabSafely(currentTabId);
    }

    if (
      restoreActiveTab &&
      typeof snapshot?.previousActiveTabId === 'number' &&
      snapshot.previousActiveTabId !== currentTabId
    ) {
      await this.restoreActiveTabSafely(snapshot.previousActiveTabId);
    }
  }
}
