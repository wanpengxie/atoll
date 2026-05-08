import { BaseTool, type TabSessionSnapshot } from '../base-tool';
import type { ToolResult } from 'coagent-xhs-shared';
import { CDPInterceptor } from '../cdp-interceptor';

type RankType = 'day' | 'week' | 'month';

interface GetTrendingTopicsArgs {
  rankType?: RankType; // 榜单类型，支持 day/week/month，默认 day
  limit?: number; // 返回条数，默认 20，最大 100
  keyword?: string; // 关键词（如果提供，则走笔记搜索页）
  searchword?: string; // keyword 的别名（兼容 URL 参数命名）
}

interface NewrankHotWordItem {
  hotWord?: string;
  hotScore?: number;
  noteLabel?: Array<{
    label?: string;
  }>;
  rankDate?: string;
  noteCount?: number;
  hotNoteCount?: string | number;
  rankType?: RankType;
}

interface NewrankHotWordResponse {
  code?: number;
  msg?: string | null;
  data?: {
    list?: NewrankHotWordItem[];
  };
}

interface NoteSearchItem {
  id?: string;
  title?: string;
  type?: string;
  createTime?: string;
  desc?: string;
  likedCount?: string | number;
  collectedCount?: string | number;
  commentsCount?: string | number;
  sharedCount?: string | number;
  interactiveCount?: string | number;
  readCount?: string | number;
  predReadnum?: string | number;
  coverUrl?: string;
  cover?: string;
  firstFrameUrl?: string;
  noteCounterTypeV1?: string;
  noteCounterTypeV2?: string;
  officialKeyword?: string[];
  topics?: Array<{ name?: string }>;
  user?: {
    nickname?: string;
    userid?: string;
    redId?: string;
    fans?: number;
    accountTypeV1?: string;
    accountTypeV2?: string;
  };
}

interface NoteSearchResponse {
  code?: number;
  msg?: string;
  data?: {
    list?: NoteSearchItem[];
  };
}

interface NoteSearchStatResponse {
  code?: number;
  msg?: string;
  data?: {
    noteCount?: string | number;
    businessCount?: string | number;
    deleteNoteCount?: string | number;
    sumLike?: number;
    sumCollect?: number;
    sumComment?: number;
    incompleteCount?: number;
  };
}

const NEWRANK_URLS = {
  HOME: 'https://xh.newrank.cn/',
  NOTE_SEARCH: 'https://xh.newrank.cn/notes/notesSearch',
};

const API_PATTERNS = {
  HOT_WORD_LIST: '/api/xhsv2/nr/app/xh/v2/rank/home/hotWordList',
  NOTE_SEARCH: '/api/xh/xdnphb/nr/app/xhs/note/search',
  NOTE_SEARCH_STAT: '/api/xh/xdnphb/nr/app/xhs/note/search/statisticalFuzzy',
};

const RANK_TAB_TEXT: Record<RankType, string> = {
  day: '日榜',
  week: '周榜',
  month: '月榜',
};

const NOTE_SEARCH_PAGE_SIZE = 20;
const NOTE_SEARCH_INITIAL_TIMEOUT = 20000;
const NOTE_SEARCH_SCROLL_TIMEOUT = 8000;
const NOTE_SEARCH_SCROLL_DELAY_MIN_MS = 1500;
const NOTE_SEARCH_SCROLL_DELAY_MAX_MS = 5000;
const NOTE_SEARCH_MAX_SCROLL_ROUNDS = 12;

/**
 * xhs_get_trending_topics - 通过新红获取小红书热点数据
 *
 * 逻辑：
 * 1) 不传 keyword/searchword：抓取首页热搜词榜单（day/week/month）
 * 2) 传 keyword/searchword：抓取笔记搜索页热点笔记列表（notesSearch）
 */
export class XhsGetTrendingTopicsTool extends BaseTool {
  name = 'xhs_get_trending_topics';

  async execute(args: GetTrendingTopicsArgs = {}): Promise<ToolResult> {
    const keyword = this.normalizeKeyword(args);
    const rankType = this.normalizeRankType(args.rankType);
    const limit = this.normalizeLimit(args.limit);

    if (keyword) {
      return this.executeKeywordNoteSearch(keyword, limit);
    }

    let interceptor: CDPInterceptor | null = null;
    let tabSnapshot: TabSessionSnapshot | null = null;
    let toolTabId: number | undefined;

    try {
      tabSnapshot = await this.captureTabSessionSnapshot();
      const tab = await this.findOrCreateNewrankTab(NEWRANK_URLS.HOME);
      if (!tab.id) {
        throw new Error('无法创建标签页');
      }
      toolTabId = tab.id;

      interceptor = new CDPInterceptor(tab.id, [API_PATTERNS.HOT_WORD_LIST]);
      await interceptor.attach();

      // 每次重载首页，确保触发最新的日榜请求
      await chrome.tabs.update(tab.id, { url: NEWRANK_URLS.HOME });
      await this.waitForTabLoad(tab.id, 30000);

      const dayResponse = await this.waitForRankTypeResponse(interceptor, 'day', 20000);

      if (!dayResponse) {
        return this.createErrorResult(
          '未捕获到新红热搜接口响应，请确认已登录 xh.newrank.cn 后重试'
        );
      }

      let targetResponse = dayResponse;

      if (rankType !== 'day') {
        const switchResult = await this.switchRankTab(tab.id, rankType);
        if (!switchResult.success) {
          return this.createErrorResult(switchResult.error || '切换榜单失败');
        }

        const switchedResponse = await this.waitForRankTypeResponse(
          interceptor,
          rankType,
          20000
        );

        if (!switchedResponse) {
          return this.createErrorResult(`切换到 ${rankType} 榜后未捕获到接口响应`);
        }

        targetResponse = switchedResponse;
      }

      if (targetResponse.code !== 2000 || !targetResponse.data?.list) {
        const errorMsg = targetResponse.msg || `接口返回异常 code=${targetResponse.code || 'unknown'}`;
        return this.createErrorResult(`获取热点失败: ${errorMsg}`);
      }

      const topics = this.buildTopics(targetResponse.data.list, limit);
      const result = {
        success: true,
        source: 'xh.newrank.cn',
        rank_type: rankType,
        total: topics.length,
        fetched_at: new Date().toISOString(),
        topics,
      };

      return {
        content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
        isError: false,
      };
    } catch (error) {
      console.error('[xhs_get_trending_topics] Error:', error);
      return this.createErrorResult(
        error instanceof Error ? error.message : '获取热点数据失败'
      );
    } finally {
      if (interceptor) {
        await interceptor.detach();
      }
      await this.cleanupToolTabSession(toolTabId, tabSnapshot, {
        closeIfCreated: true,
        restoreActiveTab: true,
      });
    }
  }

  private async executeKeywordNoteSearch(
    keyword: string,
    limit: number
  ): Promise<ToolResult> {
    let interceptor: CDPInterceptor | null = null;
    let tabSnapshot: TabSessionSnapshot | null = null;
    let toolTabId: number | undefined;

    try {
      const targetUrl = `${NEWRANK_URLS.NOTE_SEARCH}?searchword=${encodeURIComponent(keyword)}`;
      tabSnapshot = await this.captureTabSessionSnapshot();
      const tab = await this.findOrCreateNewrankTab(targetUrl);
      if (!tab.id) {
        throw new Error('无法创建标签页');
      }
      toolTabId = tab.id;

      interceptor = new CDPInterceptor(tab.id, [
        API_PATTERNS.NOTE_SEARCH_STAT,
        API_PATTERNS.NOTE_SEARCH,
      ]);
      await interceptor.attach();

      await chrome.tabs.update(tab.id, { url: targetUrl });
      await this.waitForTabLoad(tab.id, 30000);

      const noteSearchResponse = (await interceptor.waitForResponse(
        API_PATTERNS.NOTE_SEARCH,
        NOTE_SEARCH_INITIAL_TIMEOUT
      )) as NoteSearchResponse | null;

      if (!noteSearchResponse) {
        return this.createErrorResult(
          '未捕获到笔记搜索接口响应，请确认已登录 xh.newrank.cn 后重试'
        );
      }

      if (noteSearchResponse.code !== 2000 || !noteSearchResponse.data?.list) {
        return this.createErrorResult(
          `获取笔记搜索结果失败: ${noteSearchResponse.msg || '未知错误'}`
        );
      }

      const statResponse = (await interceptor.waitForResponse(
        API_PATTERNS.NOTE_SEARCH_STAT,
        5000
      )) as NoteSearchStatResponse | null;

      let collectedNotes = this.mergeUniqueNotes([], noteSearchResponse.data.list);
      collectedNotes = await this.collectNotesByScroll(
        tab.id,
        interceptor,
        collectedNotes,
        limit
      );

      const notes = collectedNotes
        .slice(0, limit)
        .map((item, index) => this.mapNoteSearchItem(item, index));

      const stats = statResponse?.data
        ? {
            note_count: this.toInt(statResponse.data.noteCount),
            business_count: this.toInt(statResponse.data.businessCount),
            delete_note_count: this.toInt(statResponse.data.deleteNoteCount),
            sum_like: this.toInt(statResponse.data.sumLike),
            sum_collect: this.toInt(statResponse.data.sumCollect),
            sum_comment: this.toInt(statResponse.data.sumComment),
            incomplete_count: this.toInt(statResponse.data.incompleteCount),
          }
        : null;

      const result = {
        success: true,
        source: 'xh.newrank.cn',
        mode: 'note_search',
        keyword,
        total: notes.length,
        fetched_at: new Date().toISOString(),
        stats,
        notes,
      };

      return {
        content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
        isError: false,
      };
    } catch (error) {
      console.error('[xhs_get_trending_topics:note_search] Error:', error);
      return this.createErrorResult(
        error instanceof Error ? error.message : '获取关键词热点笔记失败'
      );
    } finally {
      if (interceptor) {
        await interceptor.detach();
      }
      await this.cleanupToolTabSession(toolTabId, tabSnapshot, {
        closeIfCreated: true,
        restoreActiveTab: true,
      });
    }
  }

  private normalizeRankType(rankType?: string): RankType {
    if (rankType === 'week' || rankType === 'month') {
      return rankType;
    }
    return 'day';
  }

  private normalizeKeyword(args: GetTrendingTopicsArgs): string {
    const keyword = (args.keyword || args.searchword || '').trim();
    return keyword;
  }

  private normalizeLimit(limit?: number): number {
    if (typeof limit !== 'number' || Number.isNaN(limit)) {
      return 20;
    }
    const normalized = Math.floor(limit);
    if (normalized <= 0) {
      return 20;
    }
    return Math.min(normalized, 100);
  }

  private toInt(value: string | number | undefined): number {
    if (typeof value === 'number') {
      return Number.isFinite(value) ? Math.trunc(value) : 0;
    }
    if (typeof value === 'string') {
      const cleaned = value.replace(/,/g, '');
      const parsed = Number(cleaned);
      return Number.isFinite(parsed) ? Math.trunc(parsed) : 0;
    }
    return 0;
  }

  private normalizeDesc(rawDesc: string): string {
    if (!rawDesc) {
      return '';
    }

    return rawDesc
      .replace(/<span[^>]*>/gi, '')
      .replace(/<\/span>/gi, '')
      .replace(/<br\s*\/?>/gi, '\n')
      .replace(/&nbsp;/gi, ' ')
      .replace(/&amp;/gi, '&')
      .replace(/&lt;/gi, '<')
      .replace(/&gt;/gi, '>')
      .trim();
  }

  private mapNoteSearchItem(item: NoteSearchItem, index: number) {
    const descHtml = item.desc || '';
    const descText = this.normalizeDesc(descHtml);

    return {
      rank: index + 1,
      note_id: item.id || '',
      title: item.title || '',
      note_type: item.type || '',
      create_time: item.createTime || '',
      author: {
        nickname: item.user?.nickname || '',
        user_id: item.user?.userid || '',
        red_id: item.user?.redId || '',
        fans: Number(item.user?.fans || 0),
        account_type_lv1: item.user?.accountTypeV1 || '',
        account_type_lv2: item.user?.accountTypeV2 || '',
      },
      metrics: {
        like_count: this.toInt(item.likedCount),
        collect_count: this.toInt(item.collectedCount),
        comment_count: this.toInt(item.commentsCount),
        share_count: this.toInt(item.sharedCount),
        interaction_count: this.toInt(item.interactiveCount),
        read_count: this.toInt(item.readCount),
        pred_read_count: this.toInt(item.predReadnum),
      },
      cover_url: item.coverUrl || item.cover || item.firstFrameUrl || '',
      category_lv1: item.noteCounterTypeV1 || '',
      category_lv2: item.noteCounterTypeV2 || '',
      topics: Array.isArray(item.topics)
        ? item.topics
            .map((topic) => (topic?.name || '').trim())
            .filter((topic) => topic.length > 0)
        : [],
      official_keywords: Array.isArray(item.officialKeyword) ? item.officialKeyword : [],
      // 纯文本正文（去掉高亮 span），优先给消费方使用
      desc: descText,
      // 保留接口原始 HTML 片段，便于调试高亮关键词
      desc_html: descHtml,
      // 搜索接口返回的可能是关键词命中片段而非完整正文
      desc_maybe_snippet:
        descText.includes('...') || descText.startsWith('...') || descText.endsWith('...'),
    };
  }

  private async collectNotesByScroll(
    tabId: number,
    interceptor: CDPInterceptor,
    initialNotes: NoteSearchItem[],
    limit: number
  ): Promise<NoteSearchItem[]> {
    let collected = [...initialNotes];

    if (collected.length >= limit) {
      return collected;
    }

    const neededPages = Math.max(1, Math.ceil(limit / NOTE_SEARCH_PAGE_SIZE));
    const maxRounds = Math.min(neededPages + 2, NOTE_SEARCH_MAX_SCROLL_ROUNDS);

    for (let round = 0; round < maxRounds && collected.length < limit; round += 1) {
      const scrollState = await this.scrollNoteSearchPage(tabId);
      if (!scrollState.didScroll) {
        break;
      }

      await this.delay(
        this.randomBetween(
          NOTE_SEARCH_SCROLL_DELAY_MIN_MS,
          NOTE_SEARCH_SCROLL_DELAY_MAX_MS
        )
      );

      const nextResponse = (await interceptor.waitForResponse(
        API_PATTERNS.NOTE_SEARCH,
        NOTE_SEARCH_SCROLL_TIMEOUT
      )) as NoteSearchResponse | null;

      if (!nextResponse || nextResponse.code !== 2000 || !nextResponse.data?.list?.length) {
        if (scrollState.reachedBottom) {
          break;
        }
        continue;
      }

      const beforeCount = collected.length;
      collected = this.mergeUniqueNotes(collected, nextResponse.data.list);

      if (collected.length === beforeCount && scrollState.reachedBottom) {
        break;
      }
    }

    return collected;
  }

  private mergeUniqueNotes(existing: NoteSearchItem[], incoming: NoteSearchItem[]): NoteSearchItem[] {
    if (!incoming.length) {
      return existing;
    }

    const merged = [...existing];
    const seen = new Set(merged.map((item) => this.getNoteDedupKey(item)));

    for (const item of incoming) {
      const dedupKey = this.getNoteDedupKey(item);
      if (seen.has(dedupKey)) {
        continue;
      }
      seen.add(dedupKey);
      merged.push(item);
    }

    return merged;
  }

  private getNoteDedupKey(item: NoteSearchItem): string {
    const id = (item.id || '').trim();
    if (id) {
      return `id:${id}`;
    }

    const title = (item.title || '').trim();
    const author =
      (item.user?.userid || item.user?.redId || item.user?.nickname || '').trim();
    const createTime = (item.createTime || '').trim();
    return `fallback:${title}|${author}|${createTime}`;
  }

  private async scrollNoteSearchPage(
    tabId: number
  ): Promise<{ didScroll: boolean; reachedBottom: boolean }> {
    return this.executeInTab<{ didScroll: boolean; reachedBottom: boolean }>(tabId, () => {
      const candidates = Array.from(
        document.querySelectorAll<HTMLElement>('main, section, div, ul')
      ).filter((el) => {
        const style = window.getComputedStyle(el);
        const overflowY = style.overflowY;
        const scrollable = overflowY === 'auto' || overflowY === 'scroll';
        return scrollable && el.scrollHeight - el.clientHeight > 80;
      });

      const scrollTarget =
        candidates.sort((a, b) => b.scrollHeight - a.scrollHeight)[0] ||
        (document.scrollingElement as HTMLElement | null) ||
        document.documentElement;

      const maxScrollTop = Math.max(0, scrollTarget.scrollHeight - scrollTarget.clientHeight);
      if (maxScrollTop <= 0) {
        return { didScroll: false, reachedBottom: true };
      }

      const startTop = scrollTarget.scrollTop;
      const step = Math.max(Math.floor(scrollTarget.clientHeight * 0.85), 600);
      const nextTop = Math.min(startTop + step, maxScrollTop);

      if (
        scrollTarget === document.body ||
        scrollTarget === document.documentElement ||
        scrollTarget === document.scrollingElement
      ) {
        window.scrollTo({ top: nextTop, behavior: 'auto' });
      } else {
        scrollTarget.scrollTo({ top: nextTop, behavior: 'auto' });
      }

      return {
        didScroll: nextTop > startTop + 1,
        reachedBottom: nextTop >= maxScrollTop - 2,
      };
    });
  }

  private async delay(ms: number): Promise<void> {
    await new Promise((resolve) => setTimeout(resolve, ms));
  }

  private randomBetween(min: number, max: number): number {
    if (max <= min) {
      return min;
    }
    return Math.floor(Math.random() * (max - min + 1)) + min;
  }

  private buildTopics(items: NewrankHotWordItem[], limit: number) {
    return items.slice(0, limit).map((item, index) => {
      const labels = Array.isArray(item.noteLabel)
        ? item.noteLabel
            .map((label) => (label?.label || '').trim())
            .filter((label) => label.length > 0)
        : [];

      return {
        rank: index + 1,
        keyword: item.hotWord || '',
        heat_score: Number(item.hotScore || 0),
        category: labels[0] || '',
        related_keywords: labels,
        rank_date: item.rankDate || '',
        note_count: Number(item.noteCount || 0),
        hot_note_count: Number(item.hotNoteCount || 0),
      };
    });
  }

  private async switchRankTab(
    tabId: number,
    rankType: RankType
  ): Promise<{ success: boolean; error?: string }> {
    const targetTabText = RANK_TAB_TEXT[rankType];

    return this.executeInTab<{ success: boolean; error?: string }>(
      tabId,
      (tabText: string) => {
        const allElements = Array.from(document.querySelectorAll('*'));

        // 锁定“热搜词榜单”模块，避免误点到其它榜单模块
        const titleNode = allElements.find((el) => el.textContent?.trim() === '热搜词榜单');
        let scope: Element = document.body;

        if (titleNode) {
          let container: Element | null = titleNode;
          for (let i = 0; i < 8 && container; i += 1) {
            if (container.textContent?.includes('TOP热搜词')) {
              scope = container;
              break;
            }
            container = container.parentElement;
          }
        }

        const scopedElements = Array.from(scope.querySelectorAll('*'));
        const tabElement =
          scopedElements.find((el) => el.textContent?.trim() === tabText) ||
          allElements.find((el) => el.textContent?.trim() === tabText);

        if (!tabElement) {
          return { success: false, error: `未找到榜单切换按钮: ${tabText}` };
        }

        (tabElement as HTMLElement).click();
        return { success: true };
      },
      [targetTabText]
    );
  }

  private async waitForRankTypeResponse(
    interceptor: CDPInterceptor,
    expectedRankType: RankType,
    timeoutMs: number
  ): Promise<NewrankHotWordResponse | null> {
    const startedAt = Date.now();

    while (Date.now() - startedAt < timeoutMs) {
      const remain = timeoutMs - (Date.now() - startedAt);
      const raw = (await interceptor.waitForResponse(
        API_PATTERNS.HOT_WORD_LIST,
        remain
      )) as NewrankHotWordResponse | null;

      if (!raw) {
        return null;
      }

      const actualRankType = raw.data?.list?.[0]?.rankType;
      if (!actualRankType || actualRankType === expectedRankType) {
        return raw;
      }
    }

    return null;
  }

  private async findOrCreateNewrankTab(url: string): Promise<chrome.tabs.Tab> {
    const [activeTab] = await chrome.tabs.query({ active: true, currentWindow: true });
    const tabs = await chrome.tabs.query({
      url: 'https://xh.newrank.cn/*',
    });

    const newrankTabs = tabs.filter((tab) => tab.id !== activeTab?.id);
    if (newrankTabs.length > 0) {
      const tab = newrankTabs[0];
      if (tab.id) {
        await chrome.tabs.update(tab.id, { active: true });
      }
      return tab;
    }

    const newTab = await chrome.tabs.create({
      url,
      active: true,
    });
    if (newTab.id) {
      await this.waitForTabLoad(newTab.id, 30000);
    }
    return newTab;
  }
}
