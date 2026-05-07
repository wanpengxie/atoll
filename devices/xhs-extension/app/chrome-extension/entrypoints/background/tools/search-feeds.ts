import {
  XIAOHONGSHU_TOOL_NAMES,
  ToolResult,
  XIAOHONGSHU_URLS,
  Feed,
  ERROR_MESSAGES,
} from 'xiaohongshu-mcp-shared';
import { BaseTool, type TabSessionSnapshot } from './base-tool';
import { runInMainWorld } from './inject-script';
import { waitForLoadState } from './wait-utils';
import { buildXhsExploreNoteUrl } from './xiaohongshu/xhs-url';

export class SearchFeedsTool extends BaseTool {
  name = XIAOHONGSHU_TOOL_NAMES.SEARCH_FEEDS;

  async execute(args: any): Promise<ToolResult> {
    let tabSnapshot: TabSessionSnapshot | null = null;
    let toolTabId: number | undefined;

    try {
      // 验证参数
      if (!args.keyword) {
        throw new Error(ERROR_MESSAGES.INVALID_ARGUMENTS + ': keyword is required');
      }

      const keyword = args.keyword;
      const sort = args.sort || 'general';
      const limit = args.limit || 20;

      const searchUrl = `${XIAOHONGSHU_URLS.HOME}/search_result?keyword=${encodeURIComponent(keyword)}&sort=${encodeURIComponent(sort)}&source=web_explore_feed`;

      tabSnapshot = await this.captureTabSessionSnapshot();
      const tab = await this.findOrCreateXHSTab(searchUrl);

      if (!tab.id) {
        throw new Error('Failed to create tab');
      }
      toolTabId = tab.id;

      // 等待页面加载完成和网络空闲（数据加载完成）
      await waitForLoadState(tab.id, 'networkidle', 10000);

      const feeds = await runInMainWorld<Feed[]>(
        tab.id,
        (injectedArgs: { limit?: number }) => {
          const limitValue = injectedArgs?.limit ?? 20;
          const state = (window as any).__INITIAL_STATE__;

          // 获取搜索结果数据
          const feeds = state?.search?.feeds;
          let values = feeds?._value || feeds?._rawValue || [];

          if (!Array.isArray(values)) {
            console.log('[Search] No search results found');
            return [];
          }

          console.log('[Search] Found', values.length, 'search results');
          const toNumber = (text: unknown) => {
            if (typeof text === 'number') return text;
            if (text === undefined || text === null) return 0;
            const normalized = text.toString().trim();
            if (normalized === '') return 0;
            if (/([wW万])$/.test(normalized)) {
              const num = parseFloat(normalized.replace(/([wW万])$/, ''));
              return Math.round(num * 10000);
            }
            if (/([kK千])$/.test(normalized)) {
              const num = parseFloat(normalized.replace(/([kK千])$/, ''));
              return Math.round(num * 1000);
            }
            const parsed = parseFloat(normalized.replace(/[^0-9.]/g, ''));
            return Number.isFinite(parsed) ? Math.round(parsed) : 0;
          };

          // 只处理 note 类型的数据
          const noteItems = values.filter((item: any) => item?.modelType === 'note');
          console.log('[Search] Note items:', noteItems.length);

          const mappedItems = noteItems.slice(0, limitValue).map((item: any, index: number) => {
            const xsecToken = item?.xsecToken || '';
            const result = {
              id: item?.id || '',
              title: item?.noteCard?.displayTitle || '',
              content: item?.noteCard?.displayTitle || '', // 使用 displayTitle 作为 content
              author: item?.noteCard?.user?.nickname || item?.noteCard?.user?.nickName || '',
              likes: toNumber(item?.noteCard?.interactInfo?.likedCount),
              comments: toNumber(item?.noteCard?.interactInfo?.commentCount),
              coverImage: item?.noteCard?.cover?.urlDefault || item?.noteCard?.cover?.urlPre || '',
              // 笔记 URL 必须带 xsec_token，否则跳转会被 XHS 屏蔽
              url: item?.id && xsecToken ? buildXhsExploreNoteUrl(item.id, xsecToken, 'pc_search') : '',
              xsecToken,
              createTime: new Date().toISOString(),
            };

            // 调试日志
            if (index === 0) {
              console.log('[Search] First mapped item:', result);
            }

            return result;
          });

          console.log('[Search] Mapped items count:', mappedItems.length);
          console.log('[Search] Returning results...');
          return mappedItems;
        },
        { limit }
      );

      // 调试日志
      console.log('[Search] Feeds received from runInMainWorld:', feeds);
      console.log('[Search] Feeds length:', feeds.length);
      if (feeds.length > 0) {
        console.log('[Search] First feed:', feeds[0]);
      }

      const validFeeds = feeds.filter((feed) => feed.id);
      console.log('[Search] Valid feeds after filter:', validFeeds.length);

      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              keyword,
              sort,
              feeds: validFeeds,
              count: validFeeds.length,
              message: `搜索"${keyword}"找到${validFeeds.length}条结果`,
            }),
          },
        ],
        isError: false,
      };
    } catch (error) {
      console.error('SearchFeeds error:', error);
      return {
        content: [
          {
            type: 'error',
            text: `搜索内容失败: ${error instanceof Error ? error.message : '未知错误'}`,
          },
        ],
        isError: true,
      };
    } finally {
      await this.cleanupToolTabSession(toolTabId, tabSnapshot, {
        closeIfCreated: true,
        restoreActiveTab: true,
      });
    }
  }
}
