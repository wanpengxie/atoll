import { BaseTool, type TabSessionSnapshot } from '../base-tool';
import type { ToolResult } from 'coagent-xhs-shared';
import { CDPInterceptor } from '../cdp-interceptor';
import { XHS_CREATOR_URLS, XHS_API_ENDPOINTS, XHS_TIMEOUTS } from './selectors';

interface GetCreatorMetricsArgs {
  limit?: number; // 笔记数量限制，默认 20
}

interface NoteMetrics {
  impression_count: number;
  view_count: number;
  like_count: number;
  comment_count: number;
  favorite_count: number;
  share_count: number;
}

interface NoteInfo {
  note_id: string;
  title: string;
  cover_url: string;
  type: 'normal' | 'video';
  post_time: string;
  metrics: NoteMetrics;
}

interface CreatorMetricsResult {
  success: boolean;
  data?: {
    account: {
      nickname: string;
      xiaohongshu_id: string;
      avatar_url: string;
      follower_count: number;
      following_count: number;
      note_count: number;
      liked_count: number;
    };
    summary: {
      total_impression: number;
      total_view: number;
      total_engagement: number;
      total_fans_increase: number;
    };
    notes: NoteInfo[];
  };
  error?: string;
}

// 创作者数据中心 API 路径
const API_PATTERNS = {
  NOTE_LIST: XHS_API_ENDPOINTS.NOTE_LIST,
};

/**
 * xhs_get_creator_metrics - 获取小红书创作者账号数据和笔记列表
 *
 * 工作流程：
 * 1. 导航到创作者平台数据中心
 * 2. 通过 fetch 调用内部 API 获取数据
 * 3. 返回结构化 JSON
 */
export class XhsGetCreatorMetricsTool extends BaseTool {
  name = 'xhs_get_creator_metrics';

  async execute(args: GetCreatorMetricsArgs): Promise<ToolResult> {
    const limit = args.limit || 20;
    let tabSnapshot: TabSessionSnapshot | null = null;
    let toolTabId: number | undefined;

    try {
      // 1. 找到已有的小红书标签页，或创建新标签页
      tabSnapshot = await this.captureTabSessionSnapshot();
      const tab = await this.findOrCreateXHSTab();

      if (!tab.id) {
        throw new Error('无法创建标签页');
      }
      toolTabId = tab.id;

      // 2. 先尝试 CDP 拦截 API 响应
      const patterns = Object.values(API_PATTERNS);
      const interceptor = new CDPInterceptor(tab.id, patterns);

      try {
        // 3. 先连接调试器（必须在导航之前）
        await interceptor.attach();

        // 4. 导航到数据中心
        await chrome.tabs.update(tab.id, { url: XHS_CREATOR_URLS.DATA_CENTER });

        // 5. 等待拦截到的响应数据（最多等待 20 秒）
        const interceptedData = await interceptor.waitForMultipleResponses(20000);
        const noteListResponse = interceptedData[API_PATTERNS.NOTE_LIST];

        if (noteListResponse) {
          const result = this.buildCreatorMetricsFromListData(noteListResponse, limit);
          if (!result.success) {
            return this.createErrorResult(result.error || '获取数据失败');
          }

          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(result, null, 2),
              },
            ],
            isError: false,
          };
        }
      } finally {
        await interceptor.detach();
      }

      // 6. 回退：在页面中 fetch 获取
      await this.waitForTabLoad(tab.id, XHS_TIMEOUTS.PAGE_LOAD);
      await new Promise((resolve) => setTimeout(resolve, 2000));

      const result = await this.executeInTab<CreatorMetricsResult>(
        tab.id,
        this.extractCreatorMetrics,
        [limit]
      );

      if (!result.success) {
        return this.createErrorResult(result.error || '获取数据失败');
      }

      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(result, null, 2),
          },
        ],
        isError: false,
      };
    } catch (error) {
      console.error('[xhs_get_creator_metrics] Error:', error);
      return this.createErrorResult(
        error instanceof Error ? error.message : '获取创作者数据失败'
      );
    } finally {
      await this.cleanupToolTabSession(toolTabId, tabSnapshot, {
        closeIfCreated: true,
        restoreActiveTab: true,
      });
    }
  }

  /**
   * 在页面中执行的数据提取函数
   * 注意：此函数在页面 context 中运行，无法访问外部变量
   */
  private extractCreatorMetrics = async (limit: number): Promise<CreatorMetricsResult> => {
    try {
      // 构建请求头（Cookie 由浏览器通过 credentials: 'include' 自动管理）
      const headers: HeadersInit = {
        Accept: 'application/json',
        'Content-Type': 'application/json',
      };

      // 1. 尝试从 window.__INITIAL_STATE__ 获取账号信息
      let accountInfo = {
        nickname: '',
        xiaohongshu_id: '',
        avatar_url: '',
        follower_count: 0,
        following_count: 0,
        note_count: 0,
        liked_count: 0,
      };

      // 尝试从页面状态获取账号信息
      const initialState = (window as any).__INITIAL_STATE__;
      if (initialState?.user) {
        const user = initialState.user;
        accountInfo = {
          nickname: user.nickname || user.name || '',
          xiaohongshu_id: user.red_id || user.redId || '',
          avatar_url: user.image || user.avatar || '',
          follower_count: user.fans || user.fansCount || 0,
          following_count: user.follows || user.followingCount || 0,
          note_count: user.notes || user.noteCount || 0,
          liked_count: user.liked || user.likedCount || 0,
        };
      }

      // 2. 调用笔记列表 API
      const now = Date.now();
      const thirtyDaysAgo = now - 30 * 24 * 60 * 60 * 1000;

      const listParams = new URLSearchParams({
        page_num: '1',
        page_size: String(limit),
        start_time: String(thirtyDaysAgo),
        end_time: String(now),
        sort_field: 'time',
        sort_order: 'desc',
      });

      const listUrl = `/api/galaxy/creator/datacenter/note/analyze/list?${listParams}`;

      const listResponse = await fetch(listUrl, {
        method: 'GET',
        headers,
        credentials: 'include',
      });

      if (!listResponse.ok) {
        throw new Error(`API 请求失败: ${listResponse.status}`);
      }

      const listData = await listResponse.json();

      if (!listData.success) {
        throw new Error(listData.msg || '获取笔记列表失败');
      }

      // 3. 解析笔记列表
      const noteInfos = listData.data?.note_infos || [];
      const notes: Array<{
        note_id: string;
        title: string;
        cover_url: string;
        type: 'normal' | 'video';
        post_time: string;
        metrics: {
          impression_count: number;
          view_count: number;
          like_count: number;
          comment_count: number;
          favorite_count: number;
          share_count: number;
        };
      }> = noteInfos.map((note: any) => ({
        note_id: note.note_id || '',
        title: note.title || '',
        cover_url: note.cover?.url || note.cover_url || '',
        type: note.type === 'video' ? 'video' : 'normal',
        post_time: note.time ? new Date(note.time).toISOString() : '',
        metrics: {
          impression_count: note.impression_count || note.impre_count || 0,
          view_count: note.read_count || note.view_count || 0,
          like_count: note.like_count || 0,
          comment_count: note.comment_count || 0,
          favorite_count: note.collect_count || note.favorite_count || 0,
          share_count: note.share_count || 0,
        },
      }));

      // 4. 计算汇总数据
      const summary = {
        total_impression: 0,
        total_view: 0,
        total_engagement: 0,
        total_fans_increase: 0,
      };

      notes.forEach((note) => {
        summary.total_impression += note.metrics.impression_count;
        summary.total_view += note.metrics.view_count;
        summary.total_engagement +=
          note.metrics.like_count +
          note.metrics.comment_count +
          note.metrics.favorite_count +
          note.metrics.share_count;
      });

      // 从列表数据中更新账号信息（如果有）
      if (listData.data?.user_info) {
        const userInfo = listData.data.user_info;
        accountInfo.nickname = userInfo.nickname || accountInfo.nickname;
        accountInfo.xiaohongshu_id = userInfo.red_id || accountInfo.xiaohongshu_id;
        accountInfo.avatar_url = userInfo.image || accountInfo.avatar_url;
      }

      return {
        success: true,
        data: {
          account: accountInfo,
          summary,
          notes,
        },
      };
    } catch (error) {
      console.error('[extractCreatorMetrics] Error:', error);
      return {
        success: false,
        error: error instanceof Error ? error.message : '数据提取失败',
      };
    }
  };

  private buildCreatorMetricsFromListData = (listData: any, limit: number): CreatorMetricsResult => {
    try {
      if (!listData || !listData.success) {
        throw new Error(listData?.msg || '获取笔记列表失败');
      }

      const accountInfo = {
        nickname: listData?.data?.user_info?.nickname || '',
        xiaohongshu_id: listData?.data?.user_info?.red_id || '',
        avatar_url: listData?.data?.user_info?.image || '',
        follower_count: 0,
        following_count: 0,
        note_count: 0,
        liked_count: 0,
      };

      const noteInfos = listData.data?.note_infos || [];
      const slicedNotes = limit > 0 ? noteInfos.slice(0, limit) : noteInfos;

      const notes: NoteInfo[] = slicedNotes.map((note: any) => ({
        note_id: note.note_id || '',
        title: note.title || '',
        cover_url: note.cover?.url || note.cover_url || '',
        type: note.type === 'video' ? 'video' : 'normal',
        post_time: note.time ? new Date(note.time).toISOString() : '',
        metrics: {
          impression_count: note.impression_count || note.impre_count || 0,
          view_count: note.read_count || note.view_count || 0,
          like_count: note.like_count || 0,
          comment_count: note.comment_count || 0,
          favorite_count: note.collect_count || note.favorite_count || 0,
          share_count: note.share_count || 0,
        },
      }));

      const summary = {
        total_impression: 0,
        total_view: 0,
        total_engagement: 0,
        total_fans_increase: 0,
      };

      notes.forEach((note) => {
        summary.total_impression += note.metrics.impression_count;
        summary.total_view += note.metrics.view_count;
        summary.total_engagement +=
          note.metrics.like_count +
          note.metrics.comment_count +
          note.metrics.favorite_count +
          note.metrics.share_count;
      });

      return {
        success: true,
        data: {
          account: accountInfo,
          summary,
          notes,
        },
      };
    } catch (error) {
      console.error('[buildCreatorMetricsFromListData] Error:', error);
      return {
        success: false,
        error: error instanceof Error ? error.message : '数据提取失败',
      };
    }
  };
}
