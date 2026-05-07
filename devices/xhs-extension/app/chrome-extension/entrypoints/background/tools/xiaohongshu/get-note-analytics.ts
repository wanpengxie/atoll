import { BaseTool, type TabSessionSnapshot } from '../base-tool';
import type { ToolResult } from 'xiaohongshu-mcp-shared';
import { CDPInterceptor } from '../cdp-interceptor';
import { XHS_CREATOR_URLS, XHS_API_ENDPOINTS, XHS_TIMEOUTS } from './selectors';

interface GetNoteAnalyticsArgs {
  note_id: string; // 笔记 ID（必填）
}

interface DiagnosisItem {
  scale: number; // 0-1，表示百分位
  summary: string; // 诊断描述
}

interface NoteAnalyticsResult {
  success: boolean;
  data?: {
    note_id: string;
    title: string;
    metrics: {
      impression_count: number;
      view_count: number;
      click_rate: number;
      like_count: number;
      comment_count: number;
      favorite_count: number;
      share_count: number;
      fans_increase: number;
      avg_view_time: number;
    };
    diagnosis: {
      content_rich?: DiagnosisItem;
      click_rate?: DiagnosisItem;
      engagement?: DiagnosisItem;
      fans_increase?: DiagnosisItem;
      avg_view_time?: DiagnosisItem;
    };
    audience: {
      gender?: {
        male: number;
        female: number;
      };
      age?: Record<string, number>;
      city?: Array<{ name: string; value: number }>;
      interest?: Array<{ name: string; value: number }>;
    };
  };
  error?: string;
}

// 笔记详情分析 API 路径
const API_PATTERNS = {
  NOTE_BASE: XHS_API_ENDPOINTS.NOTE_BASE,
  NOTE_AUDIENCE: XHS_API_ENDPOINTS.NOTE_AUDIENCE,
};

/**
 * xhs_get_note_analytics - 获取单篇笔记的诊断和观众画像
 *
 * 工作流程：
 * 1. 导航到笔记详情分析页面
 * 2. 分别调用 base 和 audience API
 * 3. 合并数据返回结构化 JSON
 */
export class XhsGetNoteAnalyticsTool extends BaseTool {
  name = 'xhs_get_note_analytics';

  async execute(args: GetNoteAnalyticsArgs): Promise<ToolResult> {
    const { note_id } = args;
    let tabSnapshot: TabSessionSnapshot | null = null;
    let toolTabId: number | undefined;

    if (!note_id) {
      return this.createErrorResult('缺少必填参数: note_id');
    }

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

        // 4. 导航到笔记详情分析页面
        const noteDetailUrl = `${XHS_CREATOR_URLS.NOTE_DETAIL}?note_id=${note_id}`;
        await chrome.tabs.update(tab.id, { url: noteDetailUrl });

        // 5. 等待拦截到的响应数据（最多等待 20 秒）
        const interceptedData = await interceptor.waitForMultipleResponses(20000);
        const baseResponse = interceptedData[API_PATTERNS.NOTE_BASE];
        const audienceResponse = interceptedData[API_PATTERNS.NOTE_AUDIENCE];

        const baseData = baseResponse && baseResponse.success ? baseResponse.data : null;
        const audienceData =
          audienceResponse && audienceResponse.success ? audienceResponse.data : null;

        if (baseData || audienceData) {
          const result = this.buildNoteAnalyticsFromData(note_id, baseData, audienceData);
          if (!result.success) {
            return this.createErrorResult(result.error || '获取笔记分析数据失败');
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
      const noteDetailUrl = `${XHS_CREATOR_URLS.NOTE_DETAIL}?note_id=${note_id}`;
      await chrome.tabs.update(tab.id, { url: noteDetailUrl });
      await this.waitForTabLoad(tab.id, XHS_TIMEOUTS.PAGE_LOAD);
      await new Promise((resolve) => setTimeout(resolve, 3000));

      const result = await this.executeInTab<NoteAnalyticsResult>(
        tab.id,
        this.extractNoteAnalytics,
        [note_id]
      );

      if (!result.success) {
        return this.createErrorResult(result.error || '获取笔记分析数据失败');
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
      console.error('[xhs_get_note_analytics] Error:', error);
      return this.createErrorResult(
        error instanceof Error ? error.message : '获取笔记分析数据失败'
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
   */
  private extractNoteAnalytics = async (noteId: string): Promise<NoteAnalyticsResult> => {
    try {
      const headers: HeadersInit = {
        Accept: 'application/json',
        'Content-Type': 'application/json',
      };

      // 1. 获取笔记基础数据和诊断
      const baseUrl = `/api/galaxy/creator/datacenter/note/base?note_id=${noteId}`;
      const baseResponse = await fetch(baseUrl, {
        method: 'GET',
        headers,
        credentials: 'include',
      });

      let baseData: any = null;
      if (baseResponse.ok) {
        const baseJson = await baseResponse.json();
        if (baseJson.success) {
          baseData = baseJson.data;
        }
      }

      // 2. 获取观众画像
      const audienceUrl = `/api/galaxy/creator/datacenter/note/audience/source/detail?note_id=${noteId}`;
      const audienceResponse = await fetch(audienceUrl, {
        method: 'GET',
        headers,
        credentials: 'include',
      });

      let audienceData: any = null;
      if (audienceResponse.ok) {
        const audienceJson = await audienceResponse.json();
        if (audienceJson.success) {
          audienceData = audienceJson.data;
        }
      }

      // 如果两个 API 都失败，返回错误
      if (!baseData && !audienceData) {
        throw new Error('无法获取笔记分析数据，请确保已登录创作者平台');
      }

      return this.buildNoteAnalyticsFromData(noteId, baseData, audienceData);
    } catch (error) {
      console.error('[extractNoteAnalytics] Error:', error);
      return {
        success: false,
        error: error instanceof Error ? error.message : '数据提取失败',
      };
    }
  };

  private buildNoteAnalyticsFromData(
    noteId: string,
    baseData: any,
    audienceData: any
  ): NoteAnalyticsResult {
    try {
      // 解析基础指标
      const metrics = {
        impression_count: baseData?.impression_count || baseData?.impre_count || 0,
        view_count: baseData?.read_count || baseData?.view_count || 0,
        click_rate: baseData?.click_rate || 0,
        like_count: baseData?.like_count || 0,
        comment_count: baseData?.comment_count || 0,
        favorite_count: baseData?.collect_count || baseData?.favorite_count || 0,
        share_count: baseData?.share_count || 0,
        fans_increase: baseData?.fans_increase || baseData?.new_fans_count || 0,
        avg_view_time: baseData?.avg_read_time || baseData?.avg_view_time || 0,
      };

      // 解析诊断数据
      const diagnosis: Record<string, { scale: number; summary: string }> = {};
      const diagnosisSource = baseData?.diagnosis || baseData?.diagnose || {};

      const diagnosisMapping: Record<string, string> = {
        content_rich: 'content_rich',
        click_rate: 'click_rate',
        interaction_rate: 'engagement',
        engagement: 'engagement',
        fans_increase: 'fans_increase',
        avg_read_time: 'avg_view_time',
        avg_view_time: 'avg_view_time',
      };

      for (const [apiKey, resultKey] of Object.entries(diagnosisMapping)) {
        const item = diagnosisSource[apiKey];
        if (item) {
          diagnosis[resultKey] = {
            scale: item.scale || item.percent || 0,
            summary: item.summary || item.desc || '',
          };
        }
      }

      // 解析观众画像
      const audience: {
        gender?: { male: number; female: number };
        age?: Record<string, number>;
        city?: Array<{ name: string; value: number }>;
        interest?: Array<{ name: string; value: number }>;
      } = {};

      if (audienceData) {
        // 性别分布
        const genderData = audienceData.gender || audienceData.sex;
        if (genderData) {
          audience.gender = {
            male: genderData.male || genderData.man || 0,
            female: genderData.female || genderData.woman || 0,
          };
        }

        // 年龄分布
        const ageData = audienceData.age;
        if (ageData && Array.isArray(ageData)) {
          audience.age = {};
          ageData.forEach((item: any) => {
            const key = item.name || item.range || item.label;
            const value = item.value || item.percent || item.ratio || 0;
            if (key) {
              audience.age![key] = value;
            }
          });
        } else if (ageData && typeof ageData === 'object') {
          audience.age = ageData;
        }

        // 城市分布
        const cityData = audienceData.city || audienceData.cities;
        if (cityData && Array.isArray(cityData)) {
          audience.city = cityData.slice(0, 10).map((item: any) => ({
            name: item.name || item.city || '',
            value: item.value || item.percent || item.ratio || 0,
          }));
        }

        // 兴趣分布
        const interestData = audienceData.interest || audienceData.interests || audienceData.tag;
        if (interestData && Array.isArray(interestData)) {
          audience.interest = interestData.slice(0, 10).map((item: any) => ({
            name: item.name || item.tag || item.label || '',
            value: item.value || item.percent || item.ratio || 0,
          }));
        }
      }

      return {
        success: true,
        data: {
          note_id: noteId,
          title: baseData?.title || '',
          metrics,
          diagnosis,
          audience,
        },
      };
    } catch (error) {
      console.error('[buildNoteAnalyticsFromData] Error:', error);
      return {
        success: false,
        error: error instanceof Error ? error.message : '数据提取失败',
      };
    }
  }
}
