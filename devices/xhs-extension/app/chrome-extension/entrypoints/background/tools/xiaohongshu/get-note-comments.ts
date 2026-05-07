import { BaseTool, type TabSessionSnapshot } from '../base-tool';
import type { ToolResult } from 'xiaohongshu-mcp-shared';
import { runInMainWorld } from '../inject-script';
import { validateXhsNoteUrl } from './xhs-url';

interface GetNoteCommentsArgs {
  noteUrl: string; // 笔记 URL（需要带 xsec_token 等参数）
}

/**
 * xhs_get_note_comments - 获取笔记详情和评论
 *
 * 通过 window.__INITIAL_STATE__ 读取页面数据，返回原始 JSON
 * 注意：noteUrl 需要是完整的 URL，包含 xsec_token 等必要参数
 */
export class XhsGetNoteCommentsTool extends BaseTool {
  name = 'xhs_get_note_comments';

  async execute(args: GetNoteCommentsArgs): Promise<ToolResult> {
    let tabSnapshot: TabSessionSnapshot | null = null;
    let toolTabId: number | undefined;

    try {
      const { noteUrl } = args;

      if (!noteUrl) {
        return this.createErrorResult('需要提供 noteUrl 参数');
      }

      const validated = validateXhsNoteUrl(noteUrl);
      if (!validated.ok) {
        return this.createErrorResult(validated.error);
      }

      console.log('[xhs_get_note_comments] Navigating to note URL:', noteUrl);

      // noteUrl 已包含 xsec_token，直接跳转即可
      tabSnapshot = await this.captureTabSessionSnapshot();
      const tab = await this.findOrCreateXHSTab(noteUrl);

      if (!tab.id) {
        throw new Error('无法创建标签页');
      }
      toolTabId = tab.id;

      // 等待笔记详情页数据加载
      await new Promise((resolve) => setTimeout(resolve, 2000));

      // 读取 window.__INITIAL_STATE__
      const stateData = await runInMainWorld<any>(tab.id, () => {
        const state = (window as any).__INITIAL_STATE__;
        if (!state) {
          return { error: 'INITIAL_STATE 未找到' };
        }

        // 尝试获取笔记详情和评论数据
        // 数据结构可能在 note 或其他位置
        return {
          url: window.location.href,
          note: state.note,
          comment: state.comment,
          // 返回整个 state 供调试
          _rawState: state,
        };
      });

      if (stateData?.error) {
        return this.createErrorResult(stateData.error);
      }

      // 直接返回原始数据
      return {
        content: [{ type: 'text', text: JSON.stringify(stateData, null, 2) }],
        isError: false,
      };
    } catch (error) {
      console.error('[xhs_get_note_comments] Error:', error);
      return this.createErrorResult(
        error instanceof Error ? error.message : '获取笔记评论失败'
      );
    } finally {
      await this.cleanupToolTabSession(toolTabId, tabSnapshot, {
        closeIfCreated: true,
        restoreActiveTab: true,
      });
    }
  }
}
