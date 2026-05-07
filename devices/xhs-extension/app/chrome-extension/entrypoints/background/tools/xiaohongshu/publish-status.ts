// xhs_publish_status — 查询 note 发布状态。
//
// 入参：{note_id}
// 行为：复用 XhsGetNoteAnalyticsTool（创作者中心 analytics 入口）：
//   - analytics 返回 success → status:"published"，含基础 metrics
//   - analytics 报 not found / not_published → status:"not_found"
//   - 其他错误（auth 过期 / xhs API 失败）→ status:"unknown" + message
// 说明：真正"未发布但已暂存"的 status:"pending" 状态需进入创作者中心抓取草稿列表，
// 当前 V0.1 不实现，留 PM 协调 V0.5+。

import { BaseTool } from '../base-tool';
import {
  XIAOHONGSHU_TOOL_NAMES,
  type ToolResult,
  ERROR_MESSAGES,
} from 'xiaohongshu-mcp-shared';
import { XhsGetNoteAnalyticsTool } from './get-note-analytics';

interface PublishStatusArgs {
  note_id: string;
}

export class XhsPublishStatusTool extends BaseTool {
  name = XIAOHONGSHU_TOOL_NAMES.PUBLISH_STATUS;

  private analyticsTool = new XhsGetNoteAnalyticsTool();

  async execute(args: PublishStatusArgs): Promise<ToolResult> {
    const noteId = String(args?.note_id ?? '').trim();
    if (!noteId) {
      return this.createErrorResult(`${ERROR_MESSAGES.INVALID_ARGUMENTS}: note_id is required`);
    }

    let inner: ToolResult;
    try {
      inner = await this.analyticsTool.execute({ note_id: noteId });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      return this.shapeStatus({ status: 'unknown', message, note_id: noteId });
    }

    if (inner.isError) {
      const text = inner.content?.[0]?.text ?? '';
      const lower = String(text).toLowerCase();
      const looksLikeNotFound =
        lower.includes('not found') ||
        lower.includes('未找到') ||
        lower.includes('不存在') ||
        lower.includes('未发布');
      return this.shapeStatus({
        status: looksLikeNotFound ? 'not_found' : 'unknown',
        message: text || '查询失败',
        note_id: noteId,
      });
    }

    // analytics returned data → note has been published.
    let parsed: any = null;
    try {
      parsed = JSON.parse(String(inner.content?.[0]?.text ?? '{}'));
    } catch {
      // ignore parse failure; treat as published with no detail
    }

    return this.shapeStatus({
      status: 'published',
      note_id: noteId,
      title: parsed?.data?.title ?? null,
      metrics: parsed?.data?.metrics ?? null,
    });
  }

  private shapeStatus(payload: Record<string, unknown>): ToolResult {
    return {
      content: [{ type: 'text', text: JSON.stringify({ success: true, data: payload }) }],
      isError: false,
    };
  }
}
