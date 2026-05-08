// xhs_get_my_recent — 拿当前 me 的最近 N 条笔记。
//
// 入参：{limit?: number} 默认 20
// 行为：复用 XhsAnalyzeMyProfileTool（已有从 me profile 抓笔记列表的能力），
// 把 ToolResult 解出的 notes 数组裁切到 limit；只返必要字段（note_id/title/url/post_time/metrics）。
// 真正的 "草稿/已发布" 区分留 V0.5+；当前 limit≥0。

import { BaseTool } from '../base-tool';
import {
  XIAOHONGSHU_TOOL_NAMES,
  type ToolResult,
} from 'coagent-xhs-shared';
import { XhsAnalyzeMyProfileTool } from './analyze-profile';

interface GetMyRecentArgs {
  limit?: number;
  /** Optional save path used by AnalyzeMyProfileTool internals. */
  savePath?: string;
}

export class XhsGetMyRecentTool extends BaseTool {
  name = XIAOHONGSHU_TOOL_NAMES.GET_MY_RECENT;

  private analyzeTool = new XhsAnalyzeMyProfileTool();

  async execute(args: GetMyRecentArgs): Promise<ToolResult> {
    const limit = Math.max(1, Math.min(50, Number(args?.limit ?? 20) || 20));
    const savePath = String(args?.savePath ?? '').trim() || '/dev/null';
    let inner: ToolResult;
    try {
      // analyze-profile 内部会爬 sample 笔记，sampleCount 直接传 limit 限制成本。
      inner = await this.analyzeTool.execute({ savePath, sampleCount: limit } as any);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      return this.createErrorResult(`get-my-recent 失败: ${message}`);
    }

    if (inner.isError) {
      return inner;
    }

    let parsed: any;
    try {
      parsed = JSON.parse(String(inner.content?.[0]?.text ?? '{}'));
    } catch {
      return this.createErrorResult('get-my-recent: analyze-profile 返回非 JSON');
    }

    const notes = Array.isArray(parsed?.data?.notes) ? parsed.data.notes : [];
    const trimmed = notes.slice(0, limit).map((n: any) => ({
      note_id: String(n?.note_id ?? ''),
      title: String(n?.title ?? ''),
      url: String(n?.url ?? ''),
      type: String(n?.type ?? 'normal'),
      post_time: String(n?.post_time ?? ''),
      metrics: {
        likes: Number(n?.metrics?.likes ?? 0),
        comments: Number(n?.metrics?.comments ?? 0),
        favorites: Number(n?.metrics?.favorites ?? 0),
        shares: Number(n?.metrics?.shares ?? 0),
      },
    }));

    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify({
            success: true,
            data: {
              count: trimmed.length,
              total_count:
                Number(parsed?.data?.analysis_meta?.total_notes_on_profile ?? trimmed.length) || trimmed.length,
              notes: trimmed,
            },
          }),
        },
      ],
      isError: false,
    };
  }
}
