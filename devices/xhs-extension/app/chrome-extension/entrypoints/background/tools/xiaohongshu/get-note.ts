// xhs_get_note — 拿单条 note 详情。
//
// 入参：
//   {note_id: string, url?: string}
// 行为：
//   - 若提供 `url`（必须含 xsec_token），导航到该 note 页面读取 __INITIAL_STATE__；
//   - 否则返回 missing_url 错误（小红书无 token 无法直接访问 note）。
// 返回（callback envelope）：
//   {ok:true, data:{note_id, url, title, content, images, author, metrics}}
//   或 {ok:false, error:{code,message}}

import { BaseTool, type TabSessionSnapshot } from '../base-tool';
import {
  XIAOHONGSHU_TOOL_NAMES,
  type ToolResult,
  ERROR_MESSAGES,
} from 'xiaohongshu-mcp-shared';
import { runInMainWorld } from '../inject-script';
import {
  validateXhsNoteUrl,
  buildXhsExploreNoteUrl,
} from './xhs-url';
import { waitForLoadState } from '../wait-utils';

interface GetNoteArgs {
  note_id: string;
  url?: string;
  xsec_token?: string;
  xsec_source?: string;
}

interface GetNoteResult {
  note_id: string;
  url: string;
  title: string;
  content: string;
  images: string[];
  author?: { id?: string; name?: string };
  metrics?: { likes: number; comments: number; favorites: number; shares: number };
  type?: 'normal' | 'video';
}

export class XhsGetNoteTool extends BaseTool {
  name = XIAOHONGSHU_TOOL_NAMES.GET_NOTE;

  async execute(args: GetNoteArgs): Promise<ToolResult> {
    const noteId = String(args?.note_id ?? '').trim();
    if (!noteId) {
      return this.createErrorResult(`${ERROR_MESSAGES.INVALID_ARGUMENTS}: note_id is required`);
    }

    let targetUrl = String(args?.url ?? '').trim();
    if (!targetUrl) {
      const token = String(args?.xsec_token ?? '').trim();
      const source = String(args?.xsec_source ?? '').trim() || 'pc_search';
      if (!token) {
        return this.createErrorResult(
          'note url 或 xsec_token 必填（小红书无 token 时拒绝访问）'
        );
      }
      targetUrl = buildXhsExploreNoteUrl(noteId, token, source);
      if (!targetUrl) {
        return this.createErrorResult('无法构造 note URL（note_id/xsec_token 缺失）');
      }
    }

    const validated = validateXhsNoteUrl(targetUrl);
    if (!validated.ok) {
      return this.createErrorResult(validated.error);
    }

    let tabSnapshot: TabSessionSnapshot | null = null;
    let toolTabId: number | undefined;
    try {
      tabSnapshot = await this.captureTabSessionSnapshot();
      const tab = await this.findOrCreateXHSTab(targetUrl);
      if (!tab.id) throw new Error('Failed to create xhs tab');
      toolTabId = tab.id;
      await waitForLoadState(tab.id, 'networkidle', 15000);

      const note = await runInMainWorld<GetNoteResult | null>(
        tab.id,
        (injectedArgs: Record<string, any>) => {
          const noteIdInjected = String(injectedArgs?.noteId ?? '');
          const unwrap = (v: any) => {
            if (!v || typeof v !== 'object') return v;
            return (v as any)._rawValue ?? (v as any)._value ?? v;
          };

          const state = unwrap((window as any).__INITIAL_STATE__);
          const note =
            unwrap(state?.note?.noteDetailMap?.[noteIdInjected]) ??
            (unwrap(state?.note?.firstNoteId)
              ? unwrap(state?.note?.noteDetailMap?.[noteIdInjected])
              : null);
          const candidate = unwrap(note?.note ?? note);
          if (!candidate || typeof candidate !== 'object') return null;

          const noteCard: any = candidate;
          const interactInfo = unwrap(noteCard.interactInfo) ?? {};
          const user = unwrap(noteCard.user) ?? {};
          const imageList: any[] = Array.isArray(noteCard.imageList)
            ? noteCard.imageList
            : [];

          return {
            note_id: String(noteCard.noteId ?? noteIdInjected),
            url: window.location.href,
            title: String(noteCard.title ?? ''),
            content: String(noteCard.desc ?? noteCard.content ?? ''),
            images: imageList
              .map((img) =>
                String(
                  img?.urlDefault ??
                    img?.url ??
                    img?.urlSizeLarge ??
                    img?.infoList?.[0]?.url ??
                    ''
                )
              )
              .filter(Boolean),
            author: {
              id: String(user.userId ?? user.id ?? ''),
              name: String(user.nickname ?? user.name ?? ''),
            },
            metrics: {
              likes: Number(interactInfo.likedCount ?? 0),
              comments: Number(interactInfo.commentCount ?? 0),
              favorites: Number(interactInfo.collectedCount ?? 0),
              shares: Number(interactInfo.shareCount ?? 0),
            },
            type:
              String(noteCard.type ?? 'normal') === 'video' ? 'video' : 'normal',
          };
        },
        { noteId },
        15000
      );

      if (!note) {
        return this.createErrorResult('note 未在页面 __INITIAL_STATE__ 中发现（可能登录态丢失或 note 不存在）');
      }

      return {
        content: [{ type: 'text', text: JSON.stringify({ success: true, data: note }) }],
        isError: false,
      };
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      return this.createErrorResult(`get-note 失败: ${message}`);
    } finally {
      await this.cleanupToolTabSession(toolTabId, tabSnapshot, {
        closeIfCreated: false,
        restoreActiveTab: true,
      });
    }
  }
}
