// xhs_get_note — 拿单条 note 详情。
//
// 入参（FX2 round-2 codex#t56.2 三层合同对齐后）：
//   {note_id?: string, url?: string, xsec_token?: string, xsec_source?: string}
// 调用约定：
//   - real 模式 CLI 允许 `--url` 或 `--xsec-token` 之一；
//   - daemon validator 接受 url 或 xsec_token，note_id 非必须；
//   - extension 端在 note_id 缺失时从 url 解析 note_id + xsec_token，仅当三者
//     完全无法定位 note 时才报错。
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
  safeParseUrl,
  getXsecToken,
} from './xhs-url';
import { waitForLoadState } from '../wait-utils';

interface GetNoteArgs {
  note_id?: string;
  url?: string;
  xsec_token?: string;
  xsec_source?: string;
}

// FX2: spec §R3-T1 指定的 note_id 解析正则；覆盖 /explore/<id>、
// /discovery/item/<id>、/item/<id> 三种 URL 路径形态（小红书 note 详情页和分享页）。
export const XHS_NOTE_ID_FROM_URL_PATTERN = /(?:explore|discovery\/item|item)\/([a-z0-9]+)/i;

/**
 * 从 URL 解析 note_id + xsec_token。仅在 URL 完全无法解析（既无 note_id 也
 * 无 xsec_token）时返回 null；其它情况返回 partial 结果交由调用方决定。
 */
export function parseNoteIdentityFromUrl(url: string): {
  noteId: string;
  xsecToken: string;
} | null {
  const text = String(url ?? '').trim();
  if (!text) return null;
  const idMatch = text.match(XHS_NOTE_ID_FROM_URL_PATTERN);
  const noteId = idMatch ? String(idMatch[1] ?? '').trim() : '';
  const parsed = safeParseUrl(text);
  const xsecToken = parsed ? getXsecToken(parsed) : '';
  if (!noteId && !xsecToken) return null;
  return { noteId, xsecToken };
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
    // FX2 (round-2 codex#t56.2): 三层合同对齐
    //   1) 优先用 args.note_id；
    //   2) 缺失时从 args.url 解析 note_id + xsec_token（regex 见
    //      XHS_NOTE_ID_FROM_URL_PATTERN）；
    //   3) 最后兜底：args.xsec_token + buildXhsExploreNoteUrl；
    //   4) 三路全失败才返回 INVALID_ARGUMENTS。
    let noteId = String(args?.note_id ?? '').trim();
    let xsecTokenInput = String(args?.xsec_token ?? '').trim();
    let targetUrl = String(args?.url ?? '').trim();

    if (targetUrl) {
      const fromUrl = parseNoteIdentityFromUrl(targetUrl);
      if (fromUrl) {
        if (!noteId) noteId = fromUrl.noteId;
        if (!xsecTokenInput) xsecTokenInput = fromUrl.xsecToken;
      }
    }

    if (!noteId && !xsecTokenInput && !targetUrl) {
      return this.createErrorResult(
        `${ERROR_MESSAGES.INVALID_ARGUMENTS}: note url、note_id 或 xsec_token 至少其一必填`,
      );
    }

    if (!targetUrl) {
      // 没给 URL → 必须有 note_id + xsec_token 才能构造 explore URL。
      if (!noteId || !xsecTokenInput) {
        return this.createErrorResult(
          'note url 或 (note_id + xsec_token) 必填（小红书无 token 时拒绝访问）',
        );
      }
      const source = String(args?.xsec_source ?? '').trim() || 'pc_search';
      targetUrl = buildXhsExploreNoteUrl(noteId, xsecTokenInput, source);
      if (!targetUrl) {
        return this.createErrorResult('无法构造 note URL（note_id/xsec_token 缺失）');
      }
    }

    if (!noteId) {
      // URL 给了但解析不出 note_id（罕见）→ 报清晰错误而不是页面侧 silent fail。
      return this.createErrorResult(
        `${ERROR_MESSAGES.INVALID_ARGUMENTS}: 无法从 URL 解析 note_id（请确认 URL 形如 /explore/<id> 或 /discovery/item/<id>）`,
      );
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
