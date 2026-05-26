import { BaseTool, type TabSessionSnapshot } from '../base-tool';
import type { ToolResult } from 'coagent-xhs-shared';
import { XIAOHONGSHU_URLS } from 'coagent-xhs-shared';
import { runInMainWorld } from '../inject-script';
import { validateXhsProfileUrl } from './xhs-url';
// M1.1-T2: 旧 upstream backend websocket-client 已切断；中间状态报告改为无操作。
// 后续如需把进度推回 coagent daemon，由 services/server-devicebus.ts 统一桥接。
const websocketClient = {
  sendStatus(_requestId: string, _payload: unknown): void {
    // no-op: coagent device 协议不暴露 in-flight 进度上报
  },
};

const DEFAULT_SAMPLE_COUNT = 20;
const DEFAULT_MAX_COMMENTS_PER_NOTE = 30;
const DEFAULT_COMMENT_SCROLL_ROUNDS = 3;

interface AnalyzeMyProfileArgs {}

interface AnalyzeProfileArgs {
  url: string; // 目标用户主页 URL（必填）
  sampleCount?: number; // 采集笔记数量，默认 20
}

// Internal field injected by websocket-client for progress reporting (not part of tool schema).
interface ToolCallInternalArgs {
  __toolCallRequestId?: string;
}

interface AnalyzeMyProfileArgsV2 extends AnalyzeMyProfileArgs, ToolCallInternalArgs {
  savePath: string; // 保存路径（后端落盘用，扩展端不使用）
}

interface AnalyzeProfileArgsV2 extends AnalyzeProfileArgs, ToolCallInternalArgs {
  savePath: string; // 保存路径（后端落盘用，扩展端不使用）
}

interface ProfileInfo {
  nickname: string;
  xiaohongshu_id: string;
  avatar_url: string;
  description: string;
  follower_count: number;
  following_count: number;
  note_count: number;
  liked_and_collected: number;
  tags: string[];
}

interface NoteDetail {
  note_id: string;
  title: string;
  content: string;
  type: 'normal' | 'video';
  post_time: string;
  metrics: {
    likes: number;
    comments: number;
    favorites: number;
    shares: number;
  };
  tags: string[];
  images: string[];
  url: string;
  comments?: NoteComments;
}

interface CoverInfo {
  noteId: string;
  href: string;
}

interface NoteCommentItem {
  comment_id: string;
  content: string;
  like_count: number;
  created_at: string;
  user_id: string;
  user_name: string;
  replies?: NoteCommentItem[];
}

interface NoteComments {
  total: number;
  fetched: number;
  items: NoteCommentItem[];
}

interface AnalyzeProfileResult {
  success: boolean;
  data?: {
    profile: ProfileInfo;
    notes: NoteDetail[];
    analysis_meta: {
      total_notes_on_profile: number;
      fetched_notes: number;
      attempted_notes?: number;
      timeout_notes?: number;
      skipped_notes?: number;
      error_notes?: number;
      target_notes?: number;
      analyzed_at: string;
    };
  };
  error?: string;
}

/**
 * xhs_analyze_my_profile / xhs_analyze_profile - 一站式账号分析工具
 *
 * 流程：
 * 1. 导航到用户主页
 * 2. 从 __INITIAL_STATE__ 提取用户信息和笔记列表（注意 Vue ref 解包）
 * 3. 模拟点击 feed 卡片打开 overlay，读取笔记详情，按 history.back() 关闭
 *    （每次点击间隔随机延迟，模拟人类浏览行为）
 * 4. 汇总返回结构化 JSON
 */
abstract class XhsAnalyzeProfileBaseTool extends BaseTool {
  /**
   * 随机延迟，模拟人类操作间隔
   */
  protected humanDelay(minMs: number, maxMs: number): Promise<void> {
    const delay = minMs + Math.random() * (maxMs - minMs);
    return new Promise((resolve) => setTimeout(resolve, delay));
  }

  protected async analyzeOnProfileTab(
    tabId: number,
    sampleCount: number,
    logPrefix: string,
    requestId?: string
  ): Promise<ToolResult> {
    const sendStatus = (payload: any) => {
      if (!requestId) return;
      websocketClient.sendStatus(requestId, {
        tool: this.name,
        ts: new Date().toISOString(),
        ...payload,
      });
    };

    const heartbeat = {
      stage: 'init',
      current: 0,
      total: 0,
      message: '',
    };

    const heartbeatTimer = requestId
      ? setInterval(() => {
          sendStatus({ ...heartbeat, kind: 'heartbeat' });
        }, 5000)
      : null;

    try {
    const collectCommentsForCurrentNote = async (
      noteId: string,
      commentTotal: number
    ): Promise<NoteComments> => {
      const maxComments = DEFAULT_MAX_COMMENTS_PER_NOTE;

      // Scroll a few times to trigger lazy-loading more comments, then read from __INITIAL_STATE__.
      const comments = await runInMainWorld<NoteComments>(
        tabId,
        async (args: any) => {
          const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
          const unwrap = (v: any) => {
            if (!v || typeof v !== 'object') return v;
            return (v as any)._rawValue ?? (v as any)._value ?? v;
          };

          const isObject = (v: any) => v && typeof v === 'object';

          const toText = (v: any): string => {
            if (typeof v === 'string') return v;
            if (!v || typeof v !== 'object') return '';
            const obj: any = v;
            if (typeof obj.text === 'string') return obj.text;
            if (typeof obj.content === 'string') return obj.content;
            if (typeof obj.desc === 'string') return obj.desc;
            return '';
          };

          const toIsoTime = (v: any): string => {
            if (!v) return '';
            if (typeof v === 'number') {
              // Heuristic: ms vs sec
              const ms = v > 10_000_000_000 ? v : v * 1000;
              try {
                return new Date(ms).toISOString();
              } catch {
                return '';
              }
            }
            if (typeof v === 'string') {
              // try Date parse
              const t = Date.parse(v);
              if (!Number.isNaN(t)) return new Date(t).toISOString();
              return v;
            }
            return '';
          };

          const looksLikeCommentItem = (x: any): boolean => {
            if (!isObject(x)) return false;
            const obj: any = x;
            const hasContent =
              typeof obj.content === 'string' ||
              typeof obj.desc === 'string' ||
              (isObject(obj.content) && typeof obj.content.text === 'string');
            const hasUser =
              isObject(obj.userInfo) ||
              isObject(obj.user) ||
              isObject(obj.author) ||
              typeof obj.nickname === 'string' ||
              typeof obj.userId === 'string';
            const hasId =
              typeof obj.id === 'string' ||
              typeof obj.commentId === 'string' ||
              typeof obj.comment_id === 'string';
            return (hasContent && hasUser) || (hasId && hasContent);
          };

          const isCommentArray = (arr: any): arr is any[] => {
            if (!Array.isArray(arr) || arr.length === 0) return false;
            return arr.some(looksLikeCommentItem);
          };

          const findCommentArrayDeep = (root: any, maxDepth: number): any[] | null => {
            const visited = new Set<any>();
            const queue: Array<{ v: any; depth: number }> = [{ v: root, depth: maxDepth }];

            while (queue.length) {
              const { v, depth } = queue.shift()!;
              if (!isObject(v) && !Array.isArray(v)) continue;
              if (visited.has(v)) continue;
              visited.add(v);

              const unwrapped = unwrap(v);
              if (isCommentArray(unwrapped)) return unwrapped;
              if (depth <= 0 || !isObject(unwrapped)) continue;

              const keys = Object.keys(unwrapped);
              // Safety: cap scan width
              for (const k of keys.slice(0, 80)) {
                const child = unwrap((unwrapped as any)[k]);
                if (isCommentArray(child)) return child;
                if (depth > 1 && isObject(child)) {
                  queue.push({ v: child, depth: depth - 1 });
                }
              }
            }
            return null;
          };

          const extractCommentsFromState = (): any[] => {
            const state = (window as any).__INITIAL_STATE__;
            if (!state) return [];

            // 小红书在笔记详情页里常见结构：
            // __INITIAL_STATE__.note.noteDetailMap[<noteId>].comments.list
            // （你验证过 comments 就在 noteDetailMap 里，而不一定在 state.comment）
            const noteState = unwrap(state.note);
            const noteDetailMap = unwrap(noteState?.noteDetailMap);
            if (noteDetailMap && isObject(noteDetailMap)) {
              const currentNoteIdRef = noteState?.currentNoteId;
              const currentNoteId = unwrap(currentNoteIdRef);
              const key = args.noteId || currentNoteId;
              const detail =
                unwrap((noteDetailMap as any)[key]) || unwrap((noteDetailMap as any)[currentNoteId]);
              const detailComments = unwrap(detail?.comments);
              const detailList = unwrap(detailComments?.list);
              if (isCommentArray(detailList)) return detailList;
              const arr = findCommentArrayDeep(detailComments, 2);
              if (arr) return arr;
            }

            const commentState = unwrap(state.comment);
            if (!commentState) return [];

            // Common direct paths first
            const directCandidates = [
              unwrap(commentState.commentList),
              unwrap(commentState.comments),
              unwrap(commentState.list),
              unwrap(commentState.data?.comments),
              unwrap(commentState.data?.list),
              unwrap(commentState.commentData?.comments),
              unwrap(commentState.commentData?.commentList),
              unwrap(commentState.commentData?.list),
              unwrap(commentState.commentListData?.commentList),
              unwrap(commentState.commentListData?.list),
            ];
            for (const c of directCandidates) {
              if (isCommentArray(c)) return c;
            }

            // Some implementations store by noteId
            const currentNoteIdRef = state?.note?.currentNoteId;
            const currentNoteId = unwrap(currentNoteIdRef);
            const byNoteMaps = [
              unwrap(commentState.commentsByNoteId),
              unwrap(commentState.commentByNoteId),
              unwrap(commentState.noteCommentMap),
              unwrap(commentState.noteComments),
            ];
            for (const m of byNoteMaps) {
              if (!m || !currentNoteId || !isObject(m)) continue;
              const maybe = unwrap((m as any)[currentNoteId]);
              const arr = findCommentArrayDeep(maybe, 2);
              if (arr) return arr;
            }

            // Fallback: deep scan commentState
            return findCommentArrayDeep(commentState, 3) || [];
          };

          const normalizeComment = (c: any, idx: number): any => {
            const obj: any = c || {};
            const user = obj.userInfo || obj.user || obj.author || {};

            const toNumber = (text: unknown): number => {
              if (typeof text === 'number') return text;
              if (text === undefined || text === null) return 0;
              const normalized = String(text).trim();
              if (normalized === '') return 0;
              if (/([wW万])$/.test(normalized)) {
                const num = parseFloat(normalized.replace(/([wW万])$/, ''));
                return Number.isFinite(num) ? Math.round(num * 10000) : 0;
              }
              if (/([kK千])$/.test(normalized)) {
                const num = parseFloat(normalized.replace(/([kK千])$/, ''));
                return Number.isFinite(num) ? Math.round(num * 1000) : 0;
              }
              const parsed = parseFloat(normalized.replace(/[^0-9.]/g, ''));
              return Number.isFinite(parsed) ? Math.round(parsed) : 0;
            };

            const commentId =
              obj.id || obj.commentId || obj.comment_id || obj.commentID || obj.cid || `idx_${idx}`;

            const content = toText(obj.content) || toText(obj) || '';
            const likeCount = toNumber(obj.likeCount ?? obj.likedCount ?? obj.likes);

            const createdAt = toIsoTime(obj.time || obj.createTime || obj.createdAt || obj.ctime);

            const userId =
              user.userId || user.id || user.uid || user.userid || user.user_id || obj.userId || '';
            const userName = user.nickname || user.name || user.userName || user.username || '';

            const repliesRaw =
              obj.subComments ||
              obj.subCommentList ||
              obj.replyList ||
              obj.replies ||
              obj.children ||
              [];

            let replies: any[] | undefined;
            if (Array.isArray(repliesRaw) && repliesRaw.length) {
              replies = repliesRaw.slice(0, 3).map((r, rIdx) => normalizeComment(r, rIdx));
            }

            const out: any = {
              comment_id: String(commentId),
              content,
              like_count: likeCount,
              created_at: createdAt,
              user_id: String(userId || ''),
              user_name: String(userName || ''),
            };
            if (replies && replies.length) out.replies = replies;
            return out;
          };

          const pickScrollElement = (): Element | null => {
            // Prefer a scrollable element that looks related to comments.
            const isScrollable = (el: Element): boolean => {
              const node = el as any;
              if (!node || typeof node.scrollHeight !== 'number' || typeof node.clientHeight !== 'number')
                return false;
              if (node.scrollHeight - node.clientHeight < 200) return false;
              const style = window.getComputedStyle(el as any);
              const oy = style.overflowY;
              return oy === 'auto' || oy === 'scroll';
            };

            const score = (el: Element): number => {
              let s = 0;
              const cls = String((el as any).className || '');
              if (cls.includes('comment')) s += 5;
              if ((el as any).querySelector?.('[class*="comment"]')) s += 3;
              const txt = String((el as any).innerText || '');
              if (txt.includes('评论')) s += 2;
              if ((el as any).querySelector?.('textarea, input')) s += 1;
              const node = el as any;
              s += Math.min(5, Math.floor((node.scrollHeight - node.clientHeight) / 800));
              return s;
            };

            const candidates = Array.from(document.querySelectorAll('div,section,main,article')).filter(
              isScrollable
            );

            let best: Element | null = null;
            let bestScore = -1;
            for (const c of candidates) {
              const sc = score(c);
              if (sc > bestScore) {
                best = c;
                bestScore = sc;
              }
            }

            return best;
          };

          // Wait a bit for comment state to appear (some notes load comments after mount)
          const started = Date.now();
          while (Date.now() - started < 5000) {
            const items = extractCommentsFromState();
            if (items.length > 0) break;
            await sleep(250);
          }

          const scrollEl = pickScrollElement();
          const scrollRounds = Math.max(0, Number(args.scrollRounds || 0));
          const delayMin = Math.max(0, Number(args.delayMin || 0));
          const delayMax = Math.max(delayMin, Number(args.delayMax || delayMin));

          for (let i = 0; i < scrollRounds; i++) {
            const delay = delayMin + Math.random() * (delayMax - delayMin);
            if (scrollEl) {
              const node: any = scrollEl as any;
              const step = Math.max(240, Math.floor(node.clientHeight * 0.85));
              try {
                node.scrollBy(0, step);
                node.dispatchEvent(new Event('scroll', { bubbles: true }));
              } catch {
                // ignore
              }
            } else {
              const el: any = document.scrollingElement || document.documentElement;
              const step = Math.max(240, Math.floor(window.innerHeight * 0.85));
              el.scrollBy(0, step);
              window.dispatchEvent(new Event('scroll'));
            }
            await sleep(delay);
          }

          const rawItems = extractCommentsFromState();
          const normalized = rawItems
            .map((c, idx) => normalizeComment(c, idx))
            .filter((x) => x && typeof x.content === 'string')
            .slice(0, Number(args.maxComments || 30));

          return {
            total: Number(args.commentTotal || 0),
            fetched: normalized.length,
            items: normalized,
          };
        },
        {
          noteId,
          commentTotal,
          maxComments,
          scrollRounds: DEFAULT_COMMENT_SCROLL_ROUNDS,
          delayMin: 650,
          delayMax: 1100,
        },
        20_000
      );

      return comments || { total: commentTotal, fetched: 0, items: [] };
    };

    // 等待页面数据加载
    await new Promise((resolve) => setTimeout(resolve, 2000));

    // Step 2: 从 __INITIAL_STATE__ 提取用户信息和笔记列表
    // 注意：XHS 使用 Vue 3，state 中的值是 reactive/ref 对象
    // 必须通过 _rawValue 获取真实数据
    const profileData = await runInMainWorld<{
      profile: ProfileInfo | null;
      noteCount: number;
      error?: string;
    }>(tabId, () => {
      const rawState = (window as any).__INITIAL_STATE__;
      if (!rawState) {
        return { profile: null, noteCount: 0, error: 'INITIAL_STATE 未找到' };
      }

      const userState = rawState.user;
      if (!userState) {
        return { profile: null, noteCount: 0, error: 'state.user 未找到' };
      }

      // 解包 userPageData（Vue ref，需要 _rawValue）
      const userPageData = userState.userPageData?._rawValue || userState.userPageData;
      if (!userPageData) {
        return { profile: null, noteCount: 0, error: 'userPageData 未找到' };
      }

      const basicInfo = userPageData.basicInfo || {};
      const interactions = Array.isArray(userPageData.interactions) ? userPageData.interactions : [];

      const getInteractionCount = (type: string): number => {
        const item = interactions.find((i: any) => i.type === type);
        return item ? parseInt(item.count || '0', 10) : 0;
      };

      // tags 在 userPageData 顶层，不在 basicInfo 里
      const tags = Array.isArray(userPageData.tags) ? userPageData.tags : [];

      // 解包 notes（Vue ref，需要 _rawValue），是嵌套数组
      const notesRef = userState.notes;
      const notesRaw = notesRef?._rawValue || notesRef;
      let flatNotes: any[] = [];
      if (Array.isArray(notesRaw)) {
        flatNotes = notesRaw.flat();
      }

      const profile: any = {
        nickname: basicInfo.nickname || '',
        xiaohongshu_id: basicInfo.redId || '',
        avatar_url: basicInfo.imageb || basicInfo.image || '',
        description: basicInfo.desc || '',
        follower_count: getInteractionCount('fans'),
        following_count: getInteractionCount('follows'),
        note_count: flatNotes.length,
        liked_and_collected: getInteractionCount('interaction'),
        tags: tags.map((t: any) => t.name || t),
      };

      return { profile, noteCount: flatNotes.length };
    });

    if (profileData.error && !profileData.profile) {
      return this.createErrorResult(profileData.error);
    }

    if (!profileData.profile) {
      return this.createErrorResult('无法提取用户信息，请确认页面已加载');
    }

    const totalNotes = profileData.noteCount;
    sendStatus({
      stage: 'profile_loaded',
      nickname: profileData.profile.nickname,
      total_notes_on_profile: totalNotes,
    });

    // Step 3: 模拟点击 feed 卡片，逐个读取笔记详情
    const targetNotes = Math.max(0, Math.min(sampleCount, totalNotes || sampleCount));
    const seenCoverKeys = new Set<string>();
    const seenExtractedNoteIds = new Set<string>();
    let attemptedNotes = 0;
    let timeoutNotes = 0;
    let skippedNotes = 0;
    let errorNotes = 0;

    const getVisibleCovers = async (): Promise<CoverInfo[]> => {
      return await runInMainWorld<CoverInfo[]>(tabId, () => {
        const extractNoteId = (href: string): string => {
          const clean = (href || '').split('?')[0].split('#')[0];
          const parts = clean.split('/').filter(Boolean);
          const last = parts[parts.length - 1] || '';
          const secondLast = parts[parts.length - 2] || '';
          const isId = (s: string) => /^[0-9a-f]{16,32}$/i.test(s);
          if (isId(last)) return last;
          if (isId(secondLast)) return secondLast;
          return '';
        };

        const covers = Array.from(
          document.querySelectorAll('section.note-item a.cover')
        ) as HTMLAnchorElement[];

        return covers
          .map((a) => {
            const href = a.href || a.getAttribute('href') || '';
            return { noteId: extractNoteId(href), href };
          })
          .filter((c) => c.href);
      });
    };

    const clickCover = async (cover: CoverInfo): Promise<boolean> => {
      return await runInMainWorld<boolean>(
        tabId,
        (args: any) => {
          const extractNoteId = (href: string): string => {
            const clean = (href || '').split('?')[0].split('#')[0];
            const parts = clean.split('/').filter(Boolean);
            const last = parts[parts.length - 1] || '';
            const secondLast = parts[parts.length - 2] || '';
            const isId = (s: string) => /^[0-9a-f]{16,32}$/i.test(s);
            if (isId(last)) return last;
            if (isId(secondLast)) return secondLast;
            return '';
          };

          const covers = Array.from(
            document.querySelectorAll('section.note-item a.cover')
          ) as HTMLAnchorElement[];

          const target = covers.find((a) => {
            const href = a.href || a.getAttribute('href') || '';
            if (args.noteId) {
              return extractNoteId(href) === args.noteId;
            }
            return href === args.href;
          }) as HTMLElement | undefined;

          if (!target) return false;
          try {
            target.scrollIntoView({ block: 'center', inline: 'center' });
          } catch {
            // ignore
          }
          target.click();
          return true;
        },
        { noteId: cover.noteId, href: cover.href },
      );
    };

    const scrollPageForMore = async (): Promise<{ atBottom: boolean; scrollTop: number }> => {
      return await runInMainWorld<{ atBottom: boolean; scrollTop: number }>(tabId, () => {
        const el = document.scrollingElement || document.documentElement;
        const beforeTop = el.scrollTop;
        const step = Math.max(300, Math.floor(window.innerHeight * 0.85));
        el.scrollBy(0, step);
        const atBottom = el.scrollTop + el.clientHeight >= el.scrollHeight - 10;
        return { atBottom, scrollTop: beforeTop };
      });
    };

    const initialVisibleCovers = await getVisibleCovers();
    console.log(
      `${logPrefix} Will attempt to fetch up to ${targetNotes} notes (visible covers=${initialVisibleCovers.length}, total_notes=${totalNotes})`
    );

    heartbeat.total = targetNotes;
    heartbeat.stage = 'collect_notes';
    heartbeat.message = `准备采集 ${targetNotes} 篇笔记`;
    sendStatus({
      stage: 'collect_notes_start',
      total_notes_on_profile: totalNotes,
      target_notes: targetNotes,
      visible_cover_count: initialVisibleCovers.length,
    });

    const noteDetails: NoteDetail[] = [];

    let noNewCoverRounds = 0;
    const maxNoNewCoverRounds = 4;
    const coverKey = (c: CoverInfo): string => c.noteId || c.href;

    while (noteDetails.length < targetNotes) {
      const covers = await getVisibleCovers();
      const newCovers = covers.filter((c) => coverKey(c) && !seenCoverKeys.has(coverKey(c)));

      if (newCovers.length === 0) {
        noNewCoverRounds++;
        const scrollInfo = await scrollPageForMore();
        sendStatus({
          stage: 'scroll_for_more',
          extracted: noteDetails.length,
          attempted: attemptedNotes,
          total: targetNotes,
          no_new_rounds: noNewCoverRounds,
          at_bottom: scrollInfo.atBottom,
        });
        await this.humanDelay(800, 1400);

        if (noNewCoverRounds >= maxNoNewCoverRounds && scrollInfo.atBottom) {
          console.warn(`${logPrefix} No new covers after scroll and already at bottom, stop.`);
          break;
        }

        // 继续下一轮扫描
        continue;
      }

      noNewCoverRounds = 0;

      for (const cover of newCovers) {
        if (noteDetails.length >= targetNotes) break;
        const key = coverKey(cover);
        if (!key) continue;

        try {
          seenCoverKeys.add(key);
          attemptedNotes++;

          console.log(
            `${logPrefix} Clicking note ${attemptedNotes}, extracted=${noteDetails.length}/${targetNotes} note_id=${cover.noteId || 'unknown'}`
          );
          heartbeat.current = noteDetails.length;
          heartbeat.message = `采集进度 ${noteDetails.length}/${targetNotes}（已尝试 ${attemptedNotes}）`;
          sendStatus({
            stage: 'note_click',
            current: attemptedNotes,
            total: targetNotes,
            attempted: attemptedNotes,
            extracted: noteDetails.length,
            note_id: cover.noteId,
          });

          const clicked = await clickCover(cover);
          if (!clicked) {
            skippedNotes++;
            console.warn(`${logPrefix} Cover not found when clicking note_id=${cover.noteId || 'unknown'}, skipping`);
            sendStatus({
              stage: 'note_skip',
              current: attemptedNotes,
              total: targetNotes,
              attempted: attemptedNotes,
              extracted: noteDetails.length,
              note_id: cover.noteId,
              reason: 'feed_card_not_found',
            });
            continue;
          }

          // 给页面一点时间完成路由切换/overlay 挂载，避免“闪一下就 back”导致拿到骨架数据。
          await this.humanDelay(800, 1400);

          // 等待 overlay 出现并提取当前笔记详情（用轮询替代固定长等待，减少总耗时）
          const noteData = await runInMainWorld<NoteDetail | null>(
            tabId,
            async (args: any) => {
            const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

            const started = Date.now();
            while (Date.now() - started < args.timeoutMs) {
              const rawState = (window as any).__INITIAL_STATE__;
              const noteDetailMap = rawState?.note?.noteDetailMap;
              if (!noteDetailMap) {
                await sleep(200);
                continue;
              }

              // 用 currentNoteId 定位当前笔记（noteDetailMap 会累积所有打开过的笔记）
              const currentNoteIdRef = rawState.note.currentNoteId;
              const currentNoteId =
                currentNoteIdRef?._rawValue || currentNoteIdRef?._value || currentNoteIdRef;
              if (!currentNoteId) {
                await sleep(200);
                continue;
              }

              const detail = noteDetailMap[currentNoteId];
              const note = detail?.note;
              if (!note) {
                await sleep(200);
                continue;
              }

              // XHS 的 noteDetailMap 可能会先填充一个“骨架”对象（title/noteId 有，但 desc/time/interactInfo 为空），
              // 真正的详情会稍后异步补齐。如果此时立刻 history.back()，页面会“闪一下”且抓不到详情数据。
              const interactInfo = note.interactInfo || {};
              const hasInteractCounts =
                interactInfo.likedCount != null ||
                interactInfo.commentCount != null ||
                interactInfo.collectedCount != null ||
                interactInfo.shareCount != null;
              const hasTime = note.time != null && note.time !== 0;
              const desc = note.desc || '';
              const hasDesc = typeof desc === 'string' && desc.trim().length > 0;
              const hasImages = Array.isArray(note.imageList) && note.imageList.length > 0;
              const isVideo = note.type === 'video';

              // 如果仍然是“骨架”，继续等待。
              if (!hasTime || !hasInteractCounts || (!hasDesc && !hasImages && !isVideo)) {
                await sleep(200);
                continue;
              }

              const toNumber = (text: unknown): number => {
                if (typeof text === 'number') return text;
                if (text === undefined || text === null) return 0;
                const normalized = String(text).trim();
                if (normalized === '') return 0;
                if (/([wW万])$/.test(normalized)) {
                  const num = parseFloat(normalized.replace(/([wW万])$/, ''));
                  return Number.isFinite(num) ? Math.round(num * 10000) : 0;
                }
                if (/([kK千])$/.test(normalized)) {
                  const num = parseFloat(normalized.replace(/([kK千])$/, ''));
                  return Number.isFinite(num) ? Math.round(num * 1000) : 0;
                }
                const parsed = parseFloat(normalized.replace(/[^0-9.]/g, ''));
                return Number.isFinite(parsed) ? Math.round(parsed) : 0;
              };

              // 提取标签
              const tagList: string[] = [];
              if (Array.isArray(note.tagList)) {
                note.tagList.forEach((tag: any) => {
                  if (tag.name) tagList.push(`#${tag.name}`);
                });
              }

              // 从正文提取 hashtag
              const hashtagMatches = desc.match(/#[^\s#]+/g);
              if (hashtagMatches) {
                hashtagMatches.forEach((tag: string) => {
                  if (!tagList.includes(tag)) tagList.push(tag);
                });
              }

              // 提取图片
              const images: string[] = [];
              if (Array.isArray(note.imageList)) {
                note.imageList.forEach((img: any) => {
                  const url = img.urlDefault || img.url || '';
                  if (url) images.push(url);
                });
              }

              return {
                note_id: note.noteId || '',
                title: note.title || '',
                content: desc,
                type: note.type === 'video' ? 'video' : 'normal',
                post_time: note.time ? new Date(note.time).toISOString() : '',
                metrics: {
                  likes: toNumber(interactInfo.likedCount),
                  comments: toNumber(interactInfo.commentCount),
                  favorites: toNumber(interactInfo.collectedCount),
                  shares: toNumber(interactInfo.shareCount),
                },
                tags: tagList,
                images,
                url: window.location.href,
              } as any;
            }

            return null;
          },
          { timeoutMs: 20_000 },
          25_000
        );

        if (noteData) {
          if (seenExtractedNoteIds.has(noteData.note_id)) {
            skippedNotes++;
            sendStatus({
              stage: 'note_skip',
              current: attemptedNotes,
              total: targetNotes,
              attempted: attemptedNotes,
              extracted: noteDetails.length,
              note_id: noteData.note_id,
              reason: 'duplicate_note_id',
            });
          } else {
            let finalNoteData: NoteDetail = noteData;

            const commentTotal = Number(noteData.metrics?.comments || 0);
            if (commentTotal > 0) {
              heartbeat.message = `读取评论 (note_id=${noteData.note_id})`;
              sendStatus({
                stage: 'comments_collect_start',
                current: attemptedNotes,
                total: targetNotes,
                attempted: attemptedNotes,
                extracted: noteDetails.length,
                note_id: noteData.note_id,
                comment_total: commentTotal,
                max_comments: DEFAULT_MAX_COMMENTS_PER_NOTE,
                scroll_rounds: DEFAULT_COMMENT_SCROLL_ROUNDS,
              });

              try {
                const comments = await collectCommentsForCurrentNote(noteData.note_id, commentTotal);
                finalNoteData = { ...noteData, comments };
                sendStatus({
                  stage: 'comments_collected',
                  current: attemptedNotes,
                  total: targetNotes,
                  attempted: attemptedNotes,
                  extracted: noteDetails.length,
                  note_id: noteData.note_id,
                  fetched: comments.fetched,
                });
              } catch (e) {
                sendStatus({
                  stage: 'comments_collect_error',
                  current: attemptedNotes,
                  total: targetNotes,
                  attempted: attemptedNotes,
                  extracted: noteDetails.length,
                  note_id: noteData.note_id,
                  error: e instanceof Error ? e.message : String(e),
                });
              }
            }

            seenExtractedNoteIds.add(noteData.note_id);
            noteDetails.push(finalNoteData);
          }

          heartbeat.message = `已采集 ${noteDetails.length}/${targetNotes} 篇笔记 (note_id=${noteData.note_id})`;
          sendStatus({
            stage: 'note_extracted',
            current: attemptedNotes,
            total: targetNotes,
            attempted: attemptedNotes,
            extracted: noteDetails.length,
            note_id: noteData.note_id,
            content_len: (noteData.content || '').length,
            metrics: noteData.metrics,
          });
        } else {
          timeoutNotes++;
          sendStatus({
            stage: 'note_extract_timeout',
            current: attemptedNotes,
            total: targetNotes,
            attempted: attemptedNotes,
            extracted: noteDetails.length,
            note_id: cover.noteId,
          });
        }

        // history.back() 关闭 overlay（XHS 用 Vue Router pushState 打开 overlay）
        await runInMainWorld<void>(tabId, () => {
          window.history.back();
        });

        // 等待 overlay 关闭
        await this.humanDelay(800, 1500);

        // 随机延迟模拟人类点击间隔
        if (noteDetails.length < targetNotes) {
          await this.humanDelay(1500, 3000);
        }
      } catch (error) {
        errorNotes++;
        console.warn(`${logPrefix} Failed to fetch note detail:`, error);
        sendStatus({
          stage: 'note_error',
          current: attemptedNotes,
          total: targetNotes,
          attempted: attemptedNotes,
          extracted: noteDetails.length,
          error: error instanceof Error ? error.message : String(error),
        });
        // 尝试关闭可能残留的 overlay
        try {
          await runInMainWorld<void>(tabId, () => {
            window.history.back();
          });
          await this.humanDelay(500, 800);
        } catch {
          // ignore
        }
      }
      }
    } // while

    // Step 4: 汇总结果
    const result: AnalyzeProfileResult = {
      success: true,
      data: {
        profile: profileData.profile,
        notes: noteDetails,
        analysis_meta: {
          total_notes_on_profile: totalNotes,
          fetched_notes: noteDetails.length,
          attempted_notes: attemptedNotes,
          timeout_notes: timeoutNotes,
          skipped_notes: skippedNotes,
          error_notes: errorNotes,
          target_notes: targetNotes,
          analyzed_at: new Date().toISOString(),
        },
      },
    };

    sendStatus({
      stage: 'done',
      fetched_notes: noteDetails.length,
      total_notes_on_profile: totalNotes,
      attempted_notes: attemptedNotes,
      timeout_notes: timeoutNotes,
      skipped_notes: skippedNotes,
      error_notes: errorNotes,
    });

    return {
      content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
      isError: false,
    };
    } finally {
      if (heartbeatTimer) {
        clearInterval(heartbeatTimer);
      }
    }
  }
}

/**
 * xhs_analyze_my_profile - 分析当前登录用户主页（必须走点击路线，否则容易触发反爬）
 *
 * - 不接受 url 参数（必须走点击路线）
 * - savePath 由后端用于落盘（扩展端不使用）
 * - 自动从 explore 页点击侧边栏“我”进入主页后采集数据
 */
export class XhsAnalyzeMyProfileTool extends XhsAnalyzeProfileBaseTool {
  name = 'xhs_analyze_my_profile';

  async execute(args: AnalyzeMyProfileArgsV2): Promise<ToolResult> {
    const logPrefix = '[xhs_analyze_my_profile]';
    const requestId = args?.__toolCallRequestId;
    let tabSnapshot: TabSessionSnapshot | null = null;
    let toolTabId: number | undefined;

    try {
      tabSnapshot = await this.captureTabSessionSnapshot();
      const tab = await this.findOrCreateXHSTab(XIAOHONGSHU_URLS.EXPLORE);
      if (!tab.id) throw new Error('无法创建标签页');
      toolTabId = tab.id;

      // 等待侧边栏加载
      await new Promise((resolve) => setTimeout(resolve, 2000));

      const clicked = await runInMainWorld<boolean>(tab.id, () => {
        const btn = document.querySelector('.side-bar .user a.link-wrapper') as HTMLElement | null;
        if (!btn) return false;
        btn.click();
        return true;
      });

      if (!clicked) {
        return this.createErrorResult('未找到侧边栏“我”按钮，请确认已登录小红书');
      }

      return await this.analyzeOnProfileTab(tab.id, DEFAULT_SAMPLE_COUNT, logPrefix, requestId);
    } catch (error) {
      console.error('[xhs_analyze_my_profile] Error:', error);
      return this.createErrorResult(error instanceof Error ? error.message : '账号分析失败');
    } finally {
      await this.cleanupToolTabSession(toolTabId, tabSnapshot, {
        closeIfCreated: true,
        restoreActiveTab: true,
      });
    }
  }
}

/**
 * xhs_analyze_profile - 分析指定用户主页（必须传 url）
 *
 * 注意：
 * - 如果是分析自己的主页，请务必改用 xhs_analyze_my_profile（点击路线），否则可能触发反爬
 * - url 必须包含 xsec_token，否则会被 XHS 屏蔽无法访问
 * - savePath 由后端用于落盘（扩展端不使用）
 */
export class XhsAnalyzeProfileTool extends XhsAnalyzeProfileBaseTool {
  name = 'xhs_analyze_profile';

  async execute(args: AnalyzeProfileArgsV2): Promise<ToolResult> {
    const logPrefix = '[xhs_analyze_profile]';
    const sampleCount = args?.sampleCount || DEFAULT_SAMPLE_COUNT;
    const requestId = args?.__toolCallRequestId;
    let tabSnapshot: TabSessionSnapshot | null = null;
    let toolTabId: number | undefined;

    try {
      const url = (args?.url || '').trim();
      if (!url) {
        return this.createErrorResult('必须提供 url 参数（目标用户主页 URL）');
      }

      const validated = validateXhsProfileUrl(url);
      if (!validated.ok) {
        return this.createErrorResult(validated.error);
      }

      // 先打开 explore 页用于检测“我”的主页链接，避免误用（自己的主页必须走 xhs_analyze_my_profile）
      tabSnapshot = await this.captureTabSessionSnapshot();
      const tab = await this.findOrCreateXHSTab(XIAOHONGSHU_URLS.EXPLORE);
      if (!tab.id) throw new Error('无法创建标签页');
      toolTabId = tab.id;
      await new Promise((resolve) => setTimeout(resolve, 1500));

      // 防误用：从侧边栏拿到“我”的 profile 链接，如果与目标一致则提示改用 my 工具
      try {
        const myHref = await runInMainWorld<string | null>(tab.id, () => {
          const el = document.querySelector('.side-bar .user a.link-wrapper') as HTMLAnchorElement | null;
          return el?.getAttribute('href') || null;
        });

        const toAbs = (href: string): string => {
          if (href.startsWith('http://') || href.startsWith('https://')) return href;
          if (href.startsWith('/')) return `${XIAOHONGSHU_URLS.HOME}${href}`;
          return href;
        };

        const extractUserId = (u: string): string => {
          const m = u.match(/\/user\/profile\/([^/?#]+)/);
          return m?.[1] || '';
        };

        const myUrl = myHref ? toAbs(myHref) : '';
        const myId = myUrl ? extractUserId(myUrl) : '';
        const targetId = extractUserId(url);
        if (myId && targetId && myId === targetId) {
          return this.createErrorResult(
            '检测到目标 url 为当前登录账号主页，请改用 xhs_analyze_my_profile（点击路线）'
          );
        }
      } catch {
        // ignore guard failures
      }

      console.log(`${logPrefix} Navigating to profile URL:`, url);
      await chrome.tabs.update(tab.id, { url });
      await this.waitForTabLoad(tab.id);

      return await this.analyzeOnProfileTab(tab.id, sampleCount, logPrefix, requestId);
    } catch (error) {
      console.error('[xhs_analyze_profile] Error:', error);
      return this.createErrorResult(error instanceof Error ? error.message : '账号分析失败');
    } finally {
      await this.cleanupToolTabSession(toolTabId, tabSnapshot, {
        closeIfCreated: true,
        restoreActiveTab: true,
      });
    }
  }
}
