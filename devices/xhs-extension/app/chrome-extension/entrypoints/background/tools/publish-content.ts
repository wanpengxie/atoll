import {
  XIAOHONGSHU_TOOL_NAMES,
  ToolResult,
  PublishContentArgs,
  XIAOHONGSHU_URLS,
} from 'coagent-xhs-shared';
import { BaseTool } from './base-tool';
import { runInMainWorld } from './inject-script';

// ─── publish 等真发布完成（M1.1 Fix-T4 §1 → R3-T4 FX6 → R4-T2 收紧） ─────────
//
// 旧实现把表单填完即视为成功 → daemon 收到 ok callback 但用户根本没点
// publish。M1.1 Fix-T4 在 background SW 挂 `chrome.tabs.onUpdated` +
// `chrome.tabs.onRemoved` 监听 publish tab —— 但**任意离开 publish 页**都
// 当 success（包括 `/post/list` 创作者中心列表）。round-2 codex#t59.1 +
// claude t59-M1/M2 共识 critical：
//
//   - 用户取消发布回到列表页也被记成 ok callback；daemon 端写错 dispatch.completed
//   - spec §1 要求"监听 publish-result API 或 URL navigate"，API watch 完全缺失
//
// R3-T4 FX6 收紧：
//   1) 增加 `chrome.webRequest.onCompleted` filter，监听 creator/edith
//      `/api/galaxy/note/*` 任意 URL；status 200 即 settle resolve（webRequest
//      不能直接读 body，URL 模式 + 状态码组合作信号；note_id 由后续
//      onUpdated NOTE_DETAIL_PATTERN 命中时补，没补到留 null 也不阻塞 ok）
//   2) `chrome.tabs.onUpdated` 改为辅助信号 —— 仅当 url 命中 NOTE_DETAIL_PATTERN
//      （`/explore/<hex>` / `/discovery/item/<hex>`）才 resolve；其它非空 url
//      （如 `/post/list`、空白 `/`、其它 xhs 域）视为用户取消发布 →
//      reject `PublishCanceledError`
//   3) tab 关闭 / 10min 超时仍按原 canceled / timeout 路径
//
// R4-T2 进一步收紧（修 R3-T4 FX6 引入的 false-positive 回归）：
//   - PUBLISH_API_URL_PATTERN 由 `/api/galaxy/note/` 前缀通配收紧为
//     `/api/galaxy/note/(publish|submit)` 精确尾段（带 `[/?#]|$` 边界守卫，
//     拒绝 `/publishabc` 字母后缀延伸误命中）。
//   - onApiCompleted 内强制 `details.method === 'POST'`，把 publish form 期间
//     XHS 发起的 GET autosave / draft / template fetch / counter polling
//     200 一律挡在主信号之外。
//   - 详见 PUBLISH_API_URL_PATTERN 注释里的"如何抓新端点"运维兜底。
//
// `waitForPublishCompletion` 抽成顶层 export 是为了 vitest 可注入 chromeApi
// hook，单测里用 fake event 驱动完整生命周期（含 webRequest 注入位）。

/** 完成发布后回吐给 daemon 的最小载荷。 */
export interface PublishCompletionResult {
  /** 跳转到的最终 URL（通常是创作者中心或笔记详情）。 */
  url: string;
  /** 从 URL 解析出的笔记 ID（hex）；无法解析时为 null，不阻塞 ok。 */
  note_id: string | null;
}

/** webRequest.onCompleted 注入接口；测试用 fake 事件驱动。 */
export interface WebRequestOnCompletedLike {
  addListener(
    callback: (details: chrome.webRequest.WebResponseCacheDetails) => void,
    filter: chrome.webRequest.RequestFilter,
    opt_extraInfoSpec?: string[]
  ): void;
  removeListener(callback: (details: chrome.webRequest.WebResponseCacheDetails) => void): void;
}

/** 等待选项；chromeApi / storageLocal / 时间钩子仅供单测注入。 */
export interface PublishWaitOptions {
  /** 最长等待时间，默认 10 分钟（spec §Fix-T4-1）。 */
  timeoutMs?: number;
  chromeApi?: {
    tabsOnUpdated: Pick<chrome.tabs.TabUpdatedEvent, 'addListener' | 'removeListener'>;
    tabsOnRemoved: Pick<chrome.tabs.TabRemovedEvent, 'addListener' | 'removeListener'>;
    /** R3-T4 FX6: publish-result API watch；测试可注入 fake。 */
    webRequestOnCompleted?: WebRequestOnCompletedLike;
  };
  /**
   * R3-T4 FX9：correlation_id 用于 chrome.storage.local 持久化 wait state。
   * 不传时只在内存里运行（与 R3 之前的行为一致），SW evict 后丢失。
   */
  correlationId?: string;
  /**
   * R3-T4 FX9：wait 起始时间（ms epoch）。recovery re-arm 路径需要传原始
   * startedAt 以保留全局 deadline 不变；普通 dispatch 路径默认 Date.now()。
   */
  startedAt?: number;
  /** R3-T4 FX9：测试可注入 fake storage；缺省走 chrome.storage.local。 */
  storageLocal?: StorageLocalLike;
}

/**
 * R3-T4 FX9：publish-wait 持久化 state。SW evict 后由 background startup hook
 * 扫 publish_wait:* 重建发布完成判定，避免 daemon 永远收不到 callback。
 */
export interface PublishWaitState {
  correlationId: string;
  tabId: number;
  /** ms epoch */
  startedAt: number;
  /** publish wait 的最长等待时间（ms）。recovery 据此判超时。 */
  timeoutMs: number;
}

/** R3-T4 FX9：chrome.storage.local 抽象，单测可替换。 */
export interface StorageLocalLike {
  get(keys?: string | string[] | null): Promise<Record<string, unknown>>;
  set(items: Record<string, unknown>): Promise<void>;
  remove(keys: string | string[]): Promise<void>;
}

/** R3-T4 FX9：publish-wait state 在 chrome.storage.local 的 key 前缀。 */
export const PUBLISH_WAIT_KEY_PREFIX = 'publish_wait:';

function publishWaitKey(correlationId: string): string {
  return `${PUBLISH_WAIT_KEY_PREFIX}${correlationId}`;
}

/** R3-T4 FX9：默认 storage 抽象，从 chrome.storage.local 读写。 */
function defaultStorageLocal(): StorageLocalLike | null {
  const c: any = (globalThis as any).chrome;
  if (!c?.storage?.local) return null;
  return c.storage.local as StorageLocalLike;
}

/** R3-T4 FX9：写入 publish-wait state（覆盖式）。storage 缺失时静默跳过。 */
export async function writePublishWaitState(
  state: PublishWaitState,
  storage?: StorageLocalLike,
): Promise<void> {
  const target = storage ?? defaultStorageLocal();
  if (!target) return;
  try {
    await target.set({ [publishWaitKey(state.correlationId)]: state });
  } catch (err) {
    console.warn('[publish-content] writePublishWaitState failed', err);
  }
}

/** R3-T4 FX9：删除 publish-wait state；幂等。 */
export async function removePublishWaitState(
  correlationId: string,
  storage?: StorageLocalLike,
): Promise<void> {
  const target = storage ?? defaultStorageLocal();
  if (!target) return;
  try {
    await target.remove(publishWaitKey(correlationId));
  } catch (err) {
    console.warn('[publish-content] removePublishWaitState failed', err);
  }
}

/** R3-T4 FX9：列出全部 publish-wait state。SW restart 时由 recovery 调用。 */
export async function listPublishWaitStates(
  storage?: StorageLocalLike,
): Promise<PublishWaitState[]> {
  const target = storage ?? defaultStorageLocal();
  if (!target) return [];
  try {
    const all = await target.get(null);
    const out: PublishWaitState[] = [];
    for (const [k, v] of Object.entries(all)) {
      if (!k.startsWith(PUBLISH_WAIT_KEY_PREFIX)) continue;
      if (!isPublishWaitState(v)) continue;
      out.push(v);
    }
    return out;
  } catch (err) {
    console.warn('[publish-content] listPublishWaitStates failed', err);
    return [];
  }
}

function isPublishWaitState(v: unknown): v is PublishWaitState {
  if (!v || typeof v !== 'object') return false;
  const r = v as Record<string, unknown>;
  return (
    typeof r.correlationId === 'string' &&
    typeof r.tabId === 'number' &&
    typeof r.startedAt === 'number' &&
    typeof r.timeoutMs === 'number'
  );
}

/** 抛给上层 callback 的取消错误，code 透传到 daemon。 */
export class PublishCanceledError extends Error {
  readonly code = 'publish_canceled';
  constructor(message = 'publish tab closed before completion') {
    super(message);
    this.name = 'PublishCanceledError';
  }
}

/** 抛给上层 callback 的超时错误，code 透传到 daemon。 */
export class PublishTimeoutError extends Error {
  readonly code = 'publish_timeout';
  constructor(message = 'publish wait timed out') {
    super(message);
    this.name = 'PublishTimeoutError';
  }
}

/**
 * publish 表单 URL 模式 —— 用户停留在该页面意味着尚未发布。
 * 离开此页面**不再**直接视为成功，需要进一步辨别去向（NOTE_DETAIL = 成功，
 * 其它 = 取消）。
 */
export const PUBLISH_PAGE_PATTERN = /creator\.xiaohongshu\.com\/(?:publish|new)\/(?:publish|note)/i;

/**
 * XHS 笔记详情 URL 模式，用于提取 note_id（hex 20+ 位），同时作为
 * "发布完成"权威跳转目标。R3-T4 FX6：onUpdated 仅在 url 命中本模式时 resolve。
 * 兼容 `/explore/<id>` 与 `/discovery/item/<id>` 两种形态。
 */
export const NOTE_ID_URL_PATTERN = /\/(?:explore|discovery\/item)\/([0-9a-f]{20,})/i;

/** R3-T4 FX6：NOTE_DETAIL 别名导出，语义更清晰，用作"发布成功"信号。 */
export const NOTE_DETAIL_PATTERN = NOTE_ID_URL_PATTERN;

/**
 * R3-T4 FX6 + R4-T2：publish-result API URL 模式（webRequest filter 不支持
 * 原生 regex，所以 manifest match-pattern 仍宽匹配 `/api/galaxy/note/*`，
 * 真正的精确尾段校验放在 onCompleted callback 内由本 regex 二次过滤）。
 *
 * R4-T2 收紧背景：旧实现末尾通配 `/api/galaxy/note/` 任何子路径都算 publish
 * 完成。XHS publish form 编辑期间会发起 autosave / draft / template fetch /
 * counter polling 等同前缀请求，任意 200 都会触发 settle resolve，重新引入
 * R2 修复要消除的 false-success（用户没真正点 publish 也被记成 ok callback）。
 *
 * 收紧策略：
 *   - 仅匹配 `(publish|submit)` 两条已知 publish-result 端点（尾段必须严格
 *     收口，正则末尾的 `(?:[/?#]|$)` 防 `/publishabc` 字母后缀误命中）。
 *   - 配合 onApiCompleted 内强制 `method === 'POST'`，把 GET autosave / draft
 *     /template / list 一律挡在主信号之外。
 *
 * 兜底（如未来抓到额外 publish-result 端点）：
 *   1. chrome.devtools 打开 publish 页 → Network 面板 → fetch/XHR 过滤 →
 *      点 publish 按钮，找 status=200 / method=POST / 落在 `creator|edith
 *      .xiaohongshu.com/api/galaxy/note/...` 下的端点。
 *   2. 把新尾段加入下方 alternation（如 `(?:publish|submit|<new>)`），同步
 *      在 publish-content.test.ts `PUBLISH_API_URL_PATTERN` 单测里加一条 positive。
 *   3. 端点漏抓不致命：`onUpdated` NOTE_DETAIL_PATTERN URL fallback 仍能 settle，
 *      10min timeout 兜底。漏匹配只是 fast-path 失效；放过假端点才是要避免的 bug。
 */
export const PUBLISH_API_URL_PATTERN =
  /^https?:\/\/(?:creator|edith)\.xiaohongshu\.com\/api\/galaxy\/note\/(?:publish|submit)(?:[/?#]|$)/i;

/**
 * R3-T4 FX6：chrome.webRequest.onCompleted filter 的 urls 字段，list pattern。
 * 单独 export 让 background recovery hook 与单测使用一致的过滤集合。
 */
export const PUBLISH_API_FILTER_URLS = [
  '*://creator.xiaohongshu.com/api/galaxy/note/*',
  '*://edith.xiaohongshu.com/api/galaxy/note/*',
];

/** 默认 publish 等待超时：10 分钟。 */
export const DEFAULT_PUBLISH_WAIT_TIMEOUT_MS = 10 * 60 * 1000;

/**
 * 等待 publish tab 真正发布完成。
 *
 * R3-T4 FX6 后语义（spec §1）：
 *   - publish-result API 200（`creator|edith.xiaohongshu.com/api/galaxy/note/*`）
 *     → settle resolve（webRequest 不能读 body，URL+status 即信号；note_id 待
 *     onUpdated NOTE_DETAIL 命中时补；url 当前 tab url 已可用则取，否则空串）
 *   - tab 跳转到 NOTE_DETAIL_PATTERN（`/explore/<hex>` / `/discovery/item/<hex>`）
 *     → settle resolve（拿得到 note_id）
 *   - tab 跳转到任意非 PUBLISH_PAGE 且非 NOTE_DETAIL 的 url（典型 `/post/list`、
 *     `/`、其它 xhs 页）→ reject `PublishCanceledError`（用户取消发布走人）
 *   - tab 关闭 → reject `PublishCanceledError`
 *   - 超时 → reject `PublishTimeoutError`
 */
export function waitForPublishCompletion(
  tabId: number,
  opts: PublishWaitOptions = {}
): Promise<PublishCompletionResult> {
  const timeoutMs = opts.timeoutMs ?? DEFAULT_PUBLISH_WAIT_TIMEOUT_MS;
  const tabsOnUpdated = opts.chromeApi?.tabsOnUpdated ?? chrome.tabs.onUpdated;
  const tabsOnRemoved = opts.chromeApi?.tabsOnRemoved ?? chrome.tabs.onRemoved;
  // webRequest 在 SW 环境下本身可用，但单测里没有；注入位允许测试驱动 fake。
  const webRequestOnCompleted: WebRequestOnCompletedLike | undefined =
    opts.chromeApi?.webRequestOnCompleted ??
    (typeof chrome !== 'undefined' && chrome?.webRequest?.onCompleted
      ? (chrome.webRequest.onCompleted as unknown as WebRequestOnCompletedLike)
      : undefined);
  // R3-T4 FX9: 持久化 publish-wait state 到 chrome.storage.local，SW evict 时
  // 由 recoverPublishWaitStates 兜底。仅当 correlationId 提供时启用（背景 init
  // 之外的纯单测路径不强制）。
  const correlationId = opts.correlationId ?? null;
  const startedAt = opts.startedAt ?? Date.now();
  const storage = opts.storageLocal ?? defaultStorageLocal();
  if (correlationId && storage) {
    void writePublishWaitState(
      { correlationId, tabId, startedAt, timeoutMs },
      storage,
    );
  }

  return new Promise<PublishCompletionResult>((resolve, reject) => {
    let settled = false;
    /** 最近一次已知的 publish tab url（onUpdated 看到的就缓存），webRequest
     *  resolve 时拿来作 fallback url。 */
    let lastKnownUrl = '';

    const settle = (fn: () => void) => {
      if (settled) return;
      settled = true;
      try {
        tabsOnUpdated.removeListener(onUpdated);
      } catch {
        /* ignore */
      }
      try {
        tabsOnRemoved.removeListener(onRemoved);
      } catch {
        /* ignore */
      }
      if (webRequestOnCompleted) {
        try {
          webRequestOnCompleted.removeListener(onApiCompleted);
        } catch {
          /* ignore */
        }
      }
      clearTimeout(timer);
      // R3-T4 FX9：出口删 storage 条目；SW 没 evict 走的就是这里。
      if (correlationId && storage) {
        void removePublishWaitState(correlationId, storage);
      }
      fn();
    };

    const onUpdated = (
      id: number,
      changeInfo: chrome.tabs.TabChangeInfo,
      tab?: chrome.tabs.Tab
    ) => {
      if (id !== tabId) return;
      const url = (changeInfo.url ?? tab?.url ?? '').trim();
      if (!url) return;
      lastKnownUrl = url;
      // 仍在 publish 表单页 → 还没点 publish；继续等。
      if (PUBLISH_PAGE_PATTERN.test(url)) return;
      // 命中笔记详情页 → 发布成功，提取 note_id。
      const match = url.match(NOTE_DETAIL_PATTERN);
      if (match) {
        const note_id = match[1];
        settle(() => resolve({ url, note_id }));
        return;
      }
      // R3-T4 FX6：离开 publish 表单但**不是**笔记详情（例如跳到 `/post/list`
      // 列表页、空白页、登录页）→ 用户取消发布，绝不能算 success。
      settle(() =>
        reject(
          new PublishCanceledError(
            `publish tab navigated away to non-detail page: ${url}`,
          ),
        ),
      );
    };

    /**
     * R3-T4 FX6 + R4-T2：webRequest.onCompleted。XHS 后端 publish 接口在 200
     * 时即代表笔记已落库，UI 才会跳转。早期信号比 onUpdated 命中详情页快几百
     * ms，用作主信号；后续 onUpdated 若再命中详情页，由 settled 守门只 resolve
     * 一次。
     *
     * R4-T2 三层守门（按 short-circuit 顺序）：
     *   1) tabId 命中
     *   2) method === 'POST'（publish 永远是 POST；GET 都是 autosave/draft/list/template）
     *   3) statusCode === 200
     *   4) URL 命中收紧后的 PUBLISH_API_URL_PATTERN（`/api/galaxy/note/(publish|submit)$` 尾段守卫）
     */
    const onApiCompleted = (details: chrome.webRequest.WebResponseCacheDetails) => {
      if (typeof details?.tabId === 'number' && details.tabId !== tabId) return;
      // R4-T2: method 校验。chrome.webRequest.WebResponseCacheDetails 在
      // 实际 chrome SDK 里有 `method: string`，但旧 @types 可能漏标，统一用
      // any cast 保守取。空/未知方法一律不 settle（安全侧）。
      const method = String((details as { method?: string }).method ?? '').toUpperCase();
      if (method !== 'POST') return;
      if (details.statusCode !== 200) return;
      // 二次校验 URL 模式（manifest filter 已经粗筛，这里精确尾段过滤掉
      // /api/galaxy/note/draft、/list、/template/* 等同前缀同 200 的 false-positive）。
      const apiUrl = String(details.url ?? '');
      if (!PUBLISH_API_URL_PATTERN.test(apiUrl)) return;
      // webRequest 不能直接拿 response body → note_id 暂留 null（除非 tab 已经
      // 抢先跳到详情页）。url 选取顺序：
      //   1) lastKnownUrl 若已是 NOTE_DETAIL（拿得到 note_id）→ 优先
      //   2) 否则 API URL 自身（更真实地反映"发布完成"动作；publish 表单 url
      //      不再算 completion url，避免回传给 daemon 显得没跳转）
      const detailMatch = lastKnownUrl.match(NOTE_DETAIL_PATTERN);
      if (detailMatch) {
        settle(() => resolve({ url: lastKnownUrl, note_id: detailMatch[1] }));
        return;
      }
      settle(() => resolve({ url: apiUrl, note_id: null }));
    };

    const onRemoved = (id: number) => {
      if (id !== tabId) return;
      settle(() => reject(new PublishCanceledError()));
    };

    const timer = setTimeout(() => {
      settle(() => reject(new PublishTimeoutError(`publish wait timed out after ${timeoutMs}ms`)));
    }, timeoutMs);

    tabsOnUpdated.addListener(onUpdated);
    tabsOnRemoved.addListener(onRemoved);
    if (webRequestOnCompleted) {
      // webRequest filter 仅支持 manifest match-pattern；regex 二次校验在
      // onApiCompleted 内做。extraInfoSpec 留空（不需要 responseHeaders）。
      webRequestOnCompleted.addListener(onApiCompleted, {
        urls: PUBLISH_API_FILTER_URLS,
      });
    }
  });
}

// ─── R3-T4 FX9：MV3 SW evict 后的 publish-wait 恢复 ────────────────────────────
//
// MV3 service worker 在 ~5min idle 后会被 evict；如果用户此时还在 publish 表单
// 上没点 publish，原本挂在 SW 内存里的 promise + listener 全部丢失，daemon 永远
// 收不到 callback（dispatch 卡死）。本函数由 background entrypoint 在 SW 启动
// 时调用一次，扫 chrome.storage.local 中所有 `publish_wait:*` 条目并据当前 tab
// 状态恢复语义：
//
//   - 已超时        → postCallback `publish_timeout`
//   - tab 不存在    → postCallback `publish_canceled`
//   - tab 命中详情  → postCallback ok（带 url + note_id）
//   - tab 仍在表单页 → 重新 register listener 续命，settle 时再 postCallback
//   - tab 跳到其他 → postCallback `publish_canceled`
//
// 重 register 路径不卡 recovery 主循环（fire-and-forget）；listener settle
// 后会通过 storage remove 自身，避免重复恢复。

/** Recovery 入口依赖。chrome.tabs.get / postCallback / now 都可注入便于单测。 */
export interface PublishWaitRecoveryDeps {
  /** chrome.storage.local 抽象；测试可换 fake。 */
  storageLocal?: StorageLocalLike;
  /** chrome.tabs.get 抽象；测试可换 fake。 */
  tabsGet?: (tabId: number) => Promise<chrome.tabs.Tab | null | undefined>;
  /** 必填：把恢复结论送给 daemon 的 callback。封装 coagentDeviceClient.postCallback。 */
  postCallback: (
    correlationId: string,
    payload: { status: 'ok' | 'error'; result: Record<string, unknown> | null; error: { code: string; message: string } | null },
  ) => Promise<void>;
  /** 当前时间（ms epoch）；测试注入便于断言超时分支。 */
  now?: () => number;
  /** 重 register listener 时复用的 chromeApi（默认走真实 chrome.* API）。 */
  chromeApi?: PublishWaitOptions['chromeApi'];
}

/** Recovery 单条状态的处理结果，便于 background 日志 / 单测断言。 */
export type PublishWaitRecoveryOutcome =
  | 'timeout'
  | 'canceled_tab_missing'
  | 'canceled_navigated_away'
  | 'completed_detail'
  | 'rearmed_listener';

export interface PublishWaitRecoverySummary {
  correlationId: string;
  outcome: PublishWaitRecoveryOutcome;
}

/**
 * Default chrome.tabs.get wrapper — async / null on missing tab. chrome.tabs.get
 * 在 tab 不存在时通常会通过 callback 抛 `Error: No tab with id`，这里统一吞成
 * null 让上层走 canceled 分支。
 */
function defaultTabsGet(tabId: number): Promise<chrome.tabs.Tab | null | undefined> {
  if (typeof chrome === 'undefined' || !chrome?.tabs?.get) {
    return Promise.resolve(null);
  }
  return new Promise((resolve) => {
    try {
      chrome.tabs.get(tabId, (tab) => {
        const lastError = (chrome.runtime as any)?.lastError;
        if (lastError) {
          resolve(null);
          return;
        }
        resolve(tab ?? null);
      });
    } catch {
      resolve(null);
    }
  });
}

/**
 * R3-T4 FX9：扫描 chrome.storage.local 中残留的 publish-wait state，按当前 tab
 * 状态恢复语义。返回每条的处理结论（main 流程不消费，主要给单测和日志看）。
 */
export async function recoverPublishWaitStates(
  deps: PublishWaitRecoveryDeps,
): Promise<PublishWaitRecoverySummary[]> {
  const storage = deps.storageLocal ?? defaultStorageLocal();
  const states = await listPublishWaitStates(storage ?? undefined);
  if (states.length === 0) return [];
  const now = (deps.now ?? Date.now)();
  const tabsGet = deps.tabsGet ?? defaultTabsGet;
  const summaries: PublishWaitRecoverySummary[] = [];

  for (const state of states) {
    try {
      // 1) 已超时 → publish_timeout
      const deadline = state.startedAt + state.timeoutMs;
      if (now > deadline) {
        await deps.postCallback(state.correlationId, {
          status: 'error',
          result: null,
          error: { code: 'publish_timeout', message: `publish wait timed out (recovered after deadline ${deadline})` },
        });
        await removePublishWaitState(state.correlationId, storage ?? undefined);
        summaries.push({ correlationId: state.correlationId, outcome: 'timeout' });
        continue;
      }
      // 2) 取 tab 状态
      let tab: chrome.tabs.Tab | null | undefined = null;
      try {
        tab = await tabsGet(state.tabId);
      } catch {
        tab = null;
      }
      const tabUrl = (tab?.url ?? '').trim();
      if (!tab || !tabUrl) {
        await deps.postCallback(state.correlationId, {
          status: 'error',
          result: null,
          error: { code: 'publish_canceled', message: `publish tab no longer exists (tabId=${state.tabId})` },
        });
        await removePublishWaitState(state.correlationId, storage ?? undefined);
        summaries.push({ correlationId: state.correlationId, outcome: 'canceled_tab_missing' });
        continue;
      }
      // 3) tab 命中详情页 → success
      const detailMatch = tabUrl.match(NOTE_DETAIL_PATTERN);
      if (detailMatch) {
        await deps.postCallback(state.correlationId, {
          status: 'ok',
          result: { url: tabUrl, note_id: detailMatch[1] },
          error: null,
        });
        await removePublishWaitState(state.correlationId, storage ?? undefined);
        summaries.push({ correlationId: state.correlationId, outcome: 'completed_detail' });
        continue;
      }
      // 4) tab 仍在 publish 表单 → 重 register listener 续命
      if (PUBLISH_PAGE_PATTERN.test(tabUrl)) {
        const remaining = Math.max(1_000, deadline - now);
        // fire-and-forget；listener settle 时会自行 remove storage 条目并 postCallback。
        void waitForPublishCompletion(state.tabId, {
          timeoutMs: remaining,
          correlationId: state.correlationId,
          startedAt: state.startedAt,
          chromeApi: deps.chromeApi,
          storageLocal: storage ?? undefined,
        })
          .then((completion) =>
            deps.postCallback(state.correlationId, {
              status: 'ok',
              result: { url: completion.url, note_id: completion.note_id },
              error: null,
            }),
          )
          .catch((err: any) =>
            deps.postCallback(state.correlationId, {
              status: 'error',
              result: null,
              error: {
                code: typeof err?.code === 'string' ? err.code : 'publish_failed',
                message: err instanceof Error ? err.message : String(err),
              },
            }),
          );
        summaries.push({ correlationId: state.correlationId, outcome: 'rearmed_listener' });
        continue;
      }
      // 5) tab 跳到其他 url → 用户已离开 publish 流程，视为取消
      await deps.postCallback(state.correlationId, {
        status: 'error',
        result: null,
        error: { code: 'publish_canceled', message: `publish tab navigated away to non-detail page during SW evict: ${tabUrl}` },
      });
      await removePublishWaitState(state.correlationId, storage ?? undefined);
      summaries.push({ correlationId: state.correlationId, outcome: 'canceled_navigated_away' });
    } catch (err) {
      console.warn('[publish-content] recovery failed for', state.correlationId, err);
    }
  }
  return summaries;
}

type PublishDebugLog = {
  ts: string;
  step: string;
  detail?: string;
};

type PublishScriptResult = {
  success: boolean;
  message: string;
  isScheduled?: boolean;
  publishAt?: string;
  manualPublishPending?: boolean;
  debugLogs?: PublishDebugLog[];
};

// This function will be serialized and executed in the page context
// Following the same logic as the Go version
const publishContentExecutor = async (args: Record<string, any>): Promise<PublishScriptResult> => {
  // All code must be self-contained since this runs in page context
  const params = args;
  const debugLogs: PublishDebugLog[] = [];
  const pushLog = (step: string, detail?: string) => {
    if (debugLogs.length >= 160) {
      debugLogs.shift();
    }
    debugLogs.push({
      ts: new Date().toISOString(),
      step,
      detail,
    });
  };

  console.log('[PublishContent] Starting execution with params:', params);
  pushLog('init', `titleLen=${String(params.title || '').length}, imageCount=${params.images?.length || 0}`);

  const waitForElement = (selector: string, timeout = 60000): Promise<Element> => {
    console.log('[waitForElement] Looking for:', selector);
    pushLog('wait.start', `${selector}, timeout=${timeout}`);
    return new Promise((resolve, reject) => {
      const existing = document.querySelector(selector);
      if (existing) {
        console.log('[waitForElement] Found immediately:', selector);
        pushLog('wait.ok', selector);
        resolve(existing);
        return;
      }

      let timer: NodeJS.Timeout;
      const observer = new MutationObserver(() => {
        const target = document.querySelector(selector);
        if (target) {
          observer.disconnect();
          clearTimeout(timer);
          pushLog('wait.ok', selector);
          resolve(target);
        }
      });

      timer = setTimeout(() => {
        observer.disconnect();
        pushLog('wait.timeout', selector);
        reject(new Error('未找到元素 ' + selector));
      }, timeout);

      observer.observe(document.body, { childList: true, subtree: true });
    });
  };

  // 检查元素是否可见 - 与 Go 版本的 isElementVisible 保持一致
  const isElementVisible = (element: Element): boolean => {
    const style = window.getComputedStyle(element);
    const rect = element.getBoundingClientRect();

    // 检查是否有隐藏样式
    if (style.display === 'none' || style.visibility === 'hidden') {
      return false;
    }

    // 检查位置是否在屏幕外
    if (rect.left < -9000 || rect.top < -9000) {
      return false;
    }

    return rect.width > 0 && rect.height > 0;
  };

  const isAlreadyImagePage = (): boolean => {
    const hasImageUploadBtn = Array.from(document.querySelectorAll('button')).some(
      (btn) => (btn.textContent || '').trim() === '上传图片'
    );
    const hasPublishBtn = Array.from(document.querySelectorAll('button')).some(
      (btn) => (btn.textContent || '').trim() === '发布'
    );
    const hasEditor = Boolean(
      document.querySelector(
        'div[role="textbox"][contenteditable="true"], div.tiptap.ProseMirror, div.ql-editor'
      )
    );
    return hasImageUploadBtn || (hasPublishBtn && hasEditor);
  };

  // 点击上传图文标签页 - 对应 Go 版本的 NewPublishImageAction
  const clickUploadTab = async () => {
    console.log('[clickUploadTab] Starting');
    pushLog('step1.start', '点击上传图文 Tab');

    // 等待 tab 容器可用（比 upload-content 更通用，已在图文态时也适用）
    await waitForElement('div.creator-tab');
    console.log('[clickUploadTab] creator-tab visible');

    // 等待页面稳定
    await new Promise((resolve) => setTimeout(resolve, 1000));

    // 查找所有的 creator-tab 元素
    const createElems = document.querySelectorAll('div.creator-tab');
    console.log(`[clickUploadTab] Found ${createElems.length} creator-tab elements`);

    // 过滤可见元素
    const visibleElems: Element[] = [];
    for (const elem of createElems) {
      if (isElementVisible(elem)) {
        visibleElems.push(elem);
      }
    }

    if (visibleElems.length === 0) {
      throw new Error('没有找到上传图文元素');
    }

    // 查找并点击"上传图文"
    let clicked = false;
    for (const elem of visibleElems) {
      const text = (elem.textContent || '').trim();
      console.log(`[clickUploadTab] Tab text: "${text}"`);
      if (text === '上传图文') {
        clickWithEvidence('step1.click', '点击上传图文 Tab', elem as HTMLElement);
        console.log('[clickUploadTab] Clicked 上传图文 tab');
        clicked = true;
        break;
      }
    }

    if (!clicked) {
      if (isAlreadyImagePage()) {
        console.log('[clickUploadTab] already in image page');
        pushLog('step1.skip', '已在图文发布态');
        return;
      }
      throw new Error('未找到可点击的上传图文 Tab');
    }

    await new Promise((resolve) => setTimeout(resolve, 1000));
    pushLog('step1.done', '上传图文 Tab 点击完成');
  };

  // 上传图片 - 对应 Go 版本的 uploadImages
  const uploadImages = async (images: any[]) => {
    console.log('[uploadImages] Starting image upload');
    pushLog('step2.start', `上传图片 count=${images.length}`);

    // 等待上传输入框出现
    const uploadInput = (await waitForElement('.upload-input')) as HTMLInputElement;
    console.log('[uploadImages] Found upload input');

    // 创建文件对象
    const dataTransfer = new DataTransfer();

    for (let i = 0; i < images.length; i++) {
      const file = await createFileFromResource(images[i], i);
      console.log(`[uploadImages] Created file ${i}:`, file.name, file.size);
      dataTransfer.items.add(file);
    }

    // 设置文件
    uploadInput.files = dataTransfer.files;

    // 触发事件
    const changeEvent = new Event('change', { bubbles: true, cancelable: true });
    const inputEvent = new Event('input', { bubbles: true, cancelable: true });
    uploadInput.dispatchEvent(inputEvent);
    uploadInput.dispatchEvent(changeEvent);

    console.log('[uploadImages] Files set and events dispatched');

    // 等待上传完成
    await new Promise((resolve) => setTimeout(resolve, 3000));
    pushLog('step2.done', '图片上传触发完成');
  };

  // 查找内容元素 - 对应 Go 版本的 getContentElement
  const getContentElement = async (): Promise<Element> => {
    console.log('[getContentElement] Looking for content element');

    // 方法1: 先查找新版编辑器（tiptap）
    let contentElem = document.querySelector(
      'div[role="textbox"][contenteditable="true"], div.tiptap.ProseMirror'
    );
    if (contentElem) {
      console.log('[getContentElement] Found tiptap editor');
      return contentElem;
    }

    // 方法2: 查找 div.ql-editor
    contentElem = document.querySelector('div.ql-editor');
    if (contentElem) {
      console.log('[getContentElement] Found ql-editor');
      return contentElem;
    }

    // 方法3: 通过 placeholder 查找
    const pElements = document.querySelectorAll('p');
    for (const elem of pElements) {
      const placeholder = elem.getAttribute('data-placeholder');
      if (placeholder && placeholder.includes('输入正文描述')) {
        console.log('[getContentElement] Found placeholder element');

        // 向上查找 textbox 父元素
        let current: Element | null = elem;
        for (let i = 0; i < 5 && current; i++) {
          const parentElement: HTMLElement | null = current.parentElement;
          if (!parentElement) break;

          const role = parentElement.getAttribute('role');
          if (role === 'textbox') {
            console.log('[getContentElement] Found textbox parent');
            return parentElement;
          }
          current = parentElement;
        }
      }
    }

    // 方法4: 最终兜底
    contentElem = document.querySelector('[role="textbox"][contenteditable="true"]');
    if (contentElem) {
      console.log('[getContentElement] Fallback to editable textbox');
      return contentElem;
    }

    throw new Error('没有找到内容输入框');
  };

  // 输入标签 - 对应 Go 版本的 inputTags
  const inputTags = async (contentElem: Element, tags: string[]) => {
    if (!tags || tags.length === 0) return;

    await new Promise((resolve) => setTimeout(resolve, 1000));

    // 确保元素有焦点
    if (contentElem instanceof HTMLElement) {
      contentElem.focus();
    }

    // 对于contenteditable元素，使用Selection API移动光标到末尾
    if (contentElem.getAttribute('contenteditable')) {
      const selection = window.getSelection();
      if (selection) {
        const range = document.createRange();
        range.selectNodeContents(contentElem);
        range.collapse(false); // collapse到末尾
        selection.removeAllRanges();
        selection.addRange(range);
      }

      // 使用execCommand插入换行
      document.execCommand('insertParagraph', false);
      document.execCommand('insertParagraph', false);
    } else {
      // 对于input/textarea，移动光标到末尾
      if (contentElem.tagName === 'INPUT' || contentElem.tagName === 'TEXTAREA') {
        const inputElem = contentElem as HTMLInputElement;
        inputElem.selectionStart = inputElem.selectionEnd = inputElem.value.length;
        inputElem.value += '\n\n';
      }
    }

    await new Promise((resolve) => setTimeout(resolve, 1000));

    // 输入每个标签
    for (const tag of tags) {
      await inputTag(contentElem, tag.replace(/^#/, ''));
    }
  };

  // 输入单个标签 - 对应 Go 版本的 inputTag
  const inputTag = async (contentElem: Element, tag: string) => {
    console.log('[inputTag] Starting to input tag:', tag);

    // 确保元素有焦点
    if (contentElem instanceof HTMLElement) {
      contentElem.focus();
    }

    // 对于contenteditable元素，使用execCommand
    if (contentElem.getAttribute('contenteditable')) {
      // 逐字符输入，模拟真实的键盘输入
      // 先输入 # 号
      document.execCommand('insertText', false, '#');
      await new Promise((resolve) => setTimeout(resolve, 200));

      // 逐字符输入标签文本
      for (const char of tag) {
        document.execCommand('insertText', false, char);
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
    } else if (contentElem.tagName === 'INPUT' || contentElem.tagName === 'TEXTAREA') {
      // 对于input/textarea元素
      const inputElem = contentElem as HTMLInputElement;
      const start = inputElem.selectionStart || inputElem.value.length;

      // 插入#号
      inputElem.value =
        inputElem.value.substring(0, start) + '#' + inputElem.value.substring(start);
      inputElem.selectionStart = inputElem.selectionEnd = start + 1;
      inputElem.dispatchEvent(
        new InputEvent('input', {
          data: '#',
          inputType: 'insertText',
          bubbles: true,
        })
      );
      await new Promise((resolve) => setTimeout(resolve, 200));

      // 逐字符输入
      for (const char of tag) {
        const cursorPos: number = inputElem.selectionStart || inputElem.value.length;
        inputElem.value =
          inputElem.value.substring(0, cursorPos) + char + inputElem.value.substring(cursorPos);
        inputElem.selectionStart = inputElem.selectionEnd = cursorPos + 1;
        inputElem.dispatchEvent(
          new InputEvent('input', {
            data: char,
            inputType: 'insertText',
            bubbles: true,
          })
        );
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
    }

    await new Promise((resolve) => setTimeout(resolve, 1000));

    // 查找并点击标签联想选项
    const topicContainer = document.querySelector('#creator-editor-topic-container');
    if (topicContainer) {
      console.log('[inputTag] Found topic container, waiting for items...');
      await new Promise((resolve) => setTimeout(resolve, 300)); // 额外等待选项加载

      const firstItem = topicContainer.querySelector('.item') as HTMLElement;
      if (firstItem) {
        console.log('[inputTag] Found and clicking suggestion item');
        firstItem.click();
        await new Promise((resolve) => setTimeout(resolve, 200));
      } else {
        console.log('[inputTag] No suggestion item found, adding space');
        // 没有找到联想选项，输入空格结束标签
        if (contentElem.getAttribute('contenteditable')) {
          document.execCommand('insertText', false, ' ');
        } else if (contentElem.tagName === 'INPUT' || contentElem.tagName === 'TEXTAREA') {
          const inputElem = contentElem as HTMLInputElement;
          const pos = inputElem.selectionStart || inputElem.value.length;
          inputElem.value =
            inputElem.value.substring(0, pos) + ' ' + inputElem.value.substring(pos);
          inputElem.selectionStart = inputElem.selectionEnd = pos + 1;
        }
      }
    } else {
      console.log('[inputTag] No topic container found, adding space');
      // 没有找到下拉框，输入空格结束标签
      if (contentElem.getAttribute('contenteditable')) {
        document.execCommand('insertText', false, ' ');
      } else if (contentElem.tagName === 'INPUT' || contentElem.tagName === 'TEXTAREA') {
        const inputElem = contentElem as HTMLInputElement;
        const pos = inputElem.selectionStart || inputElem.value.length;
        inputElem.value = inputElem.value.substring(0, pos) + ' ' + inputElem.value.substring(pos);
        inputElem.selectionStart = inputElem.selectionEnd = pos + 1;
      }
    }

    console.log('[inputTag] Completed tag input:', tag);
    await new Promise((resolve) => setTimeout(resolve, 500));
  };

  const normalizePublishAt = (
    input?: string
  ): { date: string; time: string; display: string } | null => {
    const value = (input || '').trim();
    if (!value) return null;

    const localPattern = /^(\d{4})[-/](\d{2})[-/](\d{2})[ T](\d{2}):(\d{2})(?::\d{2})?$/;
    const localMatch = value.match(localPattern);
    if (localMatch) {
      const date = `${localMatch[1]}-${localMatch[2]}-${localMatch[3]}`;
      const time = `${localMatch[4]}:${localMatch[5]}`;
      return { date, time, display: `${date} ${time}` };
    }

    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) {
      throw new Error('publish_at 时间格式无效，请使用 YYYY-MM-DD HH:mm 或 RFC3339');
    }

    const pad = (n: number) => String(n).padStart(2, '0');
    const date = `${parsed.getFullYear()}-${pad(parsed.getMonth() + 1)}-${pad(parsed.getDate())}`;
    const time = `${pad(parsed.getHours())}:${pad(parsed.getMinutes())}`;
    return { date, time, display: `${date} ${time}` };
  };

  const normalizeText = (text?: string): string => (text || '').replace(/\s+/g, '').trim();

  const isElementEnabled = (element: Element): boolean => {
    if (element instanceof HTMLButtonElement && element.disabled) return false;
    if (element.hasAttribute('disabled')) return false;
    if (element.getAttribute('aria-disabled') === 'true') return false;
    const dataDisabled = (element.getAttribute('data-disabled') || '').toLowerCase();
    if (dataDisabled === 'true' || dataDisabled === '1') return false;
    const className =
      typeof (element as HTMLElement).className === 'string'
        ? (element as HTMLElement).className.toLowerCase()
        : '';
    if (className.includes('disabled') || className.includes('is-disable')) return false;
    return true;
  };

  const getClickabilityReason = (element: HTMLElement): string | null => {
    if (!isElementVisible(element)) return 'not_visible';
    if (!isElementEnabled(element)) return 'disabled';
    const style = window.getComputedStyle(element);
    if (style.pointerEvents === 'none') return 'pointer_events_none';
    if (style.visibility === 'hidden') return 'visibility_hidden';

    const rect = element.getBoundingClientRect();
    if (rect.width < 2 || rect.height < 2) return 'rect_too_small';
    const x = Math.min(window.innerWidth - 1, Math.max(0, Math.floor(rect.left + rect.width / 2)));
    const y = Math.min(window.innerHeight - 1, Math.max(0, Math.floor(rect.top + rect.height / 2)));
    const topNode = document.elementFromPoint(x, y);
    if (!topNode) return 'element_from_point_null';
    if (topNode !== element && !element.contains(topNode)) {
      return `covered_by:${(topNode as HTMLElement).tagName.toLowerCase()}`;
    }
    return null;
  };

  const describeClickableElement = (element: Element | null): string => {
    if (!(element instanceof HTMLElement)) {
      return 'element=null';
    }
    const rect = element.getBoundingClientRect();
    const className = typeof element.className === 'string' ? element.className : '';
    const text = (element.textContent || '').replace(/\s+/g, ' ').trim();
    return JSON.stringify({
      tag: element.tagName.toLowerCase(),
      id: element.id || '',
      className: className.slice(0, 120),
      type: element.getAttribute('type') || '',
      ariaLabel: element.getAttribute('aria-label') || '',
      text: text.slice(0, 60),
      disabled: element instanceof HTMLButtonElement ? element.disabled : false,
      ariaDisabled: element.getAttribute('aria-disabled') || '',
      rect: {
        left: Math.round(rect.left),
        top: Math.round(rect.top),
        width: Math.round(rect.width),
        height: Math.round(rect.height),
      },
    });
  };

  const clickWithEvidence = (step: string, label: string, element: HTMLElement) => {
    pushLog(`${step}.target`, `${label}: ${describeClickableElement(element)}`);
    try {
      element.scrollIntoView({ block: 'center', inline: 'center' });
    } catch {}
    element.focus();

    const rect = element.getBoundingClientRect();
    const x = Math.min(window.innerWidth - 1, Math.max(0, Math.floor(rect.left + rect.width / 2)));
    const y = Math.min(window.innerHeight - 1, Math.max(0, Math.floor(rect.top + rect.height / 2)));
    const topNode = document.elementFromPoint(x, y);
    if (topNode && topNode !== element && !element.contains(topNode)) {
      pushLog(`${step}.cover`, `center(${x},${y})=${describeClickableElement(topNode)}`);
    }

    element.click();
    pushLog(`${step}.clicked`, label);
  };

  const findVisibleActionButton = (
    texts: string[],
    options?: { root?: ParentNode | null }
  ): HTMLElement | null => {
    const normalizedTargets = texts.map((t) => normalizeText(t));
    const root = options?.root || document;
    const candidates = root.querySelectorAll('button, [role="button"], label, span, div, a');
    let fuzzyMatch: HTMLElement | null = null;

    for (const node of candidates) {
      if (!(node instanceof HTMLElement)) continue;
      if (!isElementVisible(node) || !isElementEnabled(node)) continue;

      const currentText = normalizeText(node.textContent || '');
      if (!currentText) continue;

      for (const target of normalizedTargets) {
        if (currentText === target) return node;
        if (!fuzzyMatch && currentText.includes(target)) {
          fuzzyMatch = node;
        }
      }
    }

    return fuzzyMatch;
  };

  const getScheduleSettingsContainer = (): HTMLElement => {
    const containers = Array.from(document.querySelectorAll('.post-time-wrapper')).filter(
      (node): node is HTMLElement => node instanceof HTMLElement && isElementVisible(node)
    );

    for (const container of containers) {
      if (container.querySelector('.post-time-switch-container')) {
        return container;
      }
      if (
        container.querySelector(
          '.custom-switch-switch .d-switch-simulator, .custom-switch-switch, input[type="checkbox"], .date-picker-container input'
        )
      ) {
        return container;
      }
    }

    throw new Error('未找到定时发布设置区域');
  };

  type PublishButtonMode = 'publish' | 'schedule';

  const matchButtonByText = (
    buttons: HTMLButtonElement[],
    targets: string[]
  ): HTMLButtonElement | null => {
    for (const target of targets) {
      const normalizedTarget = normalizeText(target);
      for (const button of buttons) {
        const buttonText = normalizeText(
          button.textContent || button.getAttribute('aria-label') || ''
        );
        if (!buttonText) continue;
        if (buttonText === normalizedTarget) {
          return button;
        }
      }
    }
    return null;
  };

  const findBottomPublishButton = (mode: PublishButtonMode): HTMLButtonElement | null => {
    const containers = Array.from(document.querySelectorAll('.publish-page-publish-btn')).filter(
      (node): node is HTMLElement => node instanceof HTMLElement && isElementVisible(node)
    );

    const textTargets =
      mode === 'schedule'
        ? ['定时发布', '确认定时发布']
        : ['发布', '立即发布', '确认发布', '发布笔记'];

    for (const container of containers) {
      const buttons = Array.from(container.querySelectorAll('button')).filter(
        (button): button is HTMLButtonElement =>
          button instanceof HTMLButtonElement &&
          isElementVisible(button) &&
          isElementEnabled(button)
      );
      if (buttons.length === 0) {
        continue;
      }

      const exactByText = matchButtonByText(buttons, textTargets);
      if (exactByText) {
        return exactByText;
      }
    }

    return null;
  };

  const waitForBottomPublishButtonClickable = async (
    mode: PublishButtonMode,
    timeoutMs = 25000
  ): Promise<HTMLButtonElement> => {
    const deadline = Date.now() + timeoutMs;
    let lastReason = 'not_found';
    while (Date.now() < deadline) {
      const button = findBottomPublishButton(mode);
      if (button) {
        const reason = getClickabilityReason(button);
        if (!reason) {
          return button;
        }
        lastReason = reason;
      } else {
        lastReason = 'not_found';
      }
      await new Promise((resolve) => setTimeout(resolve, 300));
    }

    const label = mode === 'schedule' ? '定时发布按钮' : '发布按钮';
    throw new Error(`${label}不可点击(${lastReason})`);
  };

  const setInputValue = (
    input: HTMLInputElement,
    value: string,
    options?: { triggerBlur?: boolean }
  ) => {
    input.focus();
    const prototype = Object.getPrototypeOf(input);
    const setter = Object.getOwnPropertyDescriptor(prototype, 'value')?.set;
    if (setter) {
      setter.call(input, value);
    } else {
      input.value = value;
    }
    input.dispatchEvent(new Event('input', { bubbles: true }));
    input.dispatchEvent(new Event('change', { bubbles: true }));
    if (options?.triggerBlur ?? true) {
      input.dispatchEvent(new Event('blur', { bubbles: true }));
    }
  };

  const setupScheduledPublish = async (publishAt: {
    date: string;
    time: string;
    display: string;
  }) => {
    const scheduleContainer = getScheduleSettingsContainer();
    const scheduleDebug = () => {
      const checkbox = scheduleContainer.querySelector(
        'input[type="checkbox"]'
      ) as HTMLInputElement | null;
      const inputs = Array.from(scheduleContainer.querySelectorAll('input')).map((input) => {
        const type = input.getAttribute('type') || '';
        const cls = input.getAttribute('class') || '';
        const ph = input.getAttribute('placeholder') || '';
        const visible = input instanceof HTMLElement ? isElementVisible(input) : false;
        return `${type || 'text'}:${cls}:${ph}:${visible ? 'visible' : 'hidden'}`;
      });
      const hasDatePicker = Boolean(scheduleContainer.querySelector('.date-picker-container'));
      return `checkbox=${checkbox ? checkbox.checked : 'none'}, hasDatePicker=${hasDatePicker}, inputs=[${inputs.join('|')}]`;
    };
    const hasScheduleInput = () => {
      const candidates = Array.from(
        scheduleContainer.querySelectorAll(
          'input[type="datetime-local"], .date-picker-container input.d-text, .post-time-wrapper input.d-text, input[placeholder*="日期"], input[placeholder*="时间"], input[placeholder*="发布"]'
        )
      );

      return candidates.some(
        (input) =>
          input instanceof HTMLInputElement && input.type !== 'checkbox' && isElementVisible(input)
      );
    };

    const enableScheduleInput = async () => {
      const toggleSelectors = [
        '.custom-switch-switch .d-switch',
        '.custom-switch-switch .d-switch-box',
        '.custom-switch-switch .d-switch-top',
        '.custom-switch-switch .d-switch-simulator',
        '.custom-switch-switch [role="switch"]',
        '.custom-switch-switch',
        '.custom-switch-card',
        '.custom-switch-wrapper',
        '.custom-switch-text-content',
        '.custom-switch-text-content .has-tips',
      ];

      const clickToggleOnce = () => {
        for (const selector of toggleSelectors) {
          const target = scheduleContainer.querySelector(selector);
          if (!(target instanceof HTMLElement) || !isElementVisible(target)) {
            continue;
          }
          target.click();
          return true;
        }

        const scheduleCheckbox = scheduleContainer.querySelector(
          'input[type="checkbox"]'
        ) as HTMLInputElement | null;
        if (scheduleCheckbox) {
          scheduleCheckbox.click();
          return true;
        }
        return false;
      };

      const waitDurations = [350, 900, 1800];
      for (const waitMs of waitDurations) {
        if (hasScheduleInput()) {
          return true;
        }

        const clicked = clickToggleOnce();
        if (!clicked) {
          return false;
        }

        await new Promise((resolve) => setTimeout(resolve, waitMs));
      }

      // 某些页面会出现“开关已勾选但输入框未渲染”的状态，二次翻转兜底一次
      if (!hasScheduleInput()) {
        const clicked = clickToggleOnce();
        if (clicked) {
          await new Promise((resolve) => setTimeout(resolve, 700));
        }
      }

      return hasScheduleInput();
    };

    if (!hasScheduleInput()) {
      const enabled = await enableScheduleInput();
      if (!enabled) {
        throw new Error(`未找到定时发布时间输入区域 (${scheduleDebug()})`);
      }
    }

    if (!hasScheduleInput()) {
      throw new Error(`未找到定时发布时间输入区域 (${scheduleDebug()})`);
    }

    const scheduleValue = `${publishAt.date} ${publishAt.time}`;
    const datePickerInput = Array.from(
      scheduleContainer.querySelectorAll(
        '.date-picker-container input.d-text, .date-picker-container input, .post-time-wrapper input.d-text'
      )
    ).find(
      (input): input is HTMLInputElement =>
        input instanceof HTMLInputElement && input.type !== 'checkbox'
    );
    if (datePickerInput instanceof HTMLInputElement) {
      if (datePickerInput.type === 'datetime-local') {
        setInputValue(datePickerInput, `${publishAt.date}T${publishAt.time}`);
        await new Promise((resolve) => setTimeout(resolve, 200));
        return;
      }
      // d-datepicker 在当前页面是受控文本输入，避免 blur 触发组件自动回滚
      setInputValue(datePickerInput, scheduleValue, { triggerBlur: false });
      await new Promise((resolve) => setTimeout(resolve, 200));
      return;
    }

    const datetimeInput = scheduleContainer.querySelector('input[type="datetime-local"]');
    if (datetimeInput instanceof HTMLInputElement) {
      setInputValue(datetimeInput, `${publishAt.date}T${publishAt.time}`);
    } else {
      const inputs = Array.from(scheduleContainer.querySelectorAll('input'));
      const dateInput = inputs.find((input) => {
        const placeholder = input.getAttribute('placeholder') || '';
        return placeholder.includes('日期') || placeholder.includes('日历');
      });
      const timeInput = inputs.find((input) => {
        const placeholder = input.getAttribute('placeholder') || '';
        return placeholder.includes('时间');
      });

      if (dateInput instanceof HTMLInputElement && timeInput instanceof HTMLInputElement) {
        setInputValue(dateInput, publishAt.date);
        await new Promise((resolve) => setTimeout(resolve, 120));
        setInputValue(timeInput, publishAt.time);
      } else {
        const singleInput = inputs.find((input) => {
          const placeholder = input.getAttribute('placeholder') || '';
          return (
            placeholder.includes('发布时间') ||
            placeholder.includes('定时') ||
            placeholder.includes('时间')
          );
        });

        if (!(singleInput instanceof HTMLInputElement)) {
          throw new Error('未找到定时发布时间输入框');
        }
        setInputValue(singleInput, scheduleValue);
      }
    }

    await new Promise((resolve) => setTimeout(resolve, 350));
    const dialogRoot = document.querySelector('.d-modal,[role="dialog"],.modal,.dialog,.d-popover');
    const confirmCandidates = Array.from(
      (dialogRoot || scheduleContainer).querySelectorAll('button')
    ).filter(
      (button): button is HTMLButtonElement =>
        button instanceof HTMLButtonElement && isElementVisible(button) && isElementEnabled(button)
    );
    if (dialogRoot) {
      const confirmButton = matchButtonByText(confirmCandidates, [
        '确认定时发布',
        '定时发布',
        '确定',
        '确认',
      ]);
      if (confirmButton) {
        clickWithEvidence('step4.confirm_schedule', '确认定时发布时间', confirmButton);
        await new Promise((resolve) => setTimeout(resolve, 500));
      } else {
        const candidates = confirmCandidates
          .map((button) => normalizeText(button.textContent || button.getAttribute('aria-label') || ''))
          .filter(Boolean)
          .join('|');
        pushLog('step4.confirm_schedule.skip', `未命中确认按钮 candidates=[${candidates}]`);
      }
    }
  };

  // 注入发布按钮 click 事件监听器（try-catch 包裹，silent fail，不干扰原有功能）。
  //
  // 注意：M1.1 Fix-T4 §1 之前这里发出的 DUMAS_ASYNC_EVENT 期望由
  // `dumas-event-relay.content.ts` 中继到 Background；该 content script 在当前
  // 代码库里**未实现**，相关代码处于 V0.5 待办。Fix-T4 之后真正的"等点击 + 等
  // 发布完成"由 background 端 `waitForPublishCompletion(tabId)` 通过监听
  // chrome.tabs.onUpdated 完成；此处保留 postMessage 仅作为未来 relay 落地后的
  // 观测埋点，**不再是发布完成判定的关键路径**。
  const injectPublishButtonListener = (button: HTMLElement, isScheduled: boolean) => {
    try {
      button.addEventListener(
        'click',
        () => {
          try {
            const publishJobId = (params as any).__publishJobId || '';
            const ts = Date.now();
            window.postMessage(
              {
                type: 'DUMAS_ASYNC_EVENT',
                event: {
                  eventId: `pub_${publishJobId}_${ts}`,
                  taskId: publishJobId,
                  seq: 0,
                  eventType: 'publish_clicked',
                  payload: {
                    stage: 'publish_clicked',
                    publishJobId,
                    toolCallRequestId: (params as any).__toolCallRequestId || '',
                    title: String(params.title || '').slice(0, 100),
                    isScheduled,
                    timestamp: ts,
                  },
                  timestamp: ts,
                  needAck: true,
                },
              },
              '*'
            );
          } catch (_e) {
            // silent fail — 永远不干扰发布按钮原有功能
          }
        },
        { once: true }
      );
      pushLog(
        'listener.injected',
        `isScheduled=${isScheduled}, btn=${(button.textContent || '').trim().slice(0, 20)}`
      );
    } catch (e) {
      pushLog('listener.inject_failed', String(e));
    }
  };

  // 提交发布（填完即止模式：填写内容 + 注入按钮监听，不自动点击发布）
  const submitPublish = async (
    title: string,
    content: string,
    tags: string[],
    publishAtRaw?: string
  ) => {
    console.log('[submitPublish] Starting submit');
    pushLog('step3.start', '填写标题、内容、标签');

    // 输入标题
    const titleInput = document.querySelector('div.d-input input') as HTMLInputElement;
    if (!titleInput) {
      throw new Error('未找到标题输入框');
    }
    titleInput.value = title;
    titleInput.dispatchEvent(new Event('input', { bubbles: true }));

    await new Promise((resolve) => setTimeout(resolve, 1000));

    // 输入内容
    const contentElem = await getContentElement();

    // 确保元素有焦点
    if (contentElem instanceof HTMLElement) {
      contentElem.focus();
    }

    // 根据元素类型输入内容
    if (contentElem.getAttribute('contenteditable')) {
      // 对于contenteditable元素，使用execCommand
      // 先清空内容
      const selection = window.getSelection();
      if (selection) {
        const range = document.createRange();
        range.selectNodeContents(contentElem);
        selection.removeAllRanges();
        selection.addRange(range);
      }
      // 使用execCommand插入内容
      document.execCommand('insertText', false, content);
    } else if (contentElem.tagName === 'INPUT' || contentElem.tagName === 'TEXTAREA') {
      const inputElem = contentElem as HTMLInputElement;
      inputElem.value = content;
      inputElem.dispatchEvent(
        new InputEvent('input', {
          bubbles: true,
        })
      );
    }

    // 输入标签
    await inputTags(contentElem, tags);
    pushLog('step3.done', `tags=${tags.length}`);

    await new Promise((resolve) => setTimeout(resolve, 1000));

    const publishAt = normalizePublishAt(publishAtRaw);
    if (publishAt) {
      console.log('[submitPublish] 开始配置定时发布:', publishAt.display);
      pushLog('step4.start', `配置定时发布 ${publishAt.display}`);
      await setupScheduledPublish(publishAt);

      pushLog('step4.wait', '等待定时发布按钮可点击');
      const scheduleBtn = await waitForBottomPublishButtonClickable('schedule', 25000);
      injectPublishButtonListener(scheduleBtn, true);
      pushLog('step4.done', '定时发布按钮监听已注入');

      return {
        message: `定时发布已设置（${publishAt.display}），请确认内容无误后手动点击「定时发布」按钮`,
        isScheduled: true,
        publishAt: publishAt.display,
        manualPublishPending: true,
      };
    }

    // 填完即止：注入发布按钮监听器，等待用户手动确认发布
    console.log('[submitPublish] 查找发布按钮并注入监听');
    pushLog('step4.start', '注入发布按钮监听');
    const publishBtn = await waitForBottomPublishButtonClickable('publish', 25000);
    injectPublishButtonListener(publishBtn, false);
    pushLog('step4.done', '发布按钮监听已注入');

    return {
      message: '图文内容已填写完成，请确认内容无误后手动点击「发布」按钮',
      isScheduled: false,
      manualPublishPending: true,
    };
  };

  // 辅助函数：从资源创建文件
  const createFileFromResource = async (resource: any, index: number) => {
    if (!resource || typeof resource !== 'object') {
      throw new Error(`图片资源(${index}) 格式错误`);
    }

    if (resource.type === 'data') {
      const match = resource.value.match(/^data:(.*?);base64,(.*)$/);
      if (!match) {
        throw new Error('无效的 data URL 格式');
      }

      const mime = match[1];
      const binary = atob(match[2]);
      const array = new Uint8Array(binary.length);

      for (let i = 0; i < binary.length; i++) {
        array[i] = binary.charCodeAt(i);
      }

      const blob = new Blob([array], { type: mime || 'image/jpeg' });
      const fileName = resource.fileName || `image_${index}.${mime.split('/')[1] || 'jpg'}`;
      return new File([blob], fileName, { type: mime || 'image/jpeg' });
    }

    if (resource.type === 'url') {
      // 服务端应该已经将 URL 转换为 data URL，如果还是 URL 类型说明有问题
      throw new Error('图片资源类型错误：服务端应该已经预处理 URL 图片');
    }

    throw new Error(`不支持的图片资源类型: ${resource.type}`);
  };

  // 主执行逻辑
  let executionResult: PublishScriptResult;

  try {
    console.log('[PublishContent] Current URL:', window.location.href);
    console.log('[PublishContent] Page title:', document.title);

    // 步骤1: 点击上传图文标签页
    await clickUploadTab();

    // 步骤2: 上传图片
    await uploadImages(params.images);
    console.log('[PublishContent] Images uploaded');

    // 步骤3: 填写内容并注入发布按钮监听
    const submitResult = await submitPublish(
      params.title,
      params.content,
      params.tags || [],
      params.publish_at
    );
    console.log('[PublishContent] Content filled, manual publish pending');

    executionResult = {
      success: true,
      message: submitResult.message,
      isScheduled: submitResult.isScheduled,
      publishAt: submitResult.publishAt,
      manualPublishPending: submitResult.manualPublishPending,
      debugLogs,
    };
  } catch (error) {
    console.error('[PublishContent] Execution error:', error);
    const errorMessage = error instanceof Error ? error.message : '发布失败';
    pushLog('failed', errorMessage);
    executionResult = {
      success: false,
      message: errorMessage,
      debugLogs,
    };
  }

  console.log('[PublishContent] Returning result:', executionResult);
  return executionResult;
};

export class PublishContentTool extends BaseTool {
  name = XIAOHONGSHU_TOOL_NAMES.PUBLISH_CONTENT;

  async execute(args: PublishContentArgs): Promise<ToolResult> {
    const toolLogs: PublishDebugLog[] = [];
    const pushToolLog = (step: string, detail?: string) => {
      if (toolLogs.length >= 80) {
        toolLogs.shift();
      }
      toolLogs.push({
        ts: new Date().toISOString(),
        step,
        detail,
      });
    };
    let scriptLogs: PublishDebugLog[] = [];

    try {
      pushToolLog('tool.start', 'xhs_publish_content');
      // 发布参数由后端统一校验，插件端直接执行页面流程。
      const safeTitle = typeof args.title === 'string' ? args.title : '';
      const safeContent = typeof args.content === 'string' ? args.content : '';
      const safeImages = Array.isArray(args.images) ? args.images : [];
      const safeTags = Array.isArray(args.tags) ? args.tags : [];
      const tabSession = await this.captureTabSessionSnapshot();
      let publishTabId: number | undefined;

      try {
        // 导航到发布页面（与 Go 版本保持一致），发布场景强制使用独立标签页
        const tab = await this.createIsolatedXHSTab(`${XIAOHONGSHU_URLS.PUBLISH}?source=official`);

        if (!tab.id) {
          throw new Error('Failed to create tab');
        }
        publishTabId = tab.id;
        pushToolLog('tool.tab_created', `tabId=${tab.id}`);

        const publishPageProbe = await this.waitForXHSPublishPageReady(tab.id, {
          requiredCreatorTabs: ['上传图文'],
          retryIntervalsMs: [2000, 8000, 16000],
        });
        console.log('[PublishContent] Publish page ready:', publishPageProbe);
        pushToolLog(
          'tool.page_ready',
          `url=${publishPageProbe.currentUrl}, tabs=${publishPageProbe.visibleCreatorTabs.join('|')}`
        );

        // 执行发布脚本
        let result: PublishScriptResult | null = null;

        try {
          // 发布任务需要更长的超时时间：120秒
          result = await runInMainWorld<PublishScriptResult>(
            tab.id,
            publishContentExecutor,
            args,
            120000
          );
          scriptLogs = Array.isArray(result?.debugLogs) ? result.debugLogs : [];
          pushToolLog('tool.script_done', `success=${Boolean(result?.success)}`);
        } catch (executeError) {
          console.error('[PublishContent] publish script error:', executeError);
          const errorMessage =
            executeError instanceof Error ? executeError.message : String(executeError);
          pushToolLog('tool.script_error', errorMessage);
          if (
            errorMessage.includes('Cannot access') ||
            errorMessage.includes('chrome-extension://')
          ) {
            throw new Error('无法在小红书页面执行脚本，请检查是否已登录并在正确的页面');
          }
          throw new Error(`脚本执行失败: ${errorMessage}`);
        }

        // 验证结果
        console.log('[PublishContent] publish script result:', result);

        if (!result || typeof result !== 'object') {
          throw new Error(`Invalid result from publish script: ${JSON.stringify(result)}`);
        }
        if (!result.success) {
          throw new Error(result.message || 'Unknown error in page script');
        }

        // M1.1 Fix-T4 §1: 表单填好之后才进入"等真发布完成"阶段。
        // runInMainWorld 已经返回 → publish 按钮 listener 已注入。
        // 这里在 background SW 长期挂监听等用户真点击 + 后端跳转。
        // R3-T4 FX9：把 daemon 注入的 __correlationId 传给 wait，启用 storage
        // 持久化；SW evict 后由 background recoverPublishWaitStates 恢复。
        const correlationId =
          typeof (args as any)?.__correlationId === 'string'
            ? ((args as any).__correlationId as string)
            : undefined;
        pushToolLog(
          'tool.wait_publish.start',
          `tabId=${tab.id} corr=${correlationId ?? 'n/a'}`,
        );
        const completion = await waitForPublishCompletion(tab.id, {
          timeoutMs: DEFAULT_PUBLISH_WAIT_TIMEOUT_MS,
          correlationId,
        });
        pushToolLog(
          'tool.wait_publish.done',
          `url=${completion.url}, note_id=${completion.note_id ?? 'null'}`
        );

        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({
                success: true,
                message: result.message,
                title: safeTitle,
                contentLength: safeContent.length,
                imageCount: safeImages.length,
                tagCount: safeTags.length,
                isScheduled: Boolean(result.isScheduled),
                publishAt: result.publishAt || null,
                manualPublishPending: false,
                url: completion.url,
                note_id: completion.note_id,
                toolLogs,
                scriptLogs,
              }),
            },
          ],
          isError: false,
        };
      } finally {
        // 填完即止：Tab 保留不关闭，仅恢复之前的活动标签页
        await this.cleanupToolTabSession(publishTabId, tabSession, {
          closeIfCreated: false,
          restoreActiveTab: true,
        });
      }
    } catch (error) {
      console.error('PublishContent error:', error);
      const errorMessage = error instanceof Error ? error.message : '未知错误';
      // 透传 publish_canceled / publish_timeout 等结构化 code 给 daemon callback。
      const errorCode =
        typeof (error as any)?.code === 'string' ? ((error as any).code as string) : 'publish_failed';
      pushToolLog('tool.failed', `${errorCode}: ${errorMessage}`);
      return {
        content: [
          {
            type: 'error',
            text: JSON.stringify({
              success: false,
              code: errorCode,
              message: errorMessage,
              debug: {
                toolLogs,
                scriptLogs: scriptLogs.slice(-80),
              },
            }),
          },
        ],
        isError: true,
      };
    }
  }
}
