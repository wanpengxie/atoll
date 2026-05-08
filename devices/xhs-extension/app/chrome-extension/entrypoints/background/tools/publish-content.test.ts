// Vitest unit tests for publish-content.ts `waitForPublishCompletion`.
//
// Coverage (M1.1 Fix-T4 §1 → R3-T4 FX6 收紧):
//   1. URL navigates to a note-detail page          → resolve {url, note_id}
//   2. Publish API onCompleted 200                  → resolve (FX6 主信号)
//   3. Publish tab navigates to /post/list etc.     → reject `publish_canceled` (FX6)
//   4. Publish tab is closed by the user            → reject `publish_canceled`
//   5. 10-min wait elapses with no navigation       → reject `publish_timeout`
//
// We don't go through the workspace shared package — replicate the constants
// inline so vitest doesn't require a build step.

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

vi.mock('coagent-xhs-shared', () => ({
  XIAOHONGSHU_TOOL_NAMES: {
    PUBLISH_CONTENT: 'xhs_publish_content',
  },
  XIAOHONGSHU_URLS: {
    PUBLISH: 'https://creator.xiaohongshu.com/publish/publish',
  },
}));

// publish-content.ts imports BaseTool & runInMainWorld; we don't exercise the
// page-side path here. Stub them out to keep this test file lean.
vi.mock('./base-tool', () => ({
  BaseTool: class {},
}));
vi.mock('./inject-script', () => ({
  runInMainWorld: vi.fn(),
}));

import {
  waitForPublishCompletion,
  PublishCanceledError,
  PublishTimeoutError,
  PUBLISH_PAGE_PATTERN,
  NOTE_ID_URL_PATTERN,
  NOTE_DETAIL_PATTERN,
  PUBLISH_API_URL_PATTERN,
  PUBLISH_API_FILTER_URLS,
  PUBLISH_WAIT_KEY_PREFIX,
  type PublishWaitState,
  type StorageLocalLike,
  writePublishWaitState,
  removePublishWaitState,
  listPublishWaitStates,
  recoverPublishWaitStates,
} from './publish-content';

// Fake chrome.storage.local for FX9 persistence + recovery tests.
function makeFakeStorage(): StorageLocalLike & { _data: Record<string, unknown> } {
  const data: Record<string, unknown> = {};
  return {
    _data: data,
    async get(keys?: string | string[] | null) {
      if (keys == null) return { ...data };
      const list = Array.isArray(keys) ? keys : [keys];
      const out: Record<string, unknown> = {};
      for (const k of list) if (k in data) out[k] = data[k];
      return out;
    },
    async set(items: Record<string, unknown>) {
      Object.assign(data, items);
    },
    async remove(keys: string | string[]) {
      const list = Array.isArray(keys) ? keys : [keys];
      for (const k of list) delete data[k];
    },
  };
}

// Minimal fake of `chrome.tabs.onUpdated` / `chrome.tabs.onRemoved` /
// `chrome.webRequest.onCompleted` that lets us drive event firing from inside
// each test.
type TabUpdatedListener = (
  tabId: number,
  changeInfo: chrome.tabs.TabChangeInfo,
  tab?: chrome.tabs.Tab
) => void;
type TabRemovedListener = (tabId: number, info?: chrome.tabs.TabRemoveInfo) => void;
type WebReqCompletedListener = (
  details: chrome.webRequest.WebResponseCacheDetails
) => void;

function makeFakeTabsApi() {
  const updated = new Set<TabUpdatedListener>();
  const removed = new Set<TabRemovedListener>();
  const webreq = new Set<WebReqCompletedListener>();
  // Capture the latest filter passed to webRequest.addListener so tests can
  // assert manifest-pattern parity with PUBLISH_API_FILTER_URLS.
  let lastWebReqFilter: chrome.webRequest.RequestFilter | null = null;
  return {
    tabsOnUpdated: {
      addListener: (fn: TabUpdatedListener) => updated.add(fn),
      removeListener: (fn: TabUpdatedListener) => updated.delete(fn),
    },
    tabsOnRemoved: {
      addListener: (fn: TabRemovedListener) => removed.add(fn),
      removeListener: (fn: TabRemovedListener) => removed.delete(fn),
    },
    webRequestOnCompleted: {
      addListener: (
        fn: WebReqCompletedListener,
        filter: chrome.webRequest.RequestFilter,
      ) => {
        webreq.add(fn);
        lastWebReqFilter = filter;
      },
      removeListener: (fn: WebReqCompletedListener) => webreq.delete(fn),
    },
    fireUpdated: (
      tabId: number,
      changeInfo: chrome.tabs.TabChangeInfo,
      tab?: chrome.tabs.Tab
    ) => {
      for (const fn of updated) fn(tabId, changeInfo, tab);
    },
    fireRemoved: (tabId: number) => {
      for (const fn of removed) fn(tabId);
    },
    fireWebReqCompleted: (details: chrome.webRequest.WebResponseCacheDetails) => {
      for (const fn of webreq) fn(details);
    },
    listenerCounts: () => ({
      updated: updated.size,
      removed: removed.size,
      webreq: webreq.size,
    }),
    getWebReqFilter: () => lastWebReqFilter,
  };
}

beforeEach(() => {
  vi.useRealTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

// Helper — every test wires all three fakes (tabs.onUpdated / onRemoved /
// webRequest.onCompleted) consistently.
function chromeApiOf(api: ReturnType<typeof makeFakeTabsApi>) {
  return {
    tabsOnUpdated: api.tabsOnUpdated,
    tabsOnRemoved: api.tabsOnRemoved,
    webRequestOnCompleted: api.webRequestOnCompleted,
  };
}

describe('waitForPublishCompletion', () => {
  it('resolves with {url, note_id} when tab navigates to a note-detail URL', async () => {
    const api = makeFakeTabsApi();
    const resultP = waitForPublishCompletion(101, {
      timeoutMs: 5_000,
      chromeApi: chromeApiOf(api),
    });

    // First fire: still on the publish form — should not settle.
    api.fireUpdated(101, { url: 'https://creator.xiaohongshu.com/publish/publish?source=foo' });
    expect(api.listenerCounts().updated).toBe(1);

    // Second fire: navigated to discovery/item with hex id.
    const noteId = '0123456789abcdef01234567';
    api.fireUpdated(101, { url: `https://www.xiaohongshu.com/discovery/item/${noteId}?from=publish` });

    const completion = await resultP;
    expect(completion.note_id).toBe(noteId);
    expect(completion.url).toContain(`/discovery/item/${noteId}`);
    // Listeners cleaned up after settle.
    expect(api.listenerCounts()).toEqual({ updated: 0, removed: 0, webreq: 0 });
  });

  // R3-T4 FX6: webRequest.onCompleted 200 是主信号；命中即 resolve，即便 tab
  // 还没跳到详情页（note_id 留 null 但 ok 不阻塞）。
  it('resolves on chrome.webRequest.onCompleted 200 for publish API URL', async () => {
    const api = makeFakeTabsApi();
    const resultP = waitForPublishCompletion(110, {
      timeoutMs: 5_000,
      chromeApi: chromeApiOf(api),
    });

    // Filter must match the exported manifest pattern set.
    expect(api.getWebReqFilter()).toMatchObject({ urls: PUBLISH_API_FILTER_URLS });

    // Still on publish form — listener latched, no settle yet.
    api.fireUpdated(110, { url: 'https://creator.xiaohongshu.com/publish/publish' });
    expect(api.listenerCounts().webreq).toBe(1);

    // API publish 完成 → settle.
    api.fireWebReqCompleted({
      tabId: 110,
      url: 'https://creator.xiaohongshu.com/api/galaxy/note/publish?ts=1',
      statusCode: 200,
      method: 'POST',
    } as any);

    const completion = await resultP;
    // url 当前没拿到详情页跳转，note_id 保持 null（spec §1：不阻塞 ok）。
    expect(completion.note_id).toBeNull();
    expect(completion.url).toMatch(/api\/galaxy\/note\/publish/);
    expect(api.listenerCounts()).toEqual({ updated: 0, removed: 0, webreq: 0 });
  });

  it('webRequest API watch ignores non-200 / wrong-domain / wrong-tab events', async () => {
    const api = makeFakeTabsApi();
    const resultP = waitForPublishCompletion(111, {
      timeoutMs: 80,
      chromeApi: chromeApiOf(api),
    });

    // Wrong tab id — must NOT settle.
    api.fireWebReqCompleted({
      tabId: 999,
      url: 'https://creator.xiaohongshu.com/api/galaxy/note/publish',
      statusCode: 200,
      method: 'POST',
    } as any);
    // 4xx — must NOT settle.
    api.fireWebReqCompleted({
      tabId: 111,
      url: 'https://creator.xiaohongshu.com/api/galaxy/note/publish',
      statusCode: 400,
      method: 'POST',
    } as any);
    // Wrong host (Chrome match-pattern would have filtered this anyway, but the
    // regex 二次校验 is what defends against filter drift) — must NOT settle.
    api.fireWebReqCompleted({
      tabId: 111,
      url: 'https://other.example.com/api/galaxy/note/publish',
      statusCode: 200,
      method: 'POST',
    } as any);

    await expect(resultP).rejects.toBeInstanceOf(PublishTimeoutError);
  });

  // R4-T2 §3 negative regression: tabId 命中 + 200 + POST + URL 是同前缀
  // 非 publish 端点（draft / list / template / counter）→ regex 二次校验拦下，
  // 不 settle resolve；最终走 timeout。修 R3-T4 FX6 的 prefix 通配 false-success。
  it('webRequest API watch ignores same-prefix non-publish endpoints at 200 (R4-T2)', async () => {
    const api = makeFakeTabsApi();
    const resultP = waitForPublishCompletion(112, {
      timeoutMs: 80,
      chromeApi: chromeApiOf(api),
    });

    // /api/galaxy/note/draft 200 POST — autosave 风格，禁 settle。
    api.fireWebReqCompleted({
      tabId: 112,
      url: 'https://creator.xiaohongshu.com/api/galaxy/note/draft',
      statusCode: 200,
      method: 'POST',
    } as any);
    // /api/galaxy/note/list 200 POST — 创作者中心列表查询，禁 settle。
    api.fireWebReqCompleted({
      tabId: 112,
      url: 'https://edith.xiaohongshu.com/api/galaxy/note/list?cursor=abc',
      statusCode: 200,
      method: 'POST',
    } as any);
    // /api/galaxy/note/template/123 200 POST — 模板拉取，禁 settle。
    api.fireWebReqCompleted({
      tabId: 112,
      url: 'https://creator.xiaohongshu.com/api/galaxy/note/template/123',
      statusCode: 200,
      method: 'POST',
    } as any);
    // /api/galaxy/note/counter 200 POST — counter polling，禁 settle。
    api.fireWebReqCompleted({
      tabId: 112,
      url: 'https://edith.xiaohongshu.com/api/galaxy/note/counter',
      statusCode: 200,
      method: 'POST',
    } as any);

    await expect(resultP).rejects.toBeInstanceOf(PublishTimeoutError);
    expect(api.listenerCounts()).toEqual({ updated: 0, removed: 0, webreq: 0 });
  });

  // R4-T2 §2 negative regression: method !== 'POST' 一律忽略。XHS publish form
  // 期间发起的 GET autosave / draft / template / list 即便 URL 和 publish-result
  // 完全同前缀甚至同尾段，也必须不 settle（GET 永远不是 publish 提交动作）。
  it('webRequest API watch ignores non-POST methods even on publish endpoint (R4-T2)', async () => {
    const api = makeFakeTabsApi();
    const resultP = waitForPublishCompletion(113, {
      timeoutMs: 80,
      chromeApi: chromeApiOf(api),
    });

    // GET on the actual publish endpoint — 仍禁 settle（method gate 拦下）。
    api.fireWebReqCompleted({
      tabId: 113,
      url: 'https://creator.xiaohongshu.com/api/galaxy/note/publish',
      statusCode: 200,
      method: 'GET',
    } as any);
    // PUT 同上 —— 任何非 POST 都禁 settle。
    api.fireWebReqCompleted({
      tabId: 113,
      url: 'https://creator.xiaohongshu.com/api/galaxy/note/submit',
      statusCode: 200,
      method: 'PUT',
    } as any);
    // method 字段缺失 —— 视为未知方法，安全侧禁 settle。
    api.fireWebReqCompleted({
      tabId: 113,
      url: 'https://creator.xiaohongshu.com/api/galaxy/note/publish',
      statusCode: 200,
    } as any);

    await expect(resultP).rejects.toBeInstanceOf(PublishTimeoutError);
    expect(api.listenerCounts()).toEqual({ updated: 0, removed: 0, webreq: 0 });
  });

  // R3-T4 FX6 (spec §1 + codex#t59.1): 离开 publish form 但跳到 `/post/list`
  // 类创作者中心列表页 = 用户取消发布，必须 reject canceled，不能算 success。
  it('rejects with PublishCanceledError when tab navigates to /post/list (FX6)', async () => {
    const api = makeFakeTabsApi();
    const resultP = waitForPublishCompletion(120, {
      timeoutMs: 5_000,
      chromeApi: chromeApiOf(api),
    });

    api.fireUpdated(120, { url: 'https://creator.xiaohongshu.com/post/list' });

    await expect(resultP).rejects.toBeInstanceOf(PublishCanceledError);
    await expect(resultP).rejects.toMatchObject({ code: 'publish_canceled' });
    expect(api.listenerCounts()).toEqual({ updated: 0, removed: 0, webreq: 0 });
  });

  it('rejects with PublishCanceledError on any non-detail navigation away from publish', async () => {
    // 多个非 NOTE_DETAIL 目标都应当被识别为取消（防御 XHS 站内未来新增的跳转
    // 中转页绕过当前列表页）。
    const cases = [
      'https://creator.xiaohongshu.com/',
      'https://creator.xiaohongshu.com/login',
      'https://www.xiaohongshu.com/',
      'https://www.xiaohongshu.com/explore', // 探索首页（无 hex id）
    ];
    for (const url of cases) {
      const api = makeFakeTabsApi();
      const resultP = waitForPublishCompletion(121, {
        timeoutMs: 5_000,
        chromeApi: chromeApiOf(api),
      });
      api.fireUpdated(121, { url });
      await expect(resultP).rejects.toMatchObject({ code: 'publish_canceled' });
    }
  });

  it('ignores updates for unrelated tab ids', async () => {
    const api = makeFakeTabsApi();
    const resultP = waitForPublishCompletion(103, {
      timeoutMs: 200,
      chromeApi: chromeApiOf(api),
    });

    // Wrong tab id → must NOT settle.
    api.fireUpdated(999, { url: 'https://www.xiaohongshu.com/discovery/item/aaaaaaaaaaaaaaaaaaaaaaaa' });
    api.fireRemoved(999);

    await expect(resultP).rejects.toBeInstanceOf(PublishTimeoutError);
  });

  it('rejects with PublishCanceledError when the publish tab is closed', async () => {
    const api = makeFakeTabsApi();
    const resultP = waitForPublishCompletion(104, {
      timeoutMs: 5_000,
      chromeApi: chromeApiOf(api),
    });

    api.fireRemoved(104);

    await expect(resultP).rejects.toBeInstanceOf(PublishCanceledError);
    await expect(resultP).rejects.toMatchObject({ code: 'publish_canceled' });
    // Listeners cleaned up after reject too.
    expect(api.listenerCounts()).toEqual({ updated: 0, removed: 0, webreq: 0 });
  });

  it('rejects with PublishTimeoutError when no navigation happens before the deadline', async () => {
    const api = makeFakeTabsApi();
    const resultP = waitForPublishCompletion(105, {
      timeoutMs: 50,
      chromeApi: chromeApiOf(api),
    });

    await expect(resultP).rejects.toBeInstanceOf(PublishTimeoutError);
    await expect(resultP).rejects.toMatchObject({ code: 'publish_timeout' });
    expect(api.listenerCounts()).toEqual({ updated: 0, removed: 0, webreq: 0 });
  });

  it('uses changeInfo.url first, falls back to tab.url', async () => {
    const api = makeFakeTabsApi();
    const resultP = waitForPublishCompletion(106, {
      timeoutMs: 5_000,
      chromeApi: chromeApiOf(api),
    });

    const noteId = 'abcdef0123456789abcdef01';
    // changeInfo without url — fallback to tab.url
    api.fireUpdated(
      106,
      { status: 'complete' },
      { url: `https://www.xiaohongshu.com/explore/${noteId}` } as any
    );

    const completion = await resultP;
    expect(completion.note_id).toBe(noteId);
  });
});

describe('PUBLISH_PAGE_PATTERN', () => {
  it('matches the publish form URLs and rejects detail URLs', () => {
    expect(PUBLISH_PAGE_PATTERN.test('https://creator.xiaohongshu.com/publish/publish')).toBe(true);
    expect(PUBLISH_PAGE_PATTERN.test('https://creator.xiaohongshu.com/publish/publish?source=x')).toBe(
      true
    );
    expect(
      PUBLISH_PAGE_PATTERN.test('https://www.xiaohongshu.com/discovery/item/0123456789abcdef01234567')
    ).toBe(false);
  });
});

describe('NOTE_ID_URL_PATTERN', () => {
  it('extracts hex note ids from /discovery/item/ and /explore/ URLs', () => {
    const id1 = '0123456789abcdef01234567';
    expect('https://www.xiaohongshu.com/discovery/item/' + id1)
      .toMatch(NOTE_ID_URL_PATTERN);
    const id2 = 'abcdef0123456789abcdef01';
    expect('https://www.xiaohongshu.com/explore/' + id2 + '?source=publish').toMatch(
      NOTE_ID_URL_PATTERN
    );
  });

  it('NOTE_DETAIL_PATTERN is the same regex as NOTE_ID_URL_PATTERN', () => {
    // Alias kept stable so future call sites (recovery hook) can speak the
    // semantic name without forking the regex.
    expect(NOTE_DETAIL_PATTERN).toBe(NOTE_ID_URL_PATTERN);
  });
});

describe('PUBLISH_API_URL_PATTERN (R3-T4 FX6 → R4-T2 narrowed)', () => {
  it('matches creator + edith publish-result endpoints (publish | submit suffix)', () => {
    // 精确尾段 publish
    expect(PUBLISH_API_URL_PATTERN.test('https://creator.xiaohongshu.com/api/galaxy/note/publish')).toBe(true);
    expect(PUBLISH_API_URL_PATTERN.test('https://edith.xiaohongshu.com/api/galaxy/note/publish')).toBe(true);
    // 精确尾段 submit
    expect(PUBLISH_API_URL_PATTERN.test('https://creator.xiaohongshu.com/api/galaxy/note/submit')).toBe(true);
    expect(PUBLISH_API_URL_PATTERN.test('https://edith.xiaohongshu.com/api/galaxy/note/submit')).toBe(true);
    // 带 querystring / 子路径分隔符仍 match（边界守卫使用 [/?#]|$）
    expect(PUBLISH_API_URL_PATTERN.test('https://creator.xiaohongshu.com/api/galaxy/note/publish?ts=1')).toBe(true);
    expect(PUBLISH_API_URL_PATTERN.test('https://creator.xiaohongshu.com/api/galaxy/note/publish/done')).toBe(true);
    expect(PUBLISH_API_URL_PATTERN.test('https://creator.xiaohongshu.com/api/galaxy/note/publish#frag')).toBe(true);
  });

  it('does NOT match wrong hosts / wrong paths (R3-T4 FX6 baseline)', () => {
    expect(PUBLISH_API_URL_PATTERN.test('https://www.xiaohongshu.com/api/galaxy/note/publish')).toBe(false);
    expect(PUBLISH_API_URL_PATTERN.test('https://creator.xiaohongshu.com/api/other/path')).toBe(false);
    expect(PUBLISH_API_URL_PATTERN.test('https://attacker.com/api/galaxy/note/publish')).toBe(false);
  });

  // R4-T2 收紧的核心：同前缀 `/api/galaxy/note/<x>` 但 <x> 不是 publish/submit 必须 NOT match。
  // XHS publish form 期间会发起 autosave / draft / template fetch / counter polling 等同前缀
  // 200 GET，旧实现全部 settle resolve 引入 false-success；R4-T2 后这些必须被 regex 挡住。
  it('does NOT match same-prefix non-publish endpoints (R4-T2 false-positive guard)', () => {
    // 典型 publish form 期间 noise 端点
    expect(PUBLISH_API_URL_PATTERN.test('https://creator.xiaohongshu.com/api/galaxy/note/draft')).toBe(false);
    expect(PUBLISH_API_URL_PATTERN.test('https://creator.xiaohongshu.com/api/galaxy/note/draft/save')).toBe(false);
    expect(PUBLISH_API_URL_PATTERN.test('https://edith.xiaohongshu.com/api/galaxy/note/list')).toBe(false);
    expect(PUBLISH_API_URL_PATTERN.test('https://edith.xiaohongshu.com/api/galaxy/note/list?cursor=abc')).toBe(false);
    expect(PUBLISH_API_URL_PATTERN.test('https://creator.xiaohongshu.com/api/galaxy/note/template/123')).toBe(false);
    expect(PUBLISH_API_URL_PATTERN.test('https://edith.xiaohongshu.com/api/galaxy/note/counter')).toBe(false);
    expect(PUBLISH_API_URL_PATTERN.test('https://creator.xiaohongshu.com/api/galaxy/note/foo')).toBe(false);
    // 边界守卫：字母后缀延伸不可命中（防 `/publishabc` / `/submitter` 误匹配）
    expect(PUBLISH_API_URL_PATTERN.test('https://creator.xiaohongshu.com/api/galaxy/note/publishabc')).toBe(false);
    expect(PUBLISH_API_URL_PATTERN.test('https://creator.xiaohongshu.com/api/galaxy/note/submitter')).toBe(false);
  });

  it('exposes filter URL list with creator + edith match-patterns', () => {
    // chrome.webRequest manifest filter 不支持 regex；保持宽匹配做粗筛，
    // 真正的尾段精确校验由 onApiCompleted 内的 PUBLISH_API_URL_PATTERN 二次执行。
    expect(PUBLISH_API_FILTER_URLS).toEqual([
      '*://creator.xiaohongshu.com/api/galaxy/note/*',
      '*://edith.xiaohongshu.com/api/galaxy/note/*',
    ]);
  });
});

// ─── R3-T4 FX9：publish-wait 持久化 + SW evict 恢复 ────────────────────────────

describe('publish-wait persistence (R3-T4 FX9)', () => {
  it('writePublishWaitState writes under publish_wait:<correlationId>', async () => {
    const storage = makeFakeStorage();
    const state: PublishWaitState = {
      correlationId: 'corr-A',
      tabId: 42,
      startedAt: 1700_000_000_000,
      timeoutMs: 600_000,
    };
    await writePublishWaitState(state, storage);
    expect(storage._data[`${PUBLISH_WAIT_KEY_PREFIX}corr-A`]).toEqual(state);
  });

  it('listPublishWaitStates returns only well-shaped publish_wait:* entries', async () => {
    const storage = makeFakeStorage();
    storage._data[`${PUBLISH_WAIT_KEY_PREFIX}corr-1`] = {
      correlationId: 'corr-1',
      tabId: 1,
      startedAt: 1,
      timeoutMs: 1,
    };
    storage._data[`${PUBLISH_WAIT_KEY_PREFIX}corr-2`] = {
      correlationId: 'corr-2',
      tabId: 2,
      startedAt: 2,
      timeoutMs: 2,
    };
    // Mismatched / malformed entries must be ignored.
    storage._data['unrelated_key'] = { foo: 'bar' };
    storage._data[`${PUBLISH_WAIT_KEY_PREFIX}bad`] = { correlationId: 'bad', tabId: 'NaN' };

    const states = await listPublishWaitStates(storage);
    expect(states.map((s) => s.correlationId).sort()).toEqual(['corr-1', 'corr-2']);
  });

  it('removePublishWaitState removes the entry; idempotent', async () => {
    const storage = makeFakeStorage();
    storage._data[`${PUBLISH_WAIT_KEY_PREFIX}corr-X`] = {
      correlationId: 'corr-X',
      tabId: 1,
      startedAt: 1,
      timeoutMs: 1,
    };
    await removePublishWaitState('corr-X', storage);
    expect(storage._data[`${PUBLISH_WAIT_KEY_PREFIX}corr-X`]).toBeUndefined();
    // second call is a no-op
    await removePublishWaitState('corr-X', storage);
  });

  it('waitForPublishCompletion writes state on entry and removes on success exit', async () => {
    const api = makeFakeTabsApi();
    const storage = makeFakeStorage();
    const correlationId = 'corr-entry-exit-1';
    const startedAt = 1_700_000_000_000;
    const resultP = waitForPublishCompletion(200, {
      timeoutMs: 5_000,
      chromeApi: chromeApiOf(api),
      correlationId,
      startedAt,
      storageLocal: storage,
    });

    // After Promise body runs the persist write is enqueued; flush microtasks.
    await Promise.resolve();
    await Promise.resolve();
    expect(storage._data[`${PUBLISH_WAIT_KEY_PREFIX}${correlationId}`]).toEqual({
      correlationId,
      tabId: 200,
      startedAt,
      timeoutMs: 5_000,
    });

    // Drive a successful completion via NOTE_DETAIL navigation.
    const noteId = '0123456789abcdef01234567';
    api.fireUpdated(200, { url: `https://www.xiaohongshu.com/discovery/item/${noteId}` });
    await resultP;

    // Storage entry must be removed on settle.
    await Promise.resolve();
    await Promise.resolve();
    expect(storage._data[`${PUBLISH_WAIT_KEY_PREFIX}${correlationId}`]).toBeUndefined();
  });

  it('waitForPublishCompletion removes state on cancel + timeout exits', async () => {
    // canceled
    const api1 = makeFakeTabsApi();
    const storage1 = makeFakeStorage();
    const p1 = waitForPublishCompletion(201, {
      timeoutMs: 5_000,
      chromeApi: chromeApiOf(api1),
      correlationId: 'corr-cancel',
      storageLocal: storage1,
    });
    await Promise.resolve();
    expect(storage1._data[`${PUBLISH_WAIT_KEY_PREFIX}corr-cancel`]).toBeDefined();
    api1.fireRemoved(201);
    await expect(p1).rejects.toBeInstanceOf(PublishCanceledError);
    await Promise.resolve();
    expect(storage1._data[`${PUBLISH_WAIT_KEY_PREFIX}corr-cancel`]).toBeUndefined();

    // timeout
    const api2 = makeFakeTabsApi();
    const storage2 = makeFakeStorage();
    const p2 = waitForPublishCompletion(202, {
      timeoutMs: 30,
      chromeApi: chromeApiOf(api2),
      correlationId: 'corr-timeout',
      storageLocal: storage2,
    });
    await Promise.resolve();
    expect(storage2._data[`${PUBLISH_WAIT_KEY_PREFIX}corr-timeout`]).toBeDefined();
    await expect(p2).rejects.toBeInstanceOf(PublishTimeoutError);
    await Promise.resolve();
    expect(storage2._data[`${PUBLISH_WAIT_KEY_PREFIX}corr-timeout`]).toBeUndefined();
  });
});

describe('recoverPublishWaitStates (R3-T4 FX9 SW evict recovery)', () => {
  it('returns empty when no publish_wait state present', async () => {
    const storage = makeFakeStorage();
    const calls: any[] = [];
    const summaries = await recoverPublishWaitStates({
      storageLocal: storage,
      tabsGet: async () => null,
      postCallback: async (...args) => {
        calls.push(args);
      },
      now: () => 1_700_000_000_000,
    });
    expect(summaries).toEqual([]);
    expect(calls.length).toBe(0);
  });

  it('emits publish_timeout when state has aged past startedAt + timeoutMs', async () => {
    const storage = makeFakeStorage();
    const state: PublishWaitState = {
      correlationId: 'corr-old',
      tabId: 300,
      startedAt: 1_000,
      timeoutMs: 60_000,
    };
    storage._data[`${PUBLISH_WAIT_KEY_PREFIX}corr-old`] = state;

    const callbacks: any[] = [];
    const summaries = await recoverPublishWaitStates({
      storageLocal: storage,
      tabsGet: async () => ({ id: 300, url: 'https://creator.xiaohongshu.com/publish/publish' } as any),
      postCallback: async (correlationId, payload) => {
        callbacks.push({ correlationId, payload });
      },
      now: () => 999_999_999, // way past 1_000 + 60_000
    });
    expect(summaries).toEqual([{ correlationId: 'corr-old', outcome: 'timeout' }]);
    expect(callbacks).toEqual([
      {
        correlationId: 'corr-old',
        payload: {
          status: 'error',
          result: null,
          error: { code: 'publish_timeout', message: expect.stringContaining('publish wait timed out') },
        },
      },
    ]);
    expect(storage._data[`${PUBLISH_WAIT_KEY_PREFIX}corr-old`]).toBeUndefined();
  });

  it('emits publish_canceled when chrome.tabs.get returns null (tab gone)', async () => {
    const storage = makeFakeStorage();
    storage._data[`${PUBLISH_WAIT_KEY_PREFIX}corr-gone`] = {
      correlationId: 'corr-gone',
      tabId: 301,
      startedAt: 1_000_000_000_000,
      timeoutMs: 600_000,
    };
    const callbacks: any[] = [];
    const now = 1_000_000_010_000; // within deadline
    const summaries = await recoverPublishWaitStates({
      storageLocal: storage,
      tabsGet: async () => null,
      postCallback: async (correlationId, payload) => {
        callbacks.push({ correlationId, payload });
      },
      now: () => now,
    });
    expect(summaries).toEqual([{ correlationId: 'corr-gone', outcome: 'canceled_tab_missing' }]);
    expect(callbacks[0].payload.status).toBe('error');
    expect(callbacks[0].payload.error.code).toBe('publish_canceled');
    expect(storage._data[`${PUBLISH_WAIT_KEY_PREFIX}corr-gone`]).toBeUndefined();
  });

  it('emits ok with note_id when tab landed on NOTE_DETAIL_PATTERN', async () => {
    const noteId = 'abcdef0123456789abcdef01';
    const storage = makeFakeStorage();
    storage._data[`${PUBLISH_WAIT_KEY_PREFIX}corr-detail`] = {
      correlationId: 'corr-detail',
      tabId: 302,
      startedAt: 1_000_000_000_000,
      timeoutMs: 600_000,
    };
    const callbacks: any[] = [];
    const summaries = await recoverPublishWaitStates({
      storageLocal: storage,
      tabsGet: async () => ({
        id: 302,
        url: `https://www.xiaohongshu.com/discovery/item/${noteId}?from=publish`,
      } as any),
      postCallback: async (correlationId, payload) => {
        callbacks.push({ correlationId, payload });
      },
      now: () => 1_000_000_010_000,
    });
    expect(summaries).toEqual([{ correlationId: 'corr-detail', outcome: 'completed_detail' }]);
    expect(callbacks[0].payload.status).toBe('ok');
    expect(callbacks[0].payload.result.note_id).toBe(noteId);
    expect(callbacks[0].payload.result.url).toContain(`/discovery/item/${noteId}`);
    expect(storage._data[`${PUBLISH_WAIT_KEY_PREFIX}corr-detail`]).toBeUndefined();
  });

  it('emits publish_canceled when tab navigated away to non-detail page', async () => {
    const storage = makeFakeStorage();
    storage._data[`${PUBLISH_WAIT_KEY_PREFIX}corr-nav-away`] = {
      correlationId: 'corr-nav-away',
      tabId: 303,
      startedAt: 1_000_000_000_000,
      timeoutMs: 600_000,
    };
    const callbacks: any[] = [];
    const summaries = await recoverPublishWaitStates({
      storageLocal: storage,
      tabsGet: async () => ({ id: 303, url: 'https://creator.xiaohongshu.com/post/list' } as any),
      postCallback: async (correlationId, payload) => {
        callbacks.push({ correlationId, payload });
      },
      now: () => 1_000_000_010_000,
    });
    expect(summaries).toEqual([{ correlationId: 'corr-nav-away', outcome: 'canceled_navigated_away' }]);
    expect(callbacks[0].payload.status).toBe('error');
    expect(callbacks[0].payload.error.code).toBe('publish_canceled');
    expect(callbacks[0].payload.error.message).toContain('/post/list');
    expect(storage._data[`${PUBLISH_WAIT_KEY_PREFIX}corr-nav-away`]).toBeUndefined();
  });

  it('re-arms listener when tab still on PUBLISH_PAGE; success path posts callback + cleans storage', async () => {
    const storage = makeFakeStorage();
    storage._data[`${PUBLISH_WAIT_KEY_PREFIX}corr-rearm`] = {
      correlationId: 'corr-rearm',
      tabId: 304,
      startedAt: 1_000_000_000_000,
      timeoutMs: 600_000,
    };
    const api = makeFakeTabsApi();
    const callbacks: any[] = [];

    const summaries = await recoverPublishWaitStates({
      storageLocal: storage,
      tabsGet: async () => ({
        id: 304,
        url: 'https://creator.xiaohongshu.com/publish/publish',
      } as any),
      postCallback: async (correlationId, payload) => {
        callbacks.push({ correlationId, payload });
      },
      now: () => 1_000_000_010_000,
      chromeApi: chromeApiOf(api),
    });

    // Recovery scan summary: rearmed (callback comes async).
    expect(summaries).toEqual([{ correlationId: 'corr-rearm', outcome: 'rearmed_listener' }]);
    expect(callbacks.length).toBe(0);

    // Listener should have been registered on all 3 surfaces.
    expect(api.listenerCounts()).toMatchObject({ updated: 1, removed: 1, webreq: 1 });

    // Drive a successful completion: webRequest 200 settles. Then onApiCompleted
    // → resolve → postCallback fires asynchronously.
    api.fireWebReqCompleted({
      tabId: 304,
      url: 'https://creator.xiaohongshu.com/api/galaxy/note/publish',
      statusCode: 200,
      method: 'POST',
    } as any);

    // Drain microtasks for the async chain (.then(postCallback) inside recovery).
    for (let i = 0; i < 20 && callbacks.length === 0; i += 1) await Promise.resolve();

    expect(callbacks.length).toBe(1);
    expect(callbacks[0].payload.status).toBe('ok');
    expect(callbacks[0].correlationId).toBe('corr-rearm');
    // Storage cleaned up by waitForPublishCompletion settle path.
    await Promise.resolve();
    expect(storage._data[`${PUBLISH_WAIT_KEY_PREFIX}corr-rearm`]).toBeUndefined();
  });

  it('processes multiple states independently', async () => {
    const storage = makeFakeStorage();
    storage._data[`${PUBLISH_WAIT_KEY_PREFIX}corr-1`] = {
      correlationId: 'corr-1',
      tabId: 401,
      startedAt: 1_000,
      timeoutMs: 60_000,
    };
    storage._data[`${PUBLISH_WAIT_KEY_PREFIX}corr-2`] = {
      correlationId: 'corr-2',
      tabId: 402,
      startedAt: 1_000_000_000_000,
      timeoutMs: 600_000,
    };
    const callbacks: any[] = [];
    const summaries = await recoverPublishWaitStates({
      storageLocal: storage,
      tabsGet: async (id) => {
        if (id === 401) return null;
        if (id === 402) {
          return {
            id: 402,
            url: 'https://www.xiaohongshu.com/discovery/item/0123456789abcdef01234567',
          } as any;
        }
        return null;
      },
      postCallback: async (correlationId, payload) => {
        callbacks.push({ correlationId, payload });
      },
      now: () => 1_000_000_010_000, // corr-1 aged out, corr-2 fresh
    });
    // corr-1 aged out → timeout; corr-2 → completed via detail.
    const byId = new Map(summaries.map((s) => [s.correlationId, s.outcome]));
    expect(byId.get('corr-1')).toBe('timeout');
    expect(byId.get('corr-2')).toBe('completed_detail');
    expect(callbacks.length).toBe(2);
    const cbBy = new Map(callbacks.map((c) => [c.correlationId, c.payload]));
    expect(cbBy.get('corr-1').error.code).toBe('publish_timeout');
    expect(cbBy.get('corr-2').status).toBe('ok');
  });
});
