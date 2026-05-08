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

vi.mock('xiaohongshu-mcp-shared', () => ({
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
} from './publish-content';

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

describe('PUBLISH_API_URL_PATTERN (R3-T4 FX6)', () => {
  it('matches creator + edith publish API hosts', () => {
    expect(PUBLISH_API_URL_PATTERN.test('https://creator.xiaohongshu.com/api/galaxy/note/publish')).toBe(true);
    expect(PUBLISH_API_URL_PATTERN.test('https://edith.xiaohongshu.com/api/galaxy/note/foo')).toBe(true);
  });
  it('does NOT match other xhs paths or hosts', () => {
    expect(PUBLISH_API_URL_PATTERN.test('https://www.xiaohongshu.com/api/galaxy/note/publish')).toBe(false);
    expect(PUBLISH_API_URL_PATTERN.test('https://creator.xiaohongshu.com/api/other/path')).toBe(false);
    expect(PUBLISH_API_URL_PATTERN.test('https://attacker.com/api/galaxy/note/publish')).toBe(false);
  });
  it('exposes filter URL list with creator + edith match-patterns', () => {
    expect(PUBLISH_API_FILTER_URLS).toEqual([
      '*://creator.xiaohongshu.com/api/galaxy/note/*',
      '*://edith.xiaohongshu.com/api/galaxy/note/*',
    ]);
  });
});
