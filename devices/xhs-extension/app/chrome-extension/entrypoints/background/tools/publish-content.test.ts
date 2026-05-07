// Vitest unit tests for publish-content.ts `waitForPublishCompletion`
// (M1.1 Fix-T4 §1 P0 critical). Covers three exit paths:
//   1. URL navigates to a note-detail page          → resolve {url, note_id}
//   2. Publish tab is closed by the user            → reject `publish_canceled`
//   3. 10-min wait elapses with no navigation       → reject `publish_timeout`
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
} from './publish-content';

// Minimal fake of `chrome.tabs.onUpdated` / `chrome.tabs.onRemoved` that lets
// us drive event firing from inside each test.
type TabUpdatedListener = (
  tabId: number,
  changeInfo: chrome.tabs.TabChangeInfo,
  tab?: chrome.tabs.Tab
) => void;
type TabRemovedListener = (tabId: number, info?: chrome.tabs.TabRemoveInfo) => void;

function makeFakeTabsApi() {
  const updated = new Set<TabUpdatedListener>();
  const removed = new Set<TabRemovedListener>();
  return {
    tabsOnUpdated: {
      addListener: (fn: TabUpdatedListener) => updated.add(fn),
      removeListener: (fn: TabUpdatedListener) => updated.delete(fn),
    },
    tabsOnRemoved: {
      addListener: (fn: TabRemovedListener) => removed.add(fn),
      removeListener: (fn: TabRemovedListener) => removed.delete(fn),
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
    listenerCounts: () => ({ updated: updated.size, removed: removed.size }),
  };
}

beforeEach(() => {
  vi.useRealTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

describe('waitForPublishCompletion', () => {
  it('resolves with {url, note_id} when tab navigates to a note-detail URL', async () => {
    const api = makeFakeTabsApi();
    const resultP = waitForPublishCompletion(101, {
      timeoutMs: 5_000,
      chromeApi: { tabsOnUpdated: api.tabsOnUpdated, tabsOnRemoved: api.tabsOnRemoved },
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
    expect(api.listenerCounts()).toEqual({ updated: 0, removed: 0 });
  });

  it('resolves with note_id=null when target URL is a creator page (no detail id)', async () => {
    const api = makeFakeTabsApi();
    const resultP = waitForPublishCompletion(102, {
      timeoutMs: 5_000,
      chromeApi: { tabsOnUpdated: api.tabsOnUpdated, tabsOnRemoved: api.tabsOnRemoved },
    });

    api.fireUpdated(102, { url: 'https://creator.xiaohongshu.com/post/list' });

    const completion = await resultP;
    expect(completion.note_id).toBeNull();
    expect(completion.url).toBe('https://creator.xiaohongshu.com/post/list');
  });

  it('ignores updates for unrelated tab ids', async () => {
    const api = makeFakeTabsApi();
    const resultP = waitForPublishCompletion(103, {
      timeoutMs: 200,
      chromeApi: { tabsOnUpdated: api.tabsOnUpdated, tabsOnRemoved: api.tabsOnRemoved },
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
      chromeApi: { tabsOnUpdated: api.tabsOnUpdated, tabsOnRemoved: api.tabsOnRemoved },
    });

    api.fireRemoved(104);

    await expect(resultP).rejects.toBeInstanceOf(PublishCanceledError);
    await expect(resultP).rejects.toMatchObject({ code: 'publish_canceled' });
    // Listeners cleaned up after reject too.
    expect(api.listenerCounts()).toEqual({ updated: 0, removed: 0 });
  });

  it('rejects with PublishTimeoutError when no navigation happens before the deadline', async () => {
    const api = makeFakeTabsApi();
    const resultP = waitForPublishCompletion(105, {
      timeoutMs: 50,
      chromeApi: { tabsOnUpdated: api.tabsOnUpdated, tabsOnRemoved: api.tabsOnRemoved },
    });

    await expect(resultP).rejects.toBeInstanceOf(PublishTimeoutError);
    await expect(resultP).rejects.toMatchObject({ code: 'publish_timeout' });
    expect(api.listenerCounts()).toEqual({ updated: 0, removed: 0 });
  });

  it('uses changeInfo.url first, falls back to tab.url', async () => {
    const api = makeFakeTabsApi();
    const resultP = waitForPublishCompletion(106, {
      timeoutMs: 5_000,
      chromeApi: { tabsOnUpdated: api.tabsOnUpdated, tabsOnRemoved: api.tabsOnRemoved },
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
});
