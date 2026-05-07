// Vitest unit tests for sync-cookies.ts CookieSyncService
// (M1.1 Fix-T4 §2). Verifies the bind(this) leak is fixed:
//   - start()/stop() use the SAME function reference for chrome.tabs.onUpdated
//     and chrome.cookies.onChanged add/removeListener calls.
//   - Repeated start/stop cycles never accumulate listeners (count ≤ 1).
//   - Duplicate start() is a no-op (idempotent).

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

vi.mock('xiaohongshu-mcp-shared', () => ({
  XIAOHONGSHU_TOOL_NAMES: {
    SYNC_COOKIES: 'xhs_sync_cookies',
  },
  COAGENT_DEVICE_PROTOCOL: {
    CALLBACK_PATH_PREFIX: '/api/device/',
    CALLBACK_PATH_SUFFIX_SESSION: '/session',
  },
}));

// Stub BaseTool — sync-cookies extends it but we only exercise the service.
vi.mock('./base-tool', () => ({
  BaseTool: class {},
}));

// Stub connection-state — getConnectionConfig is called inside SyncCookiesTool
// during execute(), but we don't trigger execute() in these tests.
vi.mock('../connection-state', () => ({
  getConnectionConfig: vi.fn(async () => ({})),
}));

// Provide a minimal coagent-device shim so the import resolves.
vi.mock('../services/coagent-device', () => ({
  deriveHttpBaseFromWsUrl: vi.fn(() => 'http://127.0.0.1:9501'),
}));

interface FakeListenerSet<F extends (...args: any[]) => any> {
  listeners: Set<F>;
  addListener: (fn: F) => void;
  removeListener: (fn: F) => void;
}

function makeListenerSet<F extends (...args: any[]) => any>(): FakeListenerSet<F> {
  const listeners = new Set<F>();
  return {
    listeners,
    addListener: (fn: F) => {
      listeners.add(fn);
    },
    removeListener: (fn: F) => {
      listeners.delete(fn);
    },
  };
}

let tabsOnUpdated: FakeListenerSet<any>;
let cookiesOnChanged: FakeListenerSet<any>;

beforeEach(() => {
  tabsOnUpdated = makeListenerSet();
  cookiesOnChanged = makeListenerSet();
  (globalThis as any).chrome = {
    tabs: { onUpdated: tabsOnUpdated },
    cookies: {
      onChanged: cookiesOnChanged,
      // sync-cookies' SyncCookiesTool reads chrome.cookies.getAll inside execute,
      // but these tests don't call execute(); a stub keeps TS happy if invoked.
      getAll: vi.fn(async () => []),
    },
  };
});

afterEach(() => {
  delete (globalThis as any).chrome;
});

describe('CookieSyncService listener identity (M1.1 Fix-T4 §2)', () => {
  it('start() adds exactly one tabs + one cookies listener', async () => {
    const { CookieSyncService } = await import('./sync-cookies');
    const svc = new CookieSyncService();
    svc.start();
    expect(tabsOnUpdated.listeners.size).toBe(1);
    expect(cookiesOnChanged.listeners.size).toBe(1);
  });

  it('stop() actually removes the listeners (bind reference parity)', async () => {
    vi.resetModules();
    const { CookieSyncService } = await import('./sync-cookies');
    const svc = new CookieSyncService();
    svc.start();
    svc.stop();
    expect(tabsOnUpdated.listeners.size).toBe(0);
    expect(cookiesOnChanged.listeners.size).toBe(0);
  });

  it('repeated start/stop cycles never accumulate listeners (count ≤ 1)', async () => {
    vi.resetModules();
    const { CookieSyncService } = await import('./sync-cookies');
    const svc = new CookieSyncService();
    for (let i = 0; i < 5; i += 1) {
      svc.start();
      expect(tabsOnUpdated.listeners.size).toBe(1);
      expect(cookiesOnChanged.listeners.size).toBe(1);
      svc.stop();
      expect(tabsOnUpdated.listeners.size).toBe(0);
      expect(cookiesOnChanged.listeners.size).toBe(0);
    }
  });

  it('start() called twice without stop is idempotent (still 1 listener each)', async () => {
    vi.resetModules();
    const { CookieSyncService } = await import('./sync-cookies');
    const svc = new CookieSyncService();
    svc.start();
    svc.start();
    expect(tabsOnUpdated.listeners.size).toBe(1);
    expect(cookiesOnChanged.listeners.size).toBe(1);
  });

  it('add and remove use the SAME function reference (regression for bind leak)', async () => {
    vi.resetModules();
    const addSpy = vi.spyOn(tabsOnUpdated, 'addListener');
    const removeSpy = vi.spyOn(tabsOnUpdated, 'removeListener');
    const { CookieSyncService } = await import('./sync-cookies');
    const svc = new CookieSyncService();
    svc.start();
    svc.stop();
    expect(addSpy).toHaveBeenCalledTimes(1);
    expect(removeSpy).toHaveBeenCalledTimes(1);
    expect(addSpy.mock.calls[0][0]).toBe(removeSpy.mock.calls[0][0]);
  });
});
