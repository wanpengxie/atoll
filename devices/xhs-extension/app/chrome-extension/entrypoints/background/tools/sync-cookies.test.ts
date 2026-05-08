// Vitest unit tests for sync-cookies.ts CookieSyncService
// (M1.1 Fix-T4 §2). Verifies the bind(this) leak is fixed:
//   - start()/stop() use the SAME function reference for chrome.tabs.onUpdated
//     and chrome.cookies.onChanged add/removeListener calls.
//   - Repeated start/stop cycles never accumulate listeners (count ≤ 1).
//   - Duplicate start() is a no-op (idempotent).

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

vi.mock('coagent-xhs-shared', () => ({
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

// R3-T4 FX7 / round-2 review codex#t59.2 — SyncCookiesTool.execute() must NOT
// concatenate the daemon's response body verbatim into the user-visible message
// (previous implementation `device session 已上报: ${bodyText}` leaked cookie
// names + values). After the daemon-side redaction (channel-manager.js
// deviceSessionUpdate), the response is `{ok:true, result:{user_id, login_state,
// cookie_count, last_updated_at, expires_at}}` and the extension renders only
// the cookie count.
describe('SyncCookiesTool.execute response handling (R3-T4 FX7)', () => {
  // Reusable helper: mock chrome.cookies.getAll to return a small fixture of
  // sensitive cookies, mock connection-state to provide complete daemon config,
  // and stub fetch with the supplied response.
  async function setupExecute(opts: {
    fetchResponse: { ok: boolean; status: number; jsonBody?: any };
  }) {
    vi.resetModules();
    // Stub global fetch *before* import so the module captures the spy.
    const fetchSpy = vi.fn(async () => {
      const { fetchResponse } = opts;
      const body = fetchResponse.jsonBody ?? null;
      return {
        ok: fetchResponse.ok,
        status: fetchResponse.status,
        async text() {
          return body ? JSON.stringify(body) : '';
        },
        async json() {
          if (body == null) throw new Error('no json body');
          return body;
        },
      } as unknown as Response;
    });
    (globalThis as any).fetch = fetchSpy;

    // Override connection-state mock so syncToBackend has complete daemon config.
    vi.doMock('../connection-state', () => ({
      getConnectionConfig: vi.fn(async () => ({
        serverUrl: 'ws://127.0.0.1:9501/device/dev-1?key=k',
        apiKey: 'device-key',
        deviceId: 'dev-1',
        userId: 'user-001',
        daemonHttpBase: 'http://127.0.0.1:9501',
      })),
    }));

    // chrome.cookies.getAll returns sensitive cookies — these would have been
    // forwarded to daemon and (pre-fix) round-tripped back into message.
    (globalThis as any).chrome = {
      tabs: { onUpdated: tabsOnUpdated },
      cookies: {
        onChanged: cookiesOnChanged,
        getAll: vi.fn(async ({ domain }: { domain: string }) => {
          if (domain !== '.xiaohongshu.com') return [];
          return [
            { name: 'web_session', value: 'SECRET_SESSION_VALUE', domain, path: '/', secure: true, httpOnly: true, sameSite: 'lax' },
            { name: 'access-token', value: 'SECRET_TOKEN_VALUE', domain, path: '/', secure: true, httpOnly: false, sameSite: 'lax' },
          ];
        }),
      },
    };

    const { SyncCookiesTool } = await import('./sync-cookies');
    return { SyncCookiesTool, fetchSpy };
  }

  it('renders only cookie_count in success message, never raw cookie names/values', async () => {
    const { SyncCookiesTool } = await setupExecute({
      fetchResponse: {
        ok: true,
        status: 200,
        jsonBody: {
          ok: true,
          result: {
            user_id: 'user-001',
            login_state: 'logged_in',
            cookie_count: 2,
            last_updated_at: 1700000000000,
            expires_at: null,
          },
        },
      },
    });

    const tool = new SyncCookiesTool();
    const result = await tool.execute();
    expect(result.isError).toBe(false);
    const text = result.content[0]?.text ?? '';
    const parsed = JSON.parse(text);
    expect(parsed.success).toBe(true);
    expect(parsed.message).toBe('device session 已上报 (2 cookies)');
    // Defense-in-depth: even the full serialized result must not leak raw cookie
    // values (cookieCount field on the wrapper is fine — it's an integer).
    const serialized = JSON.stringify(result);
    expect(serialized).not.toContain('SECRET_SESSION_VALUE');
    expect(serialized).not.toContain('SECRET_TOKEN_VALUE');
    expect(parsed.message).not.toMatch(/web_session/);
    expect(parsed.message).not.toMatch(/access-token/);
  });

  it('falls back to plain success message when daemon response has no cookie_count', async () => {
    const { SyncCookiesTool } = await setupExecute({
      fetchResponse: {
        ok: true,
        status: 200,
        jsonBody: { ok: true, result: { user_id: 'user-001', login_state: 'logged_in' } },
      },
    });

    const tool = new SyncCookiesTool();
    const result = await tool.execute();
    const parsed = JSON.parse(result.content[0]?.text ?? '');
    expect(parsed.success).toBe(true);
    expect(parsed.message).toBe('device session 已上报');
  });

  it('renders structured error message on daemon 4xx without leaking raw body', async () => {
    const { SyncCookiesTool } = await setupExecute({
      fetchResponse: {
        ok: false,
        status: 401,
        jsonBody: {
          ok: false,
          error: { code: 'unauthorized', message: 'invalid device api key' },
        },
      },
    });

    const tool = new SyncCookiesTool();
    const result = await tool.execute();
    const parsed = JSON.parse(result.content[0]?.text ?? '');
    expect(parsed.success).toBe(false);
    expect(parsed.message).toContain('401');
    expect(parsed.message).toContain('invalid device api key');
    // Must not blanket-include raw response body — only structured fields.
    expect(parsed.message).not.toMatch(/SECRET_/);
  });

  it('regression: even if a misbehaving daemon echoes cookies on success, message must NOT include them', async () => {
    // Defensive check: if a future daemon regression starts including a raw
    // `cookies` array on success, the extension must still produce a message
    // that ignores that field. We only consume `result.cookie_count`.
    const { SyncCookiesTool } = await setupExecute({
      fetchResponse: {
        ok: true,
        status: 200,
        jsonBody: {
          ok: true,
          result: {
            user_id: 'user-001',
            login_state: 'logged_in',
            cookie_count: 2,
            // Hypothetical leakage source — must not surface in message.
            cookies: [
              { name: 'web_session', value: 'LEAKED' },
              { name: 'access-token', value: 'ALSO_LEAKED' },
            ],
          },
        },
      },
    });

    const tool = new SyncCookiesTool();
    const result = await tool.execute();
    const parsed = JSON.parse(result.content[0]?.text ?? '');
    expect(parsed.message).toBe('device session 已上报 (2 cookies)');
    expect(parsed.message).not.toContain('LEAKED');
    expect(parsed.message).not.toContain('ALSO_LEAKED');
    expect(parsed.message).not.toContain('web_session');
  });
});
