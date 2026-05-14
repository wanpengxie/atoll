// Vitest unit tests for services/resolve.ts (M1.2-T3).
//
// Covers: input validation / 2xx happy path / 4xx (400/401/404/429) / 5xx /
// network failure / abort timeout / malformed JSON / missing fields.

import { describe, it, expect, vi } from 'vitest';

vi.mock('coagent-xhs-shared', () => ({
  TIMEOUTS: {
    NETWORK_REQUEST: 30_000,
  },
}));

import { resolveDeviceConfig } from './resolve';

function fakeResponse(status: number, body: unknown = {}, opts: { malformedJson?: boolean } = {}): Response {
  const text = typeof body === 'string' ? body : JSON.stringify(body);
  return {
    ok: status >= 200 && status < 300,
    status,
    json: vi.fn(async () => {
      if (opts.malformedJson) throw new Error('not json');
      return typeof body === 'string' ? JSON.parse(body) : body;
    }),
    text: vi.fn(async () => text),
    clone() {
      return this;
    },
  } as unknown as Response;
}

describe('resolveDeviceConfig — input validation', () => {
  it('rejects empty serverUrl', async () => {
    const r = await resolveDeviceConfig({
      serverUrl: '',
      apiKey: 'k',
      fetchImpl: vi.fn(),
    });
    expect(r.ok).toBe(false);
    if (!r.ok) {
      expect(r.error.kind).toBe('invalid_input');
      expect(r.error.message).toContain('Server URL');
    }
  });

  it('rejects empty apiKey', async () => {
    const r = await resolveDeviceConfig({
      serverUrl: 'https://example.com',
      apiKey: '',
      fetchImpl: vi.fn(),
    });
    expect(r.ok).toBe(false);
    if (!r.ok) {
      expect(r.error.kind).toBe('invalid_input');
      expect(r.error.message).toContain('api-key');
    }
  });

  it('rejects malformed serverUrl', async () => {
    const r = await resolveDeviceConfig({
      serverUrl: 'not-a-url',
      apiKey: 'k',
      fetchImpl: vi.fn(),
    });
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.error.kind).toBe('invalid_input');
  });

  it('rejects non-http(s) serverUrl', async () => {
    const r = await resolveDeviceConfig({
      serverUrl: 'ws://example.com',
      apiKey: 'k',
      fetchImpl: vi.fn(),
    });
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.error.message).toContain('http');
  });
});

describe('resolveDeviceConfig — happy path', () => {
  it('returns ResolveSuccess on 200 with all fields', async () => {
    const fetchMock: typeof fetch = vi.fn(async () =>
      fakeResponse(200, {
        ws_url: 'ws://10.0.0.1:9501',
        http_url: 'http://10.0.0.1:9501',
        device_id: 'dev-A',
        user_id: 'user-001',
        channel_id: 'ch-1',
        daemon_id: 'daemon-1',
      }),
    );
    const r = await resolveDeviceConfig({
      serverUrl: 'https://coagent.example.com',
      apiKey: 'sk_dev_xxx',
      fetchImpl: fetchMock,
    });
    expect(r.ok).toBe(true);
    if (r.ok) {
      expect(r.data.ws_url).toBe('ws://10.0.0.1:9501');
      expect(r.data.device_id).toBe('dev-A');
      expect(r.data.daemon_id).toBe('daemon-1');
    }

    // POST body & path correct.
    const calls = (fetchMock as unknown as { mock: { calls: any[][] } }).mock.calls;
    expect(calls.length).toBe(1);
    const [url, init] = calls[0] as [string, RequestInit];
    expect(url).toBe('https://coagent.example.com/api/device/resolve');
    expect(init.method).toBe('POST');
    expect(JSON.parse(String(init.body))).toEqual({ api_key: 'sk_dev_xxx' });
  });

  it('strips trailing slash on serverUrl', async () => {
    const fetchMock: typeof fetch = vi.fn(async () =>
      fakeResponse(200, {
        ws_url: 'ws://x',
        http_url: 'http://x',
        device_id: 'd',
        user_id: 'u',
        channel_id: 'c',
        daemon_id: 'dm',
      }),
    );
    await resolveDeviceConfig({
      serverUrl: 'https://coagent.example.com/',
      apiKey: 'k',
      fetchImpl: fetchMock,
    });
    const calls = (fetchMock as unknown as { mock: { calls: any[][] } }).mock.calls;
    const [url] = calls[0] as [string];
    expect(url).toBe('https://coagent.example.com/api/device/resolve');
  });
});

describe('resolveDeviceConfig — error mapping', () => {
  it('maps 400 to bad_request', async () => {
    const fetchMock = vi.fn(async () => fakeResponse(400, { error: 'api_key required' }));
    const r = await resolveDeviceConfig({
      serverUrl: 'https://x',
      apiKey: 'k',
      fetchImpl: fetchMock,
    });
    expect(r.ok).toBe(false);
    if (!r.ok) {
      expect(r.error.kind).toBe('bad_request');
      expect(r.error.status).toBe(400);
      expect(r.error.message).toBe('api_key required');
    }
  });

  it('maps 401 to unauthorized', async () => {
    const fetchMock = vi.fn(async () => fakeResponse(401, { error: 'invalid' }));
    const r = await resolveDeviceConfig({
      serverUrl: 'https://x',
      apiKey: 'k',
      fetchImpl: fetchMock,
    });
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.error.kind).toBe('unauthorized');
  });

  it('maps 404 to not_found with friendly message', async () => {
    const fetchMock = vi.fn(async () => fakeResponse(404, { error: 'Device not found' }));
    const r = await resolveDeviceConfig({
      serverUrl: 'https://x',
      apiKey: 'k',
      fetchImpl: fetchMock,
    });
    expect(r.ok).toBe(false);
    if (!r.ok) {
      expect(r.error.kind).toBe('not_found');
      expect(r.error.status).toBe(404);
    }
  });

  it('maps 429 to rate_limited', async () => {
    const fetchMock = vi.fn(async () =>
      fakeResponse(429, { error: 'Too many resolve requests' }),
    );
    const r = await resolveDeviceConfig({
      serverUrl: 'https://x',
      apiKey: 'k',
      fetchImpl: fetchMock,
    });
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.error.kind).toBe('rate_limited');
  });

  it('maps 503 to unavailable', async () => {
    const fetchMock = vi.fn(async () =>
      fakeResponse(503, { error: 'Daemon endpoint not registered' }),
    );
    const r = await resolveDeviceConfig({
      serverUrl: 'https://x',
      apiKey: 'k',
      fetchImpl: fetchMock,
    });
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.error.kind).toBe('unavailable');
  });

  it('maps 502 (5xx) to unavailable', async () => {
    const fetchMock = vi.fn(async () => fakeResponse(502, ''));
    const r = await resolveDeviceConfig({
      serverUrl: 'https://x',
      apiKey: 'k',
      fetchImpl: fetchMock,
    });
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.error.kind).toBe('unavailable');
  });

  it('maps non-categorized status to unknown', async () => {
    const fetchMock = vi.fn(async () => fakeResponse(418, ''));
    const r = await resolveDeviceConfig({
      serverUrl: 'https://x',
      apiKey: 'k',
      fetchImpl: fetchMock,
    });
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.error.kind).toBe('unknown');
  });
});

describe('resolveDeviceConfig — network / parse failures', () => {
  it('maps fetch reject to network error', async () => {
    const fetchMock = vi.fn(async () => {
      throw new Error('ECONNREFUSED');
    });
    const r = await resolveDeviceConfig({
      serverUrl: 'https://x',
      apiKey: 'k',
      fetchImpl: fetchMock,
    });
    expect(r.ok).toBe(false);
    if (!r.ok) {
      expect(r.error.kind).toBe('network');
      expect(r.error.message).toContain('ECONNREFUSED');
    }
  });

  it('maps AbortError to network with timeout copy', async () => {
    const fetchMock = vi.fn(async () => {
      const err: any = new Error('aborted');
      err.name = 'AbortError';
      throw err;
    });
    const r = await resolveDeviceConfig({
      serverUrl: 'https://x',
      apiKey: 'k',
      fetchImpl: fetchMock,
      timeoutMs: 50,
    });
    expect(r.ok).toBe(false);
    if (!r.ok) {
      expect(r.error.kind).toBe('network');
      expect(r.error.message).toContain('超时');
    }
  });

  it('maps malformed JSON 200 body to parse error', async () => {
    const fetchMock = vi.fn(async () => fakeResponse(200, { x: 1 }, { malformedJson: true }));
    const r = await resolveDeviceConfig({
      serverUrl: 'https://x',
      apiKey: 'k',
      fetchImpl: fetchMock,
    });
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.error.kind).toBe('parse');
  });

  it('rejects 200 with missing fields as parse error', async () => {
    const fetchMock = vi.fn(async () =>
      fakeResponse(200, { ws_url: 'ws://x' /* missing rest */ }),
    );
    const r = await resolveDeviceConfig({
      serverUrl: 'https://x',
      apiKey: 'k',
      fetchImpl: fetchMock,
    });
    expect(r.ok).toBe(false);
    if (!r.ok) {
      expect(r.error.kind).toBe('parse');
      expect(r.error.message).toContain('字段');
    }
  });

  it('rejects 200 with empty-string fields as parse error', async () => {
    const fetchMock = vi.fn(async () =>
      fakeResponse(200, {
        ws_url: '',
        http_url: 'http://x',
        device_id: 'd',
        user_id: 'u',
        channel_id: 'c',
        daemon_id: 'dm',
      }),
    );
    const r = await resolveDeviceConfig({
      serverUrl: 'https://x',
      apiKey: 'k',
      fetchImpl: fetchMock,
    });
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.error.kind).toBe('parse');
  });
});
