// Vitest unit tests for entrypoints/background/external-bind.ts.
//
// T178/T5 keeps the externally-connectable surface proxy-only:
// getDeviceInfo for diagnostics and unbindDevice for cleanup. The old
// Token binding actions are intentionally absent.

import { describe, it, expect, vi } from 'vitest';

import type { ConnectionConfig } from './connection-state';
import {
  handleExternalMessage,
  isAllowedSenderOrigin,
  parseAllowedOriginsEnv,
  type ExternalBindDeps,
  type ExternalBindMessage,
  type ExternalSender,
} from './external-bind';

const ALLOWED = ['https://*.coagent.dev/*', 'http://localhost:*/*'];

function makeConfig(over: Partial<ConnectionConfig> = {}): ConnectionConfig {
  return {
    serverUrl: '',
    autoReconnect: true,
    apiKey: '',
    daemonHttpBase: '',
    deviceId: '',
    userId: '',
    wsUrl: '',
    httpBase: '',
    channelId: '',
    daemonId: '',
    deviceActorId: '',
    proxyEndpoint: 'ws://127.0.0.1:10387',
    ...over,
  };
}

function makeDeps(
  initial: Partial<ConnectionConfig> = {},
  opts: { uuidSequence?: string[] } = {},
): {
  deps: ExternalBindDeps;
  state: { config: ConnectionConfig; saves: Partial<ConnectionConfig>[] };
  disconnectAll: ReturnType<typeof vi.fn>;
} {
  const state = {
    config: makeConfig(initial),
    saves: [] as Partial<ConnectionConfig>[],
  };
  const disconnectAll = vi.fn();
  const uuids = (opts.uuidSequence ?? ['uuid-1', 'uuid-2']).slice();
  const status = {
    connected: false as boolean,
    serverUrl: '',
    reconnecting: false,
    lastError: '' as string,
    lastUpdated: 0,
  };
  const connectProxy = vi.fn(async (endpoint?: string) => {
    const ep = (endpoint ?? '').trim() || 'ws://127.0.0.1:10387';
    state.saves.push({
      connectionMode: 'proxy',
      proxyEndpoint: ep,
      autoReconnect: true,
    });
    state.config = {
      ...state.config,
      connectionMode: 'proxy',
      proxyEndpoint: ep,
      autoReconnect: true,
    };
    status.connected = true;
    status.lastError = '';
    status.lastUpdated = 1;
    return { connected: true, endpoint: ep };
  });
  const deps: ExternalBindDeps = {
    getConfig: async () => state.config,
    saveConfig: async (patch) => {
      state.saves.push(patch);
      state.config = { ...state.config, ...patch };
      return state.config;
    },
    disconnectAll,
    extensionVersion: '1.2.3-test',
    allowedOrigins: ALLOWED,
    generateDeviceID: () => `xhs-${uuids.shift() ?? 'fallback'}`,
    getConnectionStatus: async () => ({ ...status }),
    connectProxy,
  };
  return { deps, state, disconnectAll, connectProxy, status };
}

function senderOk(origin = 'https://app.coagent.dev'): ExternalSender {
  return { origin, url: origin + '/', id: 'webpage' };
}

describe('isAllowedSenderOrigin', () => {
  it('accepts wildcard host suffix match', () => {
    expect(isAllowedSenderOrigin({ origin: 'https://app.coagent.dev' }, ALLOWED)).toBe(true);
    expect(isAllowedSenderOrigin({ origin: 'https://coagent.dev' }, ALLOWED)).toBe(true);
  });

  it('rejects non-matching host and bad URLs', () => {
    expect(isAllowedSenderOrigin({ origin: 'https://evil.com' }, ALLOWED)).toBe(false);
    expect(isAllowedSenderOrigin({ origin: 'not-a-url' }, ALLOWED)).toBe(false);
    expect(isAllowedSenderOrigin(undefined, ALLOWED)).toBe(false);
  });

  it('respects scheme and localhost pattern', () => {
    expect(isAllowedSenderOrigin({ origin: 'http://localhost:5173' }, ALLOWED)).toBe(true);
    expect(isAllowedSenderOrigin({ origin: 'https://localhost:5173' }, ALLOWED)).toBe(false);
  });

  it('falls back to sender.url when origin is missing', () => {
    expect(isAllowedSenderOrigin({ url: 'https://app.coagent.dev/page' }, ALLOWED)).toBe(true);
  });

  it('fails closed with an empty allowlist', () => {
    expect(isAllowedSenderOrigin({ origin: 'https://app.coagent.dev' }, [])).toBe(false);
  });
});

describe('parseAllowedOriginsEnv', () => {
  it('splits on comma, trims whitespace, and drops empty fragments', () => {
    expect(parseAllowedOriginsEnv('https://a.com/*, , http://localhost:*/*')).toEqual([
      'https://a.com/*',
      'http://localhost:*/*',
    ]);
  });

  it('returns empty array for missing or empty env', () => {
    expect(parseAllowedOriginsEnv(undefined)).toEqual([]);
    expect(parseAllowedOriginsEnv('  ')).toEqual([]);
  });
});

describe('handleExternalMessage dispatch', () => {
  it('rejects disallowed origins without side effects', async () => {
    const { deps, state, disconnectAll } = makeDeps();
    const res = await handleExternalMessage(
      { action: 'getDeviceInfo' },
      { origin: 'https://evil.com' },
      deps,
    );
    expect(res.status).toBe('failed');
    if (res.status === 'failed') {
      expect(res.reason).toBe('origin_not_allowed');
      expect(res.detail).toBe('https://evil.com');
    }
    expect(state.saves).toEqual([]);
    expect(disconnectAll).not.toHaveBeenCalled();
  });

  it('rejects unknown and non-object messages', async () => {
    const { deps } = makeDeps();
    const unknown = await handleExternalMessage(
      { action: 'mystery' as unknown as ExternalBindMessage['action'] },
      senderOk(),
      deps,
    );
    expect(unknown.status).toBe('failed');
    if (unknown.status === 'failed') expect(unknown.reason).toBe('invalid_payload');

    const malformed = await handleExternalMessage(null as unknown as ExternalBindMessage, senderOk(), deps);
    expect(malformed.status).toBe('failed');
    if (malformed.status === 'failed') expect(malformed.reason).toBe('invalid_payload');
  });
});

describe('getDeviceInfo', () => {
  it('generates and persists a new device_id when storage is empty', async () => {
    const { deps, state } = makeDeps({}, { uuidSequence: ['abc'] });
    const res = await handleExternalMessage({ action: 'getDeviceInfo' }, senderOk(), deps);
    expect(res.status).toBe('ok');
    if (res.status === 'ok') {
      expect(res.device_id).toBe('xhs-abc');
      expect(res.version).toBe('1.2.3-test');
    }
    expect(state.saves).toEqual([{ deviceId: 'xhs-abc' }]);
  });

  it('returns existing device_id and proxy bind snapshot', async () => {
    const { deps, state } = makeDeps({
      deviceId: 'xhs-existing',
      connectionMode: 'proxy',
      proxyEndpoint: 'ws://127.0.0.1:10387',
    });
    const res = await handleExternalMessage({ action: 'getDeviceInfo' }, senderOk(), deps);
    expect(res.status).toBe('ok');
    if (res.status === 'ok') {
      expect(res.device_id).toBe('xhs-existing');
      expect(res.bound).toEqual({
        connection_mode: 'proxy',
        proxy_endpoint: 'ws://127.0.0.1:10387',
        connected: false,
      });
    }
    expect(state.saves).toEqual([]);
  });
});

describe('unbindDevice', () => {
  it('disconnects and disables proxy reconnect while keeping device_id', async () => {
    const { deps, state, disconnectAll } = makeDeps({
      deviceId: 'xhs-keep',
      connectionMode: 'proxy',
      proxyEndpoint: 'ws://127.0.0.1:10387',
    });
    const res = await handleExternalMessage({ action: 'unbindDevice' }, senderOk(), deps);
    expect(res.status).toBe('unbound');
    expect(disconnectAll).toHaveBeenCalledOnce();
    expect(state.saves).toEqual([{ autoReconnect: false, connectionMode: undefined }]);
    expect(state.config.deviceId).toBe('xhs-keep');
  });
});
