import { describe, it, expect, vi } from 'vitest';
import type { ConnectionConfig } from './connection-state';
import {
  handleKeepaliveAlarm,
  registerKeepaliveAlarm,
  type KeepaliveAlarm,
  type KeepaliveDeps,
} from './keepalive';

vi.mock('coagent-xhs-shared', () => ({
  COAGENT_SERVER_DEVICEBUS_PROTOCOL: {
    KEEPALIVE_ALARM_NAME: 'coagent-keepalive',
    KEEPALIVE_PERIOD_MIN: 0.4,
  },
}));

function makeConfig(over: Partial<ConnectionConfig> = {}): ConnectionConfig {
  return {
    serverUrl: '',
    autoReconnect: true,
    apiKey: '',
    daemonHttpBase: 'http://127.0.0.1:9501',
    deviceId: 'dev-1',
    userId: 'user-1',
    coagentServerUrl: 'https://coagent-server',
    wsUrl: '',
    httpBase: 'http://127.0.0.1:9501',
    channelId: 'ch-1',
    daemonId: '',
    serverWsEndpoint: 'wss://coagent.example.com/devicebus',
    deviceActorId: 'actor-1',
    deviceActorToken: 'tok-1',
    ...over,
  };
}

function makeDeps(
  opts: {
    transport?: 'server' | 'daemon' | 'none';
    connectResult?: { success: boolean; error?: string };
  } = {}
): {
  deps: KeepaliveDeps;
  create: ReturnType<typeof vi.fn>;
  addListener: ReturnType<typeof vi.fn>;
  connect: ReturnType<typeof vi.fn>;
  warn: ReturnType<typeof vi.fn>;
} {
  const transport = opts.transport ?? 'server';
  const create = vi.fn();
  const addListener = vi.fn();
  const connect = vi.fn(async () => opts.connectResult ?? { success: true });
  const warn = vi.fn();
  return {
    create,
    addListener,
    connect,
    warn,
    deps: {
      alarms: {
        create,
        onAlarm: { addListener },
      },
      loadConnectionConfig: vi.fn(async () => makeConfig()),
      selectTransport: vi.fn(() => transport),
      activeDeviceClient: vi.fn(() => ({
        transport,
        connect,
      })),
      now: () => 123,
      warn,
    },
  };
}

describe('registerKeepaliveAlarm', () => {
  it('creates the coagent keepalive alarm and registers an onAlarm listener', () => {
    const { deps, create, addListener } = makeDeps();

    registerKeepaliveAlarm(deps);

    expect(create).toHaveBeenCalledWith('coagent-keepalive', {
      periodInMinutes: 0.4,
    });
    expect(addListener).toHaveBeenCalledTimes(1);
  });
});

describe('handleKeepaliveAlarm', () => {
  const alarm: KeepaliveAlarm = { name: 'coagent-keepalive' };

  it('ignores unrelated alarms', async () => {
    const { deps, connect, warn } = makeDeps();

    await handleKeepaliveAlarm({ name: 'other' }, deps);

    expect(connect).not.toHaveBeenCalled();
    expect(warn).not.toHaveBeenCalled();
  });

  it('logs the alarm and reconnects the active transport', async () => {
    const { deps, connect, warn } = makeDeps();

    await handleKeepaliveAlarm(alarm, deps);

    expect(connect).toHaveBeenCalledTimes(1);
    expect(warn).toHaveBeenCalledWith('[Keepalive] alarm fired', {
      at: 123,
      transport: 'server',
    });
  });

  it('does not connect when no transport is configured', async () => {
    const { deps, connect, warn } = makeDeps({ transport: 'none' });

    await handleKeepaliveAlarm(alarm, deps);

    expect(connect).not.toHaveBeenCalled();
    expect(warn).toHaveBeenCalledWith('[Keepalive] alarm fired', {
      at: 123,
      transport: 'none',
    });
  });

  it('logs connect failures returned by the client', async () => {
    const { deps, warn } = makeDeps({
      connectResult: { success: false, error: 'offline' },
    });

    await handleKeepaliveAlarm(alarm, deps);

    expect(warn).toHaveBeenCalledWith('[Keepalive] reconnect-on-alarm failed', {
      at: 123,
      transport: 'server',
      error: 'offline',
    });
  });
});
