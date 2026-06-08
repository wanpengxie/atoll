import { COAGENT_SERVER_DEVICEBUS_PROTOCOL } from 'coagent-xhs-shared';
import type { ConnectionConfig } from './connection-state';

type DeviceTransport = 'proxy' | 'server' | 'daemon' | 'none';

interface ConnectResult {
  success: boolean;
  error?: string;
}

export interface KeepaliveClient {
  transport: DeviceTransport;
  connect: () => Promise<ConnectResult>;
}

export interface KeepaliveAlarm {
  name: string;
}

export interface KeepaliveAlarmApi {
  create: (name: string, alarmInfo: { periodInMinutes: number }) => void;
  onAlarm: {
    addListener: (listener: (alarm: KeepaliveAlarm) => void) => void;
  };
}

export interface KeepaliveDeps {
  alarms: KeepaliveAlarmApi;
  loadConnectionConfig: () => Promise<ConnectionConfig>;
  selectTransport: (cfg: ConnectionConfig) => DeviceTransport;
  activeDeviceClient: () => KeepaliveClient;
  now?: () => number;
  warn?: typeof console.warn;
}

const KEEPALIVE_ALARM_NAME = COAGENT_SERVER_DEVICEBUS_PROTOCOL.KEEPALIVE_ALARM_NAME;

export function registerKeepaliveAlarm(deps: KeepaliveDeps): void {
  deps.alarms.create(KEEPALIVE_ALARM_NAME, {
    periodInMinutes: COAGENT_SERVER_DEVICEBUS_PROTOCOL.KEEPALIVE_PERIOD_MIN,
  });
  deps.alarms.onAlarm.addListener((alarm) => {
    void handleKeepaliveAlarm(alarm, deps);
  });
}

export async function handleKeepaliveAlarm(
  alarm: KeepaliveAlarm,
  deps: KeepaliveDeps
): Promise<void> {
  if (alarm.name !== KEEPALIVE_ALARM_NAME) return;

  const at = timestamp(deps);
  let transport: DeviceTransport = 'none';
  try {
    const cfg = await deps.loadConnectionConfig();
    transport = deps.selectTransport(cfg);
    logWarn(deps, '[Keepalive] alarm fired', { at, transport });
  } catch (err) {
    logWarn(deps, '[Keepalive] config load failed', {
      at,
      error: errorMessage(err),
    });
    return;
  }

  if (transport === 'none') return;
  const client = deps.activeDeviceClient();
  if (client.transport === 'none') return;

  try {
    const result = await client.connect();
    if (!result.success) {
      logWarn(deps, '[Keepalive] reconnect-on-alarm failed', {
        at: timestamp(deps),
        transport: client.transport,
        error: result.error,
      });
    }
  } catch (err) {
    logWarn(deps, '[Keepalive] reconnect-on-alarm failed', {
      at: timestamp(deps),
      transport: client.transport,
      error: errorMessage(err),
    });
  }
}

function timestamp(deps: KeepaliveDeps): number {
  return deps.now ? deps.now() : Date.now();
}

function logWarn(deps: KeepaliveDeps, message: string, details: Record<string, unknown>): void {
  (deps.warn ?? console.warn)(message, details);
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
