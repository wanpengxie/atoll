// services/coagent-device.ts — coagent daemon device WS client (stub for phase-2)。
//
// 真正的 WS 长连 / 指数重连 / cmd dispatch / callback 在 phase-3 落地；
// 本文件先定义 surface API 供 background/index.ts 引用并通过编译。

import type { ConnectionConfig } from '../connection-state';

export interface ConnectResult {
  success: boolean;
  error?: string;
}

class CoagentDeviceClient {
  private config: ConnectionConfig | null = null;

  updateConfig(config: ConnectionConfig): void {
    this.config = { ...config };
  }

  async connect(): Promise<ConnectResult> {
    // phase-3 will replace this stub with a real WebSocket client + reconnect loop.
    console.log('[CoagentDevice] connect() called (phase-2 stub)', {
      serverUrl: this.config?.serverUrl,
      hasKey: Boolean(this.config?.apiKey),
      deviceId: this.config?.deviceId,
    });
    return { success: false, error: 'CoagentDevice not yet implemented (phase-3 pending)' };
  }

  disconnect(): void {
    console.log('[CoagentDevice] disconnect() called (phase-2 stub)');
  }

  isConnected(): boolean {
    return false;
  }
}

export const coagentDeviceClient = new CoagentDeviceClient();
