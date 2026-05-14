// DispatchRouter — in-memory map from correlation_id to dispatch context
// (channel_id / device_id / user_id). Used by channel-manager.deviceCommandSend
// and channel-manager.deviceCallback to route extension callbacks back to
// the right channel without scanning sqlite.
//
// 设计取舍：
//   - 进程内 Map，重启即丢；启动时由 channel-manager.start() 调用
//     `recoverFromRows` 从 messages.sqlite 重建 in-flight 路由（M1.1 Fix-T3）。
//   - TTL 默认 24h，远长于 self_check 默认 10min；过期 entry 在 register/lookup
//     时 lazy 扫除。

const DEFAULT_TTL_MS = 24 * 60 * 60 * 1000;
/** Recovery 默认窗口：30 分钟内的 dispatch.start 才会被重新装载（spec §Fix-T3）。 */
export const DEFAULT_RECOVERY_WINDOW_MS = 30 * 60 * 1000;

export class DispatchRouter {
  constructor({ ttlMs = DEFAULT_TTL_MS, now = () => Date.now() } = {}) {
    this.ttlMs = ttlMs;
    this.now = now;
    /** @type {Map<string, {channel_id:string, device_id:string, user_id:string, expires_at:number}>} */
    this.entries = new Map();
  }

  register({ correlationId, channelId, deviceId, userId, ttlMs }) {
    const id = String(correlationId ?? '').trim();
    if (!id) throw new Error('DispatchRouter.register: correlationId required');
    const ttl = Number.isFinite(ttlMs) && ttlMs > 0 ? ttlMs : this.ttlMs;
    this.entries.set(id, {
      channel_id: String(channelId ?? '').trim(),
      device_id: String(deviceId ?? '').trim(),
      user_id: String(userId ?? '').trim(),
      expires_at: this.now() + ttl,
    });
    this._sweep();
  }

  lookup(correlationId) {
    const id = String(correlationId ?? '').trim();
    if (!id) return null;
    const entry = this.entries.get(id);
    if (!entry) return null;
    if (entry.expires_at < this.now()) {
      this.entries.delete(id);
      return null;
    }
    return { ...entry };
  }

  remove(correlationId) {
    return this.entries.delete(String(correlationId ?? '').trim());
  }

  size() {
    return this.entries.size;
  }

  _sweep() {
    if (this.entries.size < 256) return;
    const now = this.now();
    for (const [id, entry] of this.entries) {
      if (entry.expires_at < now) this.entries.delete(id);
    }
  }

  /**
   * Restart recovery — register a batch of in-flight dispatch contexts that
   * were observed in messages.sqlite but lost when the daemon process died.
   *
   * @param {Array<{
   *   correlation_id?: string,
   *   correlationId?: string,
   *   channel_id?: string,
   *   channelId?: string,
   *   device_id?: string,
   *   deviceId?: string,
   *   user_id?: string,
   *   userId?: string,
   *   ttlMs?: number,
   * }>} rows
   * @returns {{ recovered: number, skipped: number }} count summary; rows that
   *   collide with an existing entry (already re-registered by a more recent
   *   `register()` call) are silently skipped to keep recovery idempotent.
   */
  recoverFromRows(rows) {
    if (!Array.isArray(rows)) return { recovered: 0, skipped: 0 };
    let recovered = 0;
    let skipped = 0;
    for (const raw of rows) {
      if (!raw || typeof raw !== 'object') {
        skipped += 1;
        continue;
      }
      const correlationId = String(raw.correlation_id ?? raw.correlationId ?? '').trim();
      const channelId = String(raw.channel_id ?? raw.channelId ?? '').trim();
      const deviceId = String(raw.device_id ?? raw.deviceId ?? '').trim();
      const userId = String(raw.user_id ?? raw.userId ?? '').trim();
      if (!correlationId || !channelId) {
        skipped += 1;
        continue;
      }
      // Skip if already registered (deviceCommandSend may have raced ahead, or
      // a previous recovery pass already covered this correlation).
      if (this.entries.has(correlationId)) {
        skipped += 1;
        continue;
      }
      this.register({
        correlationId,
        channelId,
        deviceId,
        userId,
        ttlMs: raw.ttlMs,
      });
      recovered += 1;
    }
    return { recovered, skipped };
  }
}
