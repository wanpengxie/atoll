// DispatchRouter — in-memory map from correlation_id to dispatch context
// (channel_id / device_id / user_id). Used by channel-manager.deviceCommandSend
// and channel-manager.deviceCallback to route extension callbacks back to
// the right channel without scanning sqlite.
//
// 设计取舍：
//   - 进程内 Map，重启即丢；T3 范围内 PM 接受。后续 V0.5+ 若要持久化可换实现。
//   - TTL 默认 24h，远长于 self_check 默认 10min；过期 entry 在 register/lookup
//     时 lazy 扫除。

const DEFAULT_TTL_MS = 24 * 60 * 60 * 1000;

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
}
