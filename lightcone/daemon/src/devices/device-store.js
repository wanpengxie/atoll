// DeviceStore — in-memory device api-key store with env+server union semantics
// (T74 §1, §5). Replaces the boot-time const map produced by parseDeviceKeysEnv;
// owners (index.js) hand out a verifyKey closure that always reads the latest
// state, so handshake-time auth picks up server-side CRUD without restart.
//
// Source separation:
//   - env entries:    populated once at boot from COAGENT_DEVICE_KEYS, never
//                     overwritten at runtime — kept as dev fallback.
//   - server entries: pulled at boot via /api/daemon/{id}/devices and updated
//                     incrementally via daemon-WS device.* push events. May be
//                     wholesale-replaced on reconnect via replaceServer().
//
// Resolution priority (intentional): server > env. When both sources advertise
// the same deviceId the server map wins, so a freshly issued sk_dev_* key
// supersedes a stale env entry without operator intervention.
//
// onRevoke is invoked any time an entry leaves the server map (single remove
// or dropped from a replaceServer set). index.js wires it to
// DeviceWsServer.disconnect so an already-connected extension is forcibly
// dropped right after revoke (T74 §2 contract).

export class DeviceStore {
  /**
   * @param {object} [opts]
   * @param {Map<string,string>|Iterable<[string,string]>} [opts.envEntries]
   *   deviceId → api_key from COAGENT_DEVICE_KEYS (parseDeviceKeysEnv output).
   * @param {(deviceId:string)=>void} [opts.onRevoke]
   *   Called with deviceId whenever a server entry is removed.
   */
  constructor({ envEntries = null, onRevoke = () => {} } = {}) {
    /** @type {Map<string,string>} */
    this.envKeys = new Map();
    if (envEntries) {
      const it = envEntries instanceof Map ? envEntries.entries() : envEntries;
      for (const [k, v] of it) {
        if (typeof k === 'string' && typeof v === 'string' && k && v) {
          this.envKeys.set(k, v);
        }
      }
    }
    /** @type {Map<string, {device_id:string, api_key:string, [k:string]:any}>} */
    this.serverEntries = new Map();
    this._onRevoke = typeof onRevoke === 'function' ? onRevoke : () => {};
  }

  /** Replace the entire server-managed entry set (used on boot pull + reconnect re-pull). */
  replaceServer(entries) {
    const next = new Map();
    if (Array.isArray(entries)) {
      for (const entry of entries) {
        const id = String(entry?.device_id ?? '').trim();
        const key = String(entry?.api_key ?? '').trim();
        if (!id || !key) continue;
        next.set(id, { ...entry, device_id: id, api_key: key });
      }
    }
    // Diff: any deviceId that left the server set fires onRevoke (best-effort
    // disconnect). We do this BEFORE swapping the map so listeners that read
    // back DeviceStore.size() inside the callback observe the post-swap state
    // — but conventional revoke handlers only need the deviceId, not the entry.
    const dropped = [];
    for (const id of this.serverEntries.keys()) {
      if (!next.has(id)) dropped.push(id);
    }
    this.serverEntries = next;
    for (const id of dropped) {
      try { this._onRevoke(id); } catch { /* listener errors must not break sync */ }
    }
  }

  /** Insert/replace one server entry. Used by `device.created` / `device.updated`. */
  upsert(entry) {
    const id = String(entry?.device_id ?? '').trim();
    const key = String(entry?.api_key ?? '').trim();
    if (!id || !key) return false;
    this.serverEntries.set(id, { ...entry, device_id: id, api_key: key });
    return true;
  }

  /** Alias for upsert — explicit name for `device.updated` event handlers. */
  update(entry) {
    return this.upsert(entry);
  }

  /** Remove one server entry. Fires onRevoke if the entry existed. Idempotent. */
  remove(deviceId) {
    const id = String(deviceId ?? '').trim();
    if (!id) return false;
    if (!this.serverEntries.has(id)) return false;
    this.serverEntries.delete(id);
    try { this._onRevoke(id); } catch { /* swallow */ }
    return true;
  }

  /** verifyKey closure entry: server > env, never the other way around. */
  verifyKey({ deviceId, key } = {}) {
    if (!deviceId || !key) return false;
    const fromServer = this.serverEntries.get(deviceId);
    if (fromServer) return fromServer.api_key === key;
    const fromEnv = this.envKeys.get(deviceId);
    if (fromEnv) return fromEnv === key;
    return false;
  }

  /**
   * Stable read-only view of merged entries. Server entries shadow env entries
   * with the same deviceId. Useful for tooling / status endpoints.
   */
  snapshot() {
    /** @type {Map<string, {device_id:string, api_key:string, source:'env'|'server', [k:string]:any}>} */
    const out = new Map();
    for (const [id, key] of this.envKeys) {
      out.set(id, { device_id: id, api_key: key, source: 'env' });
    }
    for (const [id, entry] of this.serverEntries) {
      out.set(id, { ...entry, source: 'server' });
    }
    return out;
  }

  /** Number of distinct deviceIds across the union. */
  size() {
    return this.snapshot().size;
  }
}
