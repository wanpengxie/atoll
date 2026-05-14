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
//
// T78 (M1.2-FIX-C) — server-revoke authoritative tombstone:
//   Without a tombstone, `remove()` would simply delete the server entry and
//   verifyKey() would silently fall back to the env map (default
//   COAGENT_DEVICE_SOURCE=both seeds env). That violates the #74 "server
//   revoke → forced disconnect" contract: an attacker with the original env
//   key could re-authenticate after the operator revoked the device on the
//   server.
//
//   Two sticky sets enforce server authority over any deviceId the server
//   has ever managed:
//     - `serverManagedIds`  — every deviceId the server has pulled/pushed
//                              (active OR revoked). Once present, env can
//                              never serve this id again, even if the
//                              server map is later cleared.
//     - `revokedServerIds`  — currently-tombstoned ids (revoked or dropped
//                              by replaceServer). verifyKey rejects these
//                              outright before consulting any other source.
//
//   Re-issuing the same deviceId via upsert() / replaceServer() lifts the
//   tombstone (server authoritatively re-instated the id), so operators can
//   revoke + re-create without restart.
//
// T82 (M1.2-FIX-G) — fresh-boot tombstone seed:
//   The T78 tombstone state is purely in-process. After a daemon restart the
//   two sets are empty; the active-only `replaceServer(entries)` pull cannot
//   surface a revoke transition (drop diff is computed against the previous
//   in-memory `serverEntries`, which is fresh). If env still carries a stale
//   key for a server-revoked device_id, verifyKey() falls back to env and
//   re-authenticates the device — silently bypassing server revoke across
//   the restart.
//
//   `replaceServer(entries, revokedIds = [])` accepts a second arg listing
//   recently-revoked device_ids the server vouches for. Each id is added to
//   `serverManagedIds` + `revokedServerIds` (subject to the same "active set
//   wins" rule — re-issued ids in `entries` lift the tombstone, see code).
//   `_onRevoke` is NOT fired for these ids: by definition the server already
//   tore down their connections at revoke time; on fresh boot there is no
//   connection to drop either.

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
    /**
     * Sticky set of deviceIds the server has ever managed (active or revoked).
     * Used by verifyKey() to prevent env fallback for any id under server
     * authority — see T78 (M1.2-FIX-C) header note.
     * @type {Set<string>}
     */
    this.serverManagedIds = new Set();
    /**
     * Tombstoned server-managed deviceIds. verifyKey() rejects these
     * outright. Re-issuing the same id via upsert/replaceServer clears it.
     * @type {Set<string>}
     */
    this.revokedServerIds = new Set();
    this._onRevoke = typeof onRevoke === 'function' ? onRevoke : () => {};
  }

  /**
   * Replace the entire server-managed entry set (used on boot pull + reconnect
   * re-pull).
   *
   * @param {Array<{device_id:string, api_key:string}>} entries
   *   Active server-issued devices.
   * @param {string[]} [revokedIds]
   *   T82 (M1.2-FIX-G) — recently-revoked device_ids the server vouches for.
   *   Used to seed tombstones on fresh boot (in-memory tombstone state was
   *   wiped by the restart). These ids are NOT subject to drop-diff
   *   semantics: `_onRevoke` is intentionally not fired (server already
   *   disconnected those clients at revoke time; on fresh boot there is no
   *   connection to drop either). If a revokedId also appears in `entries`
   *   the active set wins (operator revoke+re-create same device_id flow).
   */
  replaceServer(entries, revokedIds = []) {
    const next = new Map();
    if (Array.isArray(entries)) {
      for (const entry of entries) {
        const id = String(entry?.device_id ?? '').trim();
        const key = String(entry?.api_key ?? '').trim();
        if (!id || !key) continue;
        next.set(id, { ...entry, device_id: id, api_key: key });
      }
    }
    // Server authoritatively re-asserts every id present in `next`: mark as
    // server-managed and lift any prior tombstone (operator may have revoked
    // and re-created the same id between syncs).
    for (const id of next.keys()) {
      this.serverManagedIds.add(id);
      this.revokedServerIds.delete(id);
    }
    // Diff: any deviceId that left the server set fires onRevoke (best-effort
    // disconnect) and gets tombstoned. We compute the drop set BEFORE swapping
    // the map so the diff is correct; we tombstone BEFORE firing onRevoke so
    // listeners observing verifyKey() inside the callback see the post-revoke
    // state (env fallback already disabled).
    const dropped = [];
    for (const id of this.serverEntries.keys()) {
      if (!next.has(id)) dropped.push(id);
    }
    this.serverEntries = next;
    for (const id of dropped) {
      this.serverManagedIds.add(id); // belt-and-braces: should already be set
      this.revokedServerIds.add(id);
      try { this._onRevoke(id); } catch { /* listener errors must not break sync */ }
    }
    // T82: seed tombstones from the server-supplied revoked list. The active
    // set is authoritative — if the same id is being re-issued in `entries`
    // we already lifted the tombstone above; do not re-tombstone it here.
    if (Array.isArray(revokedIds)) {
      for (const raw of revokedIds) {
        const id = String(raw ?? '').trim();
        if (!id) continue;
        if (next.has(id)) continue; // active re-issue wins
        this.serverManagedIds.add(id);
        this.revokedServerIds.add(id);
        // Intentionally do NOT call `_onRevoke`: these ids were revoked on
        // the server side prior to this pull (often before the daemon
        // restart). Their connections are already gone; a stale revoke
        // event would only confuse downstream listeners.
      }
    }
  }

  /** Insert/replace one server entry. Used by `device.created` / `device.updated`. */
  upsert(entry) {
    const id = String(entry?.device_id ?? '').trim();
    const key = String(entry?.api_key ?? '').trim();
    if (!id || !key) return false;
    this.serverEntries.set(id, { ...entry, device_id: id, api_key: key });
    // Server authoritatively (re-)issued this id: mark managed and lift any
    // prior tombstone (revoke + re-create same id flow).
    this.serverManagedIds.add(id);
    this.revokedServerIds.delete(id);
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
    // Tombstone BEFORE firing onRevoke so listeners observing verifyKey()
    // inside the callback see the rejected state (env fallback disabled).
    this.serverManagedIds.add(id);
    this.revokedServerIds.add(id);
    try { this._onRevoke(id); } catch { /* swallow */ }
    return true;
  }

  /**
   * verifyKey closure entry. Resolution order:
   *   1. authoritative tombstone — `revokedServerIds` rejects outright
   *   2. server entry — strict equal against current server-issued key
   *   3. server-managed but missing — race-safe reject (see T78)
   *   4. env fallback — only for ids the server has never managed
   */
  verifyKey({ deviceId, key } = {}) {
    if (!deviceId || !key) return false;
    if (this.revokedServerIds.has(deviceId)) return false;
    const fromServer = this.serverEntries.get(deviceId);
    if (fromServer) return fromServer.api_key === key;
    // Server has managed this id but we don't have an entry right now: do NOT
    // fall back to env. The id either left the server set (already in
    // revokedServerIds via the branch above) or is being mutated concurrently
    // — env fallback in either case would breach server authority.
    if (this.serverManagedIds.has(deviceId)) return false;
    const fromEnv = this.envKeys.get(deviceId);
    if (fromEnv) return fromEnv === key;
    return false;
  }

  /**
   * Stable read-only view of merged entries. Server entries shadow env entries
   * with the same deviceId; env entries for any id under server authority
   * (active OR revoked) are omitted entirely so the snapshot reflects what
   * verifyKey() would actually accept (T78). Useful for tooling / status
   * endpoints.
   */
  snapshot() {
    /** @type {Map<string, {device_id:string, api_key:string, source:'env'|'server', [k:string]:any}>} */
    const out = new Map();
    for (const [id, key] of this.envKeys) {
      if (this.serverManagedIds.has(id)) continue; // server authority — env never serves
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
