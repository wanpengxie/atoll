// Daemon registrar — thin HTTP wrappers around the server-side daemon contract
// (lightcone/src/routes/daemon.js).
//
//   POST /api/daemon/register
//     body: {machine_api_key, daemon_id, host?, port (REQUIRED, 1..65535), scheme?, capabilities?}
//     200 :  {ok:true, daemon_id}
//     400 :  {error: "port is required"} when port missing/empty
//     400 :  {error: "port must be an integer"} when port not integer
//     400 :  {error: "port must be a valid TCP port (1-65535)"} when out of range
//     401/403/4xx → throw Error('register_failed: <status>')
//
// `port` (T81, M1.2-FIX-F) is a hard precondition. Defense-in-depth check
// here so daemon-side misconfig surfaces synchronously at the call site
// rather than waiting on the network round trip; the server route enforces
// the same contract.
//
// T83 (M1.2-FIX-H): the call-site check matches the server's: trim whitespace,
// require integer, require 1..65535. Without this, env-derived nonsense
// (PUBLIC_PORT="0", "-1", "65536", "   ") would only fail at the server's
// 400 — a noisy round-trip that obscures the real "operator misconfigured
// the env var" cause. Throwing locally surfaces the misconfig at the
// register call site (bootstrap.js try/catch then logs and falls back to
// env-only mode where allowed).
//
// `scheme` (T77, M1.2-FIX-B) is the public-URL scheme the daemon advertises
// when it sits behind a TLS proxy (`http`/`https`/`ws`/`wss`). When omitted
// the server keeps the legacy ws/http default for dev clusters.
//
//   GET  /api/daemon/{daemon_id}/devices
//     header: Authorization: Bearer <machine_api_key>
//     200 :  {devices: Device[], revoked_device_ids: string[]}  // T82 (M1.2-FIX-G)
//     401/403/4xx → throw Error('fetch_devices_failed: <status>')
//
// T82 (M1.2-FIX-G) note: `revoked_device_ids` is the recently-revoked
// device_id list the server vouches for. The daemon-side caller passes it
// as `replaceServer`'s second arg so a fresh DeviceStore (post-restart)
// seeds tombstones deterministically — without it, env fallback would
// silently re-authenticate any device_id already revoked on the server.
// Older servers that omit the field surface as `[]` here, preserving the
// pre-T82 behavior on mixed deployments.
//
// Both helpers use globalThis.fetch by default (Node 22+) and accept fetchImpl
// for unit tests. Errors include the response body (best effort) for triage —
// the daemon top-level reports them via console.error and degrades gracefully
// (see env fallback path in index.js bootstrap).

function joinUrl(serverUrl, suffix) {
  const base = String(serverUrl ?? '').replace(/\/+$/, '');
  return `${base}${suffix}`;
}

async function readBodySafe(res) {
  try { return await res.text(); } catch { return ''; }
}

export async function registerDaemon({
  serverUrl,
  machineApiKey,
  daemonId,
  host = null,
  port = null,
  scheme = null,
  capabilities = [],
  fetchImpl = globalThis.fetch,
} = {}) {
  if (typeof fetchImpl !== 'function') {
    throw new Error('registerDaemon: fetchImpl is not a function (no global fetch?)');
  }
  if (!serverUrl) throw new Error('registerDaemon: serverUrl required');
  if (!machineApiKey) throw new Error('registerDaemon: machineApiKey required');
  // T81 (M1.2-FIX-F): port is required. Throw before issuing the request
  // so callers (daemon bootstrap, smoke tests) see the failure at the
  // exact call site instead of decoding a 400 server response.
  // T83 (M1.2-FIX-H): trim whitespace + require integer + require 1..65535
  // here so env-derived nonsense (`COAGENT_DAEMON_PUBLIC_PORT="0"`, "-1",
  // "65536", "   ") fails at the call site rather than surviving until
  // resolve. Mirrors the server-side contract verbatim.
  const portStr = port == null ? '' : String(port).trim();
  if (portStr === '') {
    throw new Error('registerDaemon: port is required');
  }
  const portNum = Number(portStr);
  if (!Number.isInteger(portNum)) {
    throw new Error('registerDaemon: port must be an integer');
  }
  if (portNum < 1 || portNum > 65535) {
    throw new Error('registerDaemon: port must be a valid TCP port (1-65535)');
  }

  const body = {
    machine_api_key: machineApiKey,
    daemon_id: daemonId ?? '',
    host,
    port: portNum,
    capabilities: Array.isArray(capabilities) ? capabilities : [],
  };
  if (scheme != null && scheme !== '') body.scheme = String(scheme);

  const res = await fetchImpl(joinUrl(serverUrl, '/api/daemon/register'), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await readBodySafe(res);
    throw new Error(`register_failed: ${res.status} ${text}`.trim());
  }
  return await res.json();
}

/**
 * GET /api/daemon/{daemon_id}/devices.
 *
 * @returns {Promise<{ devices: Array, revokedDeviceIds: string[] }>}
 *   `devices` is the active server-issued list; `revokedDeviceIds` is the
 *   recently-revoked device_id list the daemon should pass to
 *   DeviceStore.replaceServer as the 2nd arg. T82 (M1.2-FIX-G): the latter
 *   is required to seed tombstones across daemon restart; against an older
 *   server that omits the field it falls back to `[]` (no behavior change
 *   beyond pre-T82 default).
 */
export async function fetchDevices({
  serverUrl,
  machineApiKey,
  daemonId,
  fetchImpl = globalThis.fetch,
} = {}) {
  if (typeof fetchImpl !== 'function') {
    throw new Error('fetchDevices: fetchImpl is not a function (no global fetch?)');
  }
  if (!serverUrl) throw new Error('fetchDevices: serverUrl required');
  if (!machineApiKey) throw new Error('fetchDevices: machineApiKey required');
  if (!daemonId) throw new Error('fetchDevices: daemonId required');

  const url = joinUrl(serverUrl, `/api/daemon/${encodeURIComponent(daemonId)}/devices`);
  const res = await fetchImpl(url, {
    method: 'GET',
    headers: { Authorization: `Bearer ${machineApiKey}` },
  });
  if (!res.ok) {
    const text = await readBodySafe(res);
    throw new Error(`fetch_devices_failed: ${res.status} ${text}`.trim());
  }
  const payload = await res.json();
  const devices = Array.isArray(payload?.devices) ? payload.devices : [];
  // T82: defensive normalization — old servers omit the field; corrupt /
  // misshapen entries are filtered to non-empty strings only so downstream
  // tombstone seeding never sees garbage.
  const rawRevoked = Array.isArray(payload?.revoked_device_ids) ? payload.revoked_device_ids : [];
  const revokedDeviceIds = [];
  const seen = new Set();
  for (const raw of rawRevoked) {
    const id = typeof raw === 'string' ? raw.trim() : '';
    if (!id || seen.has(id)) continue;
    seen.add(id);
    revokedDeviceIds.push(id);
  }
  return { devices, revokedDeviceIds };
}
