// Daemon registrar — thin HTTP wrappers around the server-side daemon contract
// (lightcone/src/routes/daemon.js).
//
//   POST /api/daemon/register
//     body: {machine_api_key, daemon_id, host?, port (REQUIRED), scheme?, capabilities?}
//     200 :  {ok:true, daemon_id}
//     400 :  {error: "port is required"} when port missing/empty
//     400 :  {error: "port must be an integer"} when port not integer
//     401/403/4xx → throw Error('register_failed: <status>')
//
// `port` (T81, M1.2-FIX-F) is a hard precondition. Defense-in-depth check
// here so daemon-side misconfig surfaces synchronously at the call site
// rather than waiting on the network round trip; the server route enforces
// the same contract.
//
// `scheme` (T77, M1.2-FIX-B) is the public-URL scheme the daemon advertises
// when it sits behind a TLS proxy (`http`/`https`/`ws`/`wss`). When omitted
// the server keeps the legacy ws/http default for dev clusters.
//
//   GET  /api/daemon/{daemon_id}/devices
//     header: Authorization: Bearer <machine_api_key>
//     200 :  {devices: Device[]}
//     401/403/4xx → throw Error('fetch_devices_failed: <status>')
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
  if (port == null || port === '') {
    throw new Error('registerDaemon: port is required');
  }

  const body = {
    machine_api_key: machineApiKey,
    daemon_id: daemonId ?? '',
    host,
    port,
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
  return Array.isArray(payload?.devices) ? payload.devices : [];
}
