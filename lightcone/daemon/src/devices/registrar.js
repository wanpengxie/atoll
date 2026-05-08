// Daemon registrar — thin HTTP wrappers around the server-side daemon contract
// (lightcone/src/routes/daemon.js).
//
//   POST /api/daemon/register
//     body: {machine_api_key, daemon_id, host?, port?, capabilities?}
//     200 :  {ok:true, daemon_id}
//     401/403/4xx → throw Error('register_failed: <status>')
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
  capabilities = [],
  fetchImpl = globalThis.fetch,
} = {}) {
  if (typeof fetchImpl !== 'function') {
    throw new Error('registerDaemon: fetchImpl is not a function (no global fetch?)');
  }
  if (!serverUrl) throw new Error('registerDaemon: serverUrl required');
  if (!machineApiKey) throw new Error('registerDaemon: machineApiKey required');

  const body = {
    machine_api_key: machineApiKey,
    daemon_id: daemonId ?? '',
    host,
    capabilities: Array.isArray(capabilities) ? capabilities : [],
  };
  if (port != null) body.port = port;

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
