// Boot/reconnect register + pull (T74 §1, §3, T81 M1.2-FIX-F).
//
// Runs on every WS open. On failure the daemon logs and (when DEVICE_SOURCE
// allows) keeps the env fallback already populated in the device-store.
//
// Returned closure is stateful: `resolvedDaemonId` is cached after the first
// successful register so reconnects can pass the previously-acked daemon_id
// back to the server (the server handler is idempotent — re-registering on
// every reconnect is intentional and refreshes daemon_host / capabilities /
// last_heartbeat).
//
// T81 (M1.2-FIX-F) — port is a hard precondition for register. When
// PUBLIC_PORT is not set:
//   - DEVICE_SOURCE='env'    → already returns early (no register needed)
//   - DEVICE_SOURCE='server' → log error and skip; without env fallback the
//                              device map stays empty (verifyKey rejects all)
//   - DEVICE_SOURCE='both'   → log error and skip; env fallback carries the
//                              load until operator configures the port
// In every case we DO NOT issue the register call, because the server now
// returns 400 for null/empty port and a noisy error trail obscures the real
// "missing PUBLIC_PORT" problem.
import { registerDaemon as registerDaemonDefault, fetchDevices as fetchDevicesDefault } from './registrar.js';

export function createBootstrapDeviceSync({
  deviceSource,
  envDeviceKeysSize = 0,
  serverUrl,
  machineApiKey,
  publicHost,
  publicPort,
  publicScheme = null,
  capabilities = [],
  deviceStore,
  registerDaemonImpl = registerDaemonDefault,
  fetchDevicesImpl = fetchDevicesDefault,
  log = (...args) => console.error(...args),
} = {}) {
  if (!deviceStore || typeof deviceStore.replaceServer !== 'function') {
    throw new Error('createBootstrapDeviceSync: deviceStore with replaceServer() required');
  }
  let resolvedDaemonId = '';
  return async function bootstrapDeviceSync() {
    if (deviceSource === 'env') {
      log(`[Daemon] device source=env — skipping register+pull (env-entries=${envDeviceKeysSize})`);
      return;
    }
    if (publicPort == null) {
      log(`[Daemon] register skipped — COAGENT_DAEMON_HTTP_PORT or COAGENT_DAEMON_PUBLIC_PORT must be set (source=${deviceSource}, env-entries=${envDeviceKeysSize})`);
      return;
    }
    try {
      const reg = await registerDaemonImpl({
        serverUrl,
        machineApiKey,
        daemonId: resolvedDaemonId || '',
        host: publicHost,
        port: publicPort,
        scheme: publicScheme,
        capabilities,
      });
      const daemonId = String(reg?.daemon_id ?? '').trim();
      if (!daemonId) throw new Error('register response missing daemon_id');
      resolvedDaemonId = daemonId;
      // T82 (M1.2-FIX-G): fetchDevices now returns { devices, revokedDeviceIds }.
      // The revoked list seeds tombstones in the DeviceStore on fresh boot;
      // without it, env fallback would silently re-authenticate ids the
      // server has already revoked. Old fetchDevices contract returned a
      // bare array — supported via the defensive branch below so a
      // partial-rollout daemon does not crash mid-deploy.
      const result = await fetchDevicesImpl({
        serverUrl,
        machineApiKey,
        daemonId,
      });
      const devices = Array.isArray(result) ? result : (result?.devices ?? []);
      const revokedDeviceIds = Array.isArray(result?.revokedDeviceIds) ? result.revokedDeviceIds : [];
      deviceStore.replaceServer(devices, revokedDeviceIds);
      log(`[Daemon] register+pull ok — daemon_id=${daemonId} devices=${devices.length} revoked=${revokedDeviceIds.length} total=${deviceStore.size()}`);
    } catch (err) {
      if (deviceSource === 'server') {
        log(`[Daemon] register+pull failed (source=server, no env fallback): ${err?.message ?? err}`);
      } else {
        log(`[Daemon] register+pull failed — falling back to env (${envDeviceKeysSize} entries): ${err?.message ?? err}`);
      }
    }
  };
}
