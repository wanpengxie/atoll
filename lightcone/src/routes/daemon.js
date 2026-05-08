import { Router } from 'express';
import {
  getDb,
  getMachineByApiKey,
  updateMachineDaemonInfo,
  getDevicesByDaemonId,
  getRevokedDeviceIdsByDaemonId,
} from '../db/index.js';
import { nowMysqlDatetime } from '../time.js';

function bearerToken(req) {
  const auth = req.headers.authorization ?? '';
  return auth.startsWith('Bearer ') ? auth.slice(7).trim() : '';
}

export function createDaemonRouter({
  getDbImpl = getDb,
  getMachineByApiKeyImpl = getMachineByApiKey,
  updateMachineDaemonInfoImpl = updateMachineDaemonInfo,
  getDevicesByDaemonIdImpl = getDevicesByDaemonId,
  getRevokedDeviceIdsByDaemonIdImpl = getRevokedDeviceIdsByDaemonId,
  nowDatetimeImpl = nowMysqlDatetime,
} = {}) {
  const router = Router();

  // POST /api/daemon/register — daemon boot handshake
  router.post('/register', async (req, res) => {
    const apiKey = String(req.body?.machine_api_key ?? '').trim();
    if (!apiKey) return res.status(401).json({ error: 'machine_api_key required' });

    const machine = await getMachineByApiKeyImpl(getDbImpl(), apiKey);
    if (!machine) return res.status(401).json({ error: 'Invalid machine API key' });

    const declaredDaemonId = String(req.body?.daemon_id ?? '').trim();
    if (declaredDaemonId && declaredDaemonId !== machine.id) {
      return res.status(403).json({ error: 'daemon_id does not match machine' });
    }

    const host = req.body?.host == null ? null : String(req.body.host);
    const portRaw = req.body?.port;
    // T81 (M1.2-FIX-F): port is now a hard precondition — t77 left it
    // optional which silently produced a row with daemon_port=null,
    // making `/api/device/resolve` return 503 long after the daemon
    // believed register succeeded. Reject upfront so the daemon-side
    // bootstrap fails fast and surfaces the missing env var.
    if (portRaw == null || portRaw === '') {
      return res.status(400).json({ error: 'port is required' });
    }
    const port = Number(portRaw);
    if (!Number.isInteger(port)) {
      return res.status(400).json({ error: 'port must be an integer' });
    }
    // T77 (M1.2-FIX-B): public scheme is optional. When daemon sits behind a
    // TLS proxy, prod clusters announce `https`/`wss` so `/api/device/resolve`
    // renders the correct extension URLs. Whitelist guards against arbitrary
    // strings being persisted; missing/empty stays null (dev default ws/http).
    const ALLOWED_DAEMON_SCHEMES = new Set(['http', 'https', 'ws', 'wss']);
    const schemeRaw = req.body?.scheme;
    let scheme = null;
    if (schemeRaw != null && schemeRaw !== '') {
      const norm = String(schemeRaw).toLowerCase().trim();
      if (norm && !ALLOWED_DAEMON_SCHEMES.has(norm)) {
        return res.status(400).json({ error: 'scheme must be one of http, https, ws, wss' });
      }
      scheme = norm || null;
    }
    const capabilities = Array.isArray(req.body?.capabilities) ? req.body.capabilities : [];

    await updateMachineDaemonInfoImpl(getDbImpl(), machine.id, {
      daemon_host: host,
      daemon_port: port,
      daemon_scheme: scheme,
      capabilities,
      status: 'online',
      last_heartbeat: nowDatetimeImpl(),
    });

    res.json({ ok: true, daemon_id: machine.id });
  });

  // GET /api/daemon/:daemon_id/devices — daemon pulls active devices
  //
  // T82 (M1.2-FIX-G): also returns the recently-revoked device_id list so
  // a freshly-booted daemon can seed tombstones deterministically. Without
  // this the daemon's in-memory `serverManagedIds`/`revokedServerIds` would
  // be empty across restart, the active-only pull would not surface any
  // revoke transition, and a stale env key for a server-revoked device_id
  // would silently re-authenticate.
  //
  // Backward compatibility: response gains a top-level `revoked_device_ids`
  // string array. Old daemons ignore unknown keys, so the contract change
  // is additive.
  router.get('/:daemon_id/devices', async (req, res) => {
    const token = bearerToken(req);
    if (!token) return res.status(401).json({ error: 'Missing Authorization header' });

    const machine = await getMachineByApiKeyImpl(getDbImpl(), token);
    if (!machine) return res.status(401).json({ error: 'Invalid machine API key' });

    const requestedId = String(req.params.daemon_id ?? '').trim();
    if (!requestedId || requestedId !== machine.id) {
      return res.status(403).json({ error: 'Machine is not authorized for this daemon' });
    }

    const db = getDbImpl();
    const [devices, revokedDeviceIds] = await Promise.all([
      getDevicesByDaemonIdImpl(db, machine.id),
      getRevokedDeviceIdsByDaemonIdImpl(db, machine.id),
    ]);
    res.json({
      devices,
      revoked_device_ids: Array.isArray(revokedDeviceIds) ? revokedDeviceIds : [],
    });
  });

  return router;
}

const router = createDaemonRouter();
export default router;
