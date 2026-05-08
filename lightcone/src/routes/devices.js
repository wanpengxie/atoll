import { Router } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { randomBytes } from 'node:crypto';
import {
  getDb,
  insertDevice,
  getDevices,
  getDeviceById,
  revokeDevice,
  getChannelById,
  getMachineById,
  isWorkspaceMember,
} from '../db/index.js';
import { sendToDaemon, isMachineOnline } from '../daemon/connections.js';

const ALLOWED_DEVICE_TYPES = new Set(['xhs', 'douyin', 'kuaishou']);

function defaultGenerateApiKey() {
  return 'sk_dev_' + randomBytes(32).toString('hex');
}

function defaultDeviceId(type) {
  return `${type}-${randomBytes(4).toString('hex')}`;
}

function requireUser(req, res) {
  if (!req.user) {
    res.status(401).json({ error: 'Unauthorized' });
    return null;
  }
  return req.user;
}

export function createDevicesRouter({
  getDbImpl = getDb,
  insertDeviceImpl = insertDevice,
  getDevicesImpl = getDevices,
  getDeviceByIdImpl = getDeviceById,
  revokeDeviceImpl = revokeDevice,
  getChannelByIdImpl = getChannelById,
  getMachineByIdImpl = getMachineById,
  isWorkspaceMemberImpl = isWorkspaceMember,
  sendToDaemonImpl = sendToDaemon,
  isMachineOnlineImpl = isMachineOnline,
  uuidv4Impl = uuidv4,
  generateApiKeyImpl = defaultGenerateApiKey,
} = {}) {
  const router = Router();

  function pushDeviceEvent(daemonId, type, payload) {
    if (!daemonId) return;
    if (typeof isMachineOnlineImpl === 'function' && !isMachineOnlineImpl(daemonId)) return;
    sendToDaemonImpl(daemonId, { type, payload });
  }

  // POST /api/devices — create device, return sk_dev key once
  router.post('/', async (req, res) => {
    const user = requireUser(req, res);
    if (!user) return;

    const deviceType = String(req.body?.device_type ?? '').trim();
    if (!deviceType) return res.status(400).json({ error: 'device_type is required' });
    if (!ALLOWED_DEVICE_TYPES.has(deviceType)) {
      return res.status(400).json({ error: `device_type must be one of: ${[...ALLOWED_DEVICE_TYPES].join(', ')}` });
    }

    const channelId = req.body?.channel_id ?? null;
    const daemonId  = req.body?.daemon_id ?? null;

    // ── Ownership: enforce user_id = req.user.id for non-service callers ──
    const bodyUserId = req.body?.user_id == null ? '' : String(req.body.user_id).trim();
    let effectiveUserId;
    if (req.isService) {
      // Service token may explicitly create devices on behalf of any user.
      effectiveUserId = bodyUserId || user.id;
    } else {
      if (bodyUserId && bodyUserId !== user.id) {
        return res.status(403).json({ error: 'Forbidden: cannot create device for another user' });
      }
      effectiveUserId = user.id;
    }

    // ── Ownership: channel_id must be a workspace the caller is a member of ──
    if (channelId != null && channelId !== '') {
      const channel = await getChannelByIdImpl(getDbImpl(), String(channelId));
      if (!channel || channel.is_del || channel.deleted_at) {
        return res.status(404).json({ error: 'Channel not found' });
      }
      if (!req.isService) {
        const ok = await isWorkspaceMemberImpl(getDbImpl(), channel.workspace_id, effectiveUserId);
        if (!ok) return res.status(403).json({ error: 'Forbidden: workspace membership required for channel' });
      }
    }

    // ── Ownership: daemon_id must be owned by caller (or platform machine) ──
    if (daemonId != null && daemonId !== '') {
      const machine = await getMachineByIdImpl(getDbImpl(), String(daemonId));
      if (!machine) {
        return res.status(404).json({ error: 'Daemon machine not found' });
      }
      if (!req.isService) {
        const ownsDaemon = machine.owner_id && machine.owner_id === effectiveUserId;
        const isPlatform = Number(machine.is_platform) === 1;
        if (!ownsDaemon && !isPlatform) {
          return res.status(403).json({ error: 'Forbidden: caller does not own this daemon' });
        }
      }
    }

    const deviceId  = String(req.body?.device_id ?? '').trim() || defaultDeviceId(deviceType);

    const id      = uuidv4Impl();
    const apiKey  = generateApiKeyImpl();
    const created = await insertDeviceImpl(getDbImpl(), {
      id,
      device_id: deviceId,
      api_key: apiKey,
      user_id: effectiveUserId,
      channel_id: channelId,
      daemon_id: daemonId,
      device_type: deviceType,
      status: 'active',
    });

    pushDeviceEvent(created.daemon_id, 'device.created', created);
    res.status(201).json(created);
  });

  // GET /api/devices — list devices, scoped to caller unless service caller opts out
  router.get('/', async (req, res) => {
    const user = requireUser(req, res);
    if (!user) return;

    const filters = {};
    const queryUserId = req.query.user_id == null ? '' : String(req.query.user_id).trim();
    const wantAll     = String(req.query.all ?? '').trim() === 'true';

    if (req.isService) {
      // Admin/service: respect explicit ?user_id=, otherwise return all unless filtered.
      // ?all=true only relevant for documenting intent — same effect as omitting user_id.
      if (queryUserId) filters.user_id = queryUserId;
      if (wantAll && filters.user_id) {
        // no-op; explicit user_id wins
      }
    } else {
      // Non-service: scope strictly to caller. Reject cross-user scope attempts.
      if (queryUserId && queryUserId !== user.id) {
        return res.status(403).json({ error: 'Forbidden: cannot list devices for another user' });
      }
      if (wantAll) {
        return res.status(403).json({ error: 'Forbidden: only admin/service caller may list all devices' });
      }
      filters.user_id = user.id;
    }

    if (req.query.channel_id) filters.channel_id = String(req.query.channel_id);
    if (req.query.daemon_id)  filters.daemon_id  = String(req.query.daemon_id);
    if (req.query.status)     filters.status     = String(req.query.status);
    if (req.query.device_type)filters.device_type= String(req.query.device_type);

    const devices = await getDevicesImpl(getDbImpl(), filters);
    // strip api_key from list view (only the create response shows it once)
    const masked = devices.map(d => ({
      ...d,
      api_key: typeof d.api_key === 'string' && d.api_key.length > 12
        ? d.api_key.slice(0, 10) + '…'
        : '',
    }));
    res.json({ devices: masked });
  });

  // DELETE /api/devices/:id — revoke; only owner (or service) may revoke
  router.delete('/:id', async (req, res) => {
    const user = requireUser(req, res);
    if (!user) return;

    const id = String(req.params.id ?? '').trim();
    const existing = await getDeviceByIdImpl(getDbImpl(), id);
    if (!existing) return res.status(404).json({ error: 'Device not found' });

    if (!req.isService && existing.user_id !== user.id) {
      return res.status(403).json({ error: 'Forbidden: cannot revoke another user\'s device' });
    }

    const updated = await revokeDeviceImpl(getDbImpl(), id);
    pushDeviceEvent(updated?.daemon_id ?? existing.daemon_id, 'device.revoked', updated ?? existing);
    res.json({ ok: true });
  });

  return router;
}

const router = createDevicesRouter();
export default router;
