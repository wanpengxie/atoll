import { Router } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { PayloadType, SenderKind } from '@coagent/payload-types';
import {
  getDb,
  getChannelById,
  getMachineByApiKey,
  getDeviceByApiKey,
  getMachineById,
} from '../db/index.js';
import { isMachineOnline } from '../daemon/connections.js';
import { requestFromDaemon } from '../daemon/index.js';

// Process-local token-bucket rate limiter for /resolve.
// Multi-instance deploys can swap this out via dependency injection.
const RESOLVE_DEFAULT_LIMIT = 30;       // requests
const RESOLVE_DEFAULT_WINDOW_MS = 60_000; // per minute
const resolveBuckets = new Map(); // key → { tokens, resetAt }

function defaultResolveRateLimit(key, { limit = RESOLVE_DEFAULT_LIMIT, windowMs = RESOLVE_DEFAULT_WINDOW_MS } = {}) {
  const now = Date.now();
  const bucket = resolveBuckets.get(key);
  if (!bucket || bucket.resetAt <= now) {
    resolveBuckets.set(key, { tokens: limit - 1, resetAt: now + windowMs });
    return true;
  }
  if (bucket.tokens > 0) {
    bucket.tokens -= 1;
    return true;
  }
  return false;
}

const STATUS_TO_PAYLOAD_TYPE = {
  accepted: PayloadType.DISPATCH_ACCEPTED,
  rejected: PayloadType.DISPATCH_REJECTED,
  completed: PayloadType.DISPATCH_COMPLETED,
  failed: PayloadType.DISPATCH_FAILED,
};

function bearerToken(req) {
  const auth = req.headers.authorization ?? '';
  return auth.startsWith('Bearer ') ? auth.slice(7).trim() : '';
}

function defaultDeviceId(machine) {
  return String(machine?.id ?? 'unknown').trim() || 'unknown';
}

function defaultDeviceName(machine, deviceId) {
  return String(machine?.name ?? '').trim() || deviceId;
}

export function createDeviceRouter({
  getDbImpl = getDb,
  getChannelByIdImpl = getChannelById,
  getMachineByApiKeyImpl = getMachineByApiKey,
  getDeviceByApiKeyImpl = getDeviceByApiKey,
  getMachineByIdImpl = getMachineById,
  isMachineOnlineImpl = isMachineOnline,
  requestFromDaemonImpl = requestFromDaemon,
  resolveRateLimitImpl = defaultResolveRateLimit,
  uuidv4Impl = uuidv4,
  daemonRequestTimeoutMs = 10_000,
} = {}) {
  const router = Router();

  // POST /api/device/resolve — extension popup → server reverse-lookup
  router.post('/resolve', async (req, res) => {
    const ip = req.ip || req.socket?.remoteAddress || 'unknown';
    if (!resolveRateLimitImpl(ip)) {
      return res.status(429).json({ error: 'Too many resolve requests' });
    }

    const apiKey = String(req.body?.api_key ?? '').trim();
    if (!apiKey) return res.status(400).json({ error: 'api_key required' });

    const device = await getDeviceByApiKeyImpl(getDbImpl(), apiKey);
    if (!device || device.status !== 'active') {
      return res.status(404).json({ error: 'Device not found' });
    }

    const daemonId = device.daemon_id;
    if (!daemonId) return res.status(503).json({ error: 'Device has no daemon assigned' });

    const machine = await getMachineByIdImpl(getDbImpl(), daemonId);
    if (!machine || !machine.daemon_host || !machine.daemon_port) {
      return res.status(503).json({ error: 'Daemon endpoint not registered' });
    }

    return res.json({
      ws_url:    `ws://${machine.daemon_host}:${machine.daemon_port}`,
      http_url:  `http://${machine.daemon_host}:${machine.daemon_port}`,
      device_id: device.device_id,
      user_id:   device.user_id,
      channel_id: device.channel_id,
      daemon_id: device.daemon_id,
    });
  });

  router.post('/result', async (req, res) => {
    const token = bearerToken(req);
    if (!token) return res.status(401).json({ error: 'Missing Authorization header' });

    const db = getDbImpl();
    const machine = await getMachineByApiKeyImpl(db, token);
    if (!machine) return res.status(401).json({ error: 'Invalid machine API key' });

    const channelId = String(req.body?.channelId ?? req.body?.channel_id ?? '').trim();
    const correlationId = String(req.body?.correlationId ?? req.body?.correlation_id ?? '').trim();
    const status = String(req.body?.status ?? '').trim();
    const payloadType = STATUS_TO_PAYLOAD_TYPE[status];
    if (!channelId) return res.status(400).json({ error: 'channel_id required' });
    if (!correlationId) return res.status(400).json({ error: 'correlation_id required' });
    if (!payloadType) {
      return res.status(400).json({ error: 'status must be accepted, rejected, completed, or failed' });
    }

    const channel = await getChannelByIdImpl(db, channelId);
    if (!channel || channel.is_del || channel.deleted_at) {
      return res.status(404).json({ error: 'Channel not found' });
    }
    const daemonId = String(channel.daemon_id ?? '').trim();
    if (!daemonId) return res.status(503).json({ error: 'Channel daemon unavailable' });
    const machineId = String(machine.id ?? '').trim();
    if (machineId !== daemonId) {
      return res.status(403).json({ error: 'Machine is not authorized for this channel' });
    }
    if (!isMachineOnlineImpl(daemonId)) return res.status(503).json({ error: 'Channel daemon offline' });

    const requestId = uuidv4Impl();
    const deviceId = defaultDeviceId(machine);
    const deviceName = defaultDeviceName(machine, deviceId);
    const payloadBody = {
      ...(req.body?.payload && typeof req.body.payload === 'object' ? req.body.payload : {}),
      status,
      device_id: deviceId,
      correlation_id: correlationId,
      ...(req.body?.result !== undefined ? { result: req.body.result } : {}),
      ...(req.body?.error !== undefined ? { error: req.body.error } : {}),
      ...(req.body?.reason !== undefined ? { reason: req.body.reason } : {}),
    };
    const result = await requestFromDaemonImpl(
      daemonId,
      {
        type: 'channel:message.send',
        requestId,
        channelId,
        senderKind: SenderKind.EXTERNAL,
        senderId: `external:device:${deviceId}`,
        payloadType,
        payloadBody,
        envelope: {
          sender: { kind: SenderKind.EXTERNAL, id: `external:device:${deviceId}`, name: deviceName },
        },
        content: JSON.stringify(payloadBody),
        correlationId,
        parentId: req.body?.parentId ?? req.body?.parent_id ?? null,
        audience: ['channel'],
        origin: 'external',
        attachments: [],
      },
      requestId,
      daemonRequestTimeoutMs,
    );

    if (!result?.ok) {
      return res.status(503).json({ error: result?.error ?? 'Channel daemon failed to persist device result' });
    }
    return res.status(202).json({
      ok: true,
      requestId,
      message: result.message ?? null,
    });
  });

  return router;
}

const router = createDeviceRouter();
export default router;
