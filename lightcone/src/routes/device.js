import { Router } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { PayloadType, SenderKind } from '@coagent/payload-types';
import { getDb, getChannelById, getMachineByApiKey } from '../db/index.js';
import { isMachineOnline } from '../daemon/connections.js';
import { requestFromDaemon } from '../daemon/index.js';

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

function defaultDeviceId(machine, body) {
  return String(body.deviceId ?? body.device_id ?? machine?.id ?? 'unknown').trim();
}

export function createDeviceRouter({
  getDbImpl = getDb,
  getChannelByIdImpl = getChannelById,
  getMachineByApiKeyImpl = getMachineByApiKey,
  isMachineOnlineImpl = isMachineOnline,
  requestFromDaemonImpl = requestFromDaemon,
  uuidv4Impl = uuidv4,
  daemonRequestTimeoutMs = 10_000,
} = {}) {
  const router = Router();

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
    if (!isMachineOnlineImpl(daemonId)) return res.status(503).json({ error: 'Channel daemon offline' });

    const requestId = uuidv4Impl();
    const deviceId = defaultDeviceId(machine, req.body ?? {});
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
        senderType: 'external',
        senderKind: SenderKind.EXTERNAL,
        senderId: `external:device:${deviceId}`,
        senderName: req.body?.deviceName ?? req.body?.device_name ?? deviceId,
        messageType: payloadType,
        payloadType,
        payloadBody,
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
