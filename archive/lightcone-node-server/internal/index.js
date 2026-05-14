import { Router } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { PayloadType, SenderKind } from '@coagent/payload-types';
import { nowMysqlDatetime } from '../time.js';
import {
  getDb,
  getAgentById,
  getMachineByApiKey,
  getAgentByApiKey,
  getTeamById,
  insertMessage,
  getCredentialGrantsForAgent,
  insertPendingAction,
  getPendingActionById,
  updatePendingAction,
  insertActionLog,
} from '../db/index.js';
import { decrypt } from '../crypto.js';
import { broadcast } from '../realtime/broadcast.js';

const router = Router();

router.use(async (req, res, next) => {
  const auth = req.headers.authorization ?? '';
  const token = auth.startsWith('Bearer ') ? auth.slice(7) : null;
  if (!token) return res.status(401).json({ error: 'Missing Authorization header' });

  if (token === process.env.ADMIN_TOKEN) {
    req.machine = { id: 'admin', server_id: process.env.DEFAULT_SERVER_ID ?? 'server-001' };
    return next();
  }

  if (token.startsWith('sk_agent_')) {
    const agent = await getAgentByApiKey(getDb(), token);
    if (!agent) return res.status(401).json({ error: 'Invalid agent API key' });
    req.machine = { id: 'admin', server_id: process.env.DEFAULT_SERVER_ID ?? 'server-001' };
    req.callerAgentId = agent.id;
    return next();
  }

  const machine = await getMachineByApiKey(getDb(), token);
  if (!machine) return res.status(401).json({ error: 'Invalid authentication: machine API key required' });
  req.machine = machine;
  return next();
});

async function requireAgent(req, res) {
  const agent = await getAgentById(getDb(), req.params.agentId);
  if (!agent || agent.is_del || agent.deleted_at) {
    res.status(404).json({ error: 'Agent not found' });
    return null;
  }
  if (req.callerAgentId && req.callerAgentId !== agent.id) {
    res.status(403).json({ error: 'Agent token can only access own endpoints' });
    return null;
  }
  if (!req.callerAgentId && req.machine.id !== 'admin' && agent.machine_id !== req.machine.id) {
    res.status(403).json({ error: 'Agent does not belong to this machine' });
    return null;
  }
  return agent;
}

// Platform MCP support remains until the V0.5 approval/credential milestone.
router.post('/:agentId/rate-check', async (req, res) => {
  const agent = await requireAgent(req, res);
  if (!agent) return;
  const { platform, credentialId, windowMinutes = 15, maxActions = 10 } = req.body;
  if (!platform) return res.status(400).json({ error: 'platform required' });

  const db = getDb();
  const windowStart = nowMysqlDatetime(Date.now() - windowMinutes * 60 * 1000);
  const [rows] = await db.execute(
    `SELECT COUNT(*) AS c FROM platform_action_log
     WHERE credential_id = ? AND platform = ? AND status = 'ok' AND executed_at >= ?`,
    [credentialId ?? '', platform, windowStart],
  );
  const count = rows[0].c;
  if (count >= maxActions) {
    return res.json({ allowed: false, current: count, limit: maxActions, retry_after: windowMinutes * 60 });
  }
  return res.json({ allowed: true, current: count, limit: maxActions });
});

router.post('/:agentId/action-log', async (req, res) => {
  const agent = await requireAgent(req, res);
  if (!agent) return;
  const { platform, actionType, credentialId, payload, result, status, error } = req.body;
  if (!platform || !actionType || !status) return res.status(400).json({ error: 'platform, actionType, status required' });

  await insertActionLog(getDb(), {
    id: uuidv4(),
    credentialId: credentialId ?? '',
    agentId: agent.id,
    teamId: null,
    platform,
    actionType,
    payload: payload ? JSON.stringify(payload).slice(0, 2000) : null,
    result: result ? JSON.stringify(result).slice(0, 2000) : null,
    status,
    error: error ?? null,
  });
  return res.json({ ok: true });
});

router.get('/:agentId/credential-grants', async (req, res) => {
  const agent = await requireAgent(req, res);
  if (!agent) return;
  const grants = await getCredentialGrantsForAgent(getDb(), agent.id);
  const result = grants.map((grant) => {
    let fields = {};
    try {
      fields = decrypt(grant.iv, grant.encrypted_data);
    } catch {
      // Malformed grants are ignored; credential cleanup is outside this ticket.
    }
    return { credentialId: grant.id, platform: grant.platform, envVars: fields };
  });
  return res.json(result);
});

router.post('/:agentId/actions/request', async (req, res) => {
  const agent = await requireAgent(req, res);
  if (!agent) return;
  const { action_type, platform, description, payload, credential_id, team_id } = req.body;
  if (!action_type || !description || !payload) {
    return res.status(400).json({ error: 'action_type, description, payload required' });
  }

  const db = getDb();
  const action = await insertPendingAction(db, {
    id: uuidv4(),
    agentId: agent.id,
    teamId: team_id ?? null,
    actionType: action_type,
    platform: platform ?? null,
    description,
    payload: typeof payload === 'string' ? payload : JSON.stringify(payload),
    credentialId: credential_id ?? null,
    idempotencyKey: null,
  });

  if (team_id) {
    const team = await getTeamById(db, team_id);
    if (team) {
      const msg = await insertMessage(db, {
        id: uuidv4(),
        teamId: team_id,
        senderKind: SenderKind.AGENT,
        senderId: agent.id,
        payloadType: PayloadType.SYSTEM_NOTICE,
        payloadBody: {
          action_id: action.id,
          action_type,
          platform: platform ?? null,
          description,
        },
        content: JSON.stringify({
          actionId: action.id,
          actionType: action_type,
          platform: platform ?? null,
          description,
          payload: typeof payload === 'string' ? JSON.parse(payload) : payload,
        }),
      });
      broadcast.message(team_id, formatMessage(msg));
    }
  }

  broadcast.actionUpdated(action);
  return res.json({ id: action.id, status: 'pending' });
});

router.post('/:agentId/actions/:actionId/execute', async (req, res) => {
  const agent = await requireAgent(req, res);
  if (!agent) return;

  const action = await getPendingActionById(getDb(), req.params.actionId);
  if (!action) return res.status(404).json({ error: 'Action not found' });
  if (action.agent_id !== agent.id) return res.status(403).json({ error: 'Forbidden' });
  if (action.status !== 'approved') {
    return res.status(400).json({ error: `Action status is '${action.status}', must be 'approved' to execute` });
  }
  if (action.executed_at || action.status === 'executed') {
    return res.status(400).json({ error: 'Action already executed' });
  }

  let parsedPayload = {};
  try {
    parsedPayload = JSON.parse(action.payload);
  } catch {
    // Keep malformed historical payloads opaque.
  }

  return res.json({
    ok: true,
    actionId: action.id,
    actionType: action.action_type,
    platform: action.platform,
    credentialId: action.credential_id,
    teamId: action.team_id,
    payload: parsedPayload,
  });
});

router.post('/:agentId/actions/:actionId/complete', async (req, res) => {
  const agent = await requireAgent(req, res);
  if (!agent) return;

  const db = getDb();
  const action = await getPendingActionById(db, req.params.actionId);
  if (!action) return res.status(404).json({ error: 'Action not found' });
  if (action.agent_id !== agent.id) return res.status(403).json({ error: 'Forbidden' });
  if (action.status !== 'approved') {
    return res.status(400).json({ error: `Action status is '${action.status}', must be 'approved' to complete` });
  }
  if (action.executed_at) {
    return res.status(400).json({ error: 'Action already completed' });
  }

  const { ok, result, error } = req.body ?? {};
  const status = ok ? 'executed' : 'failed';
  await updatePendingAction(db, action.id, {
    status,
    executed_at: nowMysqlDatetime(),
    error: ok ? null : (error ?? 'Platform action failed'),
  });

  await insertActionLog(db, {
    id: uuidv4(),
    credentialId: action.credential_id ?? '',
    agentId: agent.id,
    teamId: action.team_id,
    platform: action.platform ?? '',
    actionType: action.action_type,
    payload: action.payload,
    result: result ? JSON.stringify(result).slice(0, 2000) : null,
    status,
    error: ok ? null : (error ?? 'Platform action failed'),
  });

  const updated = await getPendingActionById(db, action.id);
  broadcast.actionUpdated(updated);
  return res.json({ ok: true, status });
});

export function formatMessage(msg) {
  const parseJson = (value, fallback = null) => {
    if (value == null) return fallback;
    if (typeof value !== 'string') return value;
    try {
      return JSON.parse(value);
    } catch {
      return fallback;
    }
  };

  return {
    id: msg.id,
    seq: msg.seq,
    teamId: msg.team_id,
    channelId: msg.channel_id ?? null,
    senderId: msg.sender_id,
    senderName: parseJson(msg.envelope_json, {})?.sender?.name ?? msg.sender_id,
    content: msg.content,
    senderKind: msg.sender_kind,
    payloadType: msg.payload_type,
    payloadBody: parseJson(msg.payload_body, null),
    parentId: msg.parent_id ?? null,
    correlationId: msg.correlation_id ?? null,
    taskId: msg.task_id ?? null,
    threadId: msg.thread_id ?? null,
    audience: parseJson(msg.audience, null),
    notBefore: msg.not_before ?? null,
    origin: msg.origin ?? null,
    expiresAt: msg.expires_at ?? null,
    tsReceived: msg.ts_received ?? null,
    envelope: parseJson(msg.envelope_json, null),
    createdAt: msg.created_at,
    updatedAt: msg.updated_at,
    attachments: [],
  };
}

export async function parseMentions(db, content, teamId = null) {
  const matches = content.match(/@([^\s@]+)/g);
  if (!matches) return null;
  const names = [...new Set(matches.map((match) => match.slice(1)))];
  const ids = [];
  for (const name of names) {
    const query = teamId
      ? `SELECT a.id FROM agents a JOIN team_members cm ON cm.member_id = a.id AND cm.team_id = ?
         WHERE (a.name = ? OR a.display_name = ? OR a.feishu_bot_name = ?)
           AND a.is_del = 0 AND a.deleted_at IS NULL AND cm.is_del = 0`
      : `SELECT id FROM agents
         WHERE (name = ? OR display_name = ? OR feishu_bot_name = ?)
           AND is_del = 0 AND deleted_at IS NULL`;
    const params = teamId ? [teamId, name, name, name] : [name, name, name];
    const [rows] = await db.execute(query, params);
    if (rows[0]) ids.push(rows[0].id);
  }
  return ids;
}

export default router;
