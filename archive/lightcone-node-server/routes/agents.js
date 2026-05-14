import { Router } from 'express';
import { v4 as uuidv4 } from 'uuid';
import {
  getDb, getAgents, getAgentById, insertAgent, updateAgent,
  addTeamMember, getTeams, getMemberTeamIds, removeTeamMember,
  deleteAgentTeamSessions, getTeamSession, getSkillsForAgent,
} from '../db/index.js';
import { broadcast } from '../realtime/broadcast.js';
import { isMachineOnline, sendToDaemon } from '../daemon/connections.js';
import { requestFromDaemon } from '../daemon/index.js';
import { requireAuth } from '../middleware/auth.js';
import { checkQuota } from '../plans.js';
import { nowMysqlDatetime } from '../time.js';

const router = Router();
const DEFAULT_SERVER_ID = process.env.DEFAULT_SERVER_ID ?? 'server-001';

router.use(requireAuth);

function fmtAgent(a) {
  return {
    id: a.id, serverId: a.server_id, machineId: a.machine_id ?? null,
    name: a.name, displayName: a.display_name, description: a.description ?? '',
    model: a.model ?? 'sonnet', runtime: a.runtime ?? 'claude',
    reasoningEffort: a.reasoning_effort ?? null, status: a.status ?? 'inactive',
    activity: a.activity ?? null, activityDetail: a.activity_detail ?? null,
    sessionId: a.session_id ?? null,
    envVars: a.env_vars ? JSON.parse(a.env_vars) : {},
    feishuAppId: a.feishu_app_id ?? null,
    feishuAppSecret: a.feishu_app_secret ?? null,
    feishuVerificationToken: a.feishu_verification_token ?? null,
    feishuTeamId: a.feishu_team_id ?? null,
    feishuBotName: a.feishu_bot_name ?? null,
    hosted: !!a.hosted,
    createdAt: a.created_at, deletedAt: a.deleted_at ?? null,
  };
}

router.get('/', async (req, res) => {
  const agents = (await getAgents(getDb(), DEFAULT_SERVER_ID, req.user.id)).filter(a => !a.is_del && !a.deleted_at);
  res.json(agents.map(fmtAgent));
});

router.post('/', async (req, res) => {
  const { name, displayName, description, model, runtime, reasoningEffort,
          machineId, envVars, teamIds, hosted } = req.body;
  if (!name) return res.status(400).json({ error: 'name required' });
  const db = getDb();

  // ── Name uniqueness (same owner, not soft-deleted) ──
  const [dup] = await db.execute(
    `SELECT id FROM agents WHERE owner_id = ? AND name = ? AND is_del = 0 AND deleted_at IS NULL LIMIT 1`,
    [req.user.id, name]
  );
  if (dup.length) return res.status(409).json({ error: `agent name '${name}' already exists` });

  // ── Preflight: ask daemon to actually spawn the CLI with this runtime/model ──
  // Only when machine is online; hosted/offline cases skip (nothing to probe against).
  if (machineId && isMachineOnline(machineId)) {
    const requestId = uuidv4();
    try {
      const result = await requestFromDaemon(
        machineId,
        { type: 'runtime:preflight', runtime, model: model ?? null, requestId },
        requestId,
        20000
      );
      if (!result?.ok) {
        return res.status(400).json({
          error: `preflight failed for runtime '${runtime}'${model ? ` model '${model}'` : ''}: ${result?.error ?? 'unknown error'}`,
        });
      }
    } catch (e) {
      return res.status(400).json({
        error: `preflight timeout or daemon unreachable: ${e.message}`,
      });
    }
  }

  // ── Quota checks ──
  const agentQuota = await checkQuota(db, DEFAULT_SERVER_ID, 'agents', req.user.id);
  if (!agentQuota.allowed)
    return res.status(403).json({ error: `Agent limit reached (${agentQuota.current}/${agentQuota.limit}). Upgrade your plan.` });

  // Resolve hosted agent → platform machine
  let resolvedMachineId = machineId;
  let isHosted = !!hosted;
  if (isHosted) {
    const hostedQuota = await checkQuota(db, DEFAULT_SERVER_ID, 'hostedAgents', req.user.id);
    if (!hostedQuota.allowed)
      return res.status(403).json({ error: `Hosted agent limit reached (${hostedQuota.current}/${hostedQuota.limit}). Upgrade your plan.` });
    const [platformRows] = await db.execute(
      `SELECT id FROM machines WHERE server_id = ? AND is_platform = 1 LIMIT 1`, [DEFAULT_SERVER_ID]
    );
    if (platformRows.length === 0)
      return res.status(400).json({ error: 'Platform hosting is not available on this server.' });
    resolvedMachineId = platformRows[0].id;
  }

  const agent = await insertAgent(db, {
    id: uuidv4(), serverId: DEFAULT_SERVER_ID, ownerId: req.user.id,
    name, displayName: displayName ?? name, description,
    model, runtime, reasoningEffort, machineId: resolvedMachineId, envVars,
    hosted: isHosted,
  });
  let teams = teamIds?.length ? teamIds : [];
  if (!teams.length) {
    const [rows] = await db.execute(
      `SELECT c.id FROM teams c
       JOIN team_members cm ON cm.team_id = c.id AND cm.member_id = ? AND cm.member_type = 'user'
       WHERE c.name = 'default' AND c.type = 'team'
         AND c.is_del = 0 AND c.deleted_at IS NULL AND cm.is_del = 0 LIMIT 1`,
      [req.user.id]
    );
    if (rows[0]) teams = [rows[0].id];
  }
  for (const cid of teams) await addTeamMember(db, cid, agent.id, 'agent');
  broadcast.agentCreated(DEFAULT_SERVER_ID, fmtAgent(agent));

  // Auto-start agent if machine is online
  const startMachineId = resolvedMachineId || machineId;
  if (startMachineId && isMachineOnline(startMachineId)) {
    const serverUrl = process.env.SERVER_URL ?? `http://localhost:${process.env.PORT ?? 3001}`;
    for (const cid of teams) {
      sendToDaemon(resolvedMachineId, {
        type: 'agent:start', agentId: agent.id, teamId: cid,
        config: {
          runtime: agent.runtime ?? 'claude', model: agent.model ?? null,
          sessionId: null, name: agent.name,
          displayName: agent.display_name, description: agent.description ?? '',
          feishuBotName: agent.feishu_bot_name ?? null,
          serverUrl, authToken: process.env.ADMIN_TOKEN ?? 'demo-token',
          envVars: agent.env_vars ? JSON.parse(agent.env_vars) : {},
        },
      });
    }
  }

  res.json(fmtAgent(agent));
});

// Owner check: only owner or service callers can access
async function requireOwner(req, res) {
  const agent = await getAgentById(getDb(), req.params.id);
  if (!agent || agent.is_del || agent.deleted_at) { res.status(404).json({ error: 'Agent not found' }); return null; }
  if (!req.isService && agent.owner_id && agent.owner_id !== req.user.id) {
    res.status(403).json({ error: 'Not your agent' }); return null;
  }
  return agent;
}

router.get('/:id', async (req, res) => {
  const agent = await requireOwner(req, res);
  if (!agent) return;
  res.json(fmtAgent(agent));
});

router.patch('/:id', async (req, res) => {
  const { name, displayName, description, model, runtime, reasoningEffort, machineId, envVars, hosted,
          feishuAppId, feishuAppSecret, feishuVerificationToken, feishuTeamId, feishuBotName } = req.body;
  const db = getDb();
  const fields = {};
  if (name != null)                       fields.name = name;
  if (displayName != null)                fields.display_name = displayName;
  if (description != null)                fields.description = description;
  if (feishuAppId !== undefined)          fields.feishu_app_id = feishuAppId || null;
  if (feishuAppSecret !== undefined)      fields.feishu_app_secret = feishuAppSecret || null;
  if (feishuVerificationToken !== undefined) fields.feishu_verification_token = feishuVerificationToken || null;
  if (feishuTeamId !== undefined)          fields.feishu_team_id = feishuTeamId || null;
  if (feishuBotName !== undefined)        fields.feishu_bot_name = feishuBotName || null;
  if (model != null)                 fields.model = model;
  if (runtime != null)               fields.runtime = runtime;
  if (reasoningEffort !== undefined)  fields.reasoning_effort = reasoningEffort;
  if (envVars != null)               fields.env_vars = JSON.stringify(envVars);

  // Handle hosted ↔ self-hosted switching
  if (hosted !== undefined) {
    if (hosted) {
      const existing = await getAgentById(db, req.params.id);
      if (existing && !existing.hosted) {
        const hostedQuota = await checkQuota(db, DEFAULT_SERVER_ID, 'hostedAgents', req.user.id);
        if (!hostedQuota.allowed)
          return res.status(403).json({ error: `Hosted agent limit reached (${hostedQuota.current}/${hostedQuota.limit}). Upgrade your plan.` });
      }
      const [platformRows] = await db.execute(
        `SELECT id FROM machines WHERE server_id = ? AND is_platform = 1 LIMIT 1`, [DEFAULT_SERVER_ID]
      );
      if (platformRows.length === 0)
        return res.status(400).json({ error: 'Platform hosting is not available on this server.' });
      fields.hosted = 1;
      fields.machine_id = platformRows[0].id;
    } else {
      fields.hosted = 0;
      if (machineId !== undefined) fields.machine_id = machineId;
    }
  } else if (machineId !== undefined) {
    fields.machine_id = machineId;
  }
  // Owner check (service callers bypass for feishuBotName sync etc.)
  if (!req.isService) {
    const existing = await getAgentById(db, req.params.id);
    if (!existing || existing.is_del || existing.deleted_at) return res.status(404).json({ error: 'Agent not found' });
    if (existing.owner_id && existing.owner_id !== req.user.id) return res.status(403).json({ error: 'Not your agent' });
  }
  // Check if runtime-affecting config changed — need to restart agent on daemon
  const restartFields = ['runtime', 'model', 'reasoning_effort', 'env_vars', 'machine_id'];
  const needsRestart = restartFields.some(f => f in fields);
  const oldAgent = needsRestart ? await getAgentById(db, req.params.id) : null;

  const agent = await updateAgent(db, req.params.id, fields);
  if (!agent) return res.status(404).json({ error: 'Agent not found' });
  broadcast.agentCreated(DEFAULT_SERVER_ID, fmtAgent(agent));

  // Restart agent on daemon if config changed
  if (needsRestart && agent.machine_id && isMachineOnline(agent.machine_id)) {
    const teamIds = await getMemberTeamIds(db, agent.id);
    // Stop all running instances
    for (const teamId of teamIds) {
      sendToDaemon(agent.machine_id, { type: 'agent:stop', agentId: agent.id, teamId });
    }
    // If machine changed, also stop on the old machine
    if (oldAgent && oldAgent.machine_id !== agent.machine_id && isMachineOnline(oldAgent.machine_id)) {
      for (const teamId of teamIds) {
        sendToDaemon(oldAgent.machine_id, { type: 'agent:stop', agentId: agent.id, teamId });
      }
    }
    // Re-start with new config
    const serverUrl = process.env.SERVER_URL ?? `http://localhost:${process.env.PORT ?? 3001}`;
    for (const teamId of teamIds) {
      const sessionId = await getTeamSession(db, agent.id, teamId);
      sendToDaemon(agent.machine_id, {
        type: 'agent:start', agentId: agent.id, teamId,
        config: {
          runtime: agent.runtime ?? 'claude', model: agent.model ?? null,
          sessionId: sessionId ?? null, name: agent.name,
          displayName: agent.display_name, description: agent.description ?? '',
          feishuBotName: agent.feishu_bot_name ?? null,
          serverUrl, authToken: process.env.ADMIN_TOKEN ?? 'demo-token',

          envVars: agent.env_vars ? JSON.parse(agent.env_vars) : {},
        },
      });
    }
  }

  res.json(fmtAgent(agent));
});

router.delete('/:id', async (req, res) => {
  const agent = await requireOwner(req, res);
  if (!agent) return;
  const db = getDb();

  // Stop daemon process
  if (agent.machine_id && isMachineOnline(agent.machine_id))
    sendToDaemon(agent.machine_id, { type: 'agent:stop', agentId: agent.id });

  // Cascade: remove from all teams
  const teamIds = await getMemberTeamIds(db, agent.id);
  for (const cid of teamIds) await removeTeamMember(db, cid, agent.id);

  // Cascade: clean agent_team_sessions
  await deleteAgentTeamSessions(db, agent.id);

  // Soft delete
  await updateAgent(db, req.params.id, { is_del: 1, deleted_at: nowMysqlDatetime(), status: 'inactive' });
  broadcast.agentDeleted(DEFAULT_SERVER_ID, req.params.id);
  res.json({ ok: true });
});

router.post('/:id/reset-sessions', async (req, res) => {
  const agent = await requireOwner(req, res);
  if (!agent) return;
  const db = getDb();
  // Stop the agent first if running
  if (agent.machine_id && isMachineOnline(agent.machine_id))
    sendToDaemon(agent.machine_id, { type: 'agent:stop', agentId: agent.id });
  // Clear all team sessions and the agent-level session
  await db.execute(
    `UPDATE agent_team_sessions
     SET is_del = 1, deleted_at = COALESCE(deleted_at, NOW())
     WHERE agent_id = ? AND is_del = 0`,
    [agent.id]
  );
  await updateAgent(db, agent.id, { session_id: null });
  res.json({ ok: true });
});

router.post('/:id/start', async (req, res) => {
  const agent = await requireOwner(req, res);
  if (!agent) return;
  const db = getDb();
  if (!agent.machine_id) return res.status(400).json({ error: 'Agent has no machine assigned' });
  if (!isMachineOnline(agent.machine_id)) return res.status(400).json({ error: 'Machine is offline' });
  const serverUrl = process.env.SERVER_URL ?? `http://localhost:${process.env.PORT ?? 3001}`;
  sendToDaemon(agent.machine_id, {
    type: 'agent:start', agentId: agent.id,
    config: {
      runtime: agent.runtime ?? 'claude', model: agent.model ?? null,
      sessionId: agent.session_id ?? null, name: agent.name,
      displayName: agent.display_name, description: agent.description ?? '',
      feishuBotName: agent.feishu_bot_name ?? null,
      serverUrl, authToken: process.env.ADMIN_TOKEN ?? 'demo-token',
      envVars: agent.env_vars ? JSON.parse(agent.env_vars) : {},
    },
  });
  res.json({ ok: true });
});

router.post('/:id/stop', async (req, res) => {
  const agent = await requireOwner(req, res);
  if (!agent) return;
  const db = getDb();
  if (!agent.machine_id) return res.status(400).json({ error: 'Agent has no machine assigned' });
  if (isMachineOnline(agent.machine_id))
    sendToDaemon(agent.machine_id, { type: 'agent:stop', agentId: agent.id });
  res.json({ ok: true });
});

router.get('/:id/workspace-files', async (req, res) => {
  const agent = await requireOwner(req, res);
  if (!agent) return;
  if (!agent.machine_id || !isMachineOnline(agent.machine_id)) return res.json({ files: [] });
  try {
    res.json((await requestFromDaemon(agent.machine_id, { type: 'workspace:list', agentId: agent.id })) ?? { files: [] });
  } catch { res.json({ files: [] }); }
});

router.get('/:id/skills', async (req, res) => {
  const agent = await requireOwner(req, res);
  if (!agent) return;
  const skills = await getSkillsForAgent(getDb(), agent.id, agent.owner_id);
  const platform = skills.filter(s => s.type === 'platform').map(fmtSkillIndex);
  const user = skills.filter(s => s.type === 'user').map(fmtSkillIndex);
  res.json({ platform, user });
});

function fmtSkillIndex(s) {
  return {
    id: s.id, name: s.name, type: s.type, description: s.description,
    tags: typeof s.tags === 'string' ? JSON.parse(s.tags) : (s.tags ?? []),
  };
}

export default router;
