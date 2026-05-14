import { Router } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { requireAuth } from '../middleware/auth.js';
import { encrypt } from '../crypto.js';
import {
  getDb,
  insertCredential, getCredentialsByOwner, getCredentialById, deleteCredential,
  insertCredentialGrant, revokeCredentialGrant, getGrantsByCredential,
  getAgentById, getMemberTeamIds, getTeamSession, getTeamById, getTeamMembers,
} from '../db/index.js';
import { isMachineOnline, sendToDaemon } from '../daemon/connections.js';

const router = Router();
router.use(requireAuth);

const DEFAULT_SERVER_ID = process.env.DEFAULT_SERVER_ID ?? 'server-001';

// ── 平台 → 所需 env key 的映射（供下发给 daemon 时使用）─────────────────────────
export const PLATFORM_ENV_KEYS = {
  x:          ['X_API_KEY', 'X_API_SECRET', 'X_ACCESS_TOKEN', 'X_ACCESS_SECRET'],
  'x-readonly': ['X_BEARER_TOKEN'],
  xhs:        ['XHS_COOKIE'],
  youtube:    ['YOUTUBE_API_KEY'],
  email:      ['SMTP_HOST', 'SMTP_PORT', 'SMTP_USER', 'SMTP_PASS', 'SMTP_FROM'],
  feishu:     ['FEISHU_APP_ID', 'FEISHU_APP_SECRET'],
  generic:    [],
};

function fmtCredential(c) {
  return {
    id: c.id,
    platform: c.platform,
    displayName: c.display_name,
    credentialType: c.credential_type,
    scopes: typeof c.scopes === 'string' ? JSON.parse(c.scopes) : (c.scopes ?? []),
    expiresAt: c.expires_at,
    createdAt: c.created_at,
    updatedAt: c.updated_at,
    // 不返回 encrypted_data / iv
  };
}

async function restartGrantedAgent(db, agentId) {
  const agent = await getAgentById(db, agentId);
  if (!agent || agent.is_del || agent.deleted_at || !agent.machine_id || !isMachineOnline(agent.machine_id)) return;

  const teamIds = await getMemberTeamIds(db, agent.id);
  for (const teamId of teamIds) {
    sendToDaemon(agent.machine_id, { type: 'agent:stop', agentId: agent.id, teamId });
  }

  await new Promise(resolve => setTimeout(resolve, 500));
  const serverUrl = process.env.SERVER_URL ?? `http://localhost:${process.env.PORT ?? 3001}`;

  for (const teamId of teamIds) {
    const sessionId = await getTeamSession(db, agent.id, teamId);
    const team = await getTeamById(db, teamId);
    const [memberRows] = await db.execute(
      `SELECT role_prompt FROM team_members WHERE team_id = ? AND member_id = ? AND is_del = 0`,
      [teamId, agent.id]
    );
    const rolePrompt = memberRows[0]?.role_prompt ?? '';
    sendToDaemon(agent.machine_id, {
      type: 'agent:start',
      agentId: agent.id,
      teamId,
      teamName: team?.name ?? teamId,
      config: {
        runtime: agent.runtime ?? 'claude',
        model: agent.model ?? null,
        sessionId: sessionId ?? null,
        name: agent.name,
        displayName: agent.display_name,
        description: agent.description ?? '',
        feishuBotName: agent.feishu_bot_name ?? null,
        rolePrompt,
        serverUrl,
        authToken: agent.agent_api_key ?? process.env.ADMIN_TOKEN ?? 'demo-token',
        envVars: agent.env_vars ? JSON.parse(agent.env_vars) : {},
      },
    });
  }
}

async function restartGranteeAgents(db, granteeType, granteeId) {
  if (granteeType === 'agent') {
    await restartGrantedAgent(db, granteeId);
    return;
  }
  if (granteeType !== 'team') return;
  const members = await getTeamMembers(db, granteeId);
  for (const member of members) {
    if (member.member_type === 'agent') await restartGrantedAgent(db, member.member_id);
  }
}

// ── GET / — 列出当前用户的凭证 ──────────────────────────────────────────────────
router.get('/', async (req, res) => {
  const creds = await getCredentialsByOwner(getDb(), req.user.id);
  res.json(creds.map(fmtCredential));
});

// ── POST / — 创建凭证（API Key 类型）──────────────────────────────────────────
router.post('/', async (req, res) => {
  const { platform, displayName, credentialType = 'api_key', fields } = req.body;
  if (!platform || !displayName || !fields || typeof fields !== 'object') {
    return res.status(400).json({ error: 'platform, displayName, fields required' });
  }

  const { iv, data } = encrypt(fields);
  const cred = await insertCredential(getDb(), {
    id: uuidv4(),
    serverId: DEFAULT_SERVER_ID,
    ownerId: req.user.id,
    platform,
    displayName,
    credentialType,
    encryptedData: data,
    iv,
    scopes: PLATFORM_ENV_KEYS[platform] ?? [],
  });
  res.status(201).json(fmtCredential(cred));
});

// ── DELETE /:id — 软删除凭证 ────────────────────────────────────────────────
router.delete('/:id', async (req, res) => {
  const cred = await getCredentialById(getDb(), req.params.id);
  if (!cred) return res.status(404).json({ error: 'Not found' });
  if (cred.owner_id !== req.user.id) return res.status(403).json({ error: 'Forbidden' });
  await deleteCredential(getDb(), req.params.id);
  res.json({ ok: true });
});

// ── GET /:id/grants — 列出授权 ──────────────────────────────────────────────
router.get('/:id/grants', async (req, res) => {
  const cred = await getCredentialById(getDb(), req.params.id);
  if (!cred) return res.status(404).json({ error: 'Not found' });
  if (cred.owner_id !== req.user.id) return res.status(403).json({ error: 'Forbidden' });
  const grants = await getGrantsByCredential(getDb(), req.params.id);
  res.json(grants.map(g => ({
    id: g.id,
    granteeType: g.grantee_type,
    granteeId: g.grantee_id,
    grantedBy: g.granted_by,
    createdAt: g.created_at,
  })));
});

// ── POST /:id/grants — 授权给 agent 或 team ─────────────────────────────────
router.post('/:id/grants', async (req, res) => {
  const { granteeType, granteeId } = req.body;
  if (!granteeType || !granteeId) return res.status(400).json({ error: 'granteeType and granteeId required' });
  if (!['agent', 'team'].includes(granteeType)) return res.status(400).json({ error: 'granteeType must be agent or team' });

  const db = getDb();
  const cred = await getCredentialById(db, req.params.id);
  if (!cred) return res.status(404).json({ error: 'Not found' });
  if (cred.owner_id !== req.user.id) return res.status(403).json({ error: 'Forbidden' });

  await insertCredentialGrant(db, {
    id: uuidv4(),
    credentialId: req.params.id,
    granteeType,
    granteeId,
    grantedBy: req.user.id,
  });
  await restartGranteeAgents(db, granteeType, granteeId);
  res.json({ ok: true });
});

// ── DELETE /:id/grants — 撤销授权 ───────────────────────────────────────────
router.delete('/:id/grants', async (req, res) => {
  const { granteeType, granteeId } = req.body;
  if (!granteeType || !granteeId) return res.status(400).json({ error: 'granteeType and granteeId required' });

  const cred = await getCredentialById(getDb(), req.params.id);
  if (!cred) return res.status(404).json({ error: 'Not found' });
  if (cred.owner_id !== req.user.id) return res.status(403).json({ error: 'Forbidden' });

  await revokeCredentialGrant(getDb(), req.params.id, granteeType, granteeId);
  await restartGranteeAgents(getDb(), granteeType, granteeId);
  res.json({ ok: true });
});

export default router;
