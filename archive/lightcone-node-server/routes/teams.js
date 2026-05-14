import { Router } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { extname } from 'path';
import {
  getDb, getTeams, getTeamById, insertTeam, getTeamMembers,
  addTeamMember, removeTeamMember, updateTeamMemberRolePrompt, getAgentById, deleteMachine,
  createBindingCode, getBindingCode, getFeishuBindingByTeam, deleteFeishuBinding,
  deleteTeamSessions, listTeamWorkspaceFiles, getTeamWorkspaceFile, upsertTeamWorkspaceFile,
  deleteTeamWorkspaceFile, deleteTeamWorkspace, deleteTeamMemory,
} from '../db/index.js';
import { broadcast } from '../realtime/broadcast.js';
import { sendToDaemon, isMachineOnline } from '../daemon/connections.js';
import { checkQuota } from '../plans.js';

const router = Router();
const DEFAULT_SERVER_ID = process.env.DEFAULT_SERVER_ID ?? 'server-001';
const DEFAULT_USER_ID   = process.env.DEFAULT_USER_ID   ?? 'user-001';

async function fmtTeam(db, c, userId) {
  const members = await getTeamMembers(db, c.id);
  const joined = members.some(m => m.member_id === userId);
  return {
    id: c.id, serverId: c.server_id, name: c.name,
    description: c.description, type: c.type,
    parentMessageId: c.parent_message_id ?? null,
    joined, createdAt: c.created_at, deletedAt: c.deleted_at ?? null,
  };
}

router.get('/', async (req, res) => {
  const userId = req.user?.id ?? DEFAULT_USER_ID;
  const db = getDb();
  const all = (await getTeams(db, DEFAULT_SERVER_ID)).filter(c => c.type === 'team');
  const visible = [];
  for (const c of all) {
    const members = await getTeamMembers(db, c.id);
    if (members.some(m => m.member_id === userId)) {
      visible.push({ ...c, _members: members });
    }
  }
  res.json(visible.map(c => ({
    id: c.id, serverId: c.server_id, name: c.name,
    description: c.description, type: c.type,
    parentMessageId: c.parent_message_id ?? null,
    joined: true, createdAt: c.created_at, deletedAt: c.deleted_at ?? null,
  })));
});

router.post('/', async (req, res) => {
  const userId = req.user?.id ?? DEFAULT_USER_ID;
  const { name, description } = req.body;
  if (!name || !name.trim())
    return res.status(400).json({ error: 'Team name is required' });
  const db = getDb();
  const quota = await checkQuota(db, DEFAULT_SERVER_ID, 'teams', userId);
  if (!quota.allowed)
    return res.status(403).json({ error: `Team limit reached (${quota.current}/${quota.limit}). Upgrade your plan.` });
  const ch = await insertTeam(db, { id: uuidv4(), serverId: DEFAULT_SERVER_ID, ownerId: userId, name, description, type: 'team' });
  await addTeamMember(db, ch.id, userId, 'user');
  broadcast.teamUpdated(DEFAULT_SERVER_ID);
  res.json(await fmtTeam(db, ch, userId));
});

router.patch('/:id', async (req, res) => {
  const db = getDb();
  const { name, description } = req.body;
  if (name) await db.execute(`UPDATE teams SET name = ? WHERE id = ?`, [name, req.params.id]);
  if (description !== undefined) await db.execute(`UPDATE teams SET description = ? WHERE id = ?`, [description, req.params.id]);
  const ch = await getTeamById(db, req.params.id);
  broadcast.teamUpdated(DEFAULT_SERVER_ID);
  res.json(await fmtTeam(db, ch, req.user?.id ?? DEFAULT_USER_ID));
});

router.delete('/:id', async (req, res) => {
  const db = getDb();
  const teamId = req.params.id;
  const ch = await getTeamById(db, teamId);
  if (!ch || ch.is_del || ch.deleted_at) return res.status(404).json({ error: 'Team not found' });
  if (ch.name === 'default') return res.status(403).json({ error: 'The default team cannot be deleted' });

  // Cascade: stop agents and remove all members
  const members = await getTeamMembers(db, teamId);
  for (const m of members) {
    if (m.member_type === 'agent') {
      const agent = await getAgentById(db, m.member_id);
      if (agent?.machine_id && isMachineOnline(agent.machine_id))
        sendToDaemon(agent.machine_id, { type: 'agent:stop', agentId: agent.id, teamId });
    }
    await removeTeamMember(db, teamId, m.member_id);
  }

  // Cascade: clean sessions and bindings
  await deleteTeamSessions(db, teamId);
  await deleteTeamMemory(db, teamId);
  await deleteTeamWorkspace(db, teamId);
  await deleteFeishuBinding(db, teamId);

  // Soft delete team and its messages
  await db.execute(
    `UPDATE messages
     SET is_del = 1, deleted_at = COALESCE(deleted_at, NOW())
     WHERE team_id = ? AND is_del = 0`,
    [teamId]
  );
  await db.execute(
    `UPDATE teams
     SET is_del = 1, deleted_at = COALESCE(deleted_at, NOW())
     WHERE id = ? AND is_del = 0`,
    [teamId]
  );
  broadcast.teamUpdated(DEFAULT_SERVER_ID);
  res.json({ ok: true });
});

router.get('/dm', async (req, res) => {
  const db = getDb();
  const [dms] = await db.execute(
    `SELECT c.*, cm.member_id as peer_id FROM teams c
     JOIN team_members cm ON cm.team_id = c.id AND cm.member_type = 'agent'
     WHERE c.server_id = ? AND c.type = 'dm'
       AND c.is_del = 0 AND c.deleted_at IS NULL AND cm.is_del = 0`,
    [DEFAULT_SERVER_ID]
  );
  const result = await Promise.all(dms.map(async dm => {
    const peer = await getAgentById(db, dm.peer_id);
    return {
      id: dm.id, name: dm.name, type: 'dm',
      peerId: dm.peer_id, peerName: peer?.name ?? dm.name,
      peerDisplayName: peer?.display_name ?? dm.name,
      peerAvatarUrl: null, peerType: 'agent', createdAt: dm.created_at,
    };
  }));
  res.json(result);
});

router.post('/dm', async (req, res) => {
  const { agentId } = req.body;
  if (!agentId) return res.status(400).json({ error: 'agentId required' });
  const db = getDb();
  const agent = await getAgentById(db, agentId);
  if (!agent) return res.status(404).json({ error: 'Agent not found' });

  const [existing] = await db.execute(
    `SELECT c.* FROM teams c
     JOIN team_members cm ON cm.team_id = c.id AND cm.member_id = ? AND cm.member_type = 'agent'
     WHERE c.server_id = ? AND c.type = 'dm'
       AND c.is_del = 0 AND c.deleted_at IS NULL AND cm.is_del = 0`,
    [agentId, DEFAULT_SERVER_ID]
  );
  const userId = req.user?.id ?? DEFAULT_USER_ID;
  if (existing[0]) return res.json({ ...(await fmtTeam(db, existing[0], userId)), peerId: agentId });

  const ch = await insertTeam(db, { id: uuidv4(), serverId: DEFAULT_SERVER_ID, name: agent.name, type: 'dm' });
  await addTeamMember(db, ch.id, userId, 'user');
  await addTeamMember(db, ch.id, agentId, 'agent');
  broadcast.dmNew(DEFAULT_SERVER_ID, ch.id);
  res.json({ ...(await fmtTeam(db, ch, userId)), peerId: agentId });
});

router.get('/unread', (req, res) => res.json({}));
router.get('/threads/followed', (req, res) => res.json({ threads: [] }));
router.post('/threads/follow', (req, res) => res.json({ ok: true }));
router.post('/threads/unfollow', (req, res) => res.json({ ok: true }));

router.get('/:id/members', async (req, res) => {
  const db = getDb();
  const members = await getTeamMembers(db, req.params.id);
  const agents = [], humans = [];
  for (const m of members) {
    if (m.member_type === 'agent') {
      const a = await getAgentById(db, m.member_id);
      if (a && !a.is_del && !a.deleted_at)
        agents.push({ id: a.id, name: a.name, displayName: a.display_name, status: a.status, avatarUrl: null, rolePrompt: m.role_prompt ?? null });
    } else {
      const [urows] = await db.execute(`SELECT * FROM users WHERE id = ?`, [m.member_id]);
      const u = urows[0];
      if (u) humans.push({ id: u.id, name: u.name, displayName: u.name, avatarUrl: u.avatar ?? null, email: '' });
    }
  }
  res.json({ agents, humans });
});

router.post('/:id/members', async (req, res) => {
  const { agentId } = req.body;
  if (!agentId) return res.status(400).json({ error: 'agentId required' });
  const db = getDb();
  const agent = await getAgentById(db, agentId);
  if (!agent || agent.is_del || agent.deleted_at) return res.status(400).json({ error: 'Agent not found in this server' });
  await addTeamMember(db, req.params.id, agentId, 'agent');

  if (agent.machine_id && isMachineOnline(agent.machine_id)) {
    const { getTeamSession } = await import('../db/index.js');
    const sessionId = await getTeamSession(db, agentId, req.params.id);
    const serverUrl = process.env.SERVER_URL ?? `http://localhost:${process.env.PORT ?? 3001}`;
    sendToDaemon(agent.machine_id, {
      type: 'agent:start', agentId, teamId: req.params.id,
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

  res.json({ ok: true });
});

router.patch('/:id/members/agent/:agentId', async (req, res) => {
  const { rolePrompt } = req.body;
  const db = getDb();
  await updateTeamMemberRolePrompt(db, req.params.id, req.params.agentId, rolePrompt ?? null);
  // Restart agent with new role prompt
  const agent = await getAgentById(db, req.params.agentId);
  if (agent && agent.machine_id && isMachineOnline(agent.machine_id)) {
    const { getTeamSession } = await import('../db/index.js');
    const serverUrl = process.env.SERVER_URL ?? `http://localhost:${process.env.PORT ?? 3001}`;
    const sessionId = await getTeamSession(db, req.params.agentId, req.params.id);
    sendToDaemon(agent.machine_id, {
      type: 'agent:start', agentId: req.params.agentId, teamId: req.params.id,
      config: {
        runtime: agent.runtime ?? 'claude', model: agent.model ?? null,
        sessionId: sessionId ?? null, name: agent.name,
        displayName: agent.display_name, description: agent.description ?? '',
        feishuBotName: agent.feishu_bot_name ?? null,
        rolePrompt: rolePrompt ?? null,
        serverUrl, authToken: process.env.ADMIN_TOKEN ?? 'demo-token',
        envVars: agent.env_vars ? JSON.parse(agent.env_vars) : {},
      },
    });
  }
  res.json({ ok: true });
});

router.delete('/:id/members/agent/:agentId', async (req, res) => {
  await removeTeamMember(getDb(), req.params.id, req.params.agentId);
  res.json({ ok: true });
});

router.delete('/:id/members/user/:userId', async (req, res) => {
  await removeTeamMember(getDb(), req.params.id, req.params.userId);
  res.json({ ok: true });
});

router.post('/:id/join', async (req, res) => {
  const userId = req.user?.id ?? DEFAULT_USER_ID;
  await addTeamMember(getDb(), req.params.id, userId, 'user');
  res.json({ ok: true });
});

router.post('/:id/leave', async (req, res) => {
  const userId = req.user?.id ?? DEFAULT_USER_ID;
  const db = getDb();
  const ch = await getTeamById(db, req.params.id);
  if (ch?.name === 'default') return res.status(400).json({ error: 'Cannot leave the default team' });
  await removeTeamMember(db, req.params.id, userId);
  res.json({ ok: true });
});

router.post('/:id/read', (req, res) => {
  if (!req.body.seq) return res.status(400).json({ error: 'seq is required' });
  res.json({ ok: true });
});
router.post('/:id/read-all', (req, res) => res.json({ ok: true }));
router.post('/:id/unread', (req, res) => {
  if (!req.body.seq) return res.status(400).json({ error: 'seq is required' });
  res.json({ ok: true });
});
router.get('/:id/threads', (req, res) => res.json({}));
router.post('/:id/threads', (req, res) => res.json({ ok: true }));

router.post('/:id/stop-all-agents', async (req, res) => {
  const db = getDb();
  const members = (await getTeamMembers(db, req.params.id)).filter(m => m.member_type === 'agent');
  const results = [];
  for (const m of members) {
    const agent = await getAgentById(db, m.member_id);
    if (!agent || !agent.machine_id) continue;
    const ok = isMachineOnline(agent.machine_id)
      ? sendToDaemon(agent.machine_id, { type: 'agent:stop', agentId: agent.id })
      : false;
    results.push({ agentId: agent.id, ok });
  }
  res.json({ ok: true, stopped: results.filter(r => r.ok).length, total: results.length, results });
});

router.post('/:id/resume-all-agents', (req, res) => res.json({ ok: true }));

// ── Feishu binding ────────────────────────────────────────────────────────────

router.get('/:id/feishu-binding', async (req, res) => {
  const db = getDb();
  const binding = await getFeishuBindingByTeam(db, req.params.id);
  const code    = await getBindingCode(db, req.params.id);
  res.json({
    bound: !!binding,
    chatId: binding?.chat_id ?? null,
    agentId: binding?.agent_id ?? null,
    bindingCode: code?.code ?? null,
    codeExpiresAt: code?.expires_at ?? null,
  });
});

router.post('/:id/feishu-code', async (req, res) => {
  const { agentId } = req.body;
  if (!agentId) return res.status(400).json({ error: 'agentId required' });
  const db = getDb();
  const agent = await getAgentById(db, agentId);
  if (!agent || agent.is_del || agent.deleted_at) return res.status(404).json({ error: 'Agent not found' });
  const code = await createBindingCode(db, req.params.id, agentId);
  const record = await getBindingCode(db, req.params.id);
  res.json({ code, expiresAt: record.expires_at });
});

router.delete('/:id/feishu-binding', async (req, res) => {
  await deleteFeishuBinding(getDb(), req.params.id);
  res.json({ ok: true });
});

// ── Team BRIEF.md (stored in team_workspace DB table) ────────────────────────

router.get('/:id/brief', async (req, res) => {
  const db = getDb();
  const content = await getTeamWorkspaceFile(db, req.params.id, 'BRIEF.md') ?? '';
  res.json({ content });
});

router.put('/:id/brief', async (req, res) => {
  const { content } = req.body;
  if (typeof content !== 'string') return res.status(400).json({ error: 'content (string) required' });
  const db = getDb();
  await upsertTeamWorkspaceFile(db, req.params.id, 'BRIEF.md', content);
  res.json({ ok: true });
});

// ── Team workspace file listing & serving ────────────────────────────────────

const IMAGE_EXTS = new Set(['.png', '.jpg', '.jpeg', '.gif', '.webp', '.svg']);
const TEXT_EXTS  = new Set(['.md', '.txt', '.json', '.html', '.css', '.js', '.ts', '.csv', '.xml', '.yaml', '.yml']);
const MIME_MAP   = {
  '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
  '.gif': 'image/gif', '.webp': 'image/webp', '.svg': 'image/svg+xml',
  '.pdf': 'application/pdf', '.html': 'text/html; charset=utf-8',
  '.md': 'text/plain; charset=utf-8', '.txt': 'text/plain; charset=utf-8',
  '.json': 'application/json', '.css': 'text/css', '.js': 'text/javascript',
};

router.get('/:id/workspace-files', async (req, res) => {
  const db = getDb();
  const rows = await listTeamWorkspaceFiles(db, req.params.id);
  const files = rows.map(r => {
    const ext = extname(r.path).toLowerCase();
    return {
      path: r.path,
      name: r.path.split('/').pop(),
      type: IMAGE_EXTS.has(ext) ? 'image' : TEXT_EXTS.has(ext) ? 'text' : 'binary',
      ext,
      modifiedAt: new Date(r.updated_at).getTime(),
    };
  });
  res.json({ files });
});

router.get('/:id/workspace-file', async (req, res) => {
  const filePath = req.query.path;
  if (!filePath || filePath.includes('..')) return res.status(400).json({ error: 'Invalid path' });
  const db = getDb();
  const content = await getTeamWorkspaceFile(db, req.params.id, filePath);
  if (content == null) return res.status(404).json({ error: 'Not found' });
  const ext = extname(filePath).toLowerCase();
  const mime = MIME_MAP[ext] ?? 'text/plain; charset=utf-8';
  res.setHeader('Content-Type', mime);
  res.setHeader('Cache-Control', 'no-cache');
  // If content is stored as base64 data URL (for binary files like images), decode it
  if (typeof content === 'string' && content.startsWith('data:')) {
    const commaIdx = content.indexOf(',');
    if (commaIdx !== -1) {
      const mimeFromData = content.slice(5, content.indexOf(';'));
      const buf = Buffer.from(content.slice(commaIdx + 1), 'base64');
      res.setHeader('Content-Type', mimeFromData || mime);
      return res.send(buf);
    }
  }
  res.send(content);
});

router.delete('/:id/workspace-file', async (req, res) => {
  const filePath = req.query.path;
  if (!filePath || filePath.includes('..')) return res.status(400).json({ error: 'Invalid path' });
  if (!filePath.startsWith('artifacts/')) return res.status(400).json({ error: 'Only artifacts can be deleted here' });
  const deleted = await deleteTeamWorkspaceFile(getDb(), req.params.id, filePath);
  if (!deleted) return res.status(404).json({ error: 'Not found' });
  res.json({ ok: true });
});

export default router;
