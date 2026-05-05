import { Router } from 'express';
import { fileURLToPath } from 'node:url';
import { v4 as uuidv4 } from 'uuid';
import { writeFileSync, mkdirSync } from 'fs';
import {
  getDb, getAgentById, getMachineByApiKey, getAgentByApiKey,
  getTeams, getAgents, getTeamMembers,
  getTeamById, getTeamByName, insertMessage,
  searchMessages, getTasksByTeam, nextTaskNumber,
  updateMessage, addTeamMember, isAgentInTeam,
  listMemoryFiles, getMemoryFile, upsertMemoryFile,
  listTeamWorkspaceFiles, getTeamWorkspaceFile, upsertTeamWorkspaceFile,
  getThreadByParent, getAgentByName, getDmTeamForAgent,
  getMemberTeamIds, deleteMessage, getAgentTeams,
  getSkillsForAgent, getSkillById, getSkillByName, searchSkills,
  insertSkill, updateSkill, insertSkillBinding, resolveUser,
  getCredentialGrantsForAgent, getCredentialById,
  insertPendingAction, getPendingActionById, updatePendingAction,
  insertActionLog,
} from '../db/index.js';
import { decrypt } from '../crypto.js';
import { broadcast } from '../realtime/broadcast.js';
import { deliverMessageToAgents } from '../scheduler/deliver.js';

const router = Router();

// ─── Auth middleware ──────────────────────────────────────────────────────────

router.use(async (req, res, next) => {
  const auth = req.headers.authorization ?? '';
  const token = auth.startsWith('Bearer ') ? auth.slice(7) : null;
  if (!token) return res.status(401).json({ error: 'Missing Authorization header' });

  if (token === process.env.ADMIN_TOKEN) {
    req.machine = { id: 'admin', server_id: process.env.DEFAULT_SERVER_ID ?? 'server-001' };
    return next();
  }

  // Per-agent token: sk_agent_*
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
  next();
});

async function requireAgent(req, res) {
  const agent = await getAgentById(getDb(), req.params.agentId);
  if (!agent || agent.is_del || agent.deleted_at) { res.status(404).json({ error: 'Agent not found' }); return null; }
  // Per-agent token: can only access own endpoints
  if (req.callerAgentId && req.callerAgentId !== agent.id) {
    res.status(403).json({ error: 'Agent token can only access own endpoints' }); return null;
  }
  if (!req.callerAgentId && req.machine.id !== 'admin' && agent.machine_id !== req.machine.id) {
    res.status(403).json({ error: 'Agent does not belong to this machine' }); return null;
  }
  return agent;
}

async function validateAgentTeamId(db, agent, teamId, res) {
  if (!teamId) return true;
  const team = await getTeamById(db, teamId);
  if (!team || team.is_del || team.deleted_at) {
    res.status(404).json({ error: `Team not found: ${teamId}` });
    return false;
  }
  if (team.server_id !== agent.server_id) {
    res.status(403).json({ error: 'Team does not belong to this server' });
    return false;
  }
  if (!(await isAgentInTeam(db, agent.id, teamId))) {
    res.status(403).json({ error: 'Agent is not a member of this team' });
    return false;
  }
  return true;
}

// ─── target 解析 ─────────────────────────────────────────────────────────────

async function resolveTarget(db, target, serverId) {
  if (target.startsWith('#')) {
    const parts = target.slice(1).split(':');
    const chName = parts[0];
    const threadShortId = parts[1] ?? null;
    const ch = await getTeamByName(db, serverId, chName);
    if (!ch) return null;
    if (threadShortId) return await getThreadByParent(db, serverId, threadShortId);
    return ch;
  }
  if (target.startsWith('dm:@')) {
    const parts = target.slice(4).split(':');
    const peerName = parts[0];
    const threadShortId = parts[1] ?? null;
    const peer = await getAgentByName(db, serverId, peerName);
    if (!peer) return null;
    const dmCh = await getDmTeamForAgent(db, serverId, peer.id);
    if (!dmCh) return null;
    if (threadShortId) return await getThreadByParent(db, serverId, threadShortId);
    return dmCh;
  }
  return null;
}

function formatTarget(ch) {
  if (ch.type === 'dm') return `dm:@${ch.name}`;
  if (ch.type === 'thread') return `thread`;
  return `#${ch.name}`;
}

// ─── formatMsgForAgent ────────────────────────────────────────────────────────

async function formatMsgForAgent(msg, db) {
  const ch = await getTeamById(db, msg.team_id);
  let teamType = ch?.type ?? 'team';
  let teamName = ch?.name ?? 'all';
  let parentTeamName = null;
  let parentTeamType = null;

  if (teamType === 'thread' && ch?.parent_message_id) {
    const [parentRows] = await db.execute(`SELECT * FROM messages WHERE id = ?`, [ch.parent_message_id]);
    if (parentRows[0]) {
      const parentCh = await getTeamById(db, parentRows[0].team_id);
      parentTeamName = parentCh?.name ?? null;
      parentTeamType = parentCh?.type ?? null;
    }
  }

  return {
    team_id: msg.team_id,
    team_type: teamType, team_name: teamName,
    parent_team_name: parentTeamName, parent_team_type: parentTeamType,
    sender_name: msg.sender_name, sender_type: msg.sender_type,
    content: msg.content, message_id: msg.id, timestamp: msg.created_at,
    attachments: [],
    task_status: msg.task_status ?? null, task_number: msg.task_number ?? null,
    task_assignee_type: msg.task_assignee_type ?? null, task_assignee_id: msg.task_assignee_id ?? null,
  };
}

// ─── receive ─────────────────────────────────────────────────────────────────

import { popInbox } from '../scheduler/inbox.js';

router.get('/:agentId/receive', async (req, res) => {
  const agent = await requireAgent(req, res);
  if (!agent) return;
  const messages = popInbox(agent.id);
  const db = getDb();
  res.json({ messages: await Promise.all(messages.map(m => formatMsgForAgent(m, db))) });
});

// ─── send ─────────────────────────────────────────────────────────────────────

router.post('/:agentId/send', async (req, res) => {
  const agent = await requireAgent(req, res);
  if (!agent) return;

  const { target, content } = req.body;
  if (!target || !content) return res.status(400).json({ error: 'target and content required' });

  const db = getDb();
  const ch = await resolveTarget(db, target, agent.server_id);
  if (!ch) return res.status(400).json({ error: `Cannot resolve target: ${target}` });

  if (!(await isAgentInTeam(db, agent.id, ch.id)))
    return res.status(403).json({ error: 'Agent is not a member of this team' });

  const mentions = await parseMentions(db, content, ch.id);
  const msg = await insertMessage(db, {
    id: uuidv4(), teamId: ch.id,
    senderType: 'agent', senderId: agent.id, senderName: agent.display_name,
    content, messageType: 'chat',
    mentions: mentions !== null ? JSON.stringify(mentions) : null,
  });

  broadcast.message(ch.id, formatMessage(msg));
  await deliverMessageToAgents(ch.id, msg);

  const shortId = msg.id.slice(0, 8);
  const replyTarget = target.includes(':') ? target : `${target}:${shortId}`;
  res.json({ messageId: msg.id, threadTarget: replyTarget });
});

// ─── resolve-team ───────────────────────────────────────────────────────────────

router.post('/:agentId/resolve-team', async (req, res) => {
  const agent = await requireAgent(req, res);
  if (!agent) return;
  const { target } = req.body;
  if (!target) return res.status(400).json({ error: 'target required' });
  const ch = await resolveTarget(getDb(), target, agent.server_id);
  if (!ch) return res.status(404).json({ error: `Cannot resolve team: ${target}` });
  res.json({ teamId: ch.id });
});

// ─── server info ──────────────────────────────────────────────────────────────

router.get('/:agentId/server', async (req, res) => {
  const agent = await requireAgent(req, res);
  if (!agent) return;
  const db = getDb();
  const serverId = agent.server_id;

  const agentTeamIds = await getMemberTeamIds(db, agent.id);
  const teamIdSet = new Set(agentTeamIds);

  const allTeams = await getTeams(db, serverId);
  const myTeams = allTeams.filter(c => c.type === 'team' && teamIdSet.has(c.id));
  const teams = myTeams.map(c => ({ id: c.id, name: c.name, description: c.description }));

  // Collect teammates (agents + humans) from all joined teams
  const teammateAgentIds = new Set();
  const teammateHumanIds = new Set();
  for (const teamId of agentTeamIds) {
    const members = await getTeamMembers(db, teamId);
    for (const m of members) {
      if (m.member_type === 'agent' && m.member_id !== agent.id) teammateAgentIds.add(m.member_id);
      if (m.member_type === 'user') teammateHumanIds.add(m.member_id);
    }
  }

  const agentList = await Promise.all([...teammateAgentIds].map(id => getAgentById(db, id)));
  const agents = agentList
    .filter(a => a && !a.is_del && !a.deleted_at)
    .map(a => ({ name: a.name, status: a.activity === 'offline' ? 'offline' : 'online' }));

  const humanList = await Promise.all([...teammateHumanIds].map(id => resolveUser(db, id)));
  const humans = humanList.filter(Boolean).map(h => ({ name: h.display_name ?? h.name }));

  res.json({ teams, agents, humans });
});

// ─── search ───────────────────────────────────────────────────────────────────

router.get('/:agentId/search', async (req, res) => {
  const agent = await requireAgent(req, res);
  if (!agent) return;

  const { q, team, limit = 10 } = req.query;
  if (!q || !q.trim()) return res.status(400).json({ error: 'q (query) required' });
  if (!team || !team.trim()) return res.status(400).json({ error: 'team is required — search is scoped to a single team' });

  const db = getDb();
  const agentTeamList = await getAgentTeams(db, agent.id);
  const agentTeamIds = agentTeamList.map(c => c.id);

  const ch = await resolveTarget(db, team, agent.server_id);
  if (!ch) return res.status(404).json({ error: `Cannot resolve team: ${team}` });
  if (!agentTeamIds.includes(ch.id))
    return res.status(403).json({ error: 'Agent is not a member of this team' });
  const scopeTeamId = ch.id;

  try {
    const rows = await searchMessages(db, agentTeamIds, q.trim(), {
      teamId: scopeTeamId, limit: Math.min(Number(limit), 20),
    });

    const results = rows.map(row => {
      const snippet = row.content.length > 200
        ? row.content.slice(0, 200) + '…'
        : row.content;
      return {
        id: row.id, seq: row.seq,
        teamName: row.team_name, teamType: row.team_type,
        senderName: row.sender_name, senderType: row.sender_type,
        content: row.content, snippet, createdAt: row.created_at,
      };
    });

    res.json({ results });
  } catch (err) {
    // Fulltext search may fail if ngram parser isn't available
    res.status(500).json({ error: `Search failed: ${err.message}` });
  }
});

// ─── tasks: list ─────────────────────────────────────────────────────────────

router.get('/:agentId/tasks', async (req, res) => {
  const agent = await requireAgent(req, res);
  if (!agent) return;
  const { team, status = 'all' } = req.query;
  if (!team) return res.status(400).json({ error: 'team required' });

  const db = getDb();
  const ch = await resolveTarget(db, team, agent.server_id);
  if (!ch) return res.status(404).json({ error: `Cannot resolve team: ${team}` });
  if (!(await isAgentInTeam(db, agent.id, ch.id)))
    return res.status(403).json({ error: 'Agent is not a member of this team' });

  const tasks = await getTasksByTeam(db, ch.id, status);
  const allAgents = await getAgents(db, agent.server_id);
  const agentMap = Object.fromEntries(allAgents.map(a => [a.id, a]));

  res.json({
    tasks: tasks.map(t => ({
      taskNumber: t.task_number, messageId: t.id, title: t.content, status: t.task_status,
      claimedByName: t.task_assignee_id ? (agentMap[t.task_assignee_id]?.display_name ?? null) : null,
      createdByName: t.sender_name, isLegacy: false,
    }))
  });
});

// ─── tasks: create ───────────────────────────────────────────────────────────

router.post('/:agentId/tasks', async (req, res) => {
  const agent = await requireAgent(req, res);
  if (!agent) return;
  const { team, tasks } = req.body;
  if (!team || !Array.isArray(tasks)) return res.status(400).json({ error: 'team and tasks array required' });

  const db = getDb();
  const ch = await resolveTarget(db, team, agent.server_id);
  if (!ch) return res.status(404).json({ error: `Cannot resolve team: ${team}` });
  if (!(await isAgentInTeam(db, agent.id, ch.id)))
    return res.status(403).json({ error: 'Agent is not a member of this team' });

  const created = [];
  for (const t of tasks) {
    const taskNumber = await nextTaskNumber(db, ch.id);
    const msg = await insertMessage(db, {
      id: uuidv4(), teamId: ch.id,
      senderType: 'agent', senderId: agent.id, senderName: agent.display_name,
      content: t.title, messageType: 'chat', taskStatus: 'todo', taskNumber,
    });
    created.push(msg);
  }

  for (const msg of created) {
    broadcast.message(ch.id, formatMessage(msg));
    broadcast.taskCreated(ch.id, [formatTaskForBroadcast(msg)]);
  }

  res.json({ tasks: created.map(m => ({ taskNumber: m.task_number, messageId: m.id, title: m.content })) });
});

// ─── tasks: claim ────────────────────────────────────────────────────────────

router.post('/:agentId/tasks/claim', async (req, res) => {
  const agent = await requireAgent(req, res);
  if (!agent) return;
  const { team, task_numbers, message_ids } = req.body;
  if (!team) return res.status(400).json({ error: 'team required' });
  if (!task_numbers?.length && !message_ids?.length)
    return res.status(400).json({ error: 'provide task_numbers or message_ids' });

  const db = getDb();
  const ch = await resolveTarget(db, team, agent.server_id);
  if (!ch) return res.status(404).json({ error: `Cannot resolve team: ${team}` });
  if (!(await isAgentInTeam(db, agent.id, ch.id)))
    return res.status(403).json({ error: 'Agent is not a member of this team' });

  const conn = await db.getConnection();
  const results = [];
  const toUpdate = [];
  try {
    await conn.beginTransaction();

    const targets = [];
    if (task_numbers?.length) {
      const placeholders = task_numbers.map(() => '?').join(',');
      const [rows] = await conn.execute(
        `SELECT * FROM messages WHERE team_id = ? AND task_number IN (${placeholders}) AND task_status IS NOT NULL`,
        [ch.id, ...task_numbers]
      );
      targets.push(...rows);
    }
    if (message_ids?.length) {
      for (const shortId of message_ids) {
        const [rows] = await conn.execute(
          `SELECT * FROM messages WHERE team_id = ? AND id LIKE ?`,
          [ch.id, `${shortId}%`]
        );
        if (rows[0]) targets.push(rows[0]);
      }
    }

    for (const t of targets) {
      if (t.task_assignee_id) {
        results.push({ taskNumber: t.task_number, messageId: t.id, success: false, reason: 'already claimed' });
        continue;
      }
      const newStatus = (t.task_status === 'todo' || !t.task_status) ? 'in_progress' : t.task_status;
      const taskNum = t.task_number ?? await nextTaskNumber(db, ch.id);
      await conn.execute(
        `UPDATE messages SET task_assignee_id = ?, task_assignee_type = 'agent',
         task_claimed_at = NOW(), task_status = ?, task_number = ?, updated_at = NOW()
         WHERE id = ?`,
        [agent.id, newStatus, taskNum, t.id]
      );
      results.push({ taskNumber: taskNum, messageId: t.id, success: true });
      toUpdate.push(t.id);
    }

    await conn.commit();
  } catch (err) {
    await conn.rollback();
    throw err;
  } finally {
    conn.release();
  }

  for (const r of results.filter(r => r.success)) {
    const updated = await db.execute(`SELECT * FROM messages WHERE id = ?`, [r.messageId]).then(([rows]) => rows[0]);
    const sysMsg = await insertMessage(db, {
      id: uuidv4(), teamId: ch.id,
      senderType: 'user', senderId: 'system', senderName: 'System', messageType: 'system',
      content: `📌 ${agent.display_name} claimed #${r.taskNumber} "${updated.content.slice(0, 40)}"`,
    });
    broadcast.message(ch.id, formatMessage(sysMsg));
    broadcast.messageUpdated(ch.id, formatMessage(updated));
    broadcast.taskUpdated(ch.id, formatTaskForBroadcast(updated));
  }

  res.json({ results });
});

// ─── tasks: unclaim ──────────────────────────────────────────────────────────

router.post('/:agentId/tasks/unclaim', async (req, res) => {
  const agent = await requireAgent(req, res);
  if (!agent) return;
  const { team, task_number } = req.body;
  const db = getDb();
  const ch = await resolveTarget(db, team, agent.server_id);
  if (!ch) return res.status(404).json({ error: `Cannot resolve team: ${team}` });
  if (!(await isAgentInTeam(db, agent.id, ch.id)))
    return res.status(403).json({ error: 'Agent is not a member of this team' });

  const [rows] = await db.execute(
    `SELECT * FROM messages WHERE team_id = ? AND task_number = ?`, [ch.id, task_number]
  );
  const t = rows[0];
  if (!t) return res.status(404).json({ error: 'Task not found' });

  await db.execute(
    `UPDATE messages SET task_assignee_id = NULL, task_assignee_type = NULL,
     task_claimed_at = NULL, updated_at = NOW() WHERE id = ?`,
    [t.id]
  );
  const [updRows] = await db.execute(`SELECT * FROM messages WHERE id = ?`, [t.id]);
  const updated = updRows[0];
  broadcast.messageUpdated(ch.id, formatMessage(updated));
  broadcast.taskUpdated(ch.id, formatTaskForBroadcast(updated));
  res.json({ ok: true });
});

// ─── tasks: update-status ────────────────────────────────────────────────────

router.post('/:agentId/tasks/update-status', async (req, res) => {
  const agent = await requireAgent(req, res);
  if (!agent) return;
  const { team, task_number, status } = req.body;
  const validStatuses = ['todo', 'in_progress', 'in_review', 'done'];
  if (!validStatuses.includes(status)) return res.status(400).json({ error: 'Invalid status' });

  const db = getDb();
  const ch = await resolveTarget(db, team, agent.server_id);
  if (!ch) return res.status(404).json({ error: `Cannot resolve team: ${team}` });
  if (!(await isAgentInTeam(db, agent.id, ch.id)))
    return res.status(403).json({ error: 'Agent is not a member of this team' });

  const [rows] = await db.execute(
    `SELECT * FROM messages WHERE team_id = ? AND task_number = ?`, [ch.id, task_number]
  );
  const t = rows[0];
  if (!t) return res.status(404).json({ error: 'Task not found' });

  const completedAt = status === 'done' ? new Date().toISOString().slice(0,19).replace('T',' ') : (t.task_completed_at ?? null);
  await db.execute(
    `UPDATE messages SET task_status = ?, task_completed_at = ?, updated_at = NOW() WHERE id = ?`,
    [status, completedAt, t.id]
  );

  const statusLabels = { in_progress: 'In Progress', in_review: 'In Review', done: 'Done', todo: 'Todo' };
  const sysMsg = await insertMessage(db, {
    id: uuidv4(), teamId: ch.id,
    senderType: 'user', senderId: 'system', senderName: 'System', messageType: 'system',
    content: `${status === 'done' ? '✅' : '👀'} ${agent.display_name} moved #${task_number} "${t.content.slice(0, 40)}" to ${statusLabels[status]}`,
  });
  broadcast.message(ch.id, formatMessage(sysMsg));

  const [updRows] = await db.execute(`SELECT * FROM messages WHERE id = ?`, [t.id]);
  const updated = updRows[0];
  broadcast.messageUpdated(ch.id, formatMessage(updated));
  broadcast.taskUpdated(ch.id, formatTaskForBroadcast(updated));
  res.json({ ok: true });
});

// ─── temporary public upload ─────────────────────────────────────────────────
// This is for chat previews, QR screenshots, and external platform handoffs.
// Durable agent deliverables belong in team_workspace artifacts/.

import path from 'path';

router.post('/:agentId/upload', async (req, res) => {
  const agent = await requireAgent(req, res);
  if (!agent) return;
  const { filename, data } = req.body;
  if (!filename || !data) return res.status(400).json({ error: 'filename and data required' });

  const ext = path.extname(filename).toLowerCase();
  if (!['.png', '.jpg', '.jpeg', '.webp'].includes(ext))
    return res.status(400).json({ error: 'Only image files allowed' });

  const safeFilename = `${Date.now()}-${Math.random().toString(36).slice(2)}${ext}`;
  const dir = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', '..', 'public', 'generated');
  mkdirSync(dir, { recursive: true });
  try {
    writeFileSync(path.join(dir, safeFilename), Buffer.from(data, 'base64'));
  } catch {
    return res.status(500).json({ error: 'Failed to write file' });
  }
  const serverUrl = process.env.SERVER_URL ?? `http://localhost:${process.env.PORT ?? 3001}`;
  res.json({
    url: `${serverUrl}/generated/${safeFilename}`,
    filename: safeFilename,
    temporary: true,
    note: 'Temporary public upload. Durable deliverables should be saved to team workspace artifacts/.',
  });
});

// ─── memory ───────────────────────────────────────────────────────────────────

router.get('/:agentId/memory', async (req, res) => {
  const agent = await requireAgent(req, res);
  if (!agent) return;
  const { path: filePath, teamId = '' } = req.query;
  const db = getDb();
  if (!(await validateAgentTeamId(db, agent, teamId, res))) return;
  if (!filePath) {
    const files = await listMemoryFiles(db, agent.id, teamId);
    return res.json({ files });
  }
  const content = await getMemoryFile(db, agent.id, teamId, filePath);
  if (content == null) return res.status(404).json({ error: 'Not found' });
  res.json({ path: filePath, content });
});

router.put('/:agentId/memory', async (req, res) => {
  const agent = await requireAgent(req, res);
  if (!agent) return;
  const { path: filePath, teamId = '' } = req.query;
  if (!filePath) return res.status(400).json({ error: 'path query param required' });
  const { content } = req.body;
  if (typeof content !== 'string') return res.status(400).json({ error: 'content (string) required' });
  const db = getDb();
  if (!(await validateAgentTeamId(db, agent, teamId, res))) return;
  await upsertMemoryFile(db, agent.id, teamId, filePath, content);
  res.json({ ok: true, path: filePath });
});

// ─── team memory (MySQL-backed) ───────────────────────────────────────────────

router.get('/:agentId/team-memory', async (req, res) => {
  const agent = await requireAgent(req, res);
  if (!agent) return;
  const { path: filePath, teamId } = req.query;
  if (!teamId) return res.status(400).json({ error: 'teamId query param required' });
  const db = getDb();
  if (!(await validateAgentTeamId(db, agent, teamId, res))) return;
  if (!filePath) {
    const files = await listTeamWorkspaceFiles(db, teamId);
    return res.json({ files });
  }
  const content = await getTeamWorkspaceFile(db, teamId, filePath);
  if (content == null) return res.status(404).json({ error: 'Not found' });
  res.json({ path: filePath, content });
});

router.put('/:agentId/team-memory', async (req, res) => {
  const agent = await requireAgent(req, res);
  if (!agent) return;
  const { path: filePath, teamId } = req.query;
  if (!filePath) return res.status(400).json({ error: 'path query param required' });
  if (!teamId) return res.status(400).json({ error: 'teamId query param required' });
  const { content } = req.body;
  if (typeof content !== 'string') return res.status(400).json({ error: 'content (string) required' });
  const db = getDb();
  if (!(await validateAgentTeamId(db, agent, teamId, res))) return;
  await upsertTeamWorkspaceFile(db, teamId, filePath, content);
  res.json({ ok: true, path: filePath });
});

// ─── skills ─────────────────────────────────────────────────────────────────

router.get('/:agentId/skills', async (req, res) => {
  const agent = await requireAgent(req, res);
  if (!agent) return;
  const db = getDb();
  const skills = await getSkillsForAgent(db, agent.id, agent.owner_id);
  res.json(skills.map(s => ({
    id: s.id, name: s.name, type: s.type,
    description: s.description,
    content: s.content ?? null,
    tags: typeof s.tags === 'string' ? JSON.parse(s.tags) : (s.tags ?? []),
    mcpConfig: s.mcp_config ? JSON.parse(s.mcp_config) : null,
  })));
});

router.get('/:agentId/skills/search', async (req, res) => {
  const agent = await requireAgent(req, res);
  if (!agent) return;
  const { q } = req.query;
  if (!q) return res.status(400).json({ error: 'q query param required' });
  const skills = await searchSkills(getDb(), agent.owner_id, q);
  res.json(skills.map(s => ({
    id: s.id, name: s.name, type: s.type,
    description: s.description,
    tags: typeof s.tags === 'string' ? JSON.parse(s.tags) : (s.tags ?? []),
  })));
});

router.get('/:agentId/skills/:skillId', async (req, res) => {
  const agent = await requireAgent(req, res);
  if (!agent) return;
  // Support lookup by ID or name
  let skill = await getSkillById(getDb(), req.params.skillId);
  if (!skill) skill = await getSkillByName(getDb(), agent.owner_id, req.params.skillId);
  if (!skill) return res.status(404).json({ error: 'Skill not found' });
  if (skill.type === 'user' && skill.owner_id !== agent.owner_id)
    return res.status(403).json({ error: 'Not accessible' });
  res.json({
    id: skill.id, name: skill.name, type: skill.type,
    description: skill.description, content: skill.content,
    tags: typeof skill.tags === 'string' ? JSON.parse(skill.tags) : (skill.tags ?? []),
  });
});

router.post('/:agentId/skills', async (req, res) => {
  const agent = await requireAgent(req, res);
  if (!agent) return;
  const { name, description, content, tags } = req.body;
  if (!name) return res.status(400).json({ error: 'name is required' });
  const { v4: uuidv4 } = await import('uuid');
  const skill = await insertSkill(getDb(), {
    id: uuidv4(), ownerId: agent.owner_id, type: 'user',
    name, description: description ?? '', content: content ?? '',
    tags: tags ?? [], createdByAgentId: agent.id,
  });
  // Auto-bind to the creating agent
  await insertSkillBinding(getDb(), {
    id: uuidv4(), skillId: skill.id, targetType: 'agent', targetId: agent.id,
  });
  res.status(201).json({ id: skill.id, name: skill.name });
});

router.patch('/:agentId/skills/:skillId', async (req, res) => {
  const agent = await requireAgent(req, res);
  if (!agent) return;
  let skill = await getSkillById(getDb(), req.params.skillId);
  if (!skill) skill = await getSkillByName(getDb(), agent.owner_id, req.params.skillId);
  if (!skill) return res.status(404).json({ error: 'Skill not found' });
  if (skill.type === 'platform') return res.status(403).json({ error: 'Cannot modify platform skills' });
  if (skill.owner_id !== agent.owner_id) return res.status(403).json({ error: 'Not accessible' });

  const { name, description, content, tags } = req.body;
  const fields = {};
  if (name != null) fields.name = name;
  if (description != null) fields.description = description;
  if (content != null) fields.content = content;
  if (tags != null) fields.tags = tags;

  const updated = await updateSkill(getDb(), skill.id, fields);
  res.json({ id: updated.id, name: updated.name });
});

// ─── rate-check (Platform MCP → server) ──────────────────────────────────────

router.post('/:agentId/rate-check', async (req, res) => {
  const agent = await requireAgent(req, res);
  if (!agent) return;
  const { platform, credentialId, windowMinutes = 15, maxActions = 10 } = req.body;
  if (!platform) return res.status(400).json({ error: 'platform required' });

  const db = getDb();
  const windowStart = new Date(Date.now() - windowMinutes * 60 * 1000)
    .toISOString().slice(0, 19).replace('T', ' ');
  const [rows] = await db.execute(
    `SELECT COUNT(*) AS c FROM platform_action_log
     WHERE credential_id = ? AND platform = ? AND status = 'ok' AND executed_at >= ?`,
    [credentialId ?? '', platform, windowStart]
  );
  const count = rows[0].c;
  if (count >= maxActions) {
    return res.json({ allowed: false, current: count, limit: maxActions, retry_after: windowMinutes * 60 });
  }
  res.json({ allowed: true, current: count, limit: maxActions });
});

// ─── action-log (Platform MCP → server) ──────────────────────────────────────

router.post('/:agentId/action-log', async (req, res) => {
  const agent = await requireAgent(req, res);
  if (!agent) return;
  const { platform, actionType, credentialId, payload, result, status, error } = req.body;
  if (!platform || !actionType || !status) return res.status(400).json({ error: 'platform, actionType, status required' });

  const { v4: uuidv4 } = await import('uuid');
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
  res.json({ ok: true });
});

// ─── credential grants (for daemon spawn-time injection) ─────────────────────

router.get('/:agentId/credential-grants', async (req, res) => {
  const agent = await requireAgent(req, res);
  if (!agent) return;
  const db = getDb();
  const grants = await getCredentialGrantsForAgent(db, agent.id);
  const result = grants.map(g => {
    let fields = {};
    try { fields = decrypt(g.iv, g.encrypted_data); } catch { /* skip malformed */ }
    return { credentialId: g.id, platform: g.platform, envVars: fields };
  });
  res.json(result);
});

// ─── pending_actions (agent-facing) ──────────────────────────────────────────

// POST /:agentId/actions/request — Agent 发起审批请求
router.post('/:agentId/actions/request', async (req, res) => {
  const agent = await requireAgent(req, res);
  if (!agent) return;
  const { action_type, platform, description, payload, credential_id, team_id } = req.body;
  if (!action_type || !description || !payload) {
    return res.status(400).json({ error: 'action_type, description, payload required' });
  }

  const db = getDb();
  const { v4: uuidv4 } = await import('uuid');
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

  // Post an action_request message to the team so the UI can render an approval card
  if (team_id) {
    const team = await getTeamById(db, team_id);
    if (team) {
      const msg = await insertMessage(db, {
        id: uuidv4(),
        teamId: team_id,
        senderType: 'agent',
        senderId: agent.id,
        senderName: agent.display_name ?? agent.name,
        messageType: 'action_request',
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
  res.json({ id: action.id, status: 'pending' });
});

// POST /:agentId/actions/:actionId/execute — Agent/MCP 校验已批准动作并取回 payload
router.post('/:agentId/actions/:actionId/execute', async (req, res) => {
  const agent = await requireAgent(req, res);
  if (!agent) return;

  const db = getDb();
  const action = await getPendingActionById(db, req.params.actionId);
  if (!action) return res.status(404).json({ error: 'Action not found' });
  if (action.agent_id !== agent.id) return res.status(403).json({ error: 'Forbidden' });
  if (action.status !== 'approved') {
    return res.status(400).json({ error: `Action status is '${action.status}', must be 'approved' to execute` });
  }
  if (action.executed_at || action.status === 'executed') {
    return res.status(400).json({ error: 'Action already executed' });
  }

  // Return payload for the agent / MCP server to execute. The action is marked
  // executed only after the platform MCP reports completion.
  let parsedPayload = {};
  try { parsedPayload = JSON.parse(action.payload); } catch { /* ignore */ }

  res.json({
    ok: true,
    actionId: action.id,
    actionType: action.action_type,
    platform: action.platform,
    credentialId: action.credential_id,
    teamId: action.team_id,
    payload: parsedPayload,
  });
});

// POST /:agentId/actions/:actionId/complete — MCP 上报平台动作执行结果
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
    executed_at: new Date().toISOString().slice(0, 19).replace('T', ' '),
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
  res.json({ ok: true, status });
});

// ─── helpers ─────────────────────────────────────────────────────────────────

export function formatMessage(msg) {
  return {
    id: msg.id, seq: msg.seq, teamId: msg.team_id,
    channelId: msg.channel_id ?? null,
    senderType: msg.sender_type, senderId: msg.sender_id, senderName: msg.sender_name,
    messageType: msg.message_type, content: msg.content,
    threadId: msg.thread_id ?? null,
    mentions: msg.mentions ? JSON.parse(msg.mentions) : null,
    taskStatus: msg.task_status ?? null, taskNumber: msg.task_number ?? null,
    taskAssigneeType: msg.task_assignee_type ?? null, taskAssigneeId: msg.task_assignee_id ?? null,
    taskAssigneeName: msg.task_assignee_name ?? null,
    taskClaimedAt: msg.task_claimed_at ?? null, taskCompletedAt: msg.task_completed_at ?? null,
    createdAt: msg.created_at, updatedAt: msg.updated_at, attachments: [],
  };
}

// 解析消息内容中的 @agentName，返回 agentId 数组
// 支持匹配 name, display_name, feishu_bot_name
// Returns null if no @mentions in content, or an array of matched agent IDs (possibly empty)
// When teamId is provided, only match agents that are members of that team
export async function parseMentions(db, content, teamId = null) {
  const matches = content.match(/@([^\s@]+)/g);
  if (!matches) return null;
  const names = [...new Set(matches.map(m => m.slice(1)))];
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

function formatTaskForBroadcast(msg) {
  return { ...formatMessage(msg), title: msg.content, isLegacy: false };
}

export default router;
