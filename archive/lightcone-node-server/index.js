import 'dotenv/config';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import express from 'express';
import { createServer } from 'http';
import cors from 'cors';
import cookieParser from 'cookie-parser';
import { PayloadType, SenderKind } from '@coagent/payload-types';
import { initDb } from './db/index.js';
import { nowIso, nowMysqlDatetime } from './time.js';
import { setupSocketIO } from './realtime/index.js';
import { setupDaemonServer } from './daemon/index.js';
import { isMachineOnline, sendToDaemon } from './daemon/connections.js';
import { formatMessageForDaemon } from './daemon/index.js';
import { loadUser } from './middleware/auth.js';
import authRouter    from './routes/auth.js';
import usersRouter   from './routes/users.js';
import workspacesRouter from './routes/workspaces.js';
import channelsRouter from './routes/channels.js';
import serversRouter from './routes/servers.js';
import machinesRouter from './routes/machines.js';
import teamsRouter from './routes/teams.js';
import messagesRouter from './routes/messages.js';
import deviceRouter from './routes/device.js';
import devicesRouter from './routes/devices.js';
import daemonRouter from './routes/daemon.js';
import agentsRouter  from './routes/agents.js';
import skillsRouter       from './routes/skills.js';
import credentialsRouter  from './routes/credentials.js';
import oauthRouter        from './routes/oauth.js';
import jobQueryRouter     from './routes/job-query.js';
import internalRouter from './internal/index.js';

const app = express();
const httpServer = createServer(app);

// ── Middleware ────────────────────────────────────────────────────────────────
app.use(cors({ origin: '*', credentials: true }));
app.use(express.json());
app.use(cookieParser());
app.use(loadUser);
app.use(express.static(path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'public')));

// ── API Routes ────────────────────────────────────────────────────────────────
app.use('/api/auth',     authRouter);
app.use('/api/users',    usersRouter);
app.use('/api/workspaces', workspacesRouter);
app.use('/api/channels', channelsRouter);
app.use('/api/servers',  serversRouter);
app.use('/api/machines', machinesRouter);
app.use('/api/teams', teamsRouter);
app.use('/api/messages', messagesRouter);
app.use('/api/device',   deviceRouter);
app.use('/api/devices',  devicesRouter);
app.use('/api/daemon',   daemonRouter);
app.use('/api/agents',   agentsRouter);
app.use('/api/skills',       skillsRouter);
app.use('/api/credentials', credentialsRouter);
app.use('/api/oauth',       oauthRouter);
app.use('/api/jobs',        jobQueryRouter);
app.use('/internal/agent', internalRouter);

// ── Feishu bridge API (bearer ADMIN_TOKEN only) ───────────────────────────────
const ADMIN_TOKEN = process.env.ADMIN_TOKEN ?? 'demo-token';

function feishuAuth(req, res) {
  const bearer = (req.headers.authorization ?? '').replace('Bearer ', '');
  if (bearer !== ADMIN_TOKEN) { res.status(403).json({ error: 'Forbidden' }); return false; }
  return true;
}

app.get('/api/feishu/agents', async (req, res) => {
  if (!feishuAuth(req, res)) return;
  const { getDb, getAgents } = await import('./db/index.js');
  const all = await getAgents(getDb(), process.env.DEFAULT_SERVER_ID ?? 'server-001');
  const active = all.filter(a => !a.is_del && !a.deleted_at && a.feishu_app_id);
  res.json(active.map(a => ({
    id: a.id, name: a.name, displayName: a.display_name,
    feishuAppId: a.feishu_app_id, feishuAppSecret: a.feishu_app_secret,
    feishuVerificationToken: a.feishu_verification_token,
    feishuTeamId: a.feishu_team_id ?? 'team-all',
  })));
});

// GET /api/feishu/binding?chatId=xxx — bridge 查询 chat_id 的绑定
app.get('/api/feishu/binding', async (req, res) => {
  if (!feishuAuth(req, res)) return;
  const { getDb, getFeishuBinding } = await import('./db/index.js');
  const { chatId } = req.query;
  if (!chatId) return res.status(400).json({ error: 'chatId required' });
  const b = await getFeishuBinding(getDb(), chatId);
  res.json(b ? { teamId: b.team_id, agentId: b.agent_id } : { teamId: null, agentId: null });
});

// POST /api/feishu/bind — bridge 建立绑定（bot.added 或绑定码）
app.post('/api/feishu/bind', async (req, res) => {
  if (!feishuAuth(req, res)) return;
  const { getDb, insertFeishuBinding, consumeBindingCode,
          insertTeam, addTeamMember, getTeamById } = await import('./db/index.js');
  const { v4: uuidv4 } = await import('uuid');
  const { chatId, agentId, teamId, bindingCode, chatName, userId } = req.body;
  const db = getDb();
  const DEFAULT_SERVER_ID = process.env.DEFAULT_SERVER_ID ?? 'server-001';

  if (bindingCode) {
    // 绑定码模式
    const record = await consumeBindingCode(db, bindingCode.toUpperCase());
    if (!record) return res.status(400).json({ error: 'Invalid or expired binding code' });
    await insertFeishuBinding(db, chatId, record.team_id, record.agent_id);
    return res.json({ ok: true, teamId: record.team_id, agentId: record.agent_id, mode: 'code' });
  }

  if (teamId) {
    // 直接绑定到已有 team
    await insertFeishuBinding(db, chatId, teamId, agentId);
    return res.json({ ok: true, teamId, agentId, mode: 'direct' });
  }

  // 自动创建新 team（bot.added 场景）
  const name = (chatName ?? chatId).replace(/[^\p{L}\p{N}_ -]/gu, '').replace(/\s+/g, '-').slice(0, 40) || 'feishu-group';
  const ch = await insertTeam(db, { id: uuidv4(), serverId: DEFAULT_SERVER_ID, name, description: `飞书群: ${chatName ?? chatId}`, type: 'team' });
  if (userId) await addTeamMember(db, ch.id, userId, 'user');
  if (agentId) await addTeamMember(db, ch.id, agentId, 'agent');
  await insertFeishuBinding(db, chatId, ch.id, agentId);
  const { broadcast } = await import('./realtime/broadcast.js');
  broadcast.teamUpdated(DEFAULT_SERVER_ID);
  res.json({ ok: true, teamId: ch.id, agentId, mode: 'auto', teamName: name });
});

// POST /api/feishu/unbind — bridge 解绑
app.delete('/api/feishu/binding', async (req, res) => {
  if (!feishuAuth(req, res)) return;
  const { getDb } = await import('./db/index.js');
  const { chatId } = req.body;
  if (!chatId) return res.status(400).json({ error: 'chatId required' });
  const db = getDb();
  await db.execute(
    `UPDATE feishu_team_bindings
     SET is_del = 1, deleted_at = COALESCE(deleted_at, NOW())
     WHERE chat_id = ? AND is_del = 0`,
    [chatId]
  );
  res.json({ ok: true });
});

// GET /api/feishu/bindings — bridge 启动时加载所有绑定
app.get('/api/feishu/bindings', async (req, res) => {
  if (!feishuAuth(req, res)) return;
  const { getDb } = await import('./db/index.js');
  const [rows] = await getDb().execute(`SELECT chat_id, team_id, agent_id FROM feishu_team_bindings WHERE is_del = 0`);
  res.json(rows.map(r => ({ chatId: r.chat_id, teamId: r.team_id, agentId: r.agent_id })));
});

// ── Pending actions (approve / reject) ───────────────────────────────────────
// Requires session auth (loadUser already ran)
app.get('/api/actions', async (req, res) => {
  if (!req.user) return res.status(401).json({ error: 'Unauthorized' });
  const { getDb, getPendingActionsByTeam } = await import('./db/index.js');
  const { teamId, status } = req.query;
  if (!teamId) return res.status(400).json({ error: 'teamId required' });
  const actions = await getPendingActionsByTeam(getDb(), teamId, { status: status ?? 'pending' });
  res.json(actions.map(fmtAction));
});

app.get('/api/actions/:id', async (req, res) => {
  if (!req.user) return res.status(401).json({ error: 'Unauthorized' });
  const { getDb, getPendingActionById } = await import('./db/index.js');
  const action = await getPendingActionById(getDb(), req.params.id);
  if (!action) return res.status(404).json({ error: 'Not found' });
  res.json(fmtAction(action));
});

async function notifyAgentAboutAction(action, content) {
  const { getDb, getAgentById } = await import('./db/index.js');
  const { pushToInbox } = await import('./scheduler/inbox.js');
  const db = getDb();
  const agent = await getAgentById(db, action.agent_id);
  const message = {
    id: `action-${action.id}-${Date.now()}`,
    team_id: action.team_id,
    seq: null,
    sender_kind: SenderKind.SYSTEM,
    sender_id: 'system',
    payload_type: PayloadType.SYSTEM_NOTICE,
    payload_body: JSON.stringify({ text: content }),
    content,
    envelope_json: JSON.stringify({ sender: { kind: SenderKind.SYSTEM, id: 'system', name: 'System' } }),
    created_at: nowIso(),
  };

  if (agent?.machine_id && isMachineOnline(agent.machine_id)) {
    sendToDaemon(agent.machine_id, {
      type: 'agent:deliver',
      agentId: action.agent_id,
      teamId: action.team_id,
      seq: null,
      message: await formatMessageForDaemon(message),
    });
    return;
  }
  pushToInbox(action.agent_id, {
    type: 'system',
    content,
    team_id: action.team_id,
    team_name: null,
    team_type: 'system',
    sender_kind: SenderKind.SYSTEM,
  });
}

async function postActionDecisionMessage(action, user, decision) {
  if (!action.team_id) return null;
  const { getDb, getAgentById, insertMessage } = await import('./db/index.js');
  const { broadcast: _broadcast } = await import('./realtime/broadcast.js');
  const { deliverMessageToAgents } = await import('./scheduler/deliver.js');
  const { formatMessage } = await import('./internal/index.js');
  const { v4: uuidv4 } = await import('uuid');
  const db = getDb();
  const agent = await getAgentById(db, action.agent_id);
  const agentHandle = agent?.name ?? agent?.display_name ?? 'agent';
  const senderName = user?.name ?? user?.display_name ?? 'User';
  const approved = decision === 'approved';
  const content = approved
    ? `@${agentHandle} 审批已通过：${action.description}`
    : `@${agentHandle} 审批已拒绝：${action.description}`;

  const msg = await insertMessage(db, {
    id: uuidv4(),
    teamId: action.team_id,
    senderKind: SenderKind.HUMAN,
    senderId: user.id,
    payloadType: PayloadType.USER_TEXT,
    payloadBody: { text: content },
    content,
    envelope: {
      sender: { kind: SenderKind.HUMAN, id: user.id, name: senderName },
      mentions: [action.agent_id],
    },
  });

  _broadcast.message(action.team_id, formatMessage(msg));
  await deliverMessageToAgents(action.team_id, msg);
  return msg;
}

app.post('/api/actions/:id/approve', async (req, res) => {
  if (!req.user) return res.status(401).json({ error: 'Unauthorized' });
  const { getDb, getPendingActionById, updatePendingAction } = await import('./db/index.js');
  const { broadcast: _broadcast } = await import('./realtime/broadcast.js');
  const db = getDb();

  const action = await getPendingActionById(db, req.params.id);
  if (!action) return res.status(404).json({ error: 'Not found' });
  if (action.status !== 'pending') return res.status(400).json({ error: `Action is already '${action.status}'` });

  await updatePendingAction(db, action.id, {
    status: 'approved',
    decided_by: req.user.id,
    decided_at: nowMysqlDatetime(),
  });
  const updated = await getPendingActionById(db, action.id);
  _broadcast.actionUpdated(updated);

  if (action.team_id) {
    await postActionDecisionMessage(action, req.user, 'approved');
    await notifyAgentAboutAction(
      action,
      `Action approved (action_id="${action.id}"). Call execute_approved_action(action_id="${action.id}") to proceed.`
    );
  } else {
    await notifyAgentAboutAction(
      action,
      `Action approved (action_id="${action.id}"). Call execute_approved_action(action_id="${action.id}") to proceed.`
    );
  }

  res.json({ ok: true, action: fmtAction(updated) });
});

app.post('/api/actions/:id/reject', async (req, res) => {
  if (!req.user) return res.status(401).json({ error: 'Unauthorized' });
  const { getDb, getPendingActionById, updatePendingAction } = await import('./db/index.js');
  const { broadcast: _broadcast } = await import('./realtime/broadcast.js');
  const db = getDb();

  const action = await getPendingActionById(db, req.params.id);
  if (!action) return res.status(404).json({ error: 'Not found' });
  if (action.status !== 'pending') return res.status(400).json({ error: `Action is already '${action.status}'` });

  await updatePendingAction(db, action.id, {
    status: 'rejected',
    decided_by: req.user.id,
    decided_at: nowMysqlDatetime(),
  });
  const updated = await getPendingActionById(db, action.id);
  _broadcast.actionUpdated(updated);

  if (action.team_id) {
    await postActionDecisionMessage(action, req.user, 'rejected');
    await notifyAgentAboutAction(
      action,
      `Action rejected (action_id="${action.id}"). The human has declined this action.`
    );
  } else {
    await notifyAgentAboutAction(
      action,
      `Action rejected (action_id="${action.id}"). The human has declined this action.`
    );
  }

  res.json({ ok: true, action: fmtAction(updated) });
});

function fmtAction(a) {
  let payload = {};
  try { payload = JSON.parse(a.payload); } catch { /* ignore */ }
  return {
    id: a.id, agentId: a.agent_id, teamId: a.team_id,
    actionType: a.action_type, platform: a.platform,
    description: a.description, payload,
    credentialId: a.credential_id,
    status: a.status, decidedBy: a.decided_by,
    decidedAt: a.decided_at, executedAt: a.executed_at,
    error: a.error, createdAt: a.created_at,
  };
}

// ── Health check ──────────────────────────────────────────────────────────────
app.get('/health', (req, res) => res.json({
  ok: true,
  ts: Date.now(),
  commit: process.env.RAILWAY_GIT_COMMIT_SHA ?? process.env.GIT_COMMIT_SHA ?? null,
}));

// ── Error handler ─────────────────────────────────────────────────────────────
app.use((err, req, res, next) => {
  console.error('[Error]', err);
  res.status(500).json({ error: err.message ?? 'Internal server error' });
});

// ── Start ─────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT ?? 3001;

initDb().then(() => {
  setupSocketIO(httpServer);
  setupDaemonServer(httpServer);
  httpServer.listen(PORT, '0.0.0.0', async () => {
    console.error(`[Server] lightcone running on http://0.0.0.0:${PORT}`);
    // ── Feishu bridge (embedded mode) ──────────────────────────────────────
    if (process.env.FEISHU_BRIDGE !== 'off') {
      try {
        const { setupFeishuBridge } = await import('../feishu-bridge/src/bridge.js');
        setupFeishuBridge(app, {
          lightconeUrl: `http://localhost:${PORT}`,
          lightconeToken: ADMIN_TOKEN,
        });
      } catch (err) {
        console.error('[Server] Feishu bridge failed to load:', err.message);
      }
    }
    // ── Guest cleanup (every hour) ─────────────────────────────────────────
    setInterval(async () => {
      try {
        const { getDb, getAgents, updateAgent, getMemberTeamIds, removeTeamMember, deleteAgentTeamSessions } = await import('./db/index.js');
        const db = getDb();
        const [guests] = await db.execute(
          `SELECT id FROM users WHERE is_guest = 1 AND created_at < NOW() - INTERVAL 7 DAY AND is_del = 0 AND deleted_at IS NULL`
        );
        for (const guest of guests) {
          // Clean up guest's agents
          const [agentRows] = await db.execute(`SELECT id FROM agents WHERE owner_id = ? AND is_del = 0 AND deleted_at IS NULL`, [guest.id]);
          for (const agent of agentRows) {
            const teamIds = await getMemberTeamIds(db, agent.id);
            for (const cid of teamIds) await removeTeamMember(db, cid, agent.id);
            await deleteAgentTeamSessions(db, agent.id);
            await updateAgent(db, agent.id, { is_del: 1, deleted_at: nowMysqlDatetime(), status: 'inactive' });
          }
          // Soft-delete guest user
          await db.execute(`UPDATE users SET is_del = 1, deleted_at = COALESCE(deleted_at, NOW()) WHERE id = ?`, [guest.id]);
          await db.execute(
            `UPDATE sessions SET is_del = 1, deleted_at = COALESCE(deleted_at, NOW())
             WHERE user_id = ? AND is_del = 0`,
            [guest.id]
          );
        }
        if (guests.length > 0) console.error(`[GuestCleanup] Cleaned ${guests.length} expired guest accounts`);
      } catch (err) {
        console.error('[GuestCleanup] Error:', err.message);
      }
    }, 60 * 60 * 1000);

    // ── Platform-hosted daemon ────────────────────────────────────────────
    if (process.env.HOSTED_DAEMON !== 'off') {
      try {
        const { startHostedDaemon } = await import('./hosted-daemon.js');
        await startHostedDaemon(PORT);
      } catch (err) {
        console.error('[Server] Hosted daemon failed to start:', err.message);
      }
    }
  });
}).catch(err => {
  console.error('[Server] DB init failed:', err);
  process.exit(1);
});
