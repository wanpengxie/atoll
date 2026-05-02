import { Router } from 'express';
import { v4 as uuidv4 } from 'uuid';
import crypto from 'crypto';
import {
  getDb, getMachines, getMachineById, insertMachine, updateMachine,
  getAgents, getTeams, deleteMachine, updateAgent,
} from '../db/index.js';
import { getUsageSummary, checkQuota } from '../plans.js';
import { sendToDaemon, isMachineOnline, unregisterDaemon } from '../daemon/connections.js';
import { requireAuth } from '../middleware/auth.js';
import { emitJsonEvent } from '../events.js';

const router = Router();

const FIXED_SERVER = {
  id: process.env.DEFAULT_SERVER_ID ?? 'server-001',
  name: 'Demo', slug: 'demo',
  ownerId: process.env.DEFAULT_USER_ID ?? 'user-001',
  plan: 'free', role: 'owner', createdAt: new Date().toISOString(),
};

const FIXED_MEMBER = {
  userId: process.env.DEFAULT_USER_ID ?? 'user-001',
  email: 'admin@demo.local',
  name: process.env.DEFAULT_USER_NAME ?? 'Admin',
  displayName: process.env.DEFAULT_USER_NAME ?? 'Admin',
  avatarUrl: null, role: 'owner', joinedAt: new Date().toISOString(),
};

function genApiKey() {
  return 'sk_machine_' + crypto.randomBytes(32).toString('hex');
}

function formatMachine(m) {
  let modelsByRuntime = {};
  try { modelsByRuntime = m.models_by_runtime ? JSON.parse(m.models_by_runtime) : {}; } catch {}
  return {
    id: m.id, serverId: m.server_id, name: m.name,
    apiKeyPrefix: m.api_key_prefix,
    runtimes: JSON.parse(m.runtimes ?? '[]'),
    modelsByRuntime,
    hostname: m.hostname ?? null, os: m.os ?? null,
    status: m.status, daemonVersion: m.daemon_version ?? null,
    lastHeartbeat: m.last_heartbeat ?? null, createdAt: m.created_at,
  };
}

router.get('/', (req, res) => res.json([FIXED_SERVER]));
router.get('/:id/members', (req, res) => res.json([FIXED_MEMBER]));
router.patch('/:id/members/:userId', (req, res) => res.json(FIXED_MEMBER));
router.get('/:id/sidebar-order', (req, res) => res.json({ teamOrder: [], agentOrder: [] }));
router.patch('/:id/sidebar-order', (req, res) => res.json({ ok: true }));

router.get('/:id/usage', async (req, res) => {
  const db = getDb();
  const sid = req.params.id;
  const [agents, machines, teams] = await Promise.all([
    getAgents(db, sid).then(a => a.filter(x => !x.deleted_at).length),
    getMachines(db, sid).then(m => m.length),
    getTeams(db, sid).then(c => c.filter(x => x.type === 'team').length),
  ]);
  res.json({ agents, machines, teams });
});

router.get('/:id/plan', async (req, res) => {
  const summary = await getUsageSummary(getDb(), req.params.id);
  res.json(summary);
});

router.post('/:id/plan', async (req, res) => {
  const bearer = (req.headers.authorization ?? '').replace('Bearer ', '');
  if (bearer !== (process.env.ADMIN_TOKEN ?? 'demo-token'))
    return res.status(403).json({ error: 'Forbidden' });
  const { plan } = req.body;
  const valid = ['free', 'pro', 'team'];
  if (!valid.includes(plan))
    return res.status(400).json({ error: `plan must be one of: ${valid.join(', ')}` });
  await getDb().execute(`UPDATE servers SET plan = ? WHERE id = ?`, [plan, req.params.id]);
  const summary = await getUsageSummary(getDb(), req.params.id);
  res.json(summary);
});

// User-facing plan upgrade (fake payment)
router.post('/:id/upgrade', requireAuth, async (req, res) => {
  const { plan, amount } = req.body;
  const { PLAN_CONFIG } = await import('../plans.js');
  const cfg = PLAN_CONFIG[plan];
  if (!cfg) return res.status(400).json({ error: 'Invalid plan' });
  // Verify fake payment amount matches plan price (price stored in cents)
  const expectedAmount = cfg.price / 100;
  if (cfg.price > 0 && Number(amount) !== expectedAmount)
    return res.status(400).json({ error: `支付金额不正确，应为 $${expectedAmount}` });
  await getDb().execute(`UPDATE servers SET plan = ? WHERE id = ?`, [plan, req.params.id]);
  const summary = await getUsageSummary(getDb(), req.params.id);
  res.json(summary);
});

router.get('/:id/invites', (req, res) => res.json([]));
router.post('/:id/invites', (req, res) => res.json({
  id: uuidv4(), invitedEmail: req.body.email,
  expiresAt: new Date(Date.now() + 7 * 86400000).toISOString(),
}));
router.delete('/:id/invites/:inviteId', (req, res) => res.json({ ok: true }));

router.get('/:id/machines', requireAuth, async (req, res) => {
  const machines = (await getMachines(getDb(), req.params.id, req.user.id))
    .filter(m => !m.is_platform)
    .map(formatMachine);
  res.json({ machines, latestDaemonVersion: '0.31.2' });
});

router.post('/:id/machines', requireAuth, async (req, res) => {
  const { name } = req.body;
  if (!name) return res.status(400).json({ error: 'name required' });
  const db = getDb();
  const quota = await checkQuota(db, req.params.id, 'machines');
  if (!quota.allowed)
    return res.status(403).json({ error: `Machine limit reached (${quota.current}/${quota.limit}). Upgrade your plan.` });
  const apiKey = genApiKey();
  const m = await insertMachine(db, {
    id: uuidv4(), serverId: req.params.id, ownerId: req.user.id,
    name, apiKey, apiKeyPrefix: apiKey.slice(0, 18),
  });
  emitJsonEvent('machine.register', {
    machine_id: m.id,
    server_id: m.server_id,
    api_key_prefix: m.api_key_prefix,
  });
  res.json({ ...formatMachine(m), apiKey });
});

router.patch('/:id/machines/:mid', async (req, res) => {
  const m = await updateMachine(getDb(), req.params.mid, { name: req.body.name });
  if (!m) return res.status(404).json({ error: 'Machine not found' });
  res.json(formatMachine(m));
});

router.delete('/:id/machines/:mid', async (req, res) => {
  const db = getDb();
  const mid = req.params.mid;

  // Cascade: stop agents on this machine and clear their machine_id
  const [agents] = await db.execute(
    `SELECT id FROM agents WHERE machine_id = ? AND is_del = 0 AND deleted_at IS NULL`, [mid]
  );
  const online = isMachineOnline(mid);
  for (const agent of agents) {
    if (online) sendToDaemon(mid, { type: 'agent:stop', agentId: agent.id });
    await updateAgent(db, agent.id, { machine_id: null, status: 'inactive' });
  }

  // Clean daemon connection
  unregisterDaemon(mid);

  await deleteMachine(db, mid);
  res.json({ ok: true });
});

router.get('/:id/machines/:mid/key', async (req, res) => {
  const m = await getMachineById(getDb(), req.params.mid);
  if (!m) return res.status(404).json({ error: 'not found' });
  res.json({ apiKey: m.api_key });
});

router.post('/:id/machines/:mid/rotate-key', async (req, res) => {
  const apiKey = genApiKey();
  await updateMachine(getDb(), req.params.mid, { api_key: apiKey, api_key_prefix: apiKey.slice(0, 18) });
  res.json({ apiKey });
});

router.get('/:id/machines/:mid/workspaces', (req, res) => res.json([]));
router.delete('/:id/machines/:mid/workspaces/:dir', (req, res) => res.json({ ok: true }));

// ── Platform machine info (admin only) ─────────────────────────────────────
router.get('/:id/platform-machine', requireAuth, async (req, res) => {
  const db = getDb();
  const [rows] = await db.execute(
    `SELECT * FROM machines WHERE server_id = ? AND is_platform = 1 LIMIT 1`,
    [req.params.id]
  );
  if (rows.length === 0) return res.json({ exists: false });
  const m = rows[0];
  res.json({
    exists: true,
    id: m.id,
    apiKey: m.api_key,
    status: m.status,
    daemonVersion: m.daemon_version ?? null,
    lastHeartbeat: m.last_heartbeat ?? null,
  });
});

export default router;
