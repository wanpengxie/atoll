import { Router } from 'express';
import bcrypt from 'bcrypt';
import { v4 as uuidv4 } from 'uuid';
import {
  getDb,
  findUserByIdentity,
  createUserWithIdentity,
  updateIdentityMeta,
  createSession,
  deleteSession,
  insertTeam,
  addTeamMember,
  insertAgent,
  getUserIdentities,
  addIdentityToUser,
  removeIdentity,
  mergeUsers,
  convertGuestToUser,
} from '../db/index.js';
import { isMachineOnline, sendToDaemon } from '../daemon/connections.js';

const DEFAULT_SERVER_ID = process.env.DEFAULT_SERVER_ID ?? 'server-001';

// 每个用户有自己独立的 default team，首次登录时自动创建
async function ensurePersonalDefaultTeam(db, userId) {
  const [rows] = await db.execute(
    `SELECT c.id FROM teams c
     JOIN team_members cm ON cm.team_id = c.id
     WHERE c.name = 'default' AND c.type = 'team'
       AND c.is_del = 0 AND c.deleted_at IS NULL
       AND cm.member_id = ? AND cm.member_type = 'user' AND cm.is_del = 0`,
    [userId]
  );
  if (rows.length) return;
  const ch = await insertTeam(db, { id: uuidv4(), serverId: DEFAULT_SERVER_ID, ownerId: userId, name: 'default', description: '' });
  await addTeamMember(db, ch.id, userId, 'user');
}

const router = Router();
const FEISHU_API = 'https://open.feishu.cn/open-apis';

// ── App access token (cached, valid ~2h) ──────────────────────────────────────
let _appToken = null;
let _appTokenExpiry = 0;

async function getAppAccessToken() {
  if (_appToken && Date.now() < _appTokenExpiry - 60_000) return _appToken;
  const res = await fetch(`${FEISHU_API}/auth/v3/app_access_token/internal`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      app_id:     process.env.FEISHU_APP_ID,
      app_secret: process.env.FEISHU_APP_SECRET,
    }),
  });
  const data = await res.json();
  if (data.code !== 0) throw new Error(`getAppAccessToken failed: ${data.msg}`);
  _appToken = data.app_access_token;
  _appTokenExpiry = Date.now() + data.expire * 1000;
  return _appToken;
}

// ── Feishu OAuth ──────────────────────────────────────────────────────────────

// State store for OAuth CSRF + link mode (5 min TTL)
const oauthStates = new Map();
function cleanupStates() {
  const now = Date.now();
  for (const [k, v] of oauthStates) { if (now > v.expires) oauthStates.delete(k); }
}

// GET /api/auth/feishu — redirect to Feishu OAuth page
// ?link=true — link mode: attach feishu identity to current logged-in user
router.get('/feishu', (req, res) => {
  cleanupStates();
  const state = Math.random().toString(36).slice(2) + Math.random().toString(36).slice(2);
  const stateData = { expires: Date.now() + 5 * 60 * 1000 };
  if (req.query.link === 'true' && req.user) {
    stateData.link = true;
    stateData.userId = req.user.id;
  }
  oauthStates.set(state, stateData);

  const redirectUri = `${process.env.SERVER_URL}/api/auth/feishu/callback`;
  const params = new URLSearchParams({
    app_id:       process.env.FEISHU_APP_ID,
    redirect_uri: redirectUri,
    scope:        'contact:user.base:readonly',
    state,
  });
  res.redirect(`${FEISHU_API}/authen/v1/authorize?${params}`);
});

// GET /api/auth/feishu/callback — exchange code for user info
router.get('/feishu/callback', async (req, res) => {
  const { code, state } = req.query;
  if (!code) return res.status(400).send('Missing code');

  // Validate OAuth state
  const stateData = oauthStates.get(state);
  if (!stateData || Date.now() > stateData.expires) {
    return res.status(400).send('Invalid or expired OAuth state');
  }
  oauthStates.delete(state);
  const isLinkMode = stateData.link === true && stateData.userId;

  try {
    const appToken = await getAppAccessToken();

    // Exchange code for user_access_token
    const tokenRes = await fetch(`${FEISHU_API}/authen/v1/oidc/access_token`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ grant_type: 'authorization_code', code, app_access_token: appToken }),
    });
    const tokenData = await tokenRes.json();
    if (tokenData.code !== 0) throw new Error(`Token exchange failed: ${tokenData.msg}`);
    const userAccessToken = tokenData.data.access_token;

    // Get user info
    const userRes = await fetch(`${FEISHU_API}/authen/v1/user_info`, {
      headers: { Authorization: `Bearer ${userAccessToken}` },
    });
    const userJson = await userRes.json();
    if (userJson.code !== 0) throw new Error(`Get user info failed: ${userJson.msg}`);

    const { open_id, union_id, name, avatar_url } = userJson.data;
    const db = getDb();
    const providerUid = union_id ?? open_id;

    // Find existing user with this feishu identity
    let existingFeishuUser = union_id ? await findUserByIdentity(db, 'feishu', union_id) : null;
    if (!existingFeishuUser) existingFeishuUser = await findUserByIdentity(db, 'feishu', open_id);

    // ── Link mode ─────────────────────────────────────────────────────────
    if (isLinkMode) {
      const keepUserId = stateData.userId;
      if (!existingFeishuUser) {
        // Feishu identity is new — attach it to current user
        await addIdentityToUser(db, keepUserId, {
          provider: 'feishu', providerUid, meta: { open_id, union_id },
        });
        console.error(`[Auth] Linked feishu ${providerUid} to user ${keepUserId}`);
        return res.redirect('/?linked=feishu');
      }
      if (existingFeishuUser.id === keepUserId) {
        // Already linked to current user
        return res.redirect('/?linked=already');
      }
      // Feishu identity belongs to a different user — merge
      const result = await mergeUsers(db, keepUserId, existingFeishuUser.id);
      console.error(`[Auth] Merged user ${existingFeishuUser.id} into ${keepUserId}:`, result.transferred);
      return res.redirect('/?linked=feishu&merged=true');
    }

    // ── Normal login mode ─────────────────────────────────────────────────

    // Guest conversion: if current session is a guest, attach feishu identity to guest user
    const guestToken = req.cookies?.session;
    let guestUser = null;
    if (guestToken) {
      const { getSessionUser } = await import('../db/index.js');
      guestUser = await getSessionUser(db, guestToken);
      if (guestUser && !guestUser.is_guest) guestUser = null;
    }

    if (guestUser && !existingFeishuUser) {
      // Attach feishu to guest user and convert
      await addIdentityToUser(db, guestUser.id, {
        provider: 'feishu', providerUid, meta: { open_id, union_id },
      });
      await db.execute(`UPDATE users SET name = ?, avatar = ? WHERE id = ?`,
        [name ?? guestUser.name, avatar_url ?? null, guestUser.id]);
      await convertGuestToUser(db, guestUser.id);
      return res.redirect('/');
    }

    let user = existingFeishuUser;
    if (!user) {
      user = await createUserWithIdentity(
        db,
        { name: name ?? '', avatar: avatar_url ?? null },
        { provider: 'feishu', providerUid, meta: { open_id, union_id } }
      );
    } else if (union_id) {
      await updateIdentityMeta(db, 'feishu', open_id, { open_id, union_id });
    }

    await ensurePersonalDefaultTeam(db, user.id);
    const token = await createSession(db, user.id);
    res
      .cookie('session', token, { httpOnly: true, maxAge: 30 * 24 * 60 * 60 * 1000, path: '/' })
      .redirect('/');
  } catch (err) {
    console.error('[Auth] Feishu OAuth error:', err);
    res.status(500).send('Login failed, please try again.');
  }
});

// ── Session API ───────────────────────────────────────────────────────────────

router.get('/me', async (req, res) => {
  if (!req.user) return res.status(401).json({ error: 'Unauthorized' });
  const identities = await getUserIdentities(getDb(), req.user.id);
  res.json({
    id:          req.user.id,
    name:        req.user.name,
    avatar:      req.user.avatar,
    isGuest:     !!req.user.is_guest,
    createdAt:   req.user.created_at,
    identities:  identities.map(i => ({
      id:          i.id,
      provider:    i.provider,
      providerUid: i.provider_uid,
      meta:        i.meta_json ? JSON.parse(i.meta_json) : null,
      createdAt:   i.created_at,
    })),
  });
});

router.post('/logout', async (req, res) => {
  const token = req.cookies?.session;
  if (token) {
    try { await deleteSession(getDb(), token); } catch {}
    res.clearCookie('session', { path: '/' });
  }
  res.json({ ok: true });
});

// ── Guest 试用 ────────────────────────────────────────────────────────────────

router.post('/guest', async (req, res) => {
  const db = getDb();

  // 1. Create guest user
  const user = await createUserWithIdentity(
    db,
    { name: '访客', isGuest: true },
    { provider: 'guest', providerUid: uuidv4(), meta: null }
  );

  // 2. Create session (7-day expiry)
  const token = await createSession(db, user.id, 7 * 24 * 60 * 60 * 1000);

  // 3. Create default hosted agent
  const agentId = uuidv4();
  const [platformRows] = await db.execute(
    `SELECT id, api_key FROM machines WHERE server_id = ? AND is_platform = 1 LIMIT 1`,
    [DEFAULT_SERVER_ID]
  );
  const platformMachine = platformRows[0] ?? null;

  await insertAgent(db, {
    id: agentId,
    serverId: DEFAULT_SERVER_ID,
    name: 'assistant',
    displayName: 'AI 助手',
    description: '你的 AI 助手，可以回答问题和协助完成任务',
    model: 'sonnet',
    runtime: 'claude',
    machineId: platformMachine?.id ?? null,
    hosted: true,
    ownerId: user.id,
  });
  await addTeamMember(db, teamId, agentId, 'agent');

  // 5. Auto-start agent if platform machine is online
  if (platformMachine && isMachineOnline(platformMachine.id)) {
    const serverUrl = process.env.SERVER_URL ?? `http://localhost:${process.env.PORT ?? 3001}`;
    sendToDaemon(platformMachine.id, {
      type: 'agent:start', agentId, teamId,
      config: {
        runtime: 'claude', model: 'sonnet', sessionId: null,
        name: 'assistant', displayName: 'AI 助手',
        description: '你的 AI 助手，可以回答问题和协助完成任务',
        serverUrl, authToken: platformMachine.api_key,
        envVars: {},
      },
    });
  }

  res.cookie('session', token, { httpOnly: true, maxAge: 7 * 24 * 60 * 60 * 1000, path: '/' })
     .json({
       ok: true,
       user: { id: user.id, name: user.name, is_guest: 1 },
       agent: { id: agentId, name: 'assistant', displayName: 'AI 助手' },
       team: { id: teamId, name: 'all' },
     });
});

// ── 用户名密码注册 ─────────────────────────────────────────────────────────────

router.post('/register', async (req, res) => {
  const { username, password, name } = req.body;
  if (!username || !password) return res.status(400).json({ error: '用户名和密码不能为空' });
  if (password.length < 6) return res.status(400).json({ error: '密码至少 6 位' });

  const db = getDb();
  const existing = await findUserByIdentity(db, 'password', username);
  if (existing) return res.status(400).json({ error: '用户名已存在' });

  const hash = await bcrypt.hash(password, 10);

  // Guest conversion: upgrade current guest user instead of creating new one
  if (req.user?.is_guest) {
    await addIdentityToUser(db, req.user.id, 'password', username);
    await db.execute(
      `UPDATE user_identities SET credential = ? WHERE provider = 'password' AND provider_uid = ?`,
      [hash, username]
    );
    await db.execute(`UPDATE users SET name = ? WHERE id = ?`, [name || username, req.user.id]);
    await convertGuestToUser(db, req.user.id);
    return res.json({ ok: true, user: { id: req.user.id, name: name || username } });
  }

  const user = await createUserWithIdentity(
    db,
    { name: name || username },
    { provider: 'password', providerUid: username, meta: null }
  );
  // 把密码 hash 存进 credential 字段
  await db.execute(
    `UPDATE user_identities SET credential = ? WHERE provider = 'password' AND provider_uid = ?`,
    [hash, username]
  );

  await ensurePersonalDefaultTeam(db, user.id);
  const token = await createSession(db, user.id);
  res.cookie('session', token, { httpOnly: true, maxAge: 30 * 24 * 60 * 60 * 1000, path: '/' })
     .json({ ok: true, user: { id: user.id, name: user.name } });
});

// ── 用户名密码登录 ─────────────────────────────────────────────────────────────

router.post('/login', async (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) return res.status(400).json({ error: '用户名和密码不能为空' });

  const db = getDb();
  const [rows] = await db.execute(
    `SELECT u.*, ui.credential FROM users u
     JOIN user_identities ui ON ui.user_id = u.id
     WHERE ui.provider = 'password' AND ui.provider_uid = ?`,
    [username]
  );
  const record = rows[0];
  if (!record) return res.status(401).json({ error: '用户名或密码错误' });

  const ok = await bcrypt.compare(password, record.credential);
  if (!ok) return res.status(401).json({ error: '用户名或密码错误' });

  await ensurePersonalDefaultTeam(db, record.id);
  const token = await createSession(db, record.id);
  res.cookie('session', token, { httpOnly: true, maxAge: 30 * 24 * 60 * 60 * 1000, path: '/' })
     .json({ ok: true, user: { id: record.id, name: record.name } });
});
// ── Account linking ───────────────────────────────────────────────────────────

// POST /api/auth/link/password — link a password identity to current user
router.post('/link/password', async (req, res) => {
  if (!req.user) return res.status(401).json({ error: 'Unauthorized' });
  const { username, password } = req.body;
  if (!username || !password) return res.status(400).json({ error: '用户名和密码不能为空' });

  const db = getDb();
  const existing = await findUserByIdentity(db, 'password', username);

  if (!existing) {
    // Username not taken — create new password identity for current user
    if (password.length < 6) return res.status(400).json({ error: '密码至少 6 位' });
    const hash = await bcrypt.hash(password, 10);
    await addIdentityToUser(db, req.user.id, {
      provider: 'password', providerUid: username, credential: hash,
    });
    console.error(`[Auth] Linked password identity "${username}" to user ${req.user.id}`);
    return res.json({ ok: true });
  }

  if (existing.id === req.user.id) {
    return res.json({ ok: true, already: true });
  }

  // Username belongs to another user — verify password then merge
  const [rows] = await db.execute(
    `SELECT credential FROM user_identities WHERE provider = 'password' AND provider_uid = ?`, [username]
  );
  if (!rows[0]?.credential) return res.status(400).json({ error: '目标账号无密码凭证' });
  const ok = await bcrypt.compare(password, rows[0].credential);
  if (!ok) return res.status(401).json({ error: '密码错误' });

  const result = await mergeUsers(db, req.user.id, existing.id);
  console.error(`[Auth] Merged user ${existing.id} into ${req.user.id} via password link:`, result.transferred);
  return res.json({ ok: true, merged: true, transferred: result.transferred });
});

// DELETE /api/auth/identities/:identityId — unlink an identity
router.delete('/identities/:identityId', async (req, res) => {
  if (!req.user) return res.status(401).json({ error: 'Unauthorized' });
  const result = await removeIdentity(getDb(), req.params.identityId, req.user.id);
  if (!result.removed) {
    return res.status(400).json({ error: '无法解除最后一个登录方式' });
  }
  res.json({ ok: true });
});

router.post('/refresh',              (req, res) => res.status(400).json({ error: 'Use /api/auth/feishu' }));
router.post('/verify-email',         (req, res) => res.json({ ok: true }));
router.post('/resend-verification',  (req, res) => res.json({ ok: true }));
router.post('/forgot-password',      (req, res) => res.json({ ok: true }));
router.post('/reset-password',       (req, res) => res.json({ ok: true }));
router.post('/accept-invite',        (req, res) => res.json({ ok: true }));
router.get('/invite-info',           (req, res) => res.json({ valid: false }));

export default router;
export { getAppAccessToken };
