/**
 * OAuth / account connection routes
 *
 * X (Twitter) — OAuth 1.0a three-legged flow
 *   Consumer Key/Secret can be set via env vars (X_CONSUMER_KEY / X_CONSUMER_SECRET)
 *   OR configured per-server through the UI (stored encrypted in platform_credentials).
 *
 * 小红书 — manual cookie flow (no official OAuth)
 *
 * Optional env:
 *   OAUTH_CALLBACK_BASE — override callback base URL (e.g. https://app.railway.app)
 */
import { Router } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { requireAuth } from '../middleware/auth.js';
import { getRequestToken, getAccessToken } from '../oauth1.js';
import { decrypt } from '../crypto.js';
import { getDb, insertCredential, getCredentialsByOwner, deleteCredential, getCredentialById } from '../db/index.js';
import { sendToDaemon } from '../daemon/connections.js';
import { setPendingBrowserLogin } from '../daemon/index.js';

const router = Router();
const DEFAULT_SERVER_ID = process.env.DEFAULT_SERVER_ID ?? 'server-001';

// ── App-level credential helpers (server-level, owner_id = '__server__') ──────

async function getAppSetting(db, platform) {
  const [rows] = await db.execute(
    `SELECT * FROM platform_credentials WHERE owner_id = '__server__' AND platform = ? AND is_del = 0 AND deleted_at IS NULL`,
    [platform]
  );
  if (!rows[0]) return null;
  try { return decrypt(rows[0].iv, rows[0].encrypted_data); } catch { return null; }
}

async function saveAppSetting(db, platform, fields) {
  // Upsert: delete old then insert fresh
  await db.execute(
    `UPDATE platform_credentials
     SET is_del = 1, deleted_at = COALESCE(deleted_at, NOW())
     WHERE owner_id = '__server__' AND platform = ? AND is_del = 0`,
    [platform]
  );
  const { iv, data } = encrypt(fields);
  await db.execute(
    `INSERT INTO platform_credentials (id, server_id, owner_id, platform, display_name, credential_type, encrypted_data, iv, scopes)
     VALUES (?, ?, '__server__', ?, ?, 'app_secret', ?, ?, '[]')`,
    [uuidv4(), DEFAULT_SERVER_ID, platform, `${platform} app credentials`, data, iv]
  );
}

// ── Platform definitions ───────────────────────────────────────────────────────
const PLATFORMS = {
  x: {
    label: 'X (Twitter)',
    oauthType: 'oauth1',
    requestTokenUrl: 'https://api.twitter.com/oauth/request_token',
    authorizeUrl:    'https://api.twitter.com/oauth/authorize',
    accessTokenUrl:  'https://api.twitter.com/oauth/access_token',
    async getConsumerKey(db)    { return process.env.X_CONSUMER_KEY    || (await getAppSetting(db, 'x'))?.X_CONSUMER_KEY    || ''; },
    async getConsumerSecret(db) { return process.env.X_CONSUMER_SECRET || (await getAppSetting(db, 'x'))?.X_CONSUMER_SECRET || ''; },
    envKeys: ['X_ACCESS_TOKEN', 'X_ACCESS_SECRET'],
    mapTokens: (t) => ({ X_ACCESS_TOKEN: t.oauth_token, X_ACCESS_SECRET: t.oauth_token_secret }),
    accountLabel: (t) => t.screen_name ? `@${t.screen_name}` : 'X 账号',
    appFields: [
      { key: 'X_CONSUMER_KEY',    label: 'API Key (Consumer Key)' },
      { key: 'X_CONSUMER_SECRET', label: 'API Secret (Consumer Secret)', type: 'password' },
    ],
  },
  'x-readonly': {
    label: 'X (只读 / Bearer Token)',
    oauthType: 'token',
    envKeys: ['X_BEARER_TOKEN'],
    accountLabel: () => 'X 只读账号',
  },
  xhs: {
    label: '小红书',
    oauthType: 'browser_profile',
    envKeys: ['XHS_PROFILE_DIR'],
    accountLabel: () => '小红书账号',
  },
  douyin: {
    label: '抖音',
    oauthType: 'browser_profile',
    envKeys: ['DOUYIN_PROFILE_DIR'],
    accountLabel: () => '抖音账号',
  },
  kuaishou: {
    label: '快手',
    oauthType: 'browser_profile',
    envKeys: ['KUAISHOU_PROFILE_DIR'],
    accountLabel: () => '快手账号',
  },
};

// ── In-memory state for OAuth 1.0a handshake ──────────────────────────────────
const pendingStates = new Map();
setInterval(() => {
  const now = Date.now();
  for (const [k, v] of pendingStates) if (v.expiresAt < now) pendingStates.delete(k);
}, 60_000);

function callbackBase(req) {
  if (process.env.OAUTH_CALLBACK_BASE) return process.env.OAUTH_CALLBACK_BASE.replace(/\/$/, '');
  const proto = req.headers['x-forwarded-proto'] ?? req.protocol;
  return `${proto}://${req.get('host')}`;
}

// ── GET /api/oauth/platforms ───────────────────────────────────────────────────
router.get('/platforms', requireAuth, async (req, res) => {
  const db = getDb();
  const existing = await getCredentialsByOwner(db, req.user.id);

  const result = await Promise.all(Object.entries(PLATFORMS).map(async ([key, p]) => {
    const connected = existing.find(c => c.platform === key);
    let appConfigured = true;
    if (p.oauthType === 'oauth1') {
      const ck = await p.getConsumerKey(db);
      appConfigured = !!ck;
    }
    return {
      platform: key,
      label: p.label,
      oauthType: p.oauthType,
      appConfigured,
      connected: !!connected,
      displayName: connected?.display_name ?? null,
      credentialId: connected?.id ?? null,
      appFields: p.appFields ?? null,
    };
  }));

  res.json(result);
});

// ── POST /api/oauth/:platform/app-setup — save app consumer key/secret ────────
router.post('/:platform/app-setup', requireAuth, async (req, res) => {
  const p = PLATFORMS[req.params.platform];
  if (!p || !p.appFields) return res.status(400).json({ error: 'Not supported' });
  const fields = {};
  for (const f of p.appFields) {
    if (req.body[f.key]) fields[f.key] = req.body[f.key];
  }
  if (!Object.keys(fields).length) return res.status(400).json({ error: 'No fields provided' });
  await saveAppSetting(getDb(), req.params.platform, fields);
  res.json({ ok: true });
});

// ── GET /api/oauth/:platform/start — begin OAuth flow ─────────────────────────
router.get('/:platform/start', requireAuth, async (req, res) => {
  const p = PLATFORMS[req.params.platform];
  if (!p) return res.redirect('/?oauth=error&reason=unsupported_platform');
  if (p.oauthType !== 'oauth1') return res.status(400).json({ error: 'Use cookie flow for this platform' });

  const db = getDb();
  const consumerKey    = await p.getConsumerKey(db);
  const consumerSecret = await p.getConsumerSecret(db);

  if (!consumerKey || !consumerSecret) {
    return res.redirect(`/?oauth=error&reason=app_not_configured&platform=${req.params.platform}`);
  }

  const callbackUrl = `${callbackBase(req)}/api/oauth/${req.params.platform}/callback`;
  let tokens;
  try {
    tokens = await getRequestToken(consumerKey, consumerSecret, callbackUrl, p.requestTokenUrl);
  } catch (err) {
    console.error(`[OAuth] ${req.params.platform} request token error:`, err.message);
    return res.redirect(`/?oauth=error&reason=request_token_failed&platform=${req.params.platform}`);
  }

  pendingStates.set(tokens.oauth_token, {
    secret: tokens.oauth_token_secret,
    userId: req.user.id,
    platform: req.params.platform,
    expiresAt: Date.now() + 10 * 60 * 1000,
  });

  res.redirect(`${p.authorizeUrl}?oauth_token=${tokens.oauth_token}`);
});

// ── GET /api/oauth/:platform/callback — handle OAuth callback ─────────────────
router.get('/:platform/callback', async (req, res) => {
  const platformKey = req.params.platform;
  const p = PLATFORMS[platformKey];
  if (!p) return res.redirect('/?oauth=error&reason=unsupported_platform');

  const { oauth_token, oauth_verifier, denied } = req.query;
  if (denied) return res.redirect('/?oauth=denied');
  if (!oauth_token || !oauth_verifier) return res.redirect('/?oauth=error&reason=missing_params');

  const state = pendingStates.get(oauth_token);
  if (!state) return res.redirect('/?oauth=error&reason=state_expired');
  pendingStates.delete(oauth_token);

  const db = getDb();
  const consumerKey    = await p.getConsumerKey(db);
  const consumerSecret = await p.getConsumerSecret(db);

  let tokenData;
  try {
    tokenData = await getAccessToken(consumerKey, consumerSecret, oauth_token, state.secret, oauth_verifier, p.accessTokenUrl);
  } catch (err) {
    console.error(`[OAuth] ${platformKey} access token error:`, err.message);
    return res.redirect(`/?oauth=error&reason=access_token_failed&platform=${platformKey}`);
  }

  const envVars = p.mapTokens(tokenData);
  const displayName = p.accountLabel(tokenData);
  const { iv, data } = encrypt(envVars);

  // Remove old credential for this platform
  const existing = await getCredentialsByOwner(db, state.userId);
  for (const c of existing) if (c.platform === platformKey) await deleteCredential(db, c.id);

  await insertCredential(db, {
    id: uuidv4(), serverId: DEFAULT_SERVER_ID, ownerId: state.userId,
    platform: platformKey, displayName, credentialType: 'oauth1',
    encryptedData: data, iv, scopes: p.envKeys,
  });

  res.redirect(`/?oauth=success&platform=${platformKey}`);
});

// ── POST /api/oauth/x-readonly/connect — save X Bearer Token ─────────────────
router.post('/x-readonly/connect', requireAuth, async (req, res) => {
  const { token } = req.body;
  if (!token?.trim()) return res.status(400).json({ error: 'token required' });

  const db = getDb();
  const existing = await getCredentialsByOwner(db, req.user.id);
  for (const c of existing) if (c.platform === 'x-readonly') await deleteCredential(db, c.id);

  const { iv, data } = encrypt({ X_BEARER_TOKEN: token.trim() });
  await insertCredential(db, {
    id: uuidv4(), serverId: DEFAULT_SERVER_ID, ownerId: req.user.id,
    platform: 'x-readonly', displayName: 'X 只读账号', credentialType: 'token',
    encryptedData: data, iv, scopes: ['X_BEARER_TOKEN'],
  });

  res.json({ ok: true });
});

// ── GET /api/oauth/browser-login/machines — list online machines for browser login ──
router.get('/browser-login/machines', requireAuth, async (req, res) => {
  const db = getDb();
  const [rows] = await db.execute(
    `SELECT id, name, hostname, is_platform FROM machines WHERE status = 'online' AND is_del = 0 AND deleted_at IS NULL`
  );
  const machines = rows.map(m => ({
    id: m.id,
    name: m.hostname ?? m.name ?? m.id,
    isPlatform: !!m.is_platform,
  }));
  res.json(machines);
});

// ── POST /api/oauth/:platform/browser-login/start — ask daemon to start browser login ──
router.post('/:platform/browser-login/start', requireAuth, async (req, res) => {
  const { platform } = req.params;
  const p = PLATFORMS[platform];
  if (!p || p.oauthType !== 'browser_profile') return res.status(400).json({ error: 'Not a browser_profile platform' });
  const { machineId } = req.body;
  if (!machineId) return res.status(400).json({ error: 'machineId required' });
  const db = getDb();
  const [rows] = await db.execute(
    `SELECT id FROM machines WHERE id = ? AND status = 'online' AND is_del = 0 AND deleted_at IS NULL`, [machineId]
  );
  if (!rows.length) return res.status(503).json({ error: '该机器不在线' });
  setPendingBrowserLogin(machineId, req.user.id, platform);
  sendToDaemon(machineId, { type: 'browser:start_login', platform, userId: req.user.id });
  res.json({ ok: true });
});

// ── POST /api/oauth/:platform/browser-login/close — ask daemon to stop browser login ──
router.post('/:platform/browser-login/close', requireAuth, async (req, res) => {
  const { platform } = req.params;
  const { machineId } = req.body;
  if (machineId) sendToDaemon(machineId, { type: 'browser:stop_login', platform });
  res.json({ ok: true });
});

export default router;
