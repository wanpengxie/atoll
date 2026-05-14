import { Router } from 'express';
import {
  getDb,
  getUsers,
  findUserByIdentity,
  createUserWithIdentity,
  resolveUser,
} from '../db/index.js';

const router = Router();

router.get('/', async (req, res) => {
  const users = await getUsers(getDb(), {
    query: String(req.query.q ?? ''),
    limit: Number(req.query.limit ?? 20),
  });
  res.json(users.map(user => ({
    id: user.id,
    name: user.name,
    avatarUrl: user.avatar ?? null,
    isGuest: !!user.is_guest,
    createdAt: user.created_at,
  })));
});

/**
 * POST /api/users/feishu-register
 * Called by feishu-bridge when a human user sends a message.
 * Returns { user, isNew } — caller uses isNew to decide whether to send onboarding DM.
 *
 * Body: { open_id, union_id?, name?, avatar? }
 * Auth: ADMIN_TOKEN (internal use only)
 */
router.post('/feishu-register', async (req, res) => {
  const authHeader = req.headers.authorization ?? '';
  if (authHeader !== `Bearer ${process.env.ADMIN_TOKEN}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const { open_id, union_id, name, avatar } = req.body;
  if (!open_id) return res.status(400).json({ error: 'open_id required' });

  try {
    const db = getDb();

    // Prefer union_id as the canonical identifier, fallback to open_id
    const providerUid = union_id ?? open_id;

    // Check union_id first, then open_id (handles upgrade case)
    let user = union_id ? await findUserByIdentity(db, 'feishu', union_id) : null;
    if (!user) user = await findUserByIdentity(db, 'feishu', open_id);

    if (user) {
      // Follow merged_into chain to canonical user
      user = await resolveUser(db, user.id) ?? user;
      // Backfill name if missing
      if (!user.name && name) {
        await db.execute('UPDATE users SET name = ? WHERE id = ?', [name, user.id]);
        user.name = name;
      }
      return res.json({ user, isNew: false });
    }

    user = await createUserWithIdentity(
      db,
      { name: name ?? '', avatar: avatar ?? null },
      { provider: 'feishu', providerUid, meta: { open_id, union_id: union_id ?? null } }
    );

    res.json({ user, isNew: true });
  } catch (err) {
    console.error('[Users] feishu-register error:', err);
    res.status(500).json({ error: err.message });
  }
});

export default router;
