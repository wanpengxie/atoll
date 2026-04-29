import { getDb, getSessionUser } from '../db/index.js';

function getToken(req) {
  // 优先读 cookie，其次读 Authorization header（Bearer token）
  if (req.cookies?.session) return req.cookies.session;
  const auth = req.headers.authorization ?? '';
  if (auth.startsWith('Bearer ')) return auth.slice(7);
  return null;
}

export async function loadUser(req, res, next) {
  const token = getToken(req);
  if (token) {
    const adminToken = process.env.ADMIN_TOKEN ?? 'demo-token';
    if (token === adminToken) {
      // Service token — mark as service caller, actual user identity comes from request body
      req.isService = true;
      req.user = { id: 'service', name: 'Service' };
    } else {
      try {
        const user = await getSessionUser(getDb(), token);
        if (user) req.user = user;
      } catch {}
    }
  }
  next();
}

export function requireAuth(req, res, next) {
  if (!req.user) return res.status(401).json({ error: 'Unauthorized' });
  next();
}
