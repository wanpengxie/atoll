import { Router } from 'express';
import { getDb, getMachineByApiKey } from '../db/index.js';
import { emitJsonEvent } from '../events.js';

const router = Router();

function bearerToken(req) {
  const auth = req.headers.authorization ?? '';
  return auth.startsWith('Bearer ') ? auth.slice(7).trim() : '';
}

router.get('/whoami', async (req, res) => {
  const token = bearerToken(req);
  if (!token) {
    return res.status(401).json({ error: 'Missing Authorization header' });
  }

  const machine = await getMachineByApiKey(getDb(), token);
  if (!machine) {
    return res.status(401).json({ error: 'Invalid machine API key' });
  }

  emitJsonEvent('machine.whoami', {
    machine_id: machine.id,
    server_id: machine.server_id,
    api_key_prefix: machine.api_key_prefix,
  });
  res.json({
    machine_id: machine.id,
    server_id: machine.server_id,
    key_valid: true,
    registered_at: machine.created_at,
  });
});

export default router;
