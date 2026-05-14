import { Router } from 'express';
import { getDb, getMachineByApiKey } from '../db/index.js';
import { emitJsonEvent } from '../events.js';

function bearerToken(req) {
  const auth = req.headers.authorization ?? '';
  return auth.startsWith('Bearer ') ? auth.slice(7).trim() : '';
}

export function createMachinesRouter({
  getDbImpl = getDb,
  getMachineByApiKeyImpl = getMachineByApiKey,
  emitJsonEventImpl = emitJsonEvent,
} = {}) {
  const router = Router();

  router.get('/whoami', async (req, res) => {
    const token = bearerToken(req);
    if (!token) {
      return res.status(401).json({ error: 'Missing Authorization header' });
    }

    const machine = await getMachineByApiKeyImpl(getDbImpl(), token);
    if (!machine) {
      return res.status(401).json({ error: 'Invalid machine API key' });
    }

    emitJsonEventImpl('machine.whoami', {
      machine_id: machine.id,
      server_id: machine.server_id,
      api_key_prefix: machine.api_key_prefix,
    });
    res.json({
      key_valid: true,
      server_id: machine.server_id,
    });
  });

  return router;
}

const router = createMachinesRouter();
export default router;
