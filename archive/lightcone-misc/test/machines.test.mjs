import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createServer } from 'node:http';
import test from 'node:test';

import express from 'express';

import { createMachinesRouter } from '../src/routes/machines.js';

function createApp(router) {
  const app = express();
  app.use(express.json());
  app.use('/api/machines', router);
  app.use((err, _req, res, _next) => {
    res.status(500).json({ error: err.message });
  });
  return app;
}

async function withServer(app, run) {
  const server = createServer(app);
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const { port } = server.address();
  try {
    await run(`http://127.0.0.1:${port}`);
  } finally {
    await new Promise((resolve, reject) => {
      server.close((err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }
}

async function requestJson(baseUrl, path, { headers = {} } = {}) {
  const response = await fetch(`${baseUrl}${path}`, { headers });
  const json = await response.json();
  return { status: response.status, json };
}

test('GET /api/machines/whoami returns minimal successful response', async () => {
  const router = createMachinesRouter({
    getDbImpl: () => ({}),
    getMachineByApiKeyImpl: async (_db, token) => {
      assert.equal(token, 'sk_machine_valid');
      return {
        id: 'machine-a',
        server_id: 'server-a',
        api_key_prefix: 'sk_machine_valid',
        created_at: '2026-05-02 00:00:00',
      };
    },
    emitJsonEventImpl: () => {},
  });

  await withServer(createApp(router), async (baseUrl) => {
    const response = await requestJson(baseUrl, '/api/machines/whoami', {
      headers: { Authorization: 'Bearer sk_machine_valid' },
    });

    assert.equal(response.status, 200);
    assert.deepEqual(response.json, {
      key_valid: true,
      server_id: 'server-a',
    });
    assert.equal(Object.hasOwn(response.json, 'machine_id'), false);
    assert.equal(Object.hasOwn(response.json, 'registered_at'), false);
  });
});
