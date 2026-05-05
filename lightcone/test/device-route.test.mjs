import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createServer } from 'node:http';
import test from 'node:test';

import express from 'express';

import { createDeviceRouter } from '../src/routes/device.js';

function createApp(router) {
  const app = express();
  app.use(express.json());
  app.use('/api/device', router);
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

async function requestJson(baseUrl, path, { token = 'machine-key', body } = {}) {
  const response = await fetch(`${baseUrl}${path}`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      ...(token ? { authorization: `Bearer ${token}` } : {}),
    },
    body: JSON.stringify(body ?? {}),
  });
  return { status: response.status, json: await response.json() };
}

test('POST /api/device/result bridges device status to daemon dispatch message', async () => {
  const daemonRequests = [];
  const router = createDeviceRouter({
    getDbImpl: () => ({}),
    getMachineByApiKeyImpl: async (_db, token) => (token === 'machine-key' ? { id: 'device-machine' } : null),
    getChannelByIdImpl: async () => ({ id: 'channel-a', daemon_id: 'daemon-a' }),
    isMachineOnlineImpl: (daemonId) => daemonId === 'daemon-a',
    requestFromDaemonImpl: async (daemonId, request, responseKey, timeoutMs) => {
      daemonRequests.push({ daemonId, request, responseKey, timeoutMs });
      return { ok: true, message: { messageId: 'msg-device-result' } };
    },
    uuidv4Impl: () => 'req-device-1',
  });

  await withServer(createApp(router), async (baseUrl) => {
    const response = await requestJson(baseUrl, '/api/device/result', {
      body: {
        channelId: 'channel-a',
        correlationId: 'corr-a',
        status: 'completed',
        deviceId: 'chrome-ext',
        result: { url: 'https://example.test/note' },
      },
    });

    assert.equal(response.status, 202);
    assert.equal(response.json.ok, true);
    assert.equal(daemonRequests.length, 1);
    assert.deepEqual(daemonRequests[0], {
      daemonId: 'daemon-a',
      request: {
        type: 'channel:message.send',
        requestId: 'req-device-1',
        channelId: 'channel-a',
        senderType: 'external',
        senderKind: 'external',
        senderId: 'external:device:chrome-ext',
        senderName: 'chrome-ext',
        messageType: 'dispatch.completed',
        payloadType: 'dispatch.completed',
        payloadBody: {
          status: 'completed',
          device_id: 'chrome-ext',
          correlation_id: 'corr-a',
          result: { url: 'https://example.test/note' },
        },
        content: JSON.stringify({
          status: 'completed',
          device_id: 'chrome-ext',
          correlation_id: 'corr-a',
          result: { url: 'https://example.test/note' },
        }),
        correlationId: 'corr-a',
        parentId: null,
        audience: ['channel'],
        origin: 'external',
        attachments: [],
      },
      responseKey: 'req-device-1',
      timeoutMs: 10000,
    });
  });
});

test('POST /api/device/result rejects missing or invalid machine bearer token', async () => {
  const router = createDeviceRouter({
    getDbImpl: () => ({}),
    getMachineByApiKeyImpl: async () => null,
  });

  await withServer(createApp(router), async (baseUrl) => {
    const missing = await requestJson(baseUrl, '/api/device/result', { token: '', body: {} });
    assert.equal(missing.status, 401);
    assert.equal(missing.json.error, 'Missing Authorization header');

    const invalid = await requestJson(baseUrl, '/api/device/result', { token: 'bad-token', body: {} });
    assert.equal(invalid.status, 401);
    assert.equal(invalid.json.error, 'Invalid machine API key');
  });
});
