import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createServer } from 'node:http';
import test from 'node:test';

import express from 'express';

import { createChannelAuth } from '../src/middleware/channel-auth.js';
import { createChannelsRouter } from '../src/routes/channels.js';
import { createWorkspacesRouter } from '../src/routes/workspaces.js';

function noop() {}

const workspaceFixtures = new Map([
  ['ws-a', { id: 'ws-a', name: 'Workspace A', owner_user_id: 'user-a', created_at: '2026-04-30 00:00:00', archived_at: null }],
  ['ws-b', { id: 'ws-b', name: 'Workspace B', owner_user_id: 'user-b', created_at: '2026-04-30 00:00:00', archived_at: null }],
]);

const workspaceMembers = new Set([
  'ws-a:user-a',
  'ws-b:user-b',
]);

const channelFixtures = new Map([
  ['channel-a', { id: 'channel-a', workspace_id: 'ws-a', name: 'Channel A', type: 'xhs-creator', capability_set: { cli_binaries: [] }, status: 'active', archived_at: null }],
]);

const channelMembers = new Set([
  'channel-a:human:user-a',
]);

function buildChannelAuth() {
  return createChannelAuth({
    getDbImpl: () => ({}),
    getWorkspaceByIdImpl: async (_db, workspaceId) => workspaceFixtures.get(workspaceId) ?? null,
    getChannelByIdImpl: async (_db, channelId) => channelFixtures.get(channelId) ?? null,
    isWorkspaceMemberImpl: async (_db, workspaceId, userId) => workspaceMembers.has(`${workspaceId}:${userId}`),
    isChannelMemberImpl: async (_db, channelId, memberType, memberId) => (
      channelMembers.has(`${channelId}:${memberType}:${memberId}`)
    ),
  });
}

function createApp(mountPath, router, user = null) {
  const app = express();
  app.use(express.json());
  if (user) {
    app.use((req, _res, next) => {
      req.user = user;
      next();
    });
  }
  app.use(mountPath, router);
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

async function requestJson(baseUrl, path, { method = 'GET', body } = {}) {
  const response = await fetch(`${baseUrl}${path}`, {
    method,
    headers: body ? { 'content-type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  const json = await response.json();
  return { status: response.status, json };
}

test('POST /api/workspaces requires auth', async () => {
  const router = createWorkspacesRouter({
    broadcastImpl: { workspaceUpdated: noop },
  });

  await withServer(createApp('/api/workspaces', router), async (baseUrl) => {
    const response = await requestJson(baseUrl, '/api/workspaces', {
      method: 'POST',
      body: { name: 'Unauthorized Workspace' },
    });

    assert.equal(response.status, 401);
    assert.deepEqual(response.json, { error: 'Unauthorized' });
  });
});

test('workspace member can read own workspace and non-member gets 403', async () => {
  const auth = buildChannelAuth();
  const router = createWorkspacesRouter({
    getDbImpl: () => ({}),
    getWorkspaceMembersImpl: async (_db, workspaceId) => {
      if (workspaceId === 'ws-a') {
        return [
          {
            user_id: 'user-a',
            role: 'owner',
            joined_at: '2026-04-30 00:00:00',
            user_name: 'User A',
            user_avatar: null,
          },
        ];
      }
      return [];
    },
    broadcastImpl: { workspaceUpdated: noop },
    requireWorkspaceReadImpl: auth.requireWorkspaceRead,
    getRequestUserIdImpl: auth.getRequestUserId,
  });

  await withServer(createApp('/api/workspaces', router, { id: 'user-a', name: 'User A' }), async (baseUrl) => {
    const ownResponse = await requestJson(baseUrl, '/api/workspaces/ws-a');
    assert.equal(ownResponse.status, 200);
    assert.equal(ownResponse.json.id, 'ws-a');
    assert.equal(ownResponse.json.members.length, 1);

    const forbiddenResponse = await requestJson(baseUrl, '/api/workspaces/ws-b');
    assert.equal(forbiddenResponse.status, 403);
    assert.deepEqual(forbiddenResponse.json, { error: 'Forbidden: workspace membership required' });
  });
});

test('POST /api/channels requires auth', async () => {
  const router = createChannelsRouter({
    broadcastImpl: { channelUpdated: noop, channelMessage: noop },
  });

  await withServer(createApp('/api/channels', router), async (baseUrl) => {
    const response = await requestJson(baseUrl, '/api/channels', {
      method: 'POST',
      body: { workspaceId: 'ws-a', name: 'Unauthorized Channel', type: 'xhs-creator' },
    });

    assert.equal(response.status, 401);
    assert.deepEqual(response.json, { error: 'Unauthorized' });
  });
});

test('GET and POST /api/channels/:id/messages require auth', async () => {
  const router = createChannelsRouter({
    broadcastImpl: { channelUpdated: noop, channelMessage: noop },
  });

  await withServer(createApp('/api/channels', router), async (baseUrl) => {
    const listResponse = await requestJson(baseUrl, '/api/channels/channel-a/messages');
    assert.equal(listResponse.status, 401);
    assert.deepEqual(listResponse.json, { error: 'Unauthorized' });

    const createResponse = await requestJson(baseUrl, '/api/channels/channel-a/messages', {
      method: 'POST',
      body: { content: 'hello' },
    });
    assert.equal(createResponse.status, 401);
    assert.deepEqual(createResponse.json, { error: 'Unauthorized' });
  });
});

test('channel creation stays allowed in own workspace and returns 403 across users', async () => {
  const auth = buildChannelAuth();
  const router = createChannelsRouter({
    getDbImpl: () => ({}),
    insertChannelImpl: async (_db, channel) => ({
      id: channel.id,
      workspace_id: channel.workspaceId,
      name: channel.name,
      type: channel.type,
      capability_set: channel.capabilitySet,
      channel_agent_id: channel.channelAgentId,
      daemon_id: channel.daemonId,
      status: channel.status,
      created_at: '2026-04-30 00:00:00',
      archived_at: null,
    }),
    addChannelMemberImpl: async () => {},
    getChannelMembersImpl: async () => [],
    broadcastImpl: { channelUpdated: noop, channelMessage: noop },
    sendToDaemonImpl: noop,
    requireWorkspaceReadImpl: auth.requireWorkspaceRead,
    requireChannelReadImpl: auth.requireChannelRead,
    requireChannelWriteImpl: auth.requireChannelWrite,
    getRequestUserIdImpl: auth.getRequestUserId,
    uuidv4Impl: () => 'channel-new',
  });

  await withServer(createApp('/api/channels', router, { id: 'user-a', name: 'User A' }), async (baseUrl) => {
    const ownResponse = await requestJson(baseUrl, '/api/channels', {
      method: 'POST',
      body: { workspaceId: 'ws-a', name: 'Own Channel', type: 'xhs-creator' },
    });
    assert.equal(ownResponse.status, 201);
    assert.equal(ownResponse.json.workspaceId, 'ws-a');

    const forbiddenResponse = await requestJson(baseUrl, '/api/channels', {
      method: 'POST',
      body: { workspaceId: 'ws-b', name: 'Forbidden Channel', type: 'xhs-creator' },
    });
    assert.equal(forbiddenResponse.status, 403);
    assert.deepEqual(forbiddenResponse.json, { error: 'Forbidden: workspace membership required' });
  });
});
