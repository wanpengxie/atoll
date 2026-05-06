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
  ['channel-a', {
    id: 'channel-a',
    workspace_id: 'ws-a',
    name: 'Channel A',
    type: 'xhs-creator',
    capability_set: { cli_binaries: [] },
    daemon_id: 'machine-a',
    status: 'active',
    archived_at: null,
  }],
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
  let idCounter = 0;
  const router = createChannelsRouter({
    getDbImpl: () => ({}),
    getMachinesImpl: async () => [{ id: 'machine-a', status: 'online' }],
    insertAgentImpl: async (_db, agent) => ({ id: agent.id }),
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
    uuidv4Impl: () => {
      idCounter += 1;
      return idCounter === 1 ? 'agent-new' : 'channel-new';
    },
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

test('POST /api/channels/:id/messages returns 503 when bound daemon is offline', async () => {
  const auth = buildChannelAuth();
  let rpcCalled = false;
  const router = createChannelsRouter({
    getDbImpl: () => ({}),
    broadcastImpl: { channelUpdated: noop, channelMessage: noop },
    isMachineOnlineImpl: () => false,
    requestFromDaemonImpl: async () => {
      rpcCalled = true;
      return { ok: true };
    },
    requireChannelReadImpl: auth.requireChannelRead,
    requireChannelWriteImpl: auth.requireChannelWrite,
    getRequestUserIdImpl: auth.getRequestUserId,
  });

  await withServer(createApp('/api/channels', router, { id: 'user-a', name: 'User A' }), async (baseUrl) => {
    const response = await requestJson(baseUrl, '/api/channels/channel-a/messages', {
      method: 'POST',
      body: { content: 'hello offline daemon' },
    });

    assert.equal(response.status, 503);
    assert.deepEqual(response.json, { error: 'Channel daemon offline' });
    assert.equal(rpcCalled, false);
  });
});

test('POST /api/channels/:id/messages proxies to daemon-first path and does not write MySQL directly', async () => {
  const auth = buildChannelAuth();
  const daemonRequests = [];
  let insertMessageCalled = false;
  const router = createChannelsRouter({
    getDbImpl: () => ({}),
    broadcastImpl: { channelUpdated: noop, channelMessage: noop },
    insertMessageImpl: async () => {
      insertMessageCalled = true;
      throw new Error('insertMessage should not be called for human channel POST');
    },
    isMachineOnlineImpl: () => true,
    requestFromDaemonImpl: async (machineId, request, responseKey, timeoutMs) => {
      daemonRequests.push({ machineId, request, responseKey, timeoutMs });
      return {
        ok: true,
        message: {
          messageId: 'msg-daemon-1',
          channelId: request.channelId,
          senderType: request.senderType,
          senderId: request.senderId,
          senderName: request.senderName,
          messageType: request.messageType,
          content: request.content,
          attachments: [],
          createdAt: '2026-04-30T00:00:00.000Z',
        },
      };
    },
    requireChannelReadImpl: auth.requireChannelRead,
    requireChannelWriteImpl: auth.requireChannelWrite,
    getRequestUserIdImpl: auth.getRequestUserId,
    uuidv4Impl: () => 'req-daemon-1',
  });

  await withServer(createApp('/api/channels', router, { id: 'user-a', name: 'User A' }), async (baseUrl) => {
    const response = await requestJson(baseUrl, '/api/channels/channel-a/messages', {
      method: 'POST',
      body: { content: 'hello daemon path' },
    });

    assert.equal(response.status, 201);
    assert.equal(insertMessageCalled, false);
    assert.equal(daemonRequests.length, 1);
    assert.deepEqual(daemonRequests[0], {
      machineId: 'machine-a',
      request: {
        type: 'channel:message.send',
        requestId: 'req-daemon-1',
        channelId: 'channel-a',
        senderType: 'human',
        senderKind: 'human',
        senderId: 'user-a',
        senderName: 'User A',
        messageType: 'chat',
        payloadType: 'user.text',
        payloadBody: { text: 'hello daemon path', attachments: [] },
        content: 'hello daemon path',
        attachments: [],
      },
      responseKey: 'req-daemon-1',
      timeoutMs: 10000,
    });
    assert.deepEqual(response.json, {
      id: 'msg-daemon-1',
      seq: null,
      teamId: null,
      channelId: 'channel-a',
      senderType: 'human',
      senderId: 'user-a',
      senderName: 'User A',
      messageType: 'chat',
      content: 'hello daemon path',
      threadId: null,
      mentions: null,
      taskStatus: null,
      taskNumber: null,
      taskAssigneeType: null,
      taskAssigneeId: null,
      taskAssigneeName: null,
      taskClaimedAt: null,
      taskCompletedAt: null,
      createdAt: '2026-04-30T00:00:00.000Z',
      updatedAt: null,
      attachments: [],
    });
  });
});

test('GET /api/channels/:id/tasks proxies task list and show to the channel daemon', async () => {
  const auth = buildChannelAuth();
  const daemonRequests = [];
  const requestIds = ['req-task-list', 'req-task-show'];
  const router = createChannelsRouter({
    getDbImpl: () => ({}),
    broadcastImpl: { channelUpdated: noop, channelMessage: noop },
    isMachineOnlineImpl: () => true,
    requestFromDaemonImpl: async (machineId, request, responseKey, timeoutMs) => {
      daemonRequests.push({ machineId, request, responseKey, timeoutMs });
      if (request.method === 'task.list') {
        return {
          ok: true,
          result: {
            tasks: [{
              task_id: 'task-a',
              channel_id: request.params.channel_id,
              title: 'Task A',
              status: 'opened',
            }],
          },
        };
      }
      return {
        ok: true,
        result: {
          task: {
            task_id: request.params.task_id,
            channel_id: request.params.channel_id,
            title: 'Task A',
            status: 'opened',
          },
          doc: { ref: 'notes/tasks/task-a.md', content: '# Task A\n' },
          messages: [],
        },
      };
    },
    requireChannelReadImpl: auth.requireChannelRead,
    requireChannelWriteImpl: auth.requireChannelWrite,
    getRequestUserIdImpl: auth.getRequestUserId,
    uuidv4Impl: () => requestIds.shift(),
  });

  await withServer(createApp('/api/channels', router, { id: 'user-a', name: 'User A' }), async (baseUrl) => {
    const listResponse = await requestJson(baseUrl, '/api/channels/channel-a/tasks?status=active&mine=true&parent=task-parent');
    const showResponse = await requestJson(baseUrl, '/api/channels/channel-a/tasks/task-a');

    assert.equal(listResponse.status, 200);
    assert.deepEqual(listResponse.json.tasks.map((task) => task.task_id), ['task-a']);
    assert.equal(showResponse.status, 200);
    assert.equal(showResponse.json.task.task_id, 'task-a');
    assert.deepEqual(daemonRequests, [
      {
        machineId: 'machine-a',
        request: {
          type: 'channel:rpc',
          requestId: 'req-task-list',
          method: 'task.list',
          channelId: 'channel-a',
          params: {
            status: 'active',
            mine: true,
            parent_task_id: 'task-parent',
            channel_id: 'channel-a',
          },
        },
        responseKey: 'req-task-list',
        timeoutMs: 10000,
      },
      {
        machineId: 'machine-a',
        request: {
          type: 'channel:rpc',
          requestId: 'req-task-show',
          method: 'task.show',
          channelId: 'channel-a',
          params: {
            task_id: 'task-a',
            channel_id: 'channel-a',
          },
        },
        responseKey: 'req-task-show',
        timeoutMs: 10000,
      },
    ]);
  });
});

test('GET /api/channels/:id/tasks maps daemon RPC task errors to their HTTP status', async () => {
  const auth = buildChannelAuth();
  const cases = [
    {
      daemonResult: {
        ok: false,
        error: { code: 'task_already_terminal', message: 'task already terminal', statusCode: 409 },
        code: 'task_already_terminal',
      },
      status: 409,
      code: 'task_already_terminal',
    },
    {
      daemonResult: {
        ok: false,
        error: { code: 'invalid_envelope', message: 'not_before requires audience=[self]' },
        code: 'invalid_envelope',
      },
      status: 400,
      code: 'invalid_envelope',
    },
    {
      daemonResult: {
        ok: false,
        error: 'schedule already exists: daily',
        code: 'schedule_exists',
      },
      status: 409,
      code: 'schedule_exists',
    },
  ];
  let index = 0;
  const router = createChannelsRouter({
    getDbImpl: () => ({}),
    broadcastImpl: { channelUpdated: noop, channelMessage: noop },
    isMachineOnlineImpl: () => true,
    requestFromDaemonImpl: async () => cases[index++].daemonResult,
    requireChannelReadImpl: auth.requireChannelRead,
    requireChannelWriteImpl: auth.requireChannelWrite,
    getRequestUserIdImpl: auth.getRequestUserId,
    uuidv4Impl: () => `req-error-${index}`,
  });

  await withServer(createApp('/api/channels', router, { id: 'user-a', name: 'User A' }), async (baseUrl) => {
    for (const item of cases) {
      const response = await requestJson(baseUrl, '/api/channels/channel-a/tasks');
      assert.equal(response.status, item.status);
      assert.equal(response.json.error.code, item.code);
      assert.equal(typeof response.json.error.message, 'string');
    }
  });
});
