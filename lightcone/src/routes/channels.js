import { Router } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { PayloadType, SenderKind } from '@coagent/payload-types';
import {
  getDb,
  insertChannel,
  insertAgent,
  getMachines,
  addChannelMember,
  getChannelMembers,
  updateChannel,
  isWorkspaceOwner,
  findUserByIdOrName,
  getChannelMessages,
  insertMessage,
} from '../db/index.js';
import { isMachineOnline, sendToDaemon } from '../daemon/connections.js';
import { requestFromDaemon } from '../daemon/index.js';
import { broadcast } from '../realtime/broadcast.js';
import { formatMessage } from '../internal/index.js';
import { requireAuth } from '../middleware/auth.js';
import { emitJsonEvent } from '../events.js';
import {
  requireWorkspaceRead,
  requireChannelRead,
  requireChannelWrite,
  getRequestUserId,
} from '../middleware/channel-auth.js';

const DEFAULT_SERVER_ID = process.env.DEFAULT_SERVER_ID ?? 'server-001';
const DEFAULT_USER_NAME = process.env.DEFAULT_USER_NAME ?? 'Admin';
const DEFAULT_DAEMON_REQUEST_TIMEOUT_MS = 10_000;

const ALLOWED_CHANNEL_TYPES = new Set(['xhs-creator']);
const ALLOWED_MEMBER_TYPES = new Set(['human', 'channel_agent', 'sub_agent', 'worker']);

const CAPABILITY_DEFAULTS_BY_TYPE = {
  'xhs-creator': { cli_binaries: ['coagent', 'xhs', 'coagent-kernel', 'coagent-msg'] },
};

const PM_DESCRIPTION_BY_TYPE = {
  'xhs-creator': '小红书内容运营 PM。负责协调 channel 内的工作，理解需求，调用 xhs 等 CLI 工具完成发布、查询、管理任务。',
};

function normalizeCapabilitySet(raw) {
  const value = raw ?? { cli_binaries: [] };
  if (typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('capabilitySet must be an object');
  }
  const keys = Object.keys(value);
  if (keys.some(key => key !== 'cli_binaries')) {
    throw new Error('capabilitySet only supports cli_binaries in M1.0');
  }
  const cliBinaries = Array.isArray(value.cli_binaries)
    ? [...new Set(value.cli_binaries.map(item => String(item).trim()).filter(Boolean))]
    : [];
  return { cli_binaries: cliBinaries };
}

function formatChannel(channel) {
  return {
    id: channel.id,
    workspaceId: channel.workspace_id,
    name: channel.name,
    type: channel.type,
    capabilitySet: typeof channel.capability_set === 'string'
      ? JSON.parse(channel.capability_set)
      : (channel.capability_set ?? { cli_binaries: [] }),
    channelAgentId: channel.channel_agent_id ?? null,
    daemonId: channel.daemon_id ?? null,
    status: channel.status,
    createdAt: channel.created_at,
    archivedAt: channel.archived_at ?? null,
  };
}

function formatChannelMember(member) {
  const displayName = member.member_type === 'human'
    ? (member.human_name ?? member.member_id)
    : (member.agent_display_name ?? member.agent_name ?? member.member_id);
  return {
    memberType: member.member_type,
    memberId: member.member_id,
    displayName,
    avatarUrl: member.human_avatar ?? null,
    joinedAt: member.joined_at,
  };
}

function defaultSenderName(senderType, senderId) {
  switch (senderType) {
    case 'human':
      return senderId;
    case 'channel_agent':
      return `channel-agent:${senderId}`;
    case 'sub_agent':
      return `sub-agent:${senderId}`;
    case 'worker':
      return `worker:${senderId}`;
    default:
      return senderId;
  }
}

function explicitStatusCode(value) {
  const statusCode = Number(value);
  return Number.isInteger(statusCode) && statusCode >= 400 && statusCode <= 599
    ? statusCode
    : null;
}

function daemonErrorStatus(result) {
  const explicit = explicitStatusCode(result?.error?.statusCode ?? result?.statusCode);
  if (explicit) return explicit;

  const code = typeof result === 'string'
    ? result
    : (result?.error?.code ?? result?.code);
  switch (code) {
    case 'bad_request':
    case 'invalid_envelope':
      return 400;
    case 'unauthorized':
      return 401;
    case 'not_found':
    case 'channel_type_config_not_found':
      return 404;
    case 'conflict':
    case 'task_already_terminal':
    case 'schedule_exists':
      return 409;
    default:
      return 503;
  }
}

function daemonRpcErrorBody(result, method) {
  const error = result?.error;
  if (error && typeof error === 'object' && !Array.isArray(error)) {
    const code = String(error.code ?? result?.code ?? 'daemon_rpc_failed');
    const message = String(error.message ?? `Channel daemon RPC failed: ${method}`);
    return { code, message };
  }
  return {
    code: String(result?.code ?? 'daemon_rpc_failed'),
    message: String(error ?? `Channel daemon RPC failed: ${method}`),
  };
}

function formatDaemonMessagePayload(message) {
  const senderType = message?.senderType ?? message?.sender_type ?? 'human';
  const senderId = message?.senderId ?? message?.sender_id ?? '';
  return {
    id: message?.messageId ?? message?.message_id ?? null,
    seq: null,
    teamId: null,
    channelId: message?.channelId ?? message?.channel_id ?? null,
    senderType,
    senderId,
    senderName: message?.senderName ?? message?.sender_name ?? defaultSenderName(senderType, senderId),
    messageType: message?.messageType ?? message?.message_type ?? 'chat',
    content: String(message?.content ?? ''),
    threadId: null,
    mentions: null,
    taskStatus: null,
    taskNumber: null,
    taskAssigneeType: null,
    taskAssigneeId: null,
    taskAssigneeName: null,
    taskClaimedAt: null,
    taskCompletedAt: null,
    createdAt: message?.createdAt ?? message?.created_at ?? new Date().toISOString(),
    updatedAt: null,
    attachments: Array.isArray(message?.attachments) ? message.attachments : [],
  };
}

export async function appendDaemonChannelMessage({
  requestId,
  channelId,
  senderType,
  senderId,
  senderName,
  content,
  messageId,
  messageType = 'chat',
}) {
  if (!ALLOWED_MEMBER_TYPES.has(senderType) && senderType !== 'human') {
    throw new Error(`Unsupported sender_type: ${senderType}`);
  }
  if (!content || !String(content).trim()) {
    throw new Error('content required');
  }

  const msg = await insertMessage(getDb(), {
    id: messageId ?? uuidv4(),
    teamId: null,
    channelId,
    senderType,
    senderId,
    senderName: senderName ?? defaultSenderName(senderType, senderId),
    messageType,
    content: String(content),
  });

  const payload = formatMessage(msg);
  payload.requestId = requestId;
  return payload;
}

export function createChannelsRouter({
  getDbImpl = getDb,
  insertChannelImpl = insertChannel,
  insertAgentImpl = insertAgent,
  getMachinesImpl = getMachines,
  addChannelMemberImpl = addChannelMember,
  getChannelMembersImpl = getChannelMembers,
  updateChannelImpl = updateChannel,
  isWorkspaceOwnerImpl = isWorkspaceOwner,
  findUserByIdOrNameImpl = findUserByIdOrName,
  getChannelMessagesImpl = getChannelMessages,
  insertMessageImpl = insertMessage,
  sendToDaemonImpl = sendToDaemon,
  isMachineOnlineImpl = isMachineOnline,
  requestFromDaemonImpl = requestFromDaemon,
  broadcastImpl = broadcast,
  formatMessageImpl = formatMessage,
  requireAuthImpl = requireAuth,
  requireWorkspaceReadImpl = requireWorkspaceRead,
  requireChannelReadImpl = requireChannelRead,
  requireChannelWriteImpl = requireChannelWrite,
  getRequestUserIdImpl = getRequestUserId,
  uuidv4Impl = uuidv4,
  defaultServerId = DEFAULT_SERVER_ID,
  defaultUserName = DEFAULT_USER_NAME,
  daemonRequestTimeoutMs = DEFAULT_DAEMON_REQUEST_TIMEOUT_MS,
} = {}) {
  const router = Router();

  async function buildDaemonChannelPayload(channel) {
    const members = await getChannelMembersImpl(getDbImpl(), channel.id);
    return {
      channelId: channel.id,
      workspaceId: channel.workspace_id,
      daemonId: channel.daemon_id ?? null,
      name: channel.name,
      type: channel.type,
      status: channel.status,
      capabilitySet: typeof channel.capability_set === 'string'
        ? JSON.parse(channel.capability_set)
        : (channel.capability_set ?? { cli_binaries: [] }),
      members: members.map(formatChannelMember),
      archivedAt: channel.archived_at ?? null,
    };
  }

  function pushChannelEvent(daemonId, payload) {
    if (!daemonId) return;
    sendToDaemonImpl(daemonId, payload);
  }

  async function proxyTaskRpc(req, res, method, params = {}) {
    const daemonId = String(req.channel.daemon_id ?? '').trim();
    if (!daemonId) {
      return res.status(503).json({ error: 'Channel daemon unavailable' });
    }
    if (!isMachineOnlineImpl(daemonId)) {
      return res.status(503).json({ error: 'Channel daemon offline' });
    }

    const requestId = uuidv4Impl();
    try {
      const result = await requestFromDaemonImpl(
        daemonId,
        {
          type: 'channel:rpc',
          requestId,
          method,
          channelId: req.channel.id,
          params: {
            ...params,
            channel_id: req.channel.id,
          },
        },
        requestId,
        daemonRequestTimeoutMs,
      );

      if (!result?.ok) {
        return res.status(daemonErrorStatus(result)).json({
          error: daemonRpcErrorBody(result, method),
        });
      }

      return res.json(result.result ?? {});
    } catch (err) {
      return res.status(503).json({ error: `Channel daemon unavailable: ${err.message}` });
    }
  }

  async function requireWorkspaceOwnerForChannel(req, res) {
    const owner = await isWorkspaceOwnerImpl(getDbImpl(), req.workspace.id, getRequestUserIdImpl(req));
    if (!owner) {
      res.status(403).json({ error: 'Forbidden: workspace owner required' });
      return false;
    }
    return true;
  }

  router.use(requireAuthImpl);

  router.post('/', requireWorkspaceReadImpl, async (req, res) => {
    const name = String(req.body?.name ?? '').trim();
    const type = String(req.body?.type ?? '').trim();
    if (!name) return res.status(400).json({ error: 'Channel name is required' });
    if (!ALLOWED_CHANNEL_TYPES.has(type)) {
      return res.status(400).json({ error: `channel.type must be one of: ${[...ALLOWED_CHANNEL_TYPES].join(', ')}` });
    }

    let capabilitySet;
    try {
      const rawCap = req.body?.capabilitySet ?? req.body?.capability_set ?? CAPABILITY_DEFAULTS_BY_TYPE[type];
      capabilitySet = normalizeCapabilitySet(rawCap);
    } catch (err) {
      return res.status(400).json({ error: err.message });
    }

    const userId = getRequestUserIdImpl(req);
    const db = getDbImpl();

    // Resolve daemon machine: explicit daemonId > first online machine > first machine > error
    let daemonId = req.body?.daemonId ?? null;
    if (!daemonId) {
      const allMachines = await getMachinesImpl(db, defaultServerId);
      if (!allMachines.length) {
        return res.status(400).json({
          error: 'No machine registered. Run `make register` before creating a channel.',
        });
      }
      const online = allMachines.find(m => m.status === 'online');
      daemonId = (online ?? allMachines[0]).id;
    }

    // Auto-provision PM agent (every channel must have one)
    let channelAgentId = req.body?.channelAgentId ?? null;
    if (!channelAgentId) {
      const agentRow = await insertAgentImpl(db, {
        id: uuidv4Impl(),
        serverId: defaultServerId,
        ownerId: userId,
        name: `${name}-pm-${Date.now().toString(36)}`,
        displayName: `${name} PM`,
        description: PM_DESCRIPTION_BY_TYPE[type] ?? `${type} channel PM agent`,
        runtime: 'claude',
        machineId: daemonId,
      });
      channelAgentId = agentRow.id;
    }

    const channel = await insertChannelImpl(db, {
      id: uuidv4Impl(),
      workspaceId: req.workspace.id,
      name,
      type,
      capabilitySet,
      channelAgentId,
      daemonId,
      status: String(req.body?.status ?? 'active'),
    });

    await addChannelMemberImpl(db, channel.id, 'human', userId);
    await addChannelMemberImpl(db, channel.id, 'channel_agent', channelAgentId);

    if (channel.daemon_id) {
      const daemonChannel = await buildDaemonChannelPayload(channel);
      pushChannelEvent(channel.daemon_id, { type: 'channel:create', channel: daemonChannel });
      if (channel.status === 'active') {
        pushChannelEvent(channel.daemon_id, { type: 'channel:start', channel: daemonChannel });
      }
    }

    emitJsonEvent('channel.create', {
      channel_id: channel.id,
      workspace_id: channel.workspace_id,
      daemon_id: channel.daemon_id ?? null,
      status: channel.status,
    });
    broadcastImpl.channelUpdated(defaultServerId, req.workspace.id);
    res.status(201).json(formatChannel(channel));
  });

  router.get('/:id', requireChannelReadImpl, async (req, res) => {
    const members = await getChannelMembersImpl(getDbImpl(), req.channel.id);
    res.json({
      ...formatChannel(req.channel),
      workspace: {
        id: req.workspace.id,
        name: req.workspace.name,
        ownerUserId: req.workspace.owner_user_id,
      },
      members: members.map(formatChannelMember),
    });
  });

  router.patch('/:id', requireChannelReadImpl, async (req, res) => {
    if (!(await requireWorkspaceOwnerForChannel(req, res))) return;

    const fields = {};
    if (typeof req.body?.name === 'string' && req.body.name.trim()) {
      fields.name = req.body.name.trim();
    }
    if (typeof req.body?.status === 'string' && req.body.status.trim()) {
      fields.status = req.body.status.trim();
    }
    if (req.body?.pause === true) fields.status = 'paused';
    if (req.body?.resume === true) fields.status = 'active';
    if (req.body?.archive === true) {
      fields.status = 'archived';
      fields.archived_at = new Date().toISOString().slice(0, 19).replace('T', ' ');
    } else if (req.body?.archive === false) {
      fields.archived_at = null;
    }
    if (req.body?.daemonId !== undefined) fields.daemon_id = req.body.daemonId;
    if (req.body?.channelAgentId !== undefined) fields.channel_agent_id = req.body.channelAgentId;

    const channel = await updateChannelImpl(getDbImpl(), req.channel.id, fields);
    const daemonId = channel.daemon_id ?? req.channel.daemon_id ?? null;
    if (daemonId) {
      const daemonChannel = await buildDaemonChannelPayload(channel);
      if (req.body?.archive === true || (fields.status === 'archived' && req.channel.status !== 'archived')) {
        pushChannelEvent(daemonId, { type: 'channel:archive', channelId: channel.id });
      } else if (req.body?.pause === true || (fields.status === 'paused' && req.channel.status !== 'paused')) {
        pushChannelEvent(daemonId, { type: 'channel:pause', channelId: channel.id });
      } else if (req.body?.resume === true || (channel.status === 'active' && req.channel.status !== 'active')) {
        pushChannelEvent(daemonId, { type: 'channel:resume', channel: daemonChannel });
      } else {
        pushChannelEvent(daemonId, {
          type: 'channel:event',
          channelId: channel.id,
          channel: daemonChannel,
          event: {
            type: 'channel.config.updated',
            created_at: new Date().toISOString(),
            source: 'server',
            payload: { channel: daemonChannel },
          },
        });
      }
    }
    broadcastImpl.channelUpdated(defaultServerId, req.workspace.id);
    res.json(formatChannel(channel));
  });

  router.post('/:id/members', requireChannelReadImpl, async (req, res) => {
    if (!(await requireWorkspaceOwnerForChannel(req, res))) return;

    const memberType = String(req.body?.memberType ?? req.body?.member_type ?? 'human').trim();
    if (!ALLOWED_MEMBER_TYPES.has(memberType)) {
      return res.status(400).json({ error: `memberType must be one of: ${[...ALLOWED_MEMBER_TYPES].join(', ')}` });
    }

    let memberId = String(req.body?.memberId ?? req.body?.userId ?? '').trim();
    if (memberType === 'human') {
      const identifier = memberId || String(req.body?.userName ?? req.body?.identifier ?? '').trim();
      if (!identifier) return res.status(400).json({ error: 'memberId or userName is required' });
      const member = await findUserByIdOrNameImpl(getDbImpl(), identifier);
      if (!member) return res.status(404).json({ error: 'User not found' });
      memberId = member.id;
    } else if (!memberId) {
      return res.status(400).json({ error: 'memberId is required' });
    }

    await addChannelMemberImpl(getDbImpl(), req.channel.id, memberType, memberId);
    const members = await getChannelMembersImpl(getDbImpl(), req.channel.id);
    const added = members.find(row => row.member_type === memberType && row.member_id === memberId);

    if (req.channel.daemon_id && added) {
      pushChannelEvent(req.channel.daemon_id, {
        type: 'channel:event',
        channelId: req.channel.id,
        event: {
          type: 'channel.member.joined',
          created_at: new Date().toISOString(),
          source: 'server',
          payload: {
            member: formatChannelMember(added),
          },
        },
      });
    }

    broadcastImpl.channelUpdated(defaultServerId, req.workspace.id);
    res.status(201).json(formatChannelMember(added));
  });

  router.get('/:id/messages', requireChannelReadImpl, async (req, res) => {
    const { limit = 50, before, after } = req.query;
    const messages = await getChannelMessagesImpl(getDbImpl(), req.channel.id, {
      limit: Number(limit),
      before: before != null ? Number(before) : undefined,
      after: after != null ? Number(after) : undefined,
    });
    res.json({ messages: messages.map(formatMessageImpl), hasMore: messages.length === Number(limit) });
  });

  router.get('/:id/tasks', requireChannelReadImpl, async (req, res) => {
    const { status, mine, parent, parent_task_id: parentTaskId } = req.query;
    return proxyTaskRpc(req, res, 'task.list', {
      ...(status != null ? { status } : {}),
      ...(mine != null ? { mine: mine === true || mine === 'true' } : {}),
      ...(parent != null || parentTaskId != null ? { parent_task_id: parentTaskId ?? parent } : {}),
    });
  });

  router.get('/:id/tasks/:taskId', requireChannelReadImpl, async (req, res) => (
    proxyTaskRpc(req, res, 'task.show', { task_id: req.params.taskId })
  ));

  router.post('/:id/messages', requireChannelWriteImpl, async (req, res) => {
    const content = typeof req.body?.content === 'string' ? req.body.content.trim() : '';
    if (!content) return res.status(400).json({ error: 'content required' });

    const userId = getRequestUserIdImpl(req);
    const daemonId = String(req.channel.daemon_id ?? '').trim();
    if (!daemonId) {
      return res.status(503).json({ error: 'Channel daemon unavailable' });
    }
    if (!isMachineOnlineImpl(daemonId)) {
      return res.status(503).json({ error: 'Channel daemon offline' });
    }

    const requestId = uuidv4Impl();
    try {
      const result = await requestFromDaemonImpl(
        daemonId,
        {
          type: 'channel:message.send',
          requestId,
          channelId: req.channel.id,
          senderType: 'human',
          senderKind: SenderKind.HUMAN,
          senderId: userId,
          senderName: req.user?.name ?? defaultUserName,
          messageType: 'chat',
          payloadType: PayloadType.USER_TEXT,
          payloadBody: { text: content, attachments: [] },
          content,
          attachments: [],
        },
        requestId,
        daemonRequestTimeoutMs,
      );

      if (!result?.ok) {
        return res.status(503).json({ error: result?.error ?? 'Channel daemon failed to persist message' });
      }

      emitJsonEvent('message.deliver', {
        message_id: result.message?.messageId ?? result.message?.message_id ?? null,
        channel_id: req.channel.id,
        request_id: requestId,
      });
      res.status(201).json(formatDaemonMessagePayload(result.message));
    } catch (err) {
      res.status(503).json({ error: `Channel daemon unavailable: ${err.message}` });
    }
  });

  return router;
}

const router = createChannelsRouter();

export default router;
