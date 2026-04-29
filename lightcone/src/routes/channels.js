import { Router } from 'express';
import { v4 as uuidv4 } from 'uuid';
import {
  getDb,
  insertChannel,
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
      capabilitySet = normalizeCapabilitySet(req.body?.capabilitySet ?? req.body?.capability_set);
    } catch (err) {
      return res.status(400).json({ error: err.message });
    }

    const userId = getRequestUserIdImpl(req);
    const channel = await insertChannelImpl(getDbImpl(), {
      id: uuidv4Impl(),
      workspaceId: req.workspace.id,
      name,
      type,
      capabilitySet,
      channelAgentId: req.body?.channelAgentId ?? null,
      daemonId: req.body?.daemonId ?? null,
      status: String(req.body?.status ?? 'active'),
    });

    await addChannelMemberImpl(getDbImpl(), channel.id, 'human', userId);
    if (channel.channel_agent_id) {
      await addChannelMemberImpl(getDbImpl(), channel.id, 'channel_agent', channel.channel_agent_id);
    }

    if (channel.daemon_id) {
      const daemonChannel = await buildDaemonChannelPayload(channel);
      pushChannelEvent(channel.daemon_id, { type: 'channel:create', channel: daemonChannel });
      if (channel.status === 'active') {
        pushChannelEvent(channel.daemon_id, { type: 'channel:start', channel: daemonChannel });
      }
    }

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
          senderId: userId,
          senderName: req.user?.name ?? defaultUserName,
          messageType: 'chat',
          content,
          attachments: [],
        },
        requestId,
        daemonRequestTimeoutMs,
      );

      if (!result?.ok) {
        return res.status(503).json({ error: result?.error ?? 'Channel daemon failed to persist message' });
      }

      res.status(201).json(formatDaemonMessagePayload(result.message));
    } catch (err) {
      res.status(503).json({ error: `Channel daemon unavailable: ${err.message}` });
    }
  });

  return router;
}

const router = createChannelsRouter();

export default router;
