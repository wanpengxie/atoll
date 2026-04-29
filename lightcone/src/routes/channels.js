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
import { broadcast } from '../realtime/broadcast.js';
import { formatMessage } from '../internal/index.js';
import {
  requireWorkspaceRead,
  requireChannelRead,
  requireChannelWrite,
  getRequestUserId,
} from '../middleware/channel-auth.js';

const router = Router();
const DEFAULT_SERVER_ID = process.env.DEFAULT_SERVER_ID ?? 'server-001';
const DEFAULT_USER_NAME = process.env.DEFAULT_USER_NAME ?? 'Admin';

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

async function requireWorkspaceOwnerForChannel(req, res) {
  const owner = await isWorkspaceOwner(getDb(), req.workspace.id, getRequestUserId(req));
  if (!owner) {
    res.status(403).json({ error: 'Forbidden: workspace owner required' });
    return false;
  }
  return true;
}

router.post('/', requireWorkspaceRead, async (req, res) => {
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

  const userId = getRequestUserId(req);
  const channel = await insertChannel(getDb(), {
    id: uuidv4(),
    workspaceId: req.workspace.id,
    name,
    type,
    capabilitySet,
    channelAgentId: req.body?.channelAgentId ?? null,
    daemonId: req.body?.daemonId ?? null,
    status: String(req.body?.status ?? 'active'),
  });

  await addChannelMember(getDb(), channel.id, 'human', userId);
  if (channel.channel_agent_id) {
    await addChannelMember(getDb(), channel.id, 'channel_agent', channel.channel_agent_id);
  }

  broadcast.channelUpdated(DEFAULT_SERVER_ID, req.workspace.id);
  res.status(201).json(formatChannel(channel));
});

router.get('/:id', requireChannelRead, async (req, res) => {
  const members = await getChannelMembers(getDb(), req.channel.id);
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

router.patch('/:id', requireChannelRead, async (req, res) => {
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

  const channel = await updateChannel(getDb(), req.channel.id, fields);
  broadcast.channelUpdated(DEFAULT_SERVER_ID, req.workspace.id);
  res.json(formatChannel(channel));
});

router.post('/:id/members', requireChannelRead, async (req, res) => {
  if (!(await requireWorkspaceOwnerForChannel(req, res))) return;

  const memberType = String(req.body?.memberType ?? req.body?.member_type ?? 'human').trim();
  if (!ALLOWED_MEMBER_TYPES.has(memberType)) {
    return res.status(400).json({ error: `memberType must be one of: ${[...ALLOWED_MEMBER_TYPES].join(', ')}` });
  }

  let memberId = String(req.body?.memberId ?? req.body?.userId ?? '').trim();
  if (memberType === 'human') {
    const identifier = memberId || String(req.body?.userName ?? req.body?.identifier ?? '').trim();
    if (!identifier) return res.status(400).json({ error: 'memberId or userName is required' });
    const member = await findUserByIdOrName(getDb(), identifier);
    if (!member) return res.status(404).json({ error: 'User not found' });
    memberId = member.id;
  } else if (!memberId) {
    return res.status(400).json({ error: 'memberId is required' });
  }

  await addChannelMember(getDb(), req.channel.id, memberType, memberId);
  const members = await getChannelMembers(getDb(), req.channel.id);
  const added = members.find(row => row.member_type === memberType && row.member_id === memberId);

  broadcast.channelUpdated(DEFAULT_SERVER_ID, req.workspace.id);
  res.status(201).json(formatChannelMember(added));
});

router.get('/:id/messages', requireChannelRead, async (req, res) => {
  const { limit = 50, before, after } = req.query;
  const messages = await getChannelMessages(getDb(), req.channel.id, {
    limit: Number(limit),
    before: before != null ? Number(before) : undefined,
    after: after != null ? Number(after) : undefined,
  });
  res.json({ messages: messages.map(formatMessage), hasMore: messages.length === Number(limit) });
});

router.post('/:id/messages', requireChannelWrite, async (req, res) => {
  const content = typeof req.body?.content === 'string' ? req.body.content.trim() : '';
  if (!content) return res.status(400).json({ error: 'content required' });

  const userId = getRequestUserId(req);
  const msg = await insertMessage(getDb(), {
    id: uuidv4(),
    teamId: null,
    channelId: req.channel.id,
    senderType: 'human',
    senderId: userId,
    senderName: req.user?.name ?? DEFAULT_USER_NAME,
    messageType: 'chat',
    content,
  });

  const payload = formatMessage(msg);
  broadcast.channelMessage(req.channel.id, payload);
  res.status(201).json(payload);
});

export async function appendDaemonChannelMessage({
  requestId,
  channelId,
  senderType,
  senderId,
  content,
}) {
  if (!ALLOWED_MEMBER_TYPES.has(senderType) && senderType !== 'human') {
    throw new Error(`Unsupported sender_type: ${senderType}`);
  }
  if (!content || !String(content).trim()) {
    throw new Error('content required');
  }

  const msg = await insertMessage(getDb(), {
    id: uuidv4(),
    teamId: null,
    channelId,
    senderType,
    senderId,
    senderName: defaultSenderName(senderType, senderId),
    messageType: 'chat',
    content: String(content),
  });

  const payload = formatMessage(msg);
  payload.requestId = requestId;
  return payload;
}

export default router;
