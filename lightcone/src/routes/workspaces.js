import { Router } from 'express';
import { v4 as uuidv4 } from 'uuid';
import {
  getDb,
  insertWorkspace,
  getUserWorkspaces,
  getWorkspaceMembers,
  addWorkspaceMember,
  updateWorkspace,
  isWorkspaceOwner,
  findUserByIdOrName,
  getWorkspaceChannels,
} from '../db/index.js';
import { broadcast } from '../realtime/broadcast.js';
import { requireAuth } from '../middleware/auth.js';
import { requireWorkspaceRead, getRequestUserId } from '../middleware/channel-auth.js';

const DEFAULT_SERVER_ID = process.env.DEFAULT_SERVER_ID ?? 'server-001';

function formatWorkspace(workspace) {
  return {
    id: workspace.id,
    name: workspace.name,
    ownerUserId: workspace.owner_user_id,
    createdAt: workspace.created_at,
    archivedAt: workspace.archived_at ?? null,
  };
}

function formatWorkspaceMember(member) {
  return {
    userId: member.user_id,
    role: member.role,
    joinedAt: member.joined_at,
    name: member.user_name ?? member.user_id,
    avatarUrl: member.user_avatar ?? null,
  };
}

function formatChannelSummary(channel) {
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

export function createWorkspacesRouter({
  getDbImpl = getDb,
  insertWorkspaceImpl = insertWorkspace,
  getUserWorkspacesImpl = getUserWorkspaces,
  getWorkspaceMembersImpl = getWorkspaceMembers,
  addWorkspaceMemberImpl = addWorkspaceMember,
  updateWorkspaceImpl = updateWorkspace,
  isWorkspaceOwnerImpl = isWorkspaceOwner,
  findUserByIdOrNameImpl = findUserByIdOrName,
  getWorkspaceChannelsImpl = getWorkspaceChannels,
  broadcastImpl = broadcast,
  requireAuthImpl = requireAuth,
  requireWorkspaceReadImpl = requireWorkspaceRead,
  getRequestUserIdImpl = getRequestUserId,
  uuidv4Impl = uuidv4,
  defaultServerId = DEFAULT_SERVER_ID,
} = {}) {
  const router = Router();

  router.use(requireAuthImpl);

  router.get('/', async (req, res) => {
    const workspaces = await getUserWorkspacesImpl(getDbImpl(), getRequestUserIdImpl(req), {
      includeArchived: req.query.includeArchived === 'true',
    });
    res.json(workspaces.map(formatWorkspace));
  });

  router.post('/', async (req, res) => {
    const name = String(req.body?.name ?? '').trim();
    if (!name) return res.status(400).json({ error: 'Workspace name is required' });

    const userId = getRequestUserIdImpl(req);
    const db = getDbImpl();
    const workspace = await insertWorkspaceImpl(db, {
      id: uuidv4Impl(),
      name,
      ownerUserId: userId,
    });
    await addWorkspaceMemberImpl(db, workspace.id, userId, 'owner');

    broadcastImpl.workspaceUpdated(defaultServerId);
    res.status(201).json(formatWorkspace(workspace));
  });

  router.get('/:id', requireWorkspaceReadImpl, async (req, res) => {
    const members = await getWorkspaceMembersImpl(getDbImpl(), req.workspace.id);
    res.json({
      ...formatWorkspace(req.workspace),
      members: members.map(formatWorkspaceMember),
    });
  });

  router.patch('/:id', requireWorkspaceReadImpl, async (req, res) => {
    const userId = getRequestUserIdImpl(req);
    const owner = await isWorkspaceOwnerImpl(getDbImpl(), req.workspace.id, userId);
    if (!owner) return res.status(403).json({ error: 'Forbidden: workspace owner required' });

    const fields = {};
    if (typeof req.body?.name === 'string' && req.body.name.trim()) {
      fields.name = req.body.name.trim();
    }
    if (req.body?.archive === true) {
      fields.archived_at = new Date().toISOString().slice(0, 19).replace('T', ' ');
    } else if (req.body?.archive === false) {
      fields.archived_at = null;
    }

    const workspace = await updateWorkspaceImpl(getDbImpl(), req.workspace.id, fields);
    broadcastImpl.workspaceUpdated(defaultServerId);
    res.json(formatWorkspace(workspace));
  });

  router.post('/:id/members', requireWorkspaceReadImpl, async (req, res) => {
    const userId = getRequestUserIdImpl(req);
    const owner = await isWorkspaceOwnerImpl(getDbImpl(), req.workspace.id, userId);
    if (!owner) return res.status(403).json({ error: 'Forbidden: workspace owner required' });

    const identifier = String(
      req.body?.userId ?? req.body?.memberId ?? req.body?.userName ?? req.body?.identifier ?? ''
    ).trim();
    if (!identifier) return res.status(400).json({ error: 'userId or userName is required' });

    const member = await findUserByIdOrNameImpl(getDbImpl(), identifier);
    if (!member) return res.status(404).json({ error: 'User not found' });

    const role = req.body?.role === 'owner' ? 'owner' : 'member';
    await addWorkspaceMemberImpl(getDbImpl(), req.workspace.id, member.id, role);

    const members = await getWorkspaceMembersImpl(getDbImpl(), req.workspace.id);
    const added = members.find(row => row.user_id === member.id);
    broadcastImpl.workspaceUpdated(defaultServerId);
    res.status(201).json(formatWorkspaceMember(added));
  });

  router.get('/:id/channels', requireWorkspaceReadImpl, async (req, res) => {
    const channels = await getWorkspaceChannelsImpl(getDbImpl(), req.workspace.id, {
      includeArchived: req.query.includeArchived === 'true',
    });
    res.json({ channels: channels.map(formatChannelSummary) });
  });

  return router;
}

const router = createWorkspacesRouter();

export default router;
