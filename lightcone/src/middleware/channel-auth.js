import {
  getDb,
  getWorkspaceById,
  getChannelById,
  isWorkspaceMember,
  isChannelMember,
} from '../db/index.js';

export function createChannelAuth({
  getDbImpl = getDb,
  getWorkspaceByIdImpl = getWorkspaceById,
  getChannelByIdImpl = getChannelById,
  isWorkspaceMemberImpl = isWorkspaceMember,
  isChannelMemberImpl = isChannelMember,
} = {}) {
  function getRequestUserId(req) {
    return req.user?.id ?? null;
  }

  function requireRequestUserId(req, res) {
    const userId = getRequestUserId(req);
    if (!userId) {
      res.status(401).json({ error: 'Unauthorized' });
      return null;
    }
    return userId;
  }

  async function resolveWorkspace(req, res) {
    const workspaceId = req.params.id ?? req.params.workspaceId ?? req.body?.workspaceId ?? req.body?.workspace_id;
    if (!workspaceId) {
      res.status(400).json({ error: 'workspaceId required' });
      return null;
    }

    const workspace = await getWorkspaceByIdImpl(getDbImpl(), workspaceId);
    if (!workspace || workspace.is_del || workspace.deleted_at) {
      res.status(404).json({ error: 'Workspace not found' });
      return null;
    }
    req.workspace = workspace;
    return workspace;
  }

  async function resolveChannel(req, res) {
    const channelId = req.params.id ?? req.params.channelId ?? req.body?.channelId ?? req.body?.channel_id;
    if (!channelId) {
      res.status(400).json({ error: 'channelId required' });
      return null;
    }

    const channel = await getChannelByIdImpl(getDbImpl(), channelId);
    if (!channel || channel.is_del || channel.deleted_at) {
      res.status(404).json({ error: 'Channel not found' });
      return null;
    }
    req.channel = channel;
    return channel;
  }

  async function requireWorkspaceRead(req, res, next) {
    const userId = requireRequestUserId(req, res);
    if (!userId) return;

    const workspace = await resolveWorkspace(req, res);
    if (!workspace) return;

    const ok = await isWorkspaceMemberImpl(getDbImpl(), workspace.id, userId);
    if (!ok) return res.status(403).json({ error: 'Forbidden: workspace membership required' });
    next();
  }

  async function requireChannelRead(req, res, next) {
    const userId = requireRequestUserId(req, res);
    if (!userId) return;

    const channel = await resolveChannel(req, res);
    if (!channel) return;

    const workspace = await getWorkspaceByIdImpl(getDbImpl(), channel.workspace_id);
    if (!workspace || workspace.is_del || workspace.deleted_at) {
      return res.status(404).json({ error: 'Workspace not found for channel' });
    }
    req.workspace = workspace;

    const ok = await isWorkspaceMemberImpl(getDbImpl(), workspace.id, userId);
    if (!ok) return res.status(403).json({ error: 'Forbidden: workspace membership required' });
    next();
  }

  async function requireChannelWrite(req, res, next) {
    const userId = requireRequestUserId(req, res);
    if (!userId) return;

    const channel = await resolveChannel(req, res);
    if (!channel) return;

    const workspace = await getWorkspaceByIdImpl(getDbImpl(), channel.workspace_id);
    if (!workspace || workspace.is_del || workspace.deleted_at) {
      return res.status(404).json({ error: 'Workspace not found for channel' });
    }
    req.workspace = workspace;

    const readable = await isWorkspaceMemberImpl(getDbImpl(), workspace.id, userId);
    if (!readable) return res.status(403).json({ error: 'Forbidden: workspace membership required' });

    const writable = await isChannelMemberImpl(getDbImpl(), channel.id, 'human', userId);
    if (!writable) return res.status(403).json({ error: 'Forbidden: channel membership required for write' });
    next();
  }

  return {
    getRequestUserId,
    requireWorkspaceRead,
    requireChannelRead,
    requireChannelWrite,
  };
}

export const {
  getRequestUserId,
  requireWorkspaceRead,
  requireChannelRead,
  requireChannelWrite,
} = createChannelAuth();
