import {
  getDb,
  getWorkspaceById,
  getChannelById,
  isWorkspaceMember,
  isChannelMember,
} from '../db/index.js';

const DEFAULT_USER_ID = process.env.DEFAULT_USER_ID ?? 'user-001';

export function getRequestUserId(req) {
  return req.user?.id ?? DEFAULT_USER_ID;
}

async function resolveWorkspace(req, res) {
  const workspaceId = req.params.id ?? req.params.workspaceId ?? req.body?.workspaceId ?? req.body?.workspace_id;
  if (!workspaceId) {
    res.status(400).json({ error: 'workspaceId required' });
    return null;
  }

  const workspace = await getWorkspaceById(getDb(), workspaceId);
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

  const channel = await getChannelById(getDb(), channelId);
  if (!channel || channel.is_del || channel.deleted_at) {
    res.status(404).json({ error: 'Channel not found' });
    return null;
  }
  req.channel = channel;
  return channel;
}

export async function requireWorkspaceRead(req, res, next) {
  const workspace = await resolveWorkspace(req, res);
  if (!workspace) return;

  const userId = getRequestUserId(req);
  const ok = await isWorkspaceMember(getDb(), workspace.id, userId);
  if (!ok) return res.status(403).json({ error: 'Forbidden: workspace membership required' });
  next();
}

export async function requireChannelRead(req, res, next) {
  const channel = await resolveChannel(req, res);
  if (!channel) return;

  const workspace = await getWorkspaceById(getDb(), channel.workspace_id);
  if (!workspace || workspace.is_del || workspace.deleted_at) {
    return res.status(404).json({ error: 'Workspace not found for channel' });
  }
  req.workspace = workspace;

  const userId = getRequestUserId(req);
  const ok = await isWorkspaceMember(getDb(), workspace.id, userId);
  if (!ok) return res.status(403).json({ error: 'Forbidden: workspace membership required' });
  next();
}

export async function requireChannelWrite(req, res, next) {
  const channel = await resolveChannel(req, res);
  if (!channel) return;

  const workspace = await getWorkspaceById(getDb(), channel.workspace_id);
  if (!workspace || workspace.is_del || workspace.deleted_at) {
    return res.status(404).json({ error: 'Workspace not found for channel' });
  }
  req.workspace = workspace;

  const userId = getRequestUserId(req);
  const readable = await isWorkspaceMember(getDb(), workspace.id, userId);
  if (!readable) return res.status(403).json({ error: 'Forbidden: workspace membership required' });

  const writable = await isChannelMember(getDb(), channel.id, 'human', userId);
  if (!writable) return res.status(403).json({ error: 'Forbidden: channel membership required for write' });
  next();
}
