// 由 setupSocketIO() 注入 io 实例
let _io = null;

export function setIo(io) {
  _io = io;
}

function emit(room, event, data) {
  if (!_io) return;
  _io.to(room).emit(event, data);
}

export const broadcast = {
  message:         (teamId, msg)                             => emit(`team:${teamId}`,      'message:new',            msg),
  messageUpdated:  (teamId, msg)                             => emit(`team:${teamId}`,      'message:updated',        msg),
  channelMessage:  (channelId, msg)                          => emit(`channel:${channelId}`, 'message:new',           msg),
  channelMessageUpdated: (channelId, msg)                    => emit(`channel:${channelId}`, 'message:updated',       msg),
  taskCreated:     (teamId, tasks)                           => emit(`team:${teamId}`,      'task:created',           { tasks }),
  taskUpdated:     (teamId, task)                            => emit(`team:${teamId}`,      'task:updated',           { task }),
  taskDeleted:     (teamId, taskId)                          => emit(`team:${teamId}`,      'task:deleted',           { taskId }),
  agentActivity:   (serverId, agentId, activity, detail, entries) =>
    emit(`server:${serverId}`, 'agent:activity', { agentId, activity, detail, entries }),
  agentCreated:    (serverId, agent)                         => emit(`server:${serverId}`,  'agent:created',          { agent }),
  agentDeleted:    (serverId, agentId)                       => emit(`server:${serverId}`,  'agent:deleted',          { agentId }),
  teamUpdated:     (serverId)                                => emit(`server:${serverId}`,  'team:updated',           {}),
  workspaceUpdated:(serverId)                                => emit(`server:${serverId}`,  'workspace:updated',      {}),
  channelUpdated:  (serverId, workspaceId = null)            => emit(`server:${serverId}`,  'channel:updated',        { workspaceId }),
  dmNew:           (serverId, teamId)                        => emit(`server:${serverId}`,  'dm:new',                 { teamId }),
  machineStatus:   (serverId, machineId, status)             => emit(`server:${serverId}`,  'machine:status',         { machineId, status }),
  machineCapabilities: (serverId, machineId, runtimes, hostname, os, daemonVersion) =>
    emit(`server:${serverId}`, 'machine:capabilities', { machineId, runtimes, hostname, os, daemonVersion }),
  actionUpdated:   (action) => action?.team_id
    ? emit(`team:${action.team_id}`, 'action:updated', { action })
    : null,
  custom:          (serverId, event, data) => emit(`server:${serverId}`, event, data),
};
