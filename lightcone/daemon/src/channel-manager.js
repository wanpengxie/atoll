import { spawn } from 'child_process';
import { randomUUID } from 'crypto';
import {
  appendFileSync,
  cpSync,
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  renameSync,
  statSync,
  unlinkSync,
  writeFileSync,
} from 'fs';
import { homedir } from 'os';
import path from 'path';
import { fileURLToPath } from 'url';
import { HEALTHY_UPTIME_MS, CronScheduler } from './cron-scheduler.js';
import { buildCoagentSpawn } from './drivers/coagent.js';
import { emitJsonEvent } from './events.js';
import { coagentProjectDir, normalizeProjectKey } from './paths.js';
import { TriggerGateway } from './trigger-gateway.js';

const DEFAULT_AGENT_NAME = 'channel-agent';
const SHUTDOWN_GRACE_MS = 5_000;

function nowIso() {
  return new Date().toISOString();
}

function toRpcError(code, message) {
  const err = new Error(message);
  err.code = code;
  return err;
}

function normalizeCapabilitySet(raw) {
  const cliBinaries = Array.isArray(raw?.cli_binaries)
    ? raw.cli_binaries
    : Array.isArray(raw?.cliBinaries)
      ? raw.cliBinaries
      : [];

  return {
    cli_binaries: [...new Set(cliBinaries.map((item) => String(item).trim()).filter(Boolean))],
  };
}

function normalizeMembers(rawMembers) {
  const members = Array.isArray(rawMembers) ? rawMembers : [];
  const seen = new Set();
  const normalized = [];

  for (const member of members) {
    const memberType = String(member?.memberType ?? member?.member_type ?? '').trim();
    const memberId = String(member?.memberId ?? member?.member_id ?? '').trim();
    if (!memberType || !memberId) continue;

    const key = `${memberType}:${memberId}`;
    if (seen.has(key)) continue;
    seen.add(key);
    normalized.push({
      memberType,
      memberId,
      displayName: member?.displayName ?? member?.display_name ?? memberId,
      joinedAt: member?.joinedAt ?? member?.joined_at ?? nowIso(),
    });
  }

  return normalized;
}

function normalizeChannelPayload(input) {
  const channelId = String(input?.channelId ?? input?.channel_id ?? input?.id ?? '').trim();
  if (!channelId) throw toRpcError('bad_request', 'channel_id is required');

  return {
    channelId,
    workspaceId: String(input?.workspaceId ?? input?.workspace_id ?? '').trim(),
    daemonId: String(input?.daemonId ?? input?.daemon_id ?? '').trim(),
    name: String(input?.name ?? '').trim(),
    type: String(input?.type ?? 'xhs-creator').trim(),
    status: String(input?.status ?? 'created').trim(),
    capabilitySet: normalizeCapabilitySet(input?.capabilitySet ?? input?.capability_set ?? {}),
    members: normalizeMembers(input?.members),
    agentName: String(input?.agentName ?? input?.agent_name ?? DEFAULT_AGENT_NAME).trim() || DEFAULT_AGENT_NAME,
    createdAt: input?.createdAt ?? input?.created_at ?? nowIso(),
    archivedAt: input?.archivedAt ?? input?.archived_at ?? null,
  };
}

function normalizeEvent(rawEvent) {
  const event = rawEvent?.event ?? rawEvent;
  const type = String(event?.type ?? '').trim();
  if (!type) throw toRpcError('bad_request', 'event.type is required');

  return {
    type,
    payload: event?.payload ?? {},
    source: event?.source ?? 'server',
    createdAt: event?.createdAt ?? event?.created_at ?? nowIso(),
  };
}

function normalizeMessage(channelId, message, defaults = {}) {
  const content = String(message?.content ?? '').trim();
  if (!content) throw toRpcError('bad_request', 'message content is required');

  return {
    messageId: String(message?.messageId ?? message?.message_id ?? randomUUID()),
    channelId,
    senderType: String(message?.senderType ?? message?.sender_type ?? defaults.senderType ?? 'channel_agent'),
    senderId: String(message?.senderId ?? message?.sender_id ?? defaults.senderId ?? DEFAULT_AGENT_NAME),
    senderName: String(message?.senderName ?? message?.sender_name ?? defaults.senderName ?? defaults.senderId ?? DEFAULT_AGENT_NAME),
    content,
    attachments: Array.isArray(message?.attachments) ? message.attachments : [],
    messageType: String(message?.messageType ?? message?.message_type ?? 'chat'),
    createdAt: message?.createdAt ?? message?.created_at ?? nowIso(),
    source: message?.source ?? defaults.source ?? 'daemon',
  };
}

function parseStructuredFile(filePath) {
  return JSON.parse(readFileSync(filePath, 'utf8'));
}

function writeStructuredFile(filePath, data) {
  writeFileSync(filePath, `${JSON.stringify(data, null, 2)}\n`, 'utf8');
}

function ensureDirectory(dirPath) {
  mkdirSync(dirPath, { recursive: true });
  return dirPath;
}

function repoRoot() {
  return path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');
}

function sortByCreatedAt(items) {
  return [...items].sort((left, right) => {
    const a = new Date(left.createdAt ?? left.created_at ?? 0).getTime();
    const b = new Date(right.createdAt ?? right.created_at ?? 0).getTime();
    return a - b;
  });
}

export class ChannelManager {
  constructor({
    serverUrl,
    machineApiKey,
    daemonSocketPath,
    daemonHttpUrl = '',
    daemonToken = '',
    projectKey = process.env.COAGENT_PROJECT_KEY,
    baseDir = null,
  }) {
    this.serverUrl = serverUrl;
    this.machineApiKey = machineApiKey;
    this.projectKey = normalizeProjectKey(projectKey);
    this.daemonSocketPath = daemonSocketPath;
    this.daemonHttpUrl = daemonHttpUrl;
    this.daemonToken = daemonToken;
    this.connection = null;
    this.channels = new Map();
    this.baseDir = ensureDirectory(baseDir ?? coagentProjectDir(this.projectKey));
    this.channelsDir = ensureDirectory(path.join(this.baseDir, 'channels'));
    this.archivedDir = ensureDirectory(path.join(this.baseDir, 'archived'));
    this.workspaceTemplateDir = path.join(repoRoot(), 'workspace-template');
    this.cronScheduler = new CronScheduler({
      onTick: async (tick) => {
        await this._handleCronTick(tick);
      },
    });
    this.triggerGateway = new TriggerGateway({
      onPass: async (channel, event, outcome) => {
        await this._recordTrace(channel, {
          kind: 'trigger',
          decision: 'pass',
          reason: outcome.reason,
          event,
        });
        await this._deliverEvent(channel, event);
      },
      onBlock: async (channel, event, outcome) => {
        await this._recordTrace(channel, {
          kind: 'trigger',
          decision: 'block',
          reason: outcome.reason,
          event,
        });
      },
    });
  }

  setConnection(connection) {
    this.connection = connection;
  }

  canHandle(message) {
    return [
      'channel:create',
      'channel:start',
      'channel:pause',
      'channel:resume',
      'channel:archive',
      'channel:event',
      'channel:message.send',
    ].includes(message?.type);
  }

  async start() {
    if (!existsSync(this.channelsDir)) return;

    for (const entry of readdirSync(this.channelsDir)) {
      const workdir = path.join(this.channelsDir, entry);
      if (!statSync(workdir).isDirectory()) continue;

      try {
        const node = this._loadNodeFromDisk(workdir);
        this.channels.set(node.channelId, node);
        if (node.status === 'active') {
          await this.startChannel({ channelId: node.channelId }, { restoring: true, notifyStatus: false });
        }
      } catch (err) {
        console.error(`[ChannelManager] Failed to restore ${entry}:`, err.message);
      }
    }
  }

  async stopAll() {
    this.cronScheduler.stop();

    for (const node of this.channels.values()) {
      await this._stopProcess(node, 'SIGTERM');
    }
  }

  async handle(message, connection = this.connection) {
    if (connection) this.setConnection(connection);

    switch (message.type) {
      case 'channel:create':
        return this.createChannel(message.channel ?? message);
      case 'channel:start':
        return this.startChannel(message.channel ?? message);
      case 'channel:pause':
        return this.pauseChannel(message.channelId ?? message.channel_id ?? message.id);
      case 'channel:resume':
        return this.startChannel(
          message.channel ?? { channelId: message.channelId ?? message.channel_id ?? message.id },
          { notifyStatus: true },
        );
      case 'channel:archive':
        return this.archiveChannel(message.channelId ?? message.channel_id ?? message.id);
      case 'channel:event':
        return this.handleEvent(message);
      case 'channel:message.send':
        return this.handleServerMessageSend(message, connection);
      default:
        return false;
    }
  }

  async rpcCall(method, params) {
    switch (method) {
      case 'schedule.cron':
        return this.registerSchedule(params, 'cron');
      case 'schedule.at':
        return this.registerSchedule(params, 'at');
      case 'schedule.list':
        return this.listSchedules(params.channel_id ?? params.channelId);
      case 'schedule.cancel':
        return this.cancelSchedule(params.channel_id ?? params.channelId, params.id ?? params.schedule_id);
      case 'channel.info':
        return this.getChannelInfo(params.channel_id ?? params.channelId);
      case 'channel.member.list':
        return this.getChannelMembers(params.channel_id ?? params.channelId);
      case 'channel.capability.list':
        return this.getChannelCapabilities(params.channel_id ?? params.channelId);
      case 'channel.list':
        return this.listChannels();
      case 'channel.start':
        return this.startChannel(params.channel_id ?? params.channelId);
      case 'channel.restart':
        return this.restartChannel(params.channel_id ?? params.channelId);
      case 'channel.stop':
        return this.stopChannel(params.channel_id ?? params.channelId);
      case 'channel.archive':
        return this.archiveChannel(params.channel_id ?? params.channelId);
      case 'message.send':
        return this.sendChannelMessage(params);
      case 'message.list':
        return this.listMessages(params.channel_id ?? params.channelId, params.limit ?? 50);
      case 'message.search':
        return this.searchMessages(params.channel_id ?? params.channelId, params.query ?? '', params.limit ?? 20);
      case 'admin.status':
        return this.getAdminStatus();
      case 'admin.machines':
        return this.listMachines();
      default:
        throw toRpcError('not_implemented', `unsupported RPC method: ${method}`);
    }
  }

  async createChannel(payload) {
    const normalized = normalizeChannelPayload(payload);
    const existing = this.channels.get(normalized.channelId);
    const workdir = existing?.workdir ?? path.join(this.channelsDir, normalized.channelId);

    this._materializeWorkdir(workdir);

    const initialStatus = existing
      ? normalized.status
      : (normalized.status === 'active' ? 'created' : normalized.status);

    const node = {
      channelId: normalized.channelId,
      workspaceId: normalized.workspaceId || existing?.workspaceId || '',
      daemonId: normalized.daemonId || existing?.daemonId || '',
      name: normalized.name || existing?.name || normalized.channelId,
      type: normalized.type || existing?.type || 'xhs-creator',
      status: initialStatus || existing?.status || 'created',
      capabilitySet: normalized.capabilitySet.cli_binaries.length > 0
        ? normalized.capabilitySet
        : (existing?.capabilitySet ?? { cli_binaries: [] }),
      members: normalized.members.length > 0 ? normalized.members : (existing?.members ?? []),
      agentName: normalized.agentName || existing?.agentName || DEFAULT_AGENT_NAME,
      createdAt: existing?.createdAt ?? normalized.createdAt,
      archivedAt: normalized.archivedAt ?? existing?.archivedAt ?? null,
      workdir,
      proc: existing?.proc ?? null,
      agentPid: existing?.agentPid ?? null,
      sessionId: existing?.sessionId ?? null,
      lastSpawnedAt: existing?.lastSpawnedAt ?? 0,
      crashCount: existing?.crashCount ?? 0,
      intentionalStop: false,
      mountedCliBinaries: existing?.mountedCliBinaries ?? [],
    };

    this.channels.set(node.channelId, node);
    this._persistNode(node);
    emitJsonEvent('channel.create', { channel_id: node.channelId, status: node.status, project_key: this.projectKey });
    return this._channelInfo(node);
  }

  async startChannel(payload, { restoring = false, notifyStatus = true } = {}) {
    const channelId = typeof payload === 'string'
      ? payload
      : (payload.channelId ?? payload.channel_id ?? payload.id);
    if (!channelId) throw toRpcError('bad_request', 'channel_id is required');

    if (typeof payload === 'object' && payload !== null && !this.channels.has(channelId)) {
      await this.createChannel(payload);
    }

    const node = this._requireNode(channelId);
    if (node.status === 'archived') {
      throw toRpcError('invalid_state', `channel ${channelId} is archived`);
    }
    if (node.proc) {
      return this._channelInfo(node);
    }

    const sessionIdPath = this._sessionIdPath(node);
    const spawnConfig = buildCoagentSpawn({
      channelId: node.channelId,
      channelName: node.name,
      workspaceId: node.workspaceId,
      workdir: node.workdir,
      capabilitySet: node.capabilitySet,
      daemonSocketPath: this.daemonSocketPath,
      daemonHttpUrl: this.daemonHttpUrl,
      daemonToken: this.daemonToken,
      sessionIdPath,
      agentName: node.agentName,
    });

    const proc = spawn(spawnConfig.command, spawnConfig.args, {
      cwd: node.workdir,
      env: spawnConfig.env,
      stdio: ['pipe', 'pipe', 'pipe'],
    });

    node.proc = proc;
    node.agentPid = proc.pid ?? null;
    node.sessionId = spawnConfig.sessionId;
    node.mountedCliBinaries = spawnConfig.mountedCliBinaries;
    node.lastSpawnedAt = Date.now();
    node.intentionalStop = false;
    node.status = 'active';
    node.archivedAt = null;
    this._persistNode(node);
    this._loadSchedulesIntoMemory(node);
    this._wireProcess(node, proc, { restoring });

    if (notifyStatus) {
      this._notifyChannelStatus(node);
    }

    emitJsonEvent('agent.spawn', { channel_id: node.channelId, pid: node.agentPid, session_id: node.sessionId });
    emitJsonEvent('channel.start', { channel_id: node.channelId, status: node.status, pid: node.agentPid });
    console.error(`[ChannelManager] Started ${channelId} pid=${node.agentPid ?? 'n/a'} entry=${spawnConfig.entry}`);
    return this._channelInfo(node);
  }

  async pauseChannel(channelId) {
    const node = this._requireNode(channelId);
    this.cronScheduler.clearChannel(channelId);
    node.status = 'paused';
    this._persistNode(node);
    await this._stopProcess(node, 'SIGTERM');
    this._notifyChannelStatus(node);
    emitJsonEvent('channel.stop', { channel_id: node.channelId, status: node.status });
    return this._channelInfo(node);
  }

  async resumeChannel(channelId) {
    const node = this._requireNode(channelId);
    if (node.status === 'archived') {
      throw toRpcError('invalid_state', `channel ${channelId} is archived`);
    }
    return this.startChannel({ channelId }, { notifyStatus: true });
  }

  async stopChannel(channelId) {
    return this.pauseChannel(channelId);
  }

  async restartChannel(channelId) {
    const node = this._requireNode(channelId);
    if (node.status === 'archived') {
      throw toRpcError('invalid_state', `channel ${channelId} is archived`);
    }
    await this._stopProcess(node, 'SIGTERM');
    node.proc = null;
    node.agentPid = null;
    node.status = 'created';
    this._persistNode(node);
    const info = await this.startChannel({ channelId }, { notifyStatus: true });
    emitJsonEvent('channel.restart', { channel_id: node.channelId, status: info.status, pid: info.agent_pid });
    return info;
  }

  async archiveChannel(channelId) {
    const node = this._requireNode(channelId);
    this.cronScheduler.clearChannel(channelId);
    node.status = 'archived';
    node.archivedAt = nowIso();
    this._persistNode(node);
    await this._stopProcess(node, 'SIGTERM');

    const archivedWorkdir = path.join(this.archivedDir, `${channelId}-${Date.now()}`);
    renameSync(node.workdir, archivedWorkdir);
    node.workdir = archivedWorkdir;
    this._persistNode(node);
    this.channels.delete(channelId);
    this._notifyChannelStatus(node);
    emitJsonEvent('channel.archive', { channel_id: node.channelId, archived_at: node.archivedAt });
    return this._channelInfo(node);
  }

  async handleEvent(payload) {
    const channelId = String(payload.channelId ?? payload.channel_id ?? payload.id ?? '').trim();
    if (!channelId && !payload.channel) {
      throw toRpcError('bad_request', 'channel_id is required');
    }

    if (payload.channel) {
      await this.createChannel(payload.channel);
    }

    const node = this._requireNode(channelId || payload.channel.channelId || payload.channel.channel_id || payload.channel.id);
    const event = normalizeEvent(payload.event ?? payload);

    if (event.type === 'channel.member.joined') {
      node.members = normalizeMembers([...node.members, event.payload?.member ?? event.payload]);
      this._persistNode(node);
    }

    if (event.type === 'channel.config.updated' && payload.channel) {
      await this.createChannel({ ...payload.channel, status: node.status });
    }

    if (event.type === 'user.message.posted') {
      const message = event.payload?.message ?? event.payload;
      const normalized = normalizeMessage(node.channelId, message, { source: 'server' });
      await this._appendMessage(node, normalized);
      emitJsonEvent('message.receive', {
        channel_id: node.channelId,
        message_id: normalized.messageId,
        sender_type: normalized.senderType,
      });
    }

    return this.triggerGateway.dispatch({ channel: node, event });
  }

  async registerSchedule(params, kind) {
    const channelId = String(params.channel_id ?? params.channelId ?? '').trim();
    const node = this._requireNode(channelId);
    const scheduleId = String(params.id ?? params.schedule_id ?? randomUUID());
    const schedule = {
      id: scheduleId,
      channel_id: node.channelId,
      kind,
      cron: kind === 'cron' ? String(params.cron ?? params.cron_expr ?? '').trim() : null,
      at: kind === 'at' ? String(params.at ?? params.next_run_at ?? '').trim() : null,
      reason: String(params.reason ?? '').trim(),
      payload: params.payload ?? {},
      created_at: nowIso(),
      created_by: String(params.created_by ?? params.createdBy ?? node.agentName),
    };

    if (kind === 'cron' && !schedule.cron) {
      throw toRpcError('bad_request', 'cron expression is required');
    }
    if (kind === 'at' && !schedule.at) {
      throw toRpcError('bad_request', 'at timestamp is required');
    }

    writeStructuredFile(this._schedulePath(node, scheduleId), schedule);
    if (node.status === 'active') {
      this.cronScheduler.register(schedule);
    }
    return schedule;
  }

  async listSchedules(channelId) {
    const node = this._requireNode(channelId);
    return sortByCreatedAt(this._readSchedules(node));
  }

  async cancelSchedule(channelId, scheduleId, options = {}) {
    if (!scheduleId) throw toRpcError('bad_request', 'schedule id is required');
    const node = this._requireNode(channelId);
    const schedulePath = this._schedulePath(node, scheduleId);

    if (existsSync(schedulePath)) {
      unlinkSync(schedulePath);
    }
    this.cronScheduler.cancel(node.channelId, scheduleId);

    return options.silent
      ? { canceled: true }
      : { channel_id: node.channelId, schedule_id: scheduleId, canceled: true };
  }

  async getChannelInfo(channelId) {
    const node = this._requireNode(channelId);
    return this._channelInfo(node);
  }

  async getChannelMembers(channelId) {
    const node = this._requireNode(channelId);
    return { members: node.members };
  }

  async getChannelCapabilities(channelId) {
    const node = this._requireNode(channelId);
    return node.capabilitySet;
  }

  async sendChannelMessage(params, options = {}) {
    const channelId = String(params.channel_id ?? params.channelId ?? '').trim();
    const node = this._requireNode(channelId);
    const message = normalizeMessage(channelId, params, {
      senderType: options.senderType ?? 'channel_agent',
      senderId: options.senderId ?? node.agentName,
      senderName: options.senderName ?? node.agentName,
      source: options.source ?? 'daemon',
    });

    await this._appendMessage(node, message);
    emitJsonEvent('message.send', {
      channel_id: node.channelId,
      message_id: message.messageId,
      sender_type: message.senderType,
    });
    await this._appendToServerView(node, message, { requestId: options.requestId });

    // Server-pushed human messages must trigger the channel agent.
    // Without this, agent-binary stays idle even though the message file exists.
    if (message.senderType === 'human' && (options.source === 'server' || message.source === 'server')) {
      await this.triggerGateway.dispatch({
        channel: node,
        event: { type: 'user.message.posted', payload: { message } },
      });
    }

    return message;
  }

  async handleServerMessageSend(message, connection = this.connection) {
    const requestId = String(message?.requestId ?? '').trim();
    if (!requestId) {
      throw toRpcError('bad_request', 'requestId is required');
    }

    try {
      const sent = await this.sendChannelMessage({
        channelId: message.channelId ?? message.channel_id,
        senderType: message.senderType ?? message.sender_type ?? 'human',
        senderId: message.senderId ?? message.sender_id,
        senderName: message.senderName ?? message.sender_name,
        messageType: message.messageType ?? message.message_type ?? 'chat',
        content: message.content,
        attachments: message.attachments,
      }, {
        requestId,
        senderType: 'human',
        senderId: String(message.senderId ?? message.sender_id ?? '').trim(),
        senderName: String(message.senderName ?? message.sender_name ?? '').trim(),
        source: 'server',
      });

      connection?.send({
        type: 'channel:message.send.result',
        requestId,
        ok: true,
        message: sent,
      });
      return sent;
    } catch (err) {
      connection?.send({
        type: 'channel:message.send.result',
        requestId,
        ok: false,
        error: err.message,
        code: err.code ?? 'rpc_error',
      });
      return null;
    }
  }

  async listMessages(channelId, limit = 50) {
    const node = this._requireNode(channelId);
    const messages = this._readMessages(node)
      .slice(-Number(limit || 50))
      .reverse();
    return { messages };
  }

  async searchMessages(channelId, query, limit = 20) {
    const node = this._requireNode(channelId);
    const needle = String(query ?? '').trim().toLowerCase();
    if (!needle) throw toRpcError('bad_request', 'query is required');

    const messages = this._readMessages(node)
      .filter((message) => String(message.content ?? '').toLowerCase().includes(needle))
      .slice(-Number(limit || 20))
      .reverse();
    return { messages };
  }

  async listChannels() {
    return { channels: [...this.channels.values()].map((node) => this._channelInfo(node)) };
  }

  async getAdminStatus() {
    const channels = [...this.channels.values()];
    return {
      ok: true,
      project_key: this.projectKey,
      server_url: this.serverUrl,
      daemon_socket: this.daemonSocketPath,
      daemon_http: this.daemonHttpUrl || null,
      connected_to_server: Boolean(this.connection?.ws?.readyState === 1),
      channels_count: channels.length,
      active_channels_count: channels.filter((node) => node.status === 'active').length,
      active_agent_pids: channels.filter((node) => node.agentPid).map((node) => node.agentPid),
    };
  }

  async listMachines() {
    return {
      machines: [{
        id: 'local',
        project_key: this.projectKey,
        server_url: this.serverUrl,
        api_key_prefix: this.machineApiKey ? this.machineApiKey.slice(0, 18) : null,
        status: this.connection?.ws?.readyState === 1 ? 'online' : 'local',
        channels_count: this.channels.size,
      }],
    };
  }

  _channelInfo(node) {
    return {
      channel_id: node.channelId,
      name: node.name,
      workspace_id: node.workspaceId,
      daemon_id: node.daemonId,
      type: node.type,
      status: node.status,
      capability_set: node.capabilitySet,
      workdir: node.workdir,
      agent_pid: node.agentPid,
      session_id_path: this._sessionIdPath(node),
      session_id: node.sessionId,
      mounted_cli_binaries: node.mountedCliBinaries,
      members: node.members,
      members_count: node.members.length,
      created_at: node.createdAt,
      archived_at: node.archivedAt,
    };
  }

  _requireNode(channelId) {
    const node = this.channels.get(String(channelId ?? '').trim());
    if (!node) {
      throw toRpcError('not_found', `channel not found locally: ${channelId}`);
    }
    return node;
  }

  _materializeWorkdir(workdir) {
    mkdirSync(workdir, { recursive: true });
    if (existsSync(this.workspaceTemplateDir)) {
      cpSync(this.workspaceTemplateDir, workdir, { recursive: true, force: false, errorOnExist: false });
    }
    ensureDirectory(path.join(workdir, 'messages'));
    ensureDirectory(path.join(workdir, 'artifacts'));
    ensureDirectory(path.join(workdir, 'schedules'));
    ensureDirectory(path.join(workdir, 'pending-view-sync'));
    ensureDirectory(path.join(workdir, 'agents', DEFAULT_AGENT_NAME, 'trace'));
  }

  _channelMetaPath(workdir) {
    return path.join(workdir, 'channel.yaml');
  }

  _sessionIdPath(node) {
    return path.join(node.workdir, 'agents', node.agentName, 'session.id');
  }

  _traceDir(node) {
    return path.join(node.workdir, 'agents', node.agentName, 'trace');
  }

  _scheduleDir(node) {
    return path.join(node.workdir, 'schedules');
  }

  _schedulePath(node, scheduleId) {
    return path.join(this._scheduleDir(node), `${scheduleId}.yaml`);
  }

  _persistNode(node) {
    ensureDirectory(path.dirname(this._sessionIdPath(node)));
    writeStructuredFile(this._channelMetaPath(node.workdir), {
      channel_id: node.channelId,
      name: node.name,
      workspace_id: node.workspaceId,
      daemon_id: node.daemonId,
      type: node.type,
      status: node.status,
      capability_set: node.capabilitySet,
      members: node.members,
      agent_name: node.agentName,
      created_at: node.createdAt,
      archived_at: node.archivedAt,
    });
  }

  _loadNodeFromDisk(workdir) {
    const meta = parseStructuredFile(this._channelMetaPath(workdir));
    const payload = normalizeChannelPayload(meta);

    return {
      channelId: payload.channelId,
      workspaceId: payload.workspaceId,
      daemonId: payload.daemonId,
      name: payload.name || payload.channelId,
      type: payload.type,
      status: payload.status,
      capabilitySet: payload.capabilitySet,
      members: payload.members,
      agentName: payload.agentName,
      createdAt: payload.createdAt,
      archivedAt: payload.archivedAt,
      workdir,
      proc: null,
      agentPid: null,
      sessionId: existsSync(path.join(workdir, 'agents', payload.agentName, 'session.id'))
        ? readFileSync(path.join(workdir, 'agents', payload.agentName, 'session.id'), 'utf8').trim()
        : null,
      lastSpawnedAt: 0,
      crashCount: 0,
      intentionalStop: false,
      mountedCliBinaries: [],
    };
  }

  _wireProcess(node, proc, { restoring }) {
    let stdoutBuffer = '';

    proc.stdout.on('data', (chunk) => {
      stdoutBuffer += chunk.toString();
      const lines = stdoutBuffer.split('\n');
      stdoutBuffer = lines.pop() ?? '';
      for (const line of lines) {
        process.stdout.write(`${line}\n`);
      }
    });

    proc.stderr.on('data', (chunk) => {
      const text = chunk.toString().trim();
      if (text) {
        console.error(`[ChannelManager][${node.channelId}] stderr: ${text.slice(0, 500)}`);
      }
    });

    proc.on('exit', async (code, signal) => {
      if (stdoutBuffer) {
        process.stdout.write(`${stdoutBuffer}\n`);
        stdoutBuffer = '';
      }

      if (this.channels.get(node.channelId)?.proc !== proc) return;

      node.proc = null;
      node.agentPid = null;

      emitJsonEvent('agent.exit', { channel_id: node.channelId, code, signal });
      console.error(`[ChannelManager] Channel ${node.channelId} process exited code=${code ?? 'n/a'} signal=${signal ?? 'n/a'}`);

      if (node.intentionalStop) {
        node.intentionalStop = false;
        return;
      }

      if (node.status !== 'active') {
        return;
      }

      const uptimeMs = Date.now() - (node.lastSpawnedAt ?? 0);
      if (uptimeMs > HEALTHY_UPTIME_MS) {
        node.crashCount = 0;
      }
      node.crashCount += 1;

      if (node.crashCount <= 1) {
        console.warn(`[ChannelManager] Respawning ${node.channelId} after unexpected exit`);
        try {
          await this.startChannel({ channelId: node.channelId }, { restoring, notifyStatus: false });
        } catch (err) {
          console.error(`[ChannelManager] Respawn failed for ${node.channelId}:`, err.message);
          emitJsonEvent('agent.error', { channel_id: node.channelId, message: err.message });
          node.status = 'failed';
          this._persistNode(node);
          this._notifyChannelStatus(node);
        }
        return;
      }

      node.status = 'failed';
      this._persistNode(node);
      this._notifyChannelStatus(node);
      emitJsonEvent('agent.error', { channel_id: node.channelId, reason: 'unexpected_exit_twice', code, signal });
      await this._recordTrace(node, {
        kind: 'agent.exit',
        decision: 'failed',
        reason: 'unexpected_exit_twice',
        code,
        signal,
      });
    });
  }

  async _stopProcess(node, signal) {
    if (!node.proc) return;

    const proc = node.proc;
    node.intentionalStop = true;

    await new Promise((resolve) => {
      let settled = false;
      const finish = () => {
        if (settled) return;
        settled = true;
        resolve();
      };

      proc.once('exit', finish);
      try {
        proc.kill(signal);
      } catch {
        finish();
        return;
      }

      setTimeout(() => {
        try {
          proc.kill('SIGKILL');
        } catch {}
        finish();
      }, SHUTDOWN_GRACE_MS);
    });
  }

  async _deliverEvent(node, event) {
    if (node.status !== 'active' || !node.proc?.stdin) {
      return;
    }

    try {
      node.proc.stdin.write(`${JSON.stringify({ type: 'event', event })}\n`);
    } catch (err) {
      console.error(`[ChannelManager] Failed to deliver event to ${node.channelId}:`, err.message);
    }
  }

  async _handleCronTick(tick) {
    const node = this.channels.get(tick.channelId);
    if (!node || node.status !== 'active') return;

    if (tick.kind === 'at') {
      await this.cancelSchedule(tick.channelId, tick.scheduleId, { silent: true });
    }

    await this.handleEvent({
      channelId: tick.channelId,
      event: {
        type: 'cron.tick',
        source: 'cron-scheduler',
        created_at: nowIso(),
        payload: {
          schedule_id: tick.scheduleId,
          reason: tick.reason,
          original_payload: tick.payload,
        },
      },
    });
  }

  _loadSchedulesIntoMemory(node) {
    this.cronScheduler.loadChannel(node.channelId, this._readSchedules(node));
  }

  _readSchedules(node) {
    const dir = this._scheduleDir(node);
    if (!existsSync(dir)) return [];

    const schedules = [];
    for (const fileName of readdirSync(dir)) {
      if (!fileName.endsWith('.yaml')) continue;
      const schedulePath = path.join(dir, fileName);
      try {
        schedules.push(parseStructuredFile(schedulePath));
      } catch (err) {
        console.error(`[ChannelManager] Failed to parse schedule ${schedulePath}:`, err.message);
      }
    }
    return schedules;
  }

  async _appendMessage(node, message) {
    const createdAt = new Date(message.createdAt);
    const bucket = Number.isNaN(createdAt.getTime())
      ? nowIso().slice(0, 10)
      : createdAt.toISOString().slice(0, 10);
    const filePath = path.join(node.workdir, 'messages', `${bucket}.jsonl`);
    appendFileSync(filePath, `${JSON.stringify(message)}\n`, 'utf8');
    return message;
  }

  _readMessages(node) {
    const messagesDir = path.join(node.workdir, 'messages');
    if (!existsSync(messagesDir)) return [];

    const messages = [];
    for (const fileName of readdirSync(messagesDir).filter((entry) => entry.endsWith('.jsonl')).sort()) {
      const filePath = path.join(messagesDir, fileName);
      const lines = readFileSync(filePath, 'utf8')
        .split('\n')
        .map((line) => line.trim())
        .filter(Boolean);

      for (const line of lines) {
        try {
          messages.push(JSON.parse(line));
        } catch {}
      }
    }
    return sortByCreatedAt(messages);
  }

  async _appendToServerView(node, message, { requestId = randomUUID() } = {}) {
    const payload = {
      type: 'message.append',
      requestId,
      message_id: message.messageId,
      channel_id: node.channelId,
      sender_type: message.senderType,
      sender_id: message.senderId,
      sender_name: message.senderName,
      message_type: message.messageType,
      content: message.content,
      attachments: message.attachments,
    };

    if (!this.connection) {
      this._enqueuePendingViewSync(node, message, payload, 'daemon connection is not ready');
      return;
    }

    let response;
    try {
      response = await this.connection.request({
        message: payload,
        expect: { type: 'message.append.ack', requestId },
        timeoutMs: 10_000,
      });
    } catch (err) {
      this._enqueuePendingViewSync(node, message, payload, err?.message ?? String(err));
      return;
    }

    if (!response?.ok) {
      this._enqueuePendingViewSync(node, message, payload, response?.error ?? 'message.append ack failed');
      return;
    }
    emitJsonEvent('message.deliver', { channel_id: node.channelId, message_id: message.messageId, request_id: requestId });
  }

  _enqueuePendingViewSync(node, message, payload, reason) {
    const pendingDir = ensureDirectory(path.join(node.workdir, 'pending-view-sync'));
    const filePath = path.join(pendingDir, `${message.messageId}.json`);
    writeFileSync(
      filePath,
      JSON.stringify({ enqueuedAt: nowIso(), reason, payload }, null, 2),
      'utf8',
    );
    this._recordTrace(node, {
      type: 'view_sync_failed',
      messageId: message.messageId,
      requestId: payload.requestId,
      reason,
    });
  }

  async _recordTrace(node, record) {
    const sessionId = node.sessionId
      || (existsSync(this._sessionIdPath(node)) ? readFileSync(this._sessionIdPath(node), 'utf8').trim() : '')
      || 'pending';
    const traceDir = ensureDirectory(this._traceDir(node));
    appendFileSync(
      path.join(traceDir, `${sessionId}.jsonl`),
      `${JSON.stringify({ ts: nowIso(), ...record })}\n`,
      'utf8',
    );
  }

  _notifyChannelStatus(node) {
    if (!this.connection) return;
    this.connection.send({
      type: 'channel.status',
      channelId: node.channelId,
      status: node.status,
      archivedAt: node.archivedAt ?? null,
    });
  }
}
