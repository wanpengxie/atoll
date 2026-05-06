import { WebSocketServer } from 'ws';
import { SenderKind } from '@coagent/payload-types';
import {
  getDb, getMachineByApiKey, updateMachine, updateAgent, getAgents,
  getAgentById, getTeamById, getMemberTeamIds,
  getTeamSession, upsertTeamSession, getAgentByApiKey,
  getChannelById, getChannelMembers, insertMessage, updateChannel,
  insertCredential, getCredentialsByOwner, deleteCredential,
} from '../db/index.js';
import { encrypt } from '../crypto.js';
import { randomUUID } from 'crypto';
import { registerDaemon, unregisterDaemon, sendToDaemon } from './connections.js';
import { broadcast } from '../realtime/broadcast.js';
import { flushInbox } from '../scheduler/inbox.js';
import { formatMessage } from '../internal/index.js';
import { nowMysqlDatetime } from '../time.js';
import { emitJsonEvent } from '../events.js';

const pendingRequests = new Map();
// machineId → { userId, platform }: tracks who initiated browser login
const pendingBrowserLogins = new Map();

function normalizeSenderKind(rawKind) {
  const kind = String(rawKind ?? '').trim();
  if (Object.values(SenderKind).includes(kind)) return kind;
  return null;
}

function appendAttachmentReferences(content, attachments) {
  if (!Array.isArray(attachments) || attachments.length === 0) return content;
  const lines = attachments.map((attachment, index) => {
    if (typeof attachment === 'string') return `- ${attachment}`;
    const name = attachment?.name ?? attachment?.path ?? attachment?.url ?? `attachment-${index + 1}`;
    const ref = attachment?.path ?? attachment?.url ?? '';
    return ref ? `- ${name}: ${ref}` : `- ${name}`;
  });
  return `${content}\n\nAttachments:\n${lines.join('\n')}`;
}

function parseChannelCapabilitySet(channel) {
  return typeof channel.capability_set === 'string'
    ? JSON.parse(channel.capability_set)
    : (channel.capability_set ?? { cli_binaries: [] });
}

async function buildDaemonChannelPayload(db, channel) {
  const members = await getChannelMembers(db, channel.id);
  return {
    channelId: channel.id,
    workspaceId: channel.workspace_id,
    daemonId: channel.daemon_id ?? null,
    name: channel.name,
    type: channel.type,
    status: channel.status,
    capabilitySet: parseChannelCapabilitySet(channel),
    archivedAt: channel.archived_at ?? null,
    members: members.map((member) => ({
      memberType: member.member_type,
      memberId: member.member_id,
      displayName: member.human_name ?? member.agent_display_name ?? member.agent_name ?? member.member_id,
      joinedAt: member.joined_at,
    })),
  };
}

export function setPendingBrowserLogin(machineId, userId, platform) {
  pendingBrowserLogins.set(machineId, { userId, platform });
}

export function setupDaemonServer(httpServer) {
  const wss = new WebSocketServer({ noServer: true });

  httpServer.on('upgrade', async (req, socket, head) => {
    const url = new URL(req.url, `http://localhost`);
    if (!url.pathname.startsWith('/daemon/connect')) return;

    const apiKey = url.searchParams.get('key');
    const machine = await getMachineByApiKey(getDb(), apiKey);

    if (!machine) {
      socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
      socket.destroy();
      console.warn('[Daemon] Rejected connection: invalid API key');
      return;
    }

    wss.handleUpgrade(req, socket, head, (ws) => {
      wss.emit('connection', ws, req, machine);
    });
  });

  wss.on('connection', (ws, req, machine) => {
    const machineId = machine.id;
    const serverId  = machine.server_id;
    console.error(`[Daemon] Machine ${machine.name} (${machineId}) connected`);
    emitJsonEvent('machine.connect', { machine_id: machineId, server_id: serverId });

    registerDaemon(machineId, ws);

    ws.on('message', async (raw) => {
      let msg;
      try { msg = JSON.parse(raw.toString()); } catch { return; }
      await handleDaemonMessage(machineId, serverId, msg, ws);
    });

    ws.on('close', async (code) => {
      console.error(`[Daemon] Machine ${machine.name} disconnected (code=${code})`);
      emitJsonEvent('machine.disconnect', { machine_id: machineId, server_id: serverId, code });
      unregisterDaemon(machineId);
      const db = getDb();
      await updateMachine(db, machineId, { status: 'offline' });
      broadcast.machineStatus(serverId, machineId, 'offline');
      const agents = (await getAgents(db, serverId)).filter(a => a.machine_id === machineId);
      for (const agent of agents) {
        await updateAgent(db, agent.id, { status: 'inactive', activity: null, activity_detail: '' });
        broadcast.agentActivity(serverId, agent.id, 'offline', 'Machine disconnected', []);
      }
    });

    ws.on('error', (err) => {
      console.error(`[Daemon] WS error for machine ${machineId}:`, err.message);
    });
  });

  setInterval(() => {
    for (const [machineId] of (global._daemonConnections ?? new Map())) {
      sendToDaemon(machineId, { type: 'ping' });
    }
  }, 30000);

  console.error('[Daemon] WebSocket server ready at /daemon/connect');
}

async function handleDaemonMessage(machineId, serverId, msg) {
  const db = getDb();
  const { type } = msg;

  switch (type) {
    case 'ready': {
      console.error(`[Daemon] ${machineId} ready — runtimes: ${msg.runtimes?.join(', ')}, version: ${msg.daemonVersion}`);
      await updateMachine(db, machineId, {
        status: 'online',
        hostname: msg.hostname ?? null,
        os: msg.os ?? null,
        runtimes: JSON.stringify(msg.runtimes ?? []),
        models_by_runtime: msg.modelsByRuntime ? JSON.stringify(msg.modelsByRuntime) : null,
        daemon_version: msg.daemonVersion ?? null,
        last_heartbeat: nowMysqlDatetime(),
      });
      broadcast.machineStatus(serverId, machineId, 'online');
      broadcast.machineCapabilities(serverId, machineId, msg.runtimes ?? [], msg.hostname, msg.os, msg.daemonVersion);

      const serverUrl = process.env.SERVER_URL ?? `http://localhost:${process.env.PORT ?? 3001}`;
      const allAgents = (await getAgents(db, serverId)).filter(a => a.machine_id === machineId && !a.is_del && !a.deleted_at);
      for (const agent of allAgents) {
        await updateAgent(db, agent.id, { status: 'active', activity: null, activity_detail: '' });
        broadcast.agentActivity(serverId, agent.id, 'idle', '', []);
        const teamIds = await getMemberTeamIds(db, agent.id);
        for (const teamId of teamIds) {
          const sessionId = await getTeamSession(db, agent.id, teamId);
          const team = await getTeamById(db, teamId);
          const [memberRows] = await db.execute(
            `SELECT role_prompt FROM team_members WHERE team_id = ? AND member_id = ?`,
            [teamId, agent.id]
          );
          const rolePrompt = memberRows[0]?.role_prompt ?? '';
          sendToDaemon(machineId, {
            type: 'agent:start', agentId: agent.id, teamId,
            teamName: team?.name ?? teamId,
            config: {
              runtime: agent.runtime ?? 'claude', model: agent.model ?? null,
              sessionId: sessionId ?? null, name: agent.name,
              displayName: agent.display_name, description: agent.description ?? '',
              feishuBotName: agent.feishu_bot_name ?? null,
              rolePrompt,
              serverUrl, authToken: agent.agent_api_key ?? process.env.ADMIN_TOKEN ?? 'demo-token',
              envVars: agent.env_vars ? JSON.parse(agent.env_vars) : {},
            },
          });
          console.error(`[Daemon] Sent agent:start to ${agent.name} (#${team?.name ?? teamId})`);
        }
      }

      for (const agent of allAgents) {
        flushInbox(agent.id, async (message) => {
          sendToDaemon(machineId, {
            type: 'agent:deliver', agentId: agent.id,
            teamId: message.team_id,
            seq: message.seq, message: await formatMessageForDaemon(message),
          });
        });
      }

      const [channelRows] = await db.execute(
        `SELECT * FROM channels
         WHERE daemon_id = ? AND is_del = 0 AND deleted_at IS NULL`,
        [machineId]
      );
      for (const channel of channelRows) {
        if (channel.status === 'archived') continue;
        const daemonChannel = await buildDaemonChannelPayload(db, channel);
        sendToDaemon(machineId, { type: 'channel:create', channel: daemonChannel });
        if (channel.status === 'active') {
          sendToDaemon(machineId, { type: 'channel:start', channel: daemonChannel });
        }
      }
      break;
    }

    case 'agent:status': {
      const { agentId, status } = msg;
      await updateAgent(db, agentId, { status });
      broadcast.agentActivity(serverId, agentId, status === 'active' ? 'online' : 'offline', '', []);
      console.error(`[Daemon] Agent ${agentId} status → ${status}`);
      if (status === 'active') {
        flushInbox(agentId, async (message) => {
          const agent = await getAgentById(db, agentId);
          if (agent?.machine_id) {
            sendToDaemon(agent.machine_id, {
              type: 'agent:deliver', agentId,
              teamId: message.team_id,
              seq: message.seq, message: await formatMessageForDaemon(message),
            });
          }
        });
      }
      break;
    }

    case 'agent:activity': {
      const { agentId, activity, detail, entries } = msg;
      await updateAgent(db, agentId, { activity, activity_detail: detail ?? '' });
      broadcast.agentActivity(serverId, agentId, activity, detail ?? '', entries ?? []);
      break;
    }

    case 'agent:session': {
      const { agentId, teamId, sessionId } = msg;
      if (teamId) {
        await upsertTeamSession(db, agentId, teamId, sessionId);
      } else {
        await updateAgent(db, agentId, { session_id: sessionId });
      }
      console.error(`[Daemon] Agent ${agentId} team=${teamId ?? 'none'} session → ${sessionId}`);
      break;
    }

    case 'agent:deliver:ack':
      console.error(`[Daemon] Deliver ack: agent=${msg.agentId} seq=${msg.seq}`);
      break;

    case 'message.append': {
      const requestId = msg.requestId ?? null;
      try {
        const messageId = String(msg.message_id ?? randomUUID());
        const channelId = msg.channel_id ?? msg.channelId;
        const senderId = msg.sender_id ?? msg.senderId;
        const senderKind = normalizeSenderKind(msg.sender_kind ?? msg.envelope?.sender?.kind);
        const payloadBody = msg.payload_body ?? msg.payloadBody ?? msg.payload?.body ?? null;
        const payloadType = String(msg.payload_type ?? msg.payloadType ?? msg.payload?.type ?? '').trim();
        const content = String(msg.content ?? payloadBody?.text ?? (payloadBody ? JSON.stringify(payloadBody) : ''));
        if (!requestId) throw new Error('requestId required');
        if (!channelId) throw new Error('channel_id required');
        if (!senderKind) throw new Error('sender_kind required');
        if (!senderId) throw new Error('sender_id required');
        if (!payloadType) throw new Error('payload_type required');
        if (!content.trim() && payloadBody == null) throw new Error('content required');

        const channel = await getChannelById(db, channelId);
        if (!channel || channel.is_del || channel.deleted_at) {
          throw new Error(`channel not found: ${channelId}`);
        }
        if (channel.daemon_id && channel.daemon_id !== machineId) {
          throw new Error(`channel ${channelId} is bound to daemon ${channel.daemon_id}, not ${machineId}`);
        }

        const message = await insertMessage(db, {
          id: messageId,
          teamId: null,
          channelId,
          senderId,
          senderKind,
          payloadType,
          payloadBody,
          content: appendAttachmentReferences(content, msg.attachments),
          parentId: msg.parent_id ?? msg.parentId ?? msg.envelope?.parent_id ?? null,
          correlationId: msg.correlation_id ?? msg.correlationId ?? msg.envelope?.correlation_id ?? null,
          taskId: msg.task_id ?? msg.taskId ?? msg.envelope?.task_id ?? null,
          threadId: msg.thread_id ?? msg.threadId ?? msg.envelope?.thread_id ?? null,
          audience: msg.audience ?? msg.envelope?.audience ?? null,
          notBefore: msg.not_before ?? msg.notBefore ?? msg.envelope?.not_before ?? null,
          origin: msg.origin ?? msg.envelope?.origin ?? null,
          expiresAt: msg.expires_at ?? msg.expiresAt ?? msg.envelope?.expires_at ?? null,
          tsReceived: msg.ts_received ?? msg.tsReceived ?? msg.envelope?.ts_received ?? null,
          envelope: msg.envelope ?? null,
          daemonRequestId: requestId,
        });

        if (!message.__deduped) {
          broadcast.channelMessage(channelId, formatMessage(message));
          emitJsonEvent('message.create', {
            message_id: message.id,
            channel_id: channelId,
            machine_id: machineId,
            sender_kind: senderKind,
          });
        }
        sendToDaemon(machineId, { type: 'message.append.ack', requestId, ok: true });
        emitJsonEvent('message.deliver', {
          message_id: message.id,
          channel_id: channelId,
          machine_id: machineId,
          request_id: requestId,
        });
      } catch (err) {
        console.error(`[Daemon] message.append failed: ${err.message}`);
        if (requestId) {
          sendToDaemon(machineId, { type: 'message.append.ack', requestId, ok: false, error: err.message });
        }
      }
      break;
    }

    case 'channel.status': {
      const channelId = msg.channelId ?? msg.channel_id;
      if (!channelId) break;

      const channel = await getChannelById(db, channelId);
      if (!channel || channel.is_del || channel.deleted_at) break;

      const fields = {
        status: msg.status ?? channel.status,
      };
      if ('archivedAt' in msg || 'archived_at' in msg) {
        fields.archived_at = msg.archivedAt ?? msg.archived_at ?? null;
      }

      await updateChannel(db, channelId, fields);
      broadcast.channelUpdated(serverId, channel.workspace_id);
      break;
    }

    case 'agent:request_start': {
      const { agentId, teamId } = msg;
      const agent = await getAgentById(db, agentId);
      if (!agent || agent.is_del || agent.deleted_at || agent.machine_id !== machineId) break;
      const sessionId = teamId ? await getTeamSession(db, agentId, teamId) : agent.session_id;
      const team = teamId ? await getTeamById(db, teamId) : null;
      const serverUrl = process.env.SERVER_URL ?? `http://localhost:${process.env.PORT ?? 3001}`;
      let rolePrompt = '';
      if (teamId) {
        const [memberRows] = await db.execute(
          `SELECT role_prompt FROM team_members WHERE team_id = ? AND member_id = ?`,
          [teamId, agentId]
        );
        rolePrompt = memberRows[0]?.role_prompt ?? '';
      }
      sendToDaemon(machineId, {
        type: 'agent:start', agentId, teamId,
        teamName: team?.name ?? teamId,
        config: {
          runtime: agent.runtime ?? 'claude', model: agent.model ?? null,
          sessionId: sessionId ?? null, name: agent.name,
          displayName: agent.display_name, description: agent.description ?? '',
          feishuBotName: agent.feishu_bot_name ?? null,
          rolePrompt,
          serverUrl, authToken: agent.agent_api_key ?? process.env.ADMIN_TOKEN ?? 'demo-token',
          envVars: agent.env_vars ? JSON.parse(agent.env_vars) : {},
        },
      });
      console.error(`[Daemon] Re-sent agent:start for ${agent.name} (#${team?.name ?? teamId})`);
      break;
    }

    case 'agent:workspace:file_tree':
    case 'agent:workspace:file_content':
    case 'machine:workspace:scan_result':
    case 'machine:workspace:delete_result':
    case 'channel:message.send.result':
    case 'channel:rpc.result':
    case 'runtime:preflight:result': {
      const key = msg.requestId ?? msg.agentId;
      const resolve = pendingRequests.get(key);
      if (resolve) { resolve(msg); pendingRequests.delete(key); }
      break;
    }

    case 'browser:screenshot':
      broadcast.custom(serverId, 'browser:screenshot', { platform: msg.platform, screenshot: msg.screenshot });
      break;

    case 'browser:login_status':
      broadcast.custom(serverId, 'browser:login_status', { platform: msg.platform, status: msg.status, message: msg.message });
      break;

    case 'browser:login_complete': {
      const pending = pendingBrowserLogins.get(machineId);
      pendingBrowserLogins.delete(machineId);
      if (pending) {
        const { userId, platform } = pending;
        const { v4: uuidv4 } = await import('uuid');
        const envKey = `${platform.toUpperCase()}_PROFILE_DIR`;
        const displayNames = { xhs: '小红书账号', douyin: '抖音账号', kuaishou: '快手账号' };
        const displayName = displayNames[platform] ?? `${platform} 账号`;
        const { iv, data } = encrypt({ [envKey]: msg.profileDir });
        const newCredId = uuidv4();

        // Find old active credentials to migrate their grants
        const [oldCreds] = await db.execute(
          `SELECT id FROM platform_credentials WHERE owner_id = ? AND platform = ? AND is_del = 0 AND deleted_at IS NULL`,
          [userId, platform]
        );

        // Soft-delete old credentials
        await db.execute(
          `UPDATE platform_credentials
           SET is_del = 1, deleted_at = COALESCE(deleted_at, NOW())
           WHERE owner_id = ? AND platform = ? AND is_del = 0 AND deleted_at IS NULL`,
          [userId, platform]
        );

        await db.execute(
          `INSERT INTO platform_credentials (id, server_id, owner_id, platform, display_name, credential_type, encrypted_data, iv, scopes)
           VALUES (?, ?, ?, ?, ?, 'browser_profile', ?, ?, ?)`,
          [newCredId, serverId, userId, platform, displayName, data, iv, JSON.stringify([envKey])]
        );

        // Migrate grants from old credentials to new one
        if (oldCreds.length > 0) {
          const oldIds = oldCreds.map(c => c.id);
          for (const oldId of oldIds) {
            await db.execute(
              `UPDATE credential_grants SET credential_id = ? WHERE credential_id = ? COLLATE utf8mb4_unicode_ci`,
              [newCredId, oldId]
            );
          }
          console.error(`[Daemon] Migrated grants from ${oldIds.length} old credential(s) to ${newCredId}`);
        }

        console.error(`[Daemon] Browser login complete: platform=${platform} user=${userId}, profile saved`);

        // Restart agents that now have grants to the new credential so they pick up new env vars
        const [grantedAgentRows] = await db.execute(
          `SELECT DISTINCT cg.grantee_id FROM credential_grants cg
           WHERE cg.credential_id = ? AND cg.grantee_type = 'agent' AND cg.revoked_at IS NULL`,
          [newCredId]
        );
        const serverUrl = process.env.SERVER_URL ?? `http://localhost:${process.env.PORT ?? 3001}`;
        for (const row of grantedAgentRows) {
          const agent = await getAgentById(db, row.grantee_id);
          if (!agent || agent.is_del || agent.deleted_at || agent.machine_id !== machineId) continue;
          const teamIds = await getMemberTeamIds(db, agent.id);
          for (const teamId of teamIds) {
            sendToDaemon(machineId, { type: 'agent:stop', agentId: agent.id, teamId });
          }
          await new Promise(r => setTimeout(r, 500));
          for (const teamId of teamIds) {
            const sessionId = await getTeamSession(db, agent.id, teamId);
            const team = await getTeamById(db, teamId);
            const [memberRows] = await db.execute(
              `SELECT role_prompt FROM team_members WHERE team_id = ? AND member_id = ?`,
              [teamId, agent.id]
            );
            const rolePrompt = memberRows[0]?.role_prompt ?? '';
            sendToDaemon(machineId, {
              type: 'agent:start', agentId: agent.id, teamId,
              teamName: team?.name ?? teamId,
              config: {
                runtime: agent.runtime ?? 'claude', model: agent.model ?? null,
                sessionId: sessionId ?? null, name: agent.name,
                displayName: agent.display_name, description: agent.description ?? '',
                feishuBotName: agent.feishu_bot_name ?? null,
                rolePrompt,
                serverUrl, authToken: agent.agent_api_key ?? process.env.ADMIN_TOKEN ?? 'demo-token',
                envVars: agent.env_vars ? JSON.parse(agent.env_vars) : {},
              },
            });
            console.error(`[Daemon] Restarted agent ${agent.name} (#${team?.name ?? teamId}) after credential update`);
          }
        }
      } else {
        console.warn(`[Daemon] browser:login_complete from machine ${machineId} but no pending login`);
      }
      broadcast.custom(serverId, 'browser:login_complete', { platform: msg.platform });
      break;
    }

    case 'browser:login_error':
      broadcast.custom(serverId, 'browser:login_error', { platform: msg.platform, error: msg.error });
      break;

    case 'pong':
      await updateMachine(db, machineId, { last_heartbeat: nowMysqlDatetime() });
      break;

    default:
      console.error(`[Daemon] Unhandled message type: ${type}`);
  }
}

export async function formatMessageForDaemon(msg) {
  const db = getDb();
  const ch = await getTeamById(db, msg.team_id);
  const parseJson = (value, fallback = null) => {
    if (value == null) return fallback;
    if (typeof value !== 'string') return value;
    try { return JSON.parse(value); } catch { return fallback; }
  };
  return {
    team_type: ch?.type ?? 'team',
    team_name: ch?.name ?? 'all',
    sender_id: msg.sender_id,
    sender_name: parseJson(msg.envelope_json, {})?.sender?.name ?? msg.sender_id,
    sender_kind: msg.sender_kind,
    content: msg.content,
    message_id: msg.id,
    timestamp: msg.created_at,
    payload_type: msg.payload_type ?? null,
    payload_body: parseJson(msg.payload_body, null),
    parent_id: msg.parent_id ?? null,
    correlation_id: msg.correlation_id ?? null,
    task_id: msg.task_id ?? null,
    thread_id: msg.thread_id ?? null,
    audience: parseJson(msg.audience, null),
    not_before: msg.not_before ?? null,
    origin: msg.origin ?? null,
    expires_at: msg.expires_at ?? null,
    ts_received: msg.ts_received ?? null,
    envelope: parseJson(msg.envelope_json, null),
    attachments: [],
  };
}

export async function requestFromDaemon(machineId, request, responseKey, timeoutMs = 10000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      pendingRequests.delete(responseKey);
      reject(new Error('Daemon request timeout'));
    }, timeoutMs);
    pendingRequests.set(responseKey, (result) => {
      clearTimeout(timer);
      resolve(result);
    });
    sendToDaemon(machineId, request);
  });
}
