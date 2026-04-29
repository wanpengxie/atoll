import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { existsSync, mkdtempSync, readFileSync, readdirSync, rmSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { promisify } from 'node:util';
import { ChannelManager } from '../src/channel-manager.js';
import { RpcServer } from '../src/rpc-server.js';

const execFileAsync = promisify(execFile);
const tempHome = mkdtempSync(path.join(os.tmpdir(), 'lightcone-daemon-smoke-'));
process.env.HOME = tempHome;

const httpPort = 24000 + Math.floor(Math.random() * 1000);
const socketPath = path.join(tempHome, '.coagent', 'daemon.sock');
const daemonToken = 'smoke-token';
const channelId = 'smoke-channel';
const requestLog = [];
const sentLog = [];

async function curlSocketRpc(payload) {
  const { stdout } = await execFileAsync('curl', [
    '--silent',
    '--show-error',
    '--max-time', '5',
    '--unix-socket', socketPath,
    '--request', 'POST',
    'http://localhost/rpc',
    '--header', 'Content-Type: application/json',
    '--data', JSON.stringify(payload),
  ], { encoding: 'utf8' });
  return JSON.parse(stdout);
}

async function curlHttpRpc(payload) {
  const { stdout } = await execFileAsync('curl', [
    '--silent',
    '--show-error',
    '--max-time', '5',
    '--request', 'POST',
    `http://127.0.0.1:${httpPort}/rpc`,
    '--header', `Authorization: Bearer ${daemonToken}`,
    '--header', 'Content-Type: application/json',
    '--data', JSON.stringify(payload),
  ], { encoding: 'utf8' });
  return JSON.parse(stdout);
}

function readTraceEntries(workdir, sessionId) {
  const tracePath = path.join(workdir, 'agents', 'channel-agent', 'trace', `${sessionId}.jsonl`);
  if (!existsSync(tracePath)) return [];

  return readFileSync(tracePath, 'utf8')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

async function waitFor(label, fn, timeoutMs = 5_000) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    const value = await fn();
    if (value) return value;
    await delay(50);
  }
  throw new Error(`timed out waiting for ${label}`);
}

const connection = {
  send(message) {
    sentLog.push(message);
  },
  async request({ message, expect }) {
    requestLog.push({ message, expect });
    return { type: expect.type, requestId: expect.requestId, ok: true };
  },
};

const channelManager = new ChannelManager({
  serverUrl: 'http://localhost:8779',
  machineApiKey: 'smoke-machine-key',
  daemonSocketPath: socketPath,
  daemonHttpUrl: `http://127.0.0.1:${httpPort}`,
  daemonToken,
});
channelManager.setConnection(connection);

const rpcServer = new RpcServer({
  channelManager,
  socketPath,
  httpPort,
  authToken: daemonToken,
});

try {
  const created = await channelManager.createChannel({
    channelId,
    workspaceId: 'workspace-smoke',
    daemonId: 'machine-smoke',
    type: 'xhs-creator',
    status: 'active',
    capabilitySet: { cli_binaries: ['xhs', 'missing-binary'] },
    members: [{ memberType: 'human', memberId: 'user-smoke', displayName: 'Smoke User' }],
  });
  assert.equal(created.status, 'created');

  const started = await channelManager.startChannel({ channelId });
  assert.equal(started.status, 'active');
  assert.ok(started.agent_pid, 'channel agent pid should exist after start');
  assert.equal(readFileSync(started.session_id_path, 'utf8').trim(), started.session_id);
  assert.deepEqual(started.mounted_cli_binaries, ['xhs']);

  await rpcServer.start();

  const infoResponse = await curlSocketRpc({
    method: 'channel.info',
    params: { channel_id: channelId },
  });
  assert.equal(infoResponse.ok, true);
  assert.equal(infoResponse.result.channel_id, channelId);

  const now = new Date();
  const cronExpression = `${now.getMinutes()} ${now.getHours()} ${now.getDate()} ${now.getMonth() + 1} ${now.getDay()}`;
  const scheduleResponse = await curlSocketRpc({
    method: 'schedule.cron',
    params: {
      channel_id: channelId,
      id: 'cron-now',
      cron: cronExpression,
      reason: 'smoke-cron',
      payload: { source: 'smoke' },
    },
  });
  assert.equal(scheduleResponse.ok, true);
  assert.ok(existsSync(path.join(started.workdir, 'schedules', 'cron-now.yaml')));

  await channelManager.cronScheduler._poll();
  await delay(50);
  let traceEntries = readTraceEntries(started.workdir, started.session_id);
  assert.ok(
    traceEntries.some((entry) => entry.kind === 'trigger' && entry.decision === 'pass' && entry.event?.type === 'cron.tick'),
    'cron.tick should pass trigger gateway',
  );

  await channelManager.handleEvent({
    channelId,
    event: {
      type: 'heartbeat',
      source: 'smoke',
      created_at: new Date().toISOString(),
      payload: {},
    },
  });
  traceEntries = readTraceEntries(started.workdir, started.session_id);
  assert.ok(
    traceEntries.some((entry) => entry.kind === 'trigger' && entry.decision === 'block' && entry.event?.type === 'heartbeat'),
    'heartbeat should be blocked by trigger gateway',
  );

  const messageResponse = await curlHttpRpc({
    method: 'message.send',
    params: {
      channel_id: channelId,
      content: 'smoke message',
    },
  });
  assert.equal(messageResponse.ok, true);
  assert.equal(requestLog.length, 1);
  assert.equal(requestLog[0].message.type, 'message.append');

  const listResponse = await curlSocketRpc({
    method: 'message.list',
    params: { channel_id: channelId, limit: 10 },
  });
  assert.equal(listResponse.ok, true);
  assert.ok(listResponse.result.messages.some((message) => message.content === 'smoke message'));

  const originalPid = started.agent_pid;
  process.kill(originalPid, 'SIGTERM');
  const respawned = await waitFor('channel respawn', async () => {
    const info = await channelManager.getChannelInfo(channelId);
    return info.agent_pid && info.agent_pid !== originalPid ? info : null;
  });
  assert.equal(respawned.session_id, started.session_id);

  const paused = await channelManager.pauseChannel(channelId);
  assert.equal(paused.status, 'paused');
  assert.equal(paused.agent_pid, null);

  const resumed = await channelManager.resumeChannel(channelId);
  assert.equal(resumed.status, 'active');
  assert.equal(resumed.session_id, started.session_id);

  const archived = await channelManager.archiveChannel(channelId);
  assert.equal(archived.status, 'archived');
  assert.ok(archived.archived_at);
  assert.ok(archived.workdir.includes(`${path.sep}.coagent${path.sep}archived${path.sep}`));

  let archivedLookupFailed = false;
  try {
    await channelManager.getChannelInfo(channelId);
  } catch (err) {
    archivedLookupFailed = err.code === 'not_found';
  }
  assert.equal(archivedLookupFailed, true);

  console.log(JSON.stringify({
    ok: true,
    workdir: started.workdir,
    archivedWorkdir: archived.workdir,
    sessionId: started.session_id,
    sentMessages: requestLog.length,
    daemonStatusEvents: sentLog.filter((message) => message.type === 'channel.status').map((message) => message.status),
    traceFiles: readdirSync(path.join(archived.workdir, 'agents', 'channel-agent', 'trace')),
  }, null, 2));
} finally {
  await rpcServer.stop().catch(() => {});
  await channelManager.stopAll().catch(() => {});
  rmSync(tempHome, { recursive: true, force: true });
}
