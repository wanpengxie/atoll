import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { existsSync, mkdtempSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';
import { ChannelManager } from '../src/channel-manager.js';
import { readStoredMessages } from '../src/message-store.js';
import { RpcServer } from '../src/rpc-server.js';

const execFileAsync = promisify(execFile);
const realMode = process.argv.includes('--real');
const tempRoot = mkdtempSync(path.join(os.tmpdir(), 'lightcone-daemon-smoke-'));
const originalHome = process.env.HOME;
if (!realMode) {
  process.env.HOME = tempRoot;
}
const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');
const cliBinDir = path.join(repoRoot, 'cli', 'bin');
const fakeBinDir = path.join(tempRoot, 'bin');

function exitPreflight(message, exitCode = 1) {
  console.error(message);
  rmSync(tempRoot, { recursive: true, force: true });
  process.exit(exitCode);
}

if (realMode) {
  if (!process.env.ANTHROPIC_API_KEY) {
    exitPreflight('ANTHROPIC_API_KEY is required for --real smoke. Set ANTHROPIC_API_KEY and retry.', 0);
  }
  try {
    await execFileAsync('claude', ['--version'], { timeout: 5000 });
  } catch (err) {
    const detail = [err.stdout, err.stderr, err.message].filter(Boolean).join('\n');
    exitPreflight(`claude CLI is required for --real smoke. Install/authenticate Claude Code and retry.${detail ? `\n${detail}` : ''}`);
  }
} else {
  mkdirSync(fakeBinDir, { recursive: true });
  writeFileSync(path.join(fakeBinDir, 'claude'), `#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');
const args = process.argv.slice(2);
const getValue = (flag) => {
  const index = args.indexOf(flag);
  return index >= 0 ? args[index + 1] : '';
};
const mode = args.includes('--resume') ? 'resume' : 'session-id';
const sessionId = getValue('--resume') || getValue('--session-id') || 'unknown';
const stdin = fs.readFileSync(0, 'utf8');
const projectSlug = process.cwd().replace(/[/.]/g, '-');
const sessionFile = path.join(process.env.HOME, '.claude', 'projects', projectSlug, sessionId + '.jsonl');
fs.mkdirSync(path.dirname(sessionFile), { recursive: true });
fs.appendFileSync(sessionFile, stdin);
process.stdout.write(JSON.stringify({ type: 'system', subtype: 'init', session_id: sessionId }) + '\\n');
process.stdout.write(JSON.stringify({ type: 'result', subtype: 'success', result: 'smoke:' + mode, session_id: sessionId }) + '\\n');
`, { mode: 0o755 });
  process.env.PATH = `${fakeBinDir}${path.delimiter}${process.env.PATH ?? ''}`;
}

const httpPort = 24000 + Math.floor(Math.random() * 1000);
const projectKey = `smoke-${process.pid}-${Date.now()}`;
process.env.COAGENT_PROJECT_KEY = projectKey;
const runtimeHome = realMode ? os.homedir() : tempRoot;
const socketPath = path.join(runtimeHome, '.coagent', projectKey, 'daemon.sock');
const daemonToken = 'smoke-token';
const channelId = 'smoke-channel';
const requestLog = [];
const sentLog = [];
const stdoutChunks = [];
const originalStdoutWrite = process.stdout.write;

process.stdout.write = function captureStdout(chunk, encoding, callback) {
  stdoutChunks.push(String(chunk));
  return originalStdoutWrite.call(this, chunk, encoding, callback);
};

function capturedJsonLineEvents() {
  return stdoutChunks.join('')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      try {
        return JSON.parse(line);
      } catch {
        return null;
      }
    })
    .filter((entry) => entry?.event);
}

function assertJsonLineEvents(expectedEvents) {
  const events = capturedJsonLineEvents();
  for (const eventName of expectedEvents) {
    assert.ok(
      events.some((entry) => entry.event === eventName),
      `daemon stdout should contain JSON Lines event ${eventName}`,
    );
  }
  return events.map((entry) => entry.event);
}

async function curlSocketRpc(payload, { token = daemonToken } = {}) {
  const headers = ['--header', 'Content-Type: application/json'];
  if (token) {
    headers.push('--header', `Authorization: Bearer ${token}`);
  }
  const { stdout } = await execFileAsync('curl', [
    '--silent',
    '--show-error',
    '--max-time', '5',
    '--unix-socket', socketPath,
    '--request', 'POST',
    'http://localhost/rpc',
    ...headers,
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

async function runCli(binary, args, env = {}, cwd = repoRoot) {
  const { stdout } = await execFileAsync(path.join(cliBinDir, binary), args, {
    cwd,
    encoding: 'utf8',
    env: {
      ...process.env,
      ...env,
    },
  });
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

function readAgentCmdline(pid) {
  try {
    return readFileSync(`/proc/${pid}/cmdline`, 'utf8').split('\0').filter(Boolean).join(' ');
  } catch {
    return '';
  }
}

function jsonlMessageFiles(workdir) {
  const messagesDir = path.join(workdir, 'messages');
  if (!existsSync(messagesDir)) return [];
  return readdirSync(messagesDir).filter((entry) => entry.endsWith('.jsonl'));
}

function sqliteFilesUnder(dir) {
  if (!existsSync(dir)) return [];
  const results = [];
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      results.push(...sqliteFilesUnder(fullPath));
    } else if (/\.(sqlite|sqlite3|db)$/i.test(entry.name)) {
      results.push(fullPath);
    }
  }
  return results;
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

function recentLines(filePath, limit = 100) {
  if (!existsSync(filePath)) return [];
  return readFileSync(filePath, 'utf8').split('\n').filter(Boolean).slice(-limit);
}

function printRecentAgentLogs(workdir, sessionId) {
  const tracePath = path.join(workdir, 'agents', 'channel-agent', 'trace', `${sessionId}.jsonl`);
  const messagesDir = path.join(workdir, 'messages');
  const messageLines = existsSync(messagesDir)
    ? readdirSync(messagesDir)
      .filter((entry) => entry.endsWith('.jsonl'))
      .flatMap((entry) => recentLines(path.join(messagesDir, entry), 100))
      .slice(-100)
    : [];
  console.error('--- recent agent trace ---');
  console.error(recentLines(tracePath, 100).join('\n'));
  console.error('--- recent channel messages ---');
  console.error(messageLines.join('\n'));
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
  projectKey,
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
    name: 'Smoke Channel',
    workspaceId: 'workspace-smoke',
    daemonId: 'machine-smoke',
    type: 'xhs-creator',
    status: 'active',
    capabilitySet: { cli_binaries: ['xhs', 'coagent-kernel'] },
    members: [{ memberType: 'human', memberId: 'user-smoke', displayName: 'Smoke User' }],
  });
  assert.equal(created.status, 'created');

  const started = await channelManager.startChannel({ channelId });
  assert.equal(started.status, 'active');
  assert.ok(started.agent_pid, 'channel agent pid should exist after start');
  assert.equal(readFileSync(started.session_id_path, 'utf8').trim(), started.session_id);
  assert.deepEqual(started.mounted_cli_binaries, ['xhs', 'coagent-kernel']);
  assert.doesNotMatch(readAgentCmdline(started.agent_pid), /chat-bridge|mcp-config/);

  await rpcServer.start();

  const cliEnv = {
    COAGENT_WORKDIR: started.workdir,
    COAGENT_CHANNEL_ID: channelId,
    COAGENT_DAEMON_SOCKET: socketPath,
    COAGENT_DAEMON_HTTP: `http://127.0.0.1:${httpPort}`,
    COAGENT_DAEMON_TOKEN: daemonToken,
    COAGENT_AGENT_NAME: started.agent_name ?? 'channel-agent',
  };

  const unauthorizedResponse = await curlSocketRpc({
    method: 'channel.info',
    params: { channel_id: channelId },
  }, { token: '' });
  assert.equal(unauthorizedResponse.ok, false);
  assert.equal(unauthorizedResponse.error?.code, 'unauthorized');

  const infoResponse = await curlSocketRpc({
    method: 'channel.info',
    params: { channel_id: channelId },
  });
  assert.equal(infoResponse.ok, true);
  assert.equal(infoResponse.result.channel_id, channelId);

  const kernelInfo = await runCli('coagent-kernel', ['channel-info'], cliEnv, started.workdir);
  assert.equal(kernelInfo.ok, true);
  assert.equal(kernelInfo.data.channel_id ?? kernelInfo.data.id, channelId);

  if (realMode) {
    await channelManager.handleEvent({
      channelId,
      event: {
        type: 'user.message.posted',
        source: 'smoke-real',
        created_at: new Date().toISOString(),
        payload: {
          message: {
            sender_kind: 'human',
            sender_id: 'smoke-user',
            sender_name: 'Smoke User',
            payload_type: 'user.text',
            payload_body: { text: 'Reply with exactly hello by running: coagent reply hello' },
            content: 'Reply with exactly hello by running: coagent reply hello',
          },
        },
      },
    });

    let llmReply;
    try {
      llmReply = await waitFor('real LLM reply', async () => {
        return requestLog.find((entry) => {
          const message = entry.message ?? {};
          return message.type === 'message.append'
            && message.channel_id === channelId
            && /hello/i.test(String(message.content ?? ''));
        });
      }, 30_000);
    } catch (err) {
      printRecentAgentLogs(started.workdir, started.session_id);
      throw err;
    }

    const archived = await channelManager.archiveChannel(channelId);
    assert.equal(archived.status, 'archived');
    const jsonLineEvents = assertJsonLineEvents(['channel.create', 'channel.start', 'message.deliver']);
    console.log(JSON.stringify({
      ok: true,
      mode: 'real',
      channelId,
      sessionId: started.session_id,
      llmReply: llmReply.message.content,
      archivedWorkdir: archived.workdir,
      jsonLineEvents,
    }));
  } else {

  await channelManager.handleEvent({
    channelId,
    event: {
      type: 'user.message.posted',
      source: 'smoke',
      created_at: new Date().toISOString(),
      payload: {
        message: {
          sender_kind: 'human',
          sender_id: 'user-smoke',
          sender_name: 'Smoke User',
          payload_type: 'user.text',
          payload_body: { text: '你好' },
          content: '你好',
        },
      },
    },
  });

  const messageResponse = await runCli('coagent', [
    'reply',
    'smoke message',
  ], cliEnv, started.workdir);
  assert.equal(messageResponse.ok, true);
  assert.equal(messageResponse.data.payloadType, 'agent.text');
  assert.equal(messageResponse.data.senderKind, 'agent');
  assert.equal(requestLog.length, 1);
  assert.equal(requestLog[0].message.type, 'message.append');
  assert.equal(requestLog[0].message.payload_type, 'agent.text');

  const smokeNode = channelManager._requireNode(channelId);
  const schemaColumns = smokeNode.messageStore.prepare('PRAGMA table_info(messages)').all().map((column) => column.name);
  assert.ok(schemaColumns.includes('payload_type'));
  assert.equal(schemaColumns.includes('legacy_json'), false);
  const initialRows = readStoredMessages(smokeNode.messageStore);
  const coreRows = initialRows.filter((message) => (message.payload_type === 'user.text' && message.payload_body?.text === '你好')
    || (message.payload_type === 'agent.text' && message.payload_body?.text === 'smoke message'));
  assert.equal(coreRows.length, 2);
  assert.equal(initialRows.every((message) => /\+08:00$/.test(message.created_at)), true);
  assert.deepEqual(jsonlMessageFiles(started.workdir), []);

  const checkResponse = await runCli('coagent', ['query', '--text', 'smoke', '--limit', '10'], cliEnv, started.workdir);
  assert.equal(checkResponse.ok, true);
  assert.ok(checkResponse.data.messages.some((message) => message.payload_type === 'agent.text'
    && message.payload_body?.text === 'smoke message'));

  const now = new Date();
  const cronExpression = `${now.getMinutes()} ${now.getHours()} ${now.getDate()} ${now.getMonth() + 1} ${now.getDay()}`;
  const scheduleResponse = await runCli('coagent-kernel', [
    'schedule-cron',
    '--cron',
    cronExpression,
    '--reason',
    'smoke-cron',
    '--payload',
    '{"source":"smoke"}',
  ], cliEnv, started.workdir);
  assert.equal(scheduleResponse.ok, true);
  assert.equal(typeof scheduleResponse.data.schedule_id, 'string');
  assert.ok(existsSync(path.join(started.workdir, 'schedules', `${scheduleResponse.data.schedule_id}.yaml`)));

  const listSchedules = await runCli('coagent-kernel', ['list-schedules'], cliEnv, started.workdir);
  assert.equal(listSchedules.ok, true);
  assert.ok(listSchedules.data.schedules.some((schedule) => schedule.id === scheduleResponse.data.schedule_id));

  await channelManager.cronScheduler._poll();
  await delay(50);
  let traceEntries = readTraceEntries(started.workdir, started.session_id);
  assert.ok(
    traceEntries.some((entry) => entry.kind === 'trigger' && entry.decision === 'react' && entry.event?.type === 'cron.tick'),
    'cron.tick should react through trigger gateway',
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

  const contentPath = path.join(started.workdir, 'smoke-note.md');
  writeFileSync(contentPath, '# smoke note\n', 'utf8');
  const xhsResponse = await runCli('xhs', [
    'publish',
    '--title',
    'Smoke Title',
    '--content',
    contentPath,
    '--images',
    '/tmp/a.png,/tmp/b.png',
    '--tags',
    'smoke,cli',
  ], cliEnv, started.workdir);
  assert.equal(xhsResponse.ok, true);
  assert.equal(typeof xhsResponse.data.note_id, 'string');
  assert.deepEqual(sqliteFilesUnder(path.join(started.workdir, 'artifacts')), []);

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
  assert.ok(archived.workdir.includes(`${path.sep}.coagent${path.sep}${projectKey}${path.sep}archived${path.sep}`));

  let archivedLookupFailed = false;
  try {
    await channelManager.getChannelInfo(channelId);
  } catch (err) {
    archivedLookupFailed = err.code === 'not_found';
  }
  assert.equal(archivedLookupFailed, true);

  const jsonLineEvents = assertJsonLineEvents(['channel.create', 'channel.start', 'message.deliver']);
  console.log(JSON.stringify({
    ok: true,
    mode: 'fake',
    workdir: started.workdir,
    archivedWorkdir: archived.workdir,
    sessionId: started.session_id,
    sentMessages: requestLog.length,
    scheduleId: scheduleResponse.data.schedule_id,
    xhsNoteId: xhsResponse.data.note_id,
    daemonStatusEvents: sentLog.filter((message) => message.type === 'channel.status').map((message) => message.status),
    jsonLineEvents,
    traceFiles: readdirSync(path.join(archived.workdir, 'agents', 'channel-agent', 'trace')),
  }));
  }
} finally {
  process.stdout.write = originalStdoutWrite;
  await rpcServer.stop().catch(() => {});
  await channelManager.stopAll().catch(() => {});
  rmSync(tempRoot, { recursive: true, force: true });
  if (realMode) {
    rmSync(path.join(os.homedir(), '.coagent', projectKey), { recursive: true, force: true });
    if (originalHome === undefined) {
      delete process.env.HOME;
    } else {
      process.env.HOME = originalHome;
    }
  }
}
