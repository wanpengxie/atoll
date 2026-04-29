import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { accessSync, constants as fsConstants, existsSync, mkdtempSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';

const testDir = path.dirname(fileURLToPath(import.meta.url));
const packageDir = path.resolve(testDir, '..');
const distEntry = path.join(packageDir, 'dist', 'index.js');

function writeExecutable(filePath, content) {
  writeFileSync(filePath, content, { mode: 0o755 });
}

function waitFor(predicate, timeoutMs = 5000) {
  const startedAt = Date.now();

  return (async () => {
    while (Date.now() - startedAt < timeoutMs) {
      const value = predicate();
      if (value) return value;
      await delay(50);
    }
    throw new Error('timed out');
  })();
}

function prepareWorkdir(rootDir) {
  const workdir = path.join(rootDir, 'channel');
  mkdirSync(path.join(workdir, 'messages'), { recursive: true });
  mkdirSync(path.join(workdir, 'artifacts'), { recursive: true });
  mkdirSync(path.join(workdir, 'schedules'), { recursive: true });
  writeFileSync(path.join(workdir, 'channel.yaml'), `${JSON.stringify({
    channel_id: 'channel-agent',
    name: 'Agent Channel',
    type: 'xhs-creator',
    status: 'active',
    capability_set: { cli_binaries: ['xhs', 'coagent-kernel', 'coagent-msg'] },
    members: [],
    created_at: '2026-04-29T12:00:00.000Z',
  }, null, 2)}\n`);
  return workdir;
}

function prepareFakeBin(rootDir) {
  const binDir = path.join(rootDir, 'bin');
  mkdirSync(binDir, { recursive: true });

  const fakeClaude = `#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');
const args = process.argv.slice(2);
const getValue = (flag) => {
  const index = args.indexOf(flag);
  return index >= 0 ? args[index + 1] : '';
};
const mode = args.includes('--resume') ? 'resume' : 'session-id';
const sessionId = getValue('--resume') || getValue('--session-id') || 'unknown';
const logPath = process.env.FAKE_CLAUDE_LOG;
const stdin = fs.readFileSync(0, 'utf8');
const projectSlug = process.cwd().replace(/[/.]/g, '-');
const sessionFile = path.join(process.env.HOME, '.claude', 'projects', projectSlug, sessionId + '.jsonl');
fs.mkdirSync(path.dirname(sessionFile), { recursive: true });
fs.appendFileSync(sessionFile, stdin);
fs.appendFileSync(logPath, JSON.stringify({ mode, sessionId, args, stdin: stdin.trim() }) + '\\n');
process.stdout.write(JSON.stringify({ type: 'system', subtype: 'init', session_id: sessionId }) + '\\n');
process.stdout.write(JSON.stringify({ type: 'result', subtype: 'success', result: 'handled:' + mode, session_id: sessionId }) + '\\n');
`;

  writeExecutable(path.join(binDir, 'claude'), fakeClaude);
  writeExecutable(path.join(binDir, 'coagent-kernel'), '#!/bin/sh\nexit 0\n');
  writeExecutable(path.join(binDir, 'coagent-msg'), '#!/bin/sh\nexit 0\n');
  writeExecutable(path.join(binDir, 'xhs'), '#!/bin/sh\nexit 0\n');

  return binDir;
}

async function runRuntimeTurn(env, eventLine, logPath, tracePath, expectedEntries) {
  const proc = spawn(process.execPath, [distEntry], {
    cwd: env.COAGENT_WORKDIR,
    env,
    stdio: ['pipe', 'pipe', 'pipe'],
  });

  proc.stdin.write(`${eventLine}\n`);

  await waitFor(() => existsSync(logPath) && readFileSync(logPath, 'utf8').trim().split('\n').filter(Boolean).length >= expectedEntries);
  await waitFor(() => {
    if (!existsSync(tracePath)) return false;
    const entries = readFileSync(tracePath, 'utf8')
      .trim()
      .split('\n')
      .filter(Boolean)
      .map((line) => JSON.parse(line));
    const completed = entries.filter((entry) => entry.kind === 'turn.completed');
    return completed.length >= expectedEntries;
  });

  proc.kill('SIGTERM');
  await new Promise((resolve) => proc.once('exit', resolve));
}

test('build artifacts exist for agent runtime', () => {
  accessSync(distEntry, fsConstants.F_OK);
});

test('system prompt exposes required CLI commands without the banned keyword', async () => {
  const { buildSystemPrompt } = await import(path.join(packageDir, 'dist', 'prompt', 'system-prompt.js'));
  const prompt = buildSystemPrompt({
    channelId: 'channel-agent',
    channelName: 'Agent Channel',
    workspaceId: 'workspace-1',
    workdir: '/tmp/channel-agent',
    agentName: 'channel-agent',
    sessionId: '11111111-1111-4111-8111-111111111111',
    sessionIdPath: '/tmp/channel-agent/agents/channel-agent/session.id',
    daemonSocket: '/tmp/daemon.sock',
    daemonHttp: '',
    daemonToken: '',
    capabilitySet: { cli_binaries: ['xhs', 'coagent-kernel', 'coagent-msg'] },
  });

  assert.equal(prompt.includes('coagent-kernel'), true);
  assert.equal(prompt.includes('coagent-msg'), true);
  assert.equal(prompt.includes('xhs publish'), true);
  assert.equal(prompt.toLowerCase().includes('m' + 'cp'), false);
});

test('runtime creates a session on the first turn and resumes it on the second turn', async () => {
  const rootDir = mkdtempSync(path.join(os.tmpdir(), 'coagent-agent-runtime-'));
  const workdir = prepareWorkdir(rootDir);
  const binDir = prepareFakeBin(rootDir);
  const logPath = path.join(rootDir, 'claude-log.jsonl');
  const sessionId = '11111111-1111-4111-8111-111111111111';
  const sessionIdPath = path.join(workdir, 'agents', 'channel-agent', 'session.id');
  const tracePath = path.join(workdir, 'agents', 'channel-agent', 'trace', `${sessionId}.jsonl`);

  const env = {
    ...process.env,
    HOME: rootDir,
    PATH: `${binDir}${path.delimiter}${process.env.PATH}`,
    FAKE_CLAUDE_LOG: logPath,
    COAGENT_CHANNEL_ID: 'channel-agent',
    COAGENT_CHANNEL_NAME: 'Agent Channel',
    COAGENT_WORKSPACE_ID: 'workspace-1',
    COAGENT_WORKDIR: workdir,
    COAGENT_AGENT_NAME: 'channel-agent',
    COAGENT_SESSION_ID: sessionId,
    COAGENT_SESSION_ID_PATH: sessionIdPath,
    COAGENT_DAEMON_SOCKET: path.join(rootDir, 'daemon.sock'),
    COAGENT_CAPABILITY_SET: JSON.stringify({ cli_binaries: ['xhs', 'coagent-kernel', 'coagent-msg'] }),
  };

  const eventLine = JSON.stringify({
    type: 'event',
    event: {
      type: 'user.message.posted',
      source: 'test',
      payload: { message: { content: 'hello' } },
    },
  });

  await runRuntimeTurn(env, eventLine, logPath, tracePath, 1);
  await runRuntimeTurn(env, eventLine, logPath, tracePath, 2);

  const logEntries = readFileSync(logPath, 'utf8')
    .trim()
    .split('\n')
    .filter(Boolean)
    .map((line) => JSON.parse(line));

  assert.equal(logEntries.length, 2);
  assert.equal(logEntries[0].mode, 'session-id');
  assert.equal(logEntries[1].mode, 'resume');
  assert.equal(logEntries[0].sessionId, sessionId);
  assert.equal(logEntries[1].sessionId, sessionId);
  assert.equal(readFileSync(sessionIdPath, 'utf8').trim(), sessionId);

  assert.equal(existsSync(tracePath), true);
  const traceEntries = readFileSync(tracePath, 'utf8')
    .trim()
    .split('\n')
    .filter(Boolean)
    .map((line) => JSON.parse(line));
  const completedModes = traceEntries
    .filter((entry) => entry.kind === 'turn.completed')
    .map((entry) => entry.mode);
  assert.equal(completedModes.includes('session-id'), true);
  assert.equal(completedModes.includes('resume'), true);
});
