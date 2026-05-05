import assert from 'node:assert/strict';
import { mkdirSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { buildAgentInheritedEnv, buildCoagentSpawn } from '../src/drivers/coagent.js';

function createRepoRoot(prefix) {
  return path.join(os.tmpdir(), `${prefix}-${Date.now()}-${Math.random().toString(16).slice(2)}`);
}

test('buildCoagentSpawn requires agent-binary dist output', () => {
  const repoRootDir = createRepoRoot('coagent-driver-missing-dist');
  const sessionIdPath = path.join(repoRootDir, 'tmp', 'session.id');

  assert.throws(
    () => buildCoagentSpawn({
      channelId: 'channel-1',
      workdir: repoRootDir,
      capabilitySet: { cli_binaries: [] },
      sessionIdPath,
      repoRootDir,
    }),
    (err) => {
      assert.equal(err.code, 'missing_build_artifact');
      assert.match(err.message, /@coagent\/agent-binary/);
      assert.match(err.message, /pnpm --filter @coagent\/agent-binary build/);
      return true;
    },
  );
});

test('buildCoagentSpawn resolves dist runtime and mounted cli shims', () => {
  const repoRootDir = createRepoRoot('coagent-driver-dist-ready');
  const agentDistDir = path.join(repoRootDir, 'agent-binary', 'dist');
  const cliBinDir = path.join(repoRootDir, 'cli', 'bin');
  const sessionIdPath = path.join(repoRootDir, 'tmp', 'session.id');

  mkdirSync(agentDistDir, { recursive: true });
  mkdirSync(cliBinDir, { recursive: true });
  writeFileSync(path.join(agentDistDir, 'index.js'), 'export {};\n', 'utf8');
  writeFileSync(path.join(cliBinDir, 'xhs'), '#!/bin/sh\nexit 0\n', 'utf8');

  const spawnConfig = buildCoagentSpawn({
    channelId: 'channel-1',
    workdir: repoRootDir,
    capabilitySet: { cli_binaries: ['xhs', 'missing-bin'] },
    sessionIdPath,
    repoRootDir,
  });

  assert.equal(spawnConfig.command, 'node');
  assert.equal(spawnConfig.args[0], path.join(agentDistDir, 'index.js'));
  assert.equal(spawnConfig.entry, path.join(agentDistDir, 'index.js'));
  assert.deepEqual(spawnConfig.mountedCliBinaries, ['xhs']);
  assert.doesNotMatch(JSON.stringify(spawnConfig), /chat-bridge|mcp_servers|mcpServers/);
});

test('buildCoagentSpawn passes explicit extraEnv without inherited-env filtering', () => {
  const repoRootDir = createRepoRoot('coagent-driver-extra-env');
  const agentDistDir = path.join(repoRootDir, 'agent-binary', 'dist');
  const sessionIdPath = path.join(repoRootDir, 'tmp', 'session.id');

  mkdirSync(agentDistDir, { recursive: true });
  writeFileSync(path.join(agentDistDir, 'index.js'), 'export {};\n', 'utf8');

  const spawnConfig = buildCoagentSpawn({
    channelId: 'channel-extra-env',
    workdir: repoRootDir,
    capabilitySet: { cli_binaries: [] },
    sessionIdPath,
    repoRootDir,
    extraEnv: { CUSTOM_FOO: 'bar' },
  });

  assert.equal(spawnConfig.env.CUSTOM_FOO, 'bar');
});

test('buildCoagentSpawn whitelists agent env and excludes daemon/server secrets', () => {
  const repoRootDir = createRepoRoot('coagent-driver-env-whitelist');
  const agentDistDir = path.join(repoRootDir, 'agent-binary', 'dist');
  const sessionIdPath = path.join(repoRootDir, 'tmp', 'session.id');

  mkdirSync(agentDistDir, { recursive: true });
  writeFileSync(path.join(agentDistDir, 'index.js'), 'export {};\n', 'utf8');

  const previousEnv = { ...process.env };
  Object.assign(process.env, {
    HOME: '/tmp/home',
    USER: 'tester',
    LANG: 'en_US.UTF-8',
    LC_ALL: 'en_US.UTF-8',
    PATH: '/usr/bin',
    TERM: 'xterm-256color',
    ANTHROPIC_API_KEY: 'anthropic-key',
    DEEPSEEK_API_KEY: 'deepseek-key',
    CLAUDE_CODE_MAX_OUTPUT: '20000',
    NODE_OPTIONS: '--no-warnings',
    ADMIN_TOKEN: 'admin-secret',
    DB_PASSWORD: 'db-secret',
    DB_USER: 'db-user',
    DB_HOST: 'db-host',
    DB_NAME: 'db-name',
    MACHINE_API_KEY: 'machine-secret',
    RANDOM_TOKEN: 'random-secret',
  });

  try {
    const inherited = buildAgentInheritedEnv(process.env);
    assert.equal(inherited.ANTHROPIC_API_KEY, 'anthropic-key');
    assert.equal(inherited.DEEPSEEK_API_KEY, 'deepseek-key');
    assert.equal(inherited.LC_ALL, 'en_US.UTF-8');
    assert.equal(inherited.CLAUDE_CODE_MAX_OUTPUT, '20000');
    assert.equal(inherited.NODE_OPTIONS, '--no-warnings');
    assert.equal('ADMIN_TOKEN' in inherited, false);
    assert.equal('DB_PASSWORD' in inherited, false);
    assert.equal('DB_USER' in inherited, false);
    assert.equal('DB_HOST' in inherited, false);
    assert.equal('DB_NAME' in inherited, false);
    assert.equal('MACHINE_API_KEY' in inherited, false);
    assert.equal('RANDOM_TOKEN' in inherited, false);

    const spawnConfig = buildCoagentSpawn({
      channelId: 'channel-env',
      workdir: repoRootDir,
      capabilitySet: { cli_binaries: [] },
      daemonToken: 'explicit-daemon-token',
      sessionIdPath,
      repoRootDir,
    });
    assert.equal(spawnConfig.env.ANTHROPIC_API_KEY, 'anthropic-key');
    assert.equal(spawnConfig.env.COAGENT_DAEMON_TOKEN, 'explicit-daemon-token');
    assert.equal('ADMIN_TOKEN' in spawnConfig.env, false);
    assert.equal('DB_PASSWORD' in spawnConfig.env, false);
    assert.equal('MACHINE_API_KEY' in spawnConfig.env, false);
  } finally {
    process.env = previousEnv;
  }
});
