import assert from 'node:assert/strict';
import { mkdirSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { buildCoagentSpawn } from '../src/drivers/coagent.js';

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
});
