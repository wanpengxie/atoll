import assert from 'node:assert/strict';
import test from 'node:test';

import { missingMachineApiKeyErrorLines, resolveMachineApiKey } from '../src/machine-api-key.js';

test('resolveMachineApiKey prefers CLI flag over env and machine.key', () => {
  const reads = [];
  const result = resolveMachineApiKey({
    cliApiKey: 'cli-key',
    env: { MACHINE_API_KEY: 'env-key' },
    projectKey: 'project-a',
    readMachineKeyFileImpl(projectKey) {
      reads.push(projectKey);
      return 'file-key';
    },
  });

  assert.deepEqual(result, { value: 'cli-key', source: 'cli' });
  assert.deepEqual(reads, []);
});

test('resolveMachineApiKey prefers env over machine.key when CLI flag is absent', () => {
  const reads = [];
  const result = resolveMachineApiKey({
    env: { MACHINE_API_KEY: 'env-key' },
    projectKey: 'project-a',
    readMachineKeyFileImpl(projectKey) {
      reads.push(projectKey);
      return 'file-key';
    },
  });

  assert.deepEqual(result, { value: 'env-key', source: 'env' });
  assert.deepEqual(reads, []);
});

test('resolveMachineApiKey reads machine.key when CLI flag and env are absent', () => {
  const reads = [];
  const result = resolveMachineApiKey({
    env: {},
    projectKey: 'project-a',
    readMachineKeyFileImpl(projectKey) {
      reads.push(projectKey);
      return 'file-key';
    },
  });

  assert.deepEqual(result, { value: 'file-key', source: 'machine.key' });
  assert.deepEqual(reads, ['project-a']);
});

test('resolveMachineApiKey reports missing key with make register hint', () => {
  const result = resolveMachineApiKey({
    env: {},
    projectKey: 'project-a',
    readMachineKeyFileImpl: () => '',
  });
  const errorLines = missingMachineApiKeyErrorLines('/tmp/project-a/machine.key');

  assert.deepEqual(result, { value: '', source: 'missing' });
  assert.match(errorLines.join('\n'), /Error: API key is required\./);
  assert.match(errorLines.join('\n'), /\/tmp\/project-a\/machine\.key/);
  assert.match(errorLines.join('\n'), /make register/);
});
