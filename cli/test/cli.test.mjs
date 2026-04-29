import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { accessSync, constants as fsConstants, mkdtempSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const testDir = path.dirname(fileURLToPath(import.meta.url));
const cliDir = path.resolve(testDir, '..');
const distEntry = path.join(cliDir, 'dist', 'index.js');
const binShim = path.join(cliDir, 'bin', 'xhs');

function runCli(args, env = {}) {
  return spawnSync(process.execPath, [distEntry, ...args], {
    cwd: cliDir,
    encoding: 'utf8',
    env: {
      ...process.env,
      ...env,
    },
  });
}

function parseJson(stdout) {
  assert.notEqual(stdout, '', 'CLI should write a JSON envelope to stdout');
  return JSON.parse(stdout);
}

test('build artifacts exist', () => {
  accessSync(distEntry, fsConstants.F_OK);
  accessSync(binShim, fsConstants.X_OK);
});

test('publish returns a success envelope with note_id and url', () => {
  const tempDir = mkdtempSync(path.join(os.tmpdir(), 'coagent-xhs-cli-'));
  const contentPath = path.join(tempDir, 'note.md');
  writeFileSync(contentPath, '# mock note\n');

  const result = runCli([
    'publish',
    '--title',
    '测试标题',
    '--content',
    contentPath,
    '--images',
    '/tmp/a.jpg,/tmp/b.jpg',
    '--tags',
    '穿搭,日常',
  ]);

  assert.equal(result.status, 0);
  const body = parseJson(result.stdout);
  assert.equal(body.ok, true);
  assert.equal(typeof body.data.note_id, 'string');
  assert.equal(body.data.note_id.length, 26);
  assert.equal(body.data.url, `https://xhs.com/explore/${body.data.note_id}`);
  assert.match(body.data.published_at, /^\d{4}-\d{2}-\d{2}T/);
});

test('search reads keyword fixture and respects limit', () => {
  const result = runCli(['search', '奶茶', '--limit', '1']);

  assert.equal(result.status, 0);
  const body = parseJson(result.stdout);
  assert.equal(body.ok, true);
  assert.equal(body.data.results.length, 1);
  assert.equal(body.data.results[0].note_id, '01HXYZ');
});

test('get-my-recent reads fixture and respects limit', () => {
  const result = runCli(['get-my-recent', '--limit', '2']);

  assert.equal(result.status, 0);
  const body = parseJson(result.stdout);
  assert.equal(body.ok, true);
  assert.equal(body.data.notes.length, 2);
  assert.equal(body.data.notes[0].note_id, '01HXYZ');
});

test('get-note returns fixture payload for existing note', () => {
  const result = runCli(['get-note', '--note-id', '01HXYZ']);

  assert.equal(result.status, 0);
  const body = parseJson(result.stdout);
  assert.equal(body.ok, true);
  assert.equal(body.data.note.title, '四月穿搭随手记');
});

test('get-note returns a standard error envelope for missing notes', () => {
  const result = runCli(['get-note', '--note-id', 'non-existent']);

  assert.equal(result.status, 1);
  const body = parseJson(result.stdout);
  assert.equal(body.ok, false);
  assert.equal(body.error.code, 'note_not_found');
});

test('publish-status always returns published in mock mode', () => {
  const result = runCli(['publish-status', '--note-id', '01HXYZ']);

  assert.equal(result.status, 0);
  const body = parseJson(result.stdout);
  assert.equal(body.ok, true);
  assert.equal(body.data.status, 'published');
  assert.equal(body.data.url, 'https://xhs.com/explore/01HXYZ');
});
