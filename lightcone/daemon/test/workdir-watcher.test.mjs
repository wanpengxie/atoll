import assert from 'node:assert/strict';
import { appendFileSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { WorkdirWatcher } from '../src/workdir-watcher.js';

function delay(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function waitFor(predicate, timeoutMs = 1_000) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (predicate()) return;
    await delay(10);
  }
  assert.fail('timed out waiting for watcher event');
}

test('workdir watcher maps channel workdir paths to protocol events', () => {
  const workdir = mkdtempSync(path.join(os.tmpdir(), 'workdir-watcher-'));
  try {
    mkdirSync(path.join(workdir, 'artifacts'), { recursive: true });
    mkdirSync(path.join(workdir, 'notes', 'tasks'), { recursive: true });
    mkdirSync(path.join(workdir, 'schedules'), { recursive: true });
    mkdirSync(path.join(workdir, 'agents', 'channel-agent'), { recursive: true });
    mkdirSync(path.join(workdir, 'agents', 'channel-agent', 'trace'), { recursive: true });
    writeFileSync(path.join(workdir, 'channel.yaml'), 'channel_id: channel-a\n', 'utf8');
    writeFileSync(path.join(workdir, 'artifacts', 'a.txt'), 'artifact\n', 'utf8');
    writeFileSync(path.join(workdir, 'notes', 'tasks', 'task.md'), '# task\n', 'utf8');
    writeFileSync(path.join(workdir, 'schedules', 'daily.yaml'), 'id: daily\n', 'utf8');
    writeFileSync(path.join(workdir, 'agents', 'channel-agent', 'session.id'), 'session\n', 'utf8');
    writeFileSync(path.join(workdir, 'agents', 'channel-agent', 'trace', 'session.jsonl'), '{}\n', 'utf8');
    writeFileSync(path.join(workdir, 'agents', 'channel-agent', 'cursor.json'), '{}\n', 'utf8');
    mkdirSync(path.join(workdir, 'agents', 'channel-agent', 'notes'), { recursive: true });
    writeFileSync(path.join(workdir, 'agents', 'channel-agent', 'notes', 'note.md'), '# note\n', 'utf8');

    const events = [];
    const watcher = new WorkdirWatcher({
      workdir,
      onEvent: (event) => events.push(event),
    });

    assert.equal(watcher.handleChange(path.join(workdir, 'channel.yaml'), 'change').type, 'channel.config.updated');
    assert.equal(watcher.handleChange(path.join(workdir, 'artifacts', 'a.txt'), 'rename').type, 'workdir.changed');
    assert.equal(watcher.handleChange(path.join(workdir, 'notes', 'tasks', 'task.md'), 'change').type, 'workdir.changed');
    assert.equal(watcher.handleChange(path.join(workdir, 'schedules', 'daily.yaml'), 'change').type, 'workdir.changed');
    assert.equal(watcher.handleChange(path.join(workdir, 'agents', 'channel-agent', 'notes', 'note.md'), 'change').type, 'workdir.changed');
    assert.equal(watcher.handleChange(path.join(workdir, 'agents', 'channel-agent', 'session.id'), 'change'), null);
    assert.equal(watcher.handleChange(path.join(workdir, 'agents', 'channel-agent', 'trace', 'session.jsonl'), 'change'), null);
    assert.equal(watcher.handleChange(path.join(workdir, 'agents', 'channel-agent', 'cursor.json'), 'change'), null);
    assert.equal(watcher.handleChange(path.join(workdir, 'pending-view-sync', 'message.json'), 'change'), null);
    assert.equal(watcher.handleChange(path.join(workdir, 'messages', 'ignored.jsonl'), 'change'), null);

    assert.deepEqual(events.map((event) => [event.type, event.payload.path]), [
      ['channel.config.updated', 'channel.yaml'],
      ['workdir.changed', 'artifacts/a.txt'],
      ['workdir.changed', 'notes/tasks/task.md'],
      ['workdir.changed', 'schedules/daily.yaml'],
      ['workdir.changed', 'agents/channel-agent/notes/note.md'],
    ]);
  } finally {
    rmSync(workdir, { recursive: true, force: true });
  }
});

test('workdir watcher does not emit events for agent trace writes after start', async () => {
  const workdir = mkdtempSync(path.join(os.tmpdir(), 'workdir-watcher-trace-'));
  const watcherEvents = [];
  let watcher = null;

  try {
    mkdirSync(path.join(workdir, 'artifacts'), { recursive: true });
    mkdirSync(path.join(workdir, 'notes', 'tasks'), { recursive: true });
    mkdirSync(path.join(workdir, 'schedules'), { recursive: true });
    mkdirSync(path.join(workdir, 'pending-view-sync'), { recursive: true });
    mkdirSync(path.join(workdir, 'agents', 'channel-agent', 'trace'), { recursive: true });
    writeFileSync(path.join(workdir, 'channel.yaml'), 'channel_id: channel-a\n', 'utf8');

    const tracePath = path.join(workdir, 'agents', 'channel-agent', 'trace', 'session-a.jsonl');
    writeFileSync(tracePath, '', 'utf8');

    watcher = new WorkdirWatcher({
      workdir,
      debounceMs: 5,
      onEvent: (event) => watcherEvents.push(event),
    });
    watcher.start();

    const watchedPaths = [...watcher.watchers.keys()].map((entry) => path.relative(workdir, entry).split(path.sep).join('/')).sort();
    assert.deepEqual(watchedPaths, ['artifacts', 'channel.yaml', 'notes', 'notes/tasks', 'schedules']);

    for (let i = 0; i < 100; i += 1) {
      appendFileSync(tracePath, `${JSON.stringify({ i })}\n`, 'utf8');
    }
    await delay(100);
    assert.equal(watcherEvents.length, 0);

    writeFileSync(path.join(workdir, 'notes', 'tasks', 'task.md'), '# task\n', 'utf8');
    await waitFor(() => watcherEvents.some((event) => (
      event.type === 'workdir.changed'
      && event.payload.path === 'notes/tasks/task.md'
    )));
  } finally {
    watcher?.stop();
    rmSync(workdir, { recursive: true, force: true });
  }
});
