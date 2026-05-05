import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { WorkdirWatcher } from '../src/workdir-watcher.js';

test('workdir watcher maps channel workdir paths to protocol events', () => {
  const workdir = mkdtempSync(path.join(os.tmpdir(), 'workdir-watcher-'));
  try {
    mkdirSync(path.join(workdir, 'artifacts'), { recursive: true });
    mkdirSync(path.join(workdir, 'notes', 'tasks'), { recursive: true });
    mkdirSync(path.join(workdir, 'agents', 'channel-agent'), { recursive: true });
    writeFileSync(path.join(workdir, 'channel.yaml'), 'channel_id: channel-a\n', 'utf8');
    writeFileSync(path.join(workdir, 'artifacts', 'a.txt'), 'artifact\n', 'utf8');
    writeFileSync(path.join(workdir, 'notes', 'tasks', 'task.md'), '# task\n', 'utf8');
    writeFileSync(path.join(workdir, 'agents', 'channel-agent', 'session.id'), 'session\n', 'utf8');

    const events = [];
    const watcher = new WorkdirWatcher({
      workdir,
      onEvent: (event) => events.push(event),
    });

    assert.equal(watcher.handleChange(path.join(workdir, 'channel.yaml'), 'change').type, 'channel.config.updated');
    assert.equal(watcher.handleChange(path.join(workdir, 'artifacts', 'a.txt'), 'rename').type, 'workdir.changed');
    assert.equal(watcher.handleChange(path.join(workdir, 'notes', 'tasks', 'task.md'), 'change').type, 'workdir.changed');
    assert.equal(watcher.handleChange(path.join(workdir, 'agents', 'channel-agent', 'session.id'), 'change').type, 'workdir.changed');
    assert.equal(watcher.handleChange(path.join(workdir, 'messages', 'ignored.jsonl'), 'change'), null);

    assert.deepEqual(events.map((event) => [event.type, event.payload.path]), [
      ['channel.config.updated', 'channel.yaml'],
      ['workdir.changed', 'artifacts/a.txt'],
      ['workdir.changed', 'notes/tasks/task.md'],
      ['workdir.changed', 'agents/channel-agent/session.id'],
    ]);
  } finally {
    rmSync(workdir, { recursive: true, force: true });
  }
});
