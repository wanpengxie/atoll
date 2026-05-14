import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';

const rootDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

function loadPanel() {
  const source = readFileSync(path.join(rootDir, 'public', 'tasks-fixture.js'), 'utf8');
  const sandbox = { window: {}, globalThis: {}, console };
  vm.runInNewContext(source, sandbox, { filename: 'tasks-fixture.js' });
  return sandbox.window.LightconeTaskPanel;
}

test('mock task panel renders tree rows, filters, and detail content', () => {
  const panel = loadPanel();
  const html = panel.renderTasksPanelHtml({
    state: { status: 'all', mineOnly: false, selectedTaskId: 'task-script' },
  });

  assert.match(html, /role="tree"/);
  assert.match(html, /data-task-id="task-campaign"/);
  assert.match(html, /data-task-id="task-research"/);
  assert.match(html, /Draft launch note/);
  assert.match(html, /dispatch\.completed/);
  assert.match(html, /Child Tasks/);
});

test('mock task panel supports status and mine filters', () => {
  const panel = loadPanel();
  const completedHtml = panel.renderTasksPanelHtml({
    state: { status: 'completed', mineOnly: false },
  });
  assert.match(completedHtml, /Draft launch note/);
  assert.doesNotMatch(completedHtml, /Review publish checklist/);

  const mineHtml = panel.renderTasksPanelHtml({
    state: { status: 'all', mineOnly: true },
  });
  assert.match(mineHtml, /Q2 creator campaign plan/);
  assert.doesNotMatch(mineHtml, /Collect creator references/);
  assert.equal(panel.activeCount(), 3);
});

test('task panel loads daemon task projection through channel fetch', async () => {
  const panel = loadPanel();
  const calls = [];
  const fetchImpl = async (url, options) => {
    calls.push({ url, options });
    if (url === '/api/channels/channel-real/tasks') {
      return {
        ok: true,
        json: async () => ({
          tasks: [{
            task_id: 'task-real',
            channel_id: 'channel-real',
            parent_task_id: null,
            type: 'note.publish',
            title: 'Real daemon task',
            initiator_kind: 'agent',
            initiator_id: 'alice',
            status: 'opened',
            opened_at: 1778025600000,
            last_event_at: 1778025600000,
            closed_at: null,
            doc_ref: 'notes/tasks/real.md',
            primary_correlation: 'corr-real',
          }],
        }),
      };
    }
    if (url === '/api/channels/channel-real/tasks/task-real') {
      return {
        ok: true,
        json: async () => ({
          task: {
            task_id: 'task-real',
            channel_id: 'channel-real',
            parent_task_id: null,
            type: 'note.publish',
            title: 'Real daemon task',
            initiator_kind: 'agent',
            initiator_id: 'alice',
            status: 'opened',
            opened_at: 1778025600000,
            last_event_at: 1778025600000,
            closed_at: null,
            doc_ref: 'notes/tasks/real.md',
            primary_correlation: 'corr-real',
          },
          doc: { ref: 'notes/tasks/real.md', content: '# Real daemon task\n\n## Brief\nLoaded from daemon.' },
          messages: [{
            id: 'msg-real-open',
            payload_type: 'task.opened',
            content: 'Opened real task',
            ts_received: 1778025600000,
          }],
          children: [],
        }),
      };
    }
    throw new Error(`unexpected url ${url}`);
  };

  const tasks = await panel.loadChannelTasks({ channelId: 'channel-real', fetchImpl, useFixtures: false });
  const html = panel.renderTasksPanelHtml({ tasks });

  assert.deepEqual(calls.map((call) => call.url), [
    '/api/channels/channel-real/tasks',
    '/api/channels/channel-real/tasks/task-real',
  ]);
  assert.equal(calls[0].options.credentials, 'include');
  assert.equal(tasks[0].task_id, 'task-real');
  assert.equal(tasks[0].doc, '# Real daemon task\n\n## Brief\nLoaded from daemon.');
  assert.match(html, /Real daemon task/);
  assert.match(html, /Loaded from daemon/);
  assert.doesNotMatch(html, /Q2 creator campaign plan/);
});
