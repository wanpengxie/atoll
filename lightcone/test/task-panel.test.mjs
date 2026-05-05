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
