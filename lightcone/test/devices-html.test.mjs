import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import test from 'node:test';

// Smoke tests for `lightcone/public/devices.html` — the lightcone repo has no
// DOM-test infrastructure (no jsdom dependency, no headless browser harness),
// so we assert on the source file directly. These cover the M1.2-FIX-A
// frontend pieces: closeKeyModal must clear the one-time api-key from DOM,
// and fmtDate must not lean on UTC `toISOString` for display.

const __dirname  = dirname(fileURLToPath(import.meta.url));
const HTML_PATH  = resolve(__dirname, '..', 'public', 'devices.html');

test('public/devices.html: closeKeyModal clears textContent and dataset.key', async () => {
  const html = await readFile(HTML_PATH, 'utf8');
  const start = html.indexOf('function closeKeyModal');
  assert.ok(start > -1, 'closeKeyModal should exist');
  // Capture the function body up to the next top-level function or end-of-script.
  const body = html.slice(start, start + 800);
  assert.match(body, /textContent\s*=\s*['"]{2}/, 'should reset key-box.textContent');
  assert.match(body, /delete\s+\w+\.dataset\.key/, 'should drop dataset.key');
  assert.match(body, /classList\.remove\(['"]open['"]\)/, 'should still hide the modal');
});

test('public/devices.html: fmtDate does not use toISOString', async () => {
  const html = await readFile(HTML_PATH, 'utf8');
  // No allowlist for now — devices.html should be 0-hit for toISOString.
  assert.equal(
    html.includes('toISOString'),
    false,
    'devices.html must not use Date#toISOString for display (UTC drift). ' +
    'Use a local-time formatter instead.',
  );
});

test('public/devices.html: fmtDate uses local Date getters', async () => {
  const html = await readFile(HTML_PATH, 'utf8');
  const start = html.indexOf('function fmtDate');
  assert.ok(start > -1, 'fmtDate should exist');
  const body = html.slice(start, start + 800);
  // Sanity: implementation should rely on local-time getters.
  assert.match(body, /getFullYear\(/);
  assert.match(body, /getHours\(/);
});
