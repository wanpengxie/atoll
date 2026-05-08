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

// ── T79 (M1.2-FIX-D, P3#11): inline onclick → data-id + delegated listener ──
// codex review note: the dynamic row template used to inline the device id
// into a JS string literal via `escHtml` (HTML-escape, not JS-string-escape).
// Switch to `data-action="revoke" data-id="…"` plus a delegated click
// listener on the table wrapper — `dataset` reads the value raw, no JS
// parsing involved.
test('public/devices.html: revoke button uses data-action + data-id (no inline onclick)', async () => {
  const html = await readFile(HTML_PATH, 'utf8');
  // The exact template line lives inside a backtick string in the script.
  assert.match(
    html,
    /<button\s+class="btn btn-danger"\s+data-action="revoke"\s+data-id="\$\{escHtml\(d\.id\)\}">Revoke<\/button>/,
    'revoke button must use data-action + data-id template',
  );
  // No remnants of the old onclick handler that smuggled d.id into a JS literal.
  assert.equal(
    html.includes("onclick=\"revokeDevice('"),
    false,
    'inline onclick="revokeDevice(\'…\')" must be gone — escHtml is HTML-escape, not JS-string-escape',
  );
});

test('public/devices.html: delegated click listener handles data-action="revoke"', async () => {
  const html = await readFile(HTML_PATH, 'utf8');
  // Listener attached to the table wrapper.
  assert.match(
    html,
    /getElementById\(['"]device-table-wrap['"]\)\.addEventListener\(['"]click['"]/,
    'delegated click listener should be attached to #device-table-wrap',
  );
  // Listener dispatches on data-action="revoke".
  assert.match(
    html,
    /data-action[^"']*['"]revoke['"]/,
    'delegated listener should branch on data-action === "revoke"',
  );
  // Reads id from dataset / getAttribute, then calls revokeDevice(id).
  assert.match(
    html,
    /revokeDevice\(\s*id\s*\)/,
    'delegated listener should call revokeDevice with the id read from the button',
  );
});
