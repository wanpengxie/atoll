import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { resolveDistEntry } from '../bin/lib/dist-entry.js';

test('resolveDistEntry throws a build hint when dist output is missing', () => {
  const binDir = mkdtempSync(path.join(os.tmpdir(), 'coagent-bin-shim-'));

  assert.throws(
    () => resolveDistEntry({
      binDir,
      relativeDistPath: '../dist/kernel/index.js',
      packageName: '@coagent/cli',
      buildCommand: 'pnpm --filter @coagent/cli build',
    }),
    (err) => {
      assert.equal(err.code, 'missing_build_artifact');
      assert.match(err.message, /@coagent\/cli/);
      assert.match(err.message, /pnpm --filter @coagent\/cli build/);
      return true;
    },
  );
});
