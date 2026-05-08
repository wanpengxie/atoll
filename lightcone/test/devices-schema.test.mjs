import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import test from 'node:test';

import { DeviceConflictError, insertDevice } from '../src/db/index.js';

// T79 (M1.2-FIX-D, P1#6): static lint-style assertions on the schema & ALTER
// migration — the test sandbox cannot stand up a real MySQL, but we can pin
// the SQL strings that initDb() executes so future refactors don't silently
// drop the active-only UNIQUE.

const __dirname = dirname(fileURLToPath(import.meta.url));
const DB_PATH   = resolve(__dirname, '..', 'src', 'db', 'index.js');

test('devices schema: CREATE TABLE has active_device_id virtual column + uq_devices_active UNIQUE', async () => {
  const src = await readFile(DB_PATH, 'utf8');
  const start = src.indexOf('CREATE TABLE IF NOT EXISTS devices');
  assert.ok(start > -1, 'CREATE TABLE devices block should exist');
  const block = src.slice(start, start + 1200);
  assert.match(
    block,
    /active_device_id\s+VARCHAR\(64\)\s+GENERATED\s+ALWAYS\s+AS/i,
    'devices.active_device_id must be a generated column',
  );
  assert.match(
    block,
    /CASE\s+WHEN\s+status\s*=\s*'active'\s+THEN\s+device_id\s+ELSE\s+NULL\s+END/i,
    'generated column must NULL out non-active rows so MySQL UNIQUE skips them',
  );
  assert.match(
    block,
    /UNIQUE\s+KEY\s+uq_devices_active\s*\(\s*daemon_id\s*,\s*active_device_id\s*\)/i,
    'CREATE TABLE must declare uq_devices_active on (daemon_id, active_device_id)',
  );
});

test('devices schema: idempotent ALTER path adds column + UNIQUE on legacy databases', async () => {
  const src = await readFile(DB_PATH, 'utf8');
  // ADD COLUMN guarded by INFORMATION_SCHEMA.COLUMNS lookup
  assert.match(
    src,
    /COLUMN_NAME\s*=\s*'active_device_id'/,
    'migration must check INFORMATION_SCHEMA.COLUMNS for active_device_id',
  );
  assert.match(
    src,
    /ALTER\s+TABLE\s+devices\s+ADD\s+COLUMN\s+active_device_id/i,
    'migration must ALTER TABLE ADD COLUMN active_device_id',
  );
  // ADD UNIQUE KEY guarded by INFORMATION_SCHEMA.STATISTICS lookup
  assert.match(
    src,
    /INDEX_NAME\s*=\s*'uq_devices_active'/,
    'migration must check INFORMATION_SCHEMA.STATISTICS for uq_devices_active',
  );
  assert.match(
    src,
    /ALTER\s+TABLE\s+devices\s+ADD\s+UNIQUE\s+KEY\s+uq_devices_active/i,
    'migration must ALTER TABLE ADD UNIQUE KEY uq_devices_active',
  );
  // Dedupe step before the ALTER UNIQUE — otherwise existing duplicates
  // would block the migration.
  assert.match(
    src,
    /ROW_NUMBER\(\)\s+OVER\s*\(\s*PARTITION\s+BY\s+daemon_id\s*,\s*device_id/i,
    'migration must dedupe legacy duplicates before adding the UNIQUE',
  );
  assert.match(
    src,
    /SET\s+d\.status\s*=\s*'revoked'/i,
    'dedupe step must mark losing rows revoked (keep newest)',
  );
});

test('insertDevice maps ER_DUP_ENTRY on uq_devices_active to DeviceConflictError', async () => {
  // Fake DB that throws an ER_DUP_ENTRY shaped like mysql2 would on the
  // active-only unique violation.
  const fakeDb = {
    async execute(sql, _params) {
      if (sql.includes('INSERT INTO devices')) {
        const err = new Error("Duplicate entry 'daemon-001-xhs-001' for key 'devices.uq_devices_active'");
        err.code = 'ER_DUP_ENTRY';
        err.errno = 1062;
        throw err;
      }
      return [[]];
    },
  };

  await assert.rejects(
    insertDevice(fakeDb, {
      id: 'd-1',
      device_id: 'xhs-001',
      api_key: 'sk_dev_a',
      user_id: 'user-001',
      channel_id: 'ch-001',
      daemon_id: 'daemon-001',
      device_type: 'xhs',
    }),
    (err) => {
      assert.equal(err instanceof DeviceConflictError, true, 'must be DeviceConflictError');
      assert.equal(err.code, 'DEVICE_CONFLICT');
      assert.equal(err.daemon_id, 'daemon-001');
      assert.equal(err.device_id, 'xhs-001');
      return true;
    },
  );
});

test('insertDevice does NOT swallow ER_DUP_ENTRY on api_key UNIQUE — bubbles as 500', async () => {
  // api_key collision is an internal RNG miss, not a caller-fixable conflict.
  // Make sure we don't mis-map it to DeviceConflictError.
  const fakeDb = {
    async execute(sql, _params) {
      if (sql.includes('INSERT INTO devices')) {
        const err = new Error("Duplicate entry 'sk_dev_xxx' for key 'devices.api_key'");
        err.code = 'ER_DUP_ENTRY';
        err.errno = 1062;
        throw err;
      }
      return [[]];
    },
  };

  await assert.rejects(
    insertDevice(fakeDb, {
      id: 'd-1',
      device_id: 'xhs-001',
      api_key: 'sk_dev_xxx',
      user_id: 'user-001',
      channel_id: 'ch-001',
      daemon_id: 'daemon-001',
      device_type: 'xhs',
    }),
    (err) => {
      // Original ER_DUP_ENTRY bubbles up — caller treats as 500.
      assert.equal(err instanceof DeviceConflictError, false);
      assert.equal(err.code, 'ER_DUP_ENTRY');
      return true;
    },
  );
});
