import mysql from 'mysql2/promise';
import { readdirSync, readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const pool = mysql.createPool({
  host:     process.env.DB_HOST     ?? 'localhost',
  port:     parseInt(process.env.DB_PORT ?? '3306'),
  user:     process.env.DB_USER     ?? 'root',
  password: process.env.DB_PASSWORD ?? '',
  database: process.env.DB_NAME ?? 'lightcone',
  waitForConnections: true,
  connectionLimit: 20,
  charset: 'utf8mb4',
  dateStrings: true,          // return DATETIME as strings, not Date objects
});

export function getDb() { return pool; }


export async function initDb() {
  const db = getDb();
  const sid = process.env.DEFAULT_SERVER_ID ?? 'server-001';
  const uid = process.env.DEFAULT_USER_ID   ?? 'user-001';

  // ── Auth tables ──────────────────────────────────────────────────────────────
  await db.execute(`
    CREATE TABLE IF NOT EXISTS users (
      id          VARCHAR(36)  PRIMARY KEY,
      name        VARCHAR(255) NOT NULL DEFAULT '',
      avatar      VARCHAR(512),
      is_guest    TINYINT      DEFAULT 0,
      deleted_at  DATETIME     DEFAULT NULL,
      merged_into VARCHAR(36)  DEFAULT NULL,
      created_at  DATETIME     DEFAULT NOW()
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
  `);

  await db.execute(`
    CREATE TABLE IF NOT EXISTS user_identities (
      id           VARCHAR(36)  PRIMARY KEY,
      user_id      VARCHAR(36)  NOT NULL,
      provider     VARCHAR(32)  NOT NULL,
      provider_uid VARCHAR(255) NOT NULL,
      credential   VARCHAR(255),
      meta_json    TEXT,
      created_at   DATETIME DEFAULT NOW(),
      UNIQUE KEY uq_provider_uid (provider, provider_uid),
      KEY idx_user_id (user_id)
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
  `);

  await db.execute(`
    CREATE TABLE IF NOT EXISTS sessions (
      token      VARCHAR(64)  PRIMARY KEY,
      user_id    VARCHAR(36)  NOT NULL,
      expires_at DATETIME     NOT NULL,
      created_at DATETIME     DEFAULT NOW(),
      KEY idx_user_id (user_id)
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
  `);

  // ── Core tables ──────────────────────────────────────────────────────────────
  await db.execute(`
    CREATE TABLE IF NOT EXISTS servers (
      id         VARCHAR(36)  PRIMARY KEY,
      name       VARCHAR(255) NOT NULL DEFAULT '',
      slug       VARCHAR(255),
      owner_id   VARCHAR(36),
      plan       VARCHAR(32)  DEFAULT 'free',
      created_at DATETIME     DEFAULT NOW()
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
  `);

  await db.execute(`
    CREATE TABLE IF NOT EXISTS teams (
      id                VARCHAR(36)   PRIMARY KEY,
      server_id         VARCHAR(36)   NOT NULL,
      owner_id          VARCHAR(36)   DEFAULT NULL,
      name              VARCHAR(255)  NOT NULL,
      description       TEXT,
      type              VARCHAR(16)   DEFAULT 'team',
      parent_message_id VARCHAR(36)   DEFAULT NULL,
      deleted_at        DATETIME      DEFAULT NULL,
      created_at        DATETIME      DEFAULT NOW(),
      updated_at        DATETIME      DEFAULT NOW()
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
  `);

  await db.execute(`
    CREATE TABLE IF NOT EXISTS team_members (
      team_id     VARCHAR(36) NOT NULL,
      member_id   VARCHAR(36) NOT NULL,
      member_type VARCHAR(16) NOT NULL,
      created_at  DATETIME    DEFAULT NOW(),
      PRIMARY KEY (team_id, member_id)
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
  `);

  await db.execute(`
    CREATE TABLE IF NOT EXISTS machines (
      id              VARCHAR(36)  PRIMARY KEY,
      server_id       VARCHAR(36)  NOT NULL,
      owner_id        VARCHAR(36),
      name            VARCHAR(255) NOT NULL,
      api_key         VARCHAR(80)  NOT NULL UNIQUE,
      api_key_prefix  VARCHAR(24),
      hostname        VARCHAR(255),
      os              VARCHAR(64),
      runtimes        TEXT,
      models_by_runtime TEXT,
      status          VARCHAR(16)  DEFAULT 'offline',
      daemon_version  VARCHAR(32),
      last_heartbeat  DATETIME,
      is_platform     TINYINT      DEFAULT 0,
      created_at      DATETIME     DEFAULT NOW()
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
  `);

  await db.execute(`
    CREATE TABLE IF NOT EXISTS agents (
      id                        VARCHAR(36)  PRIMARY KEY,
      server_id                 VARCHAR(36)  NOT NULL,
      owner_id                  VARCHAR(36),
      name                      VARCHAR(255) NOT NULL,
      display_name              VARCHAR(255),
      description               TEXT,
      model                     VARCHAR(64)  DEFAULT NULL,
      runtime                   VARCHAR(32)  DEFAULT 'claude',
      reasoning_effort          VARCHAR(16),
      machine_id                VARCHAR(36),
      env_vars                  TEXT,
      status                    VARCHAR(16)  DEFAULT 'inactive',
      session_id                VARCHAR(255),
      activity                  VARCHAR(32)  DEFAULT 'offline',
      activity_detail           TEXT,
      hosted                    TINYINT      DEFAULT 0,
      feishu_app_id             VARCHAR(255) DEFAULT NULL,
      feishu_app_secret         VARCHAR(255) DEFAULT NULL,
      feishu_verification_token VARCHAR(255) DEFAULT NULL,
      feishu_team_id            VARCHAR(255) DEFAULT NULL,
      feishu_bot_name           VARCHAR(255) DEFAULT NULL,
      deleted_at                DATETIME     DEFAULT NULL,
      created_at                DATETIME     DEFAULT NOW(),
      updated_at                DATETIME     DEFAULT NOW()
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
  `);

  await db.execute(`
    CREATE TABLE IF NOT EXISTS messages (
      seq                  BIGINT       AUTO_INCREMENT PRIMARY KEY,
      id                   VARCHAR(36)  NOT NULL UNIQUE,
      team_id              VARCHAR(36)  DEFAULT NULL,
      channel_id           VARCHAR(36)  DEFAULT NULL,
      sender_id            VARCHAR(36)  NOT NULL,
      sender_kind          VARCHAR(16)  NOT NULL,
      payload_type         VARCHAR(128) NOT NULL,
      payload_body         JSON         DEFAULT NULL,
      content              MEDIUMTEXT,
      parent_id            VARCHAR(64)  DEFAULT NULL,
      correlation_id       VARCHAR(64)  DEFAULT NULL,
      task_id              VARCHAR(64)  DEFAULT NULL,
      thread_id            VARCHAR(36),
      audience             JSON         DEFAULT NULL,
      not_before           BIGINT       DEFAULT NULL,
      origin               VARCHAR(16)  DEFAULT NULL,
      expires_at           BIGINT       DEFAULT NULL,
      ts_received          BIGINT       DEFAULT NULL,
      envelope_json        JSON         DEFAULT NULL,
      daemon_request_id    VARCHAR(128) DEFAULT NULL,
      created_at           DATETIME     DEFAULT NOW(),
      updated_at           DATETIME     DEFAULT NOW()
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
  `);

  await db.execute(`
    CREATE TABLE IF NOT EXISTS workspaces (
      id             VARCHAR(36)  PRIMARY KEY,
      name           VARCHAR(255) NOT NULL,
      owner_user_id  VARCHAR(36)  NOT NULL,
      created_at     DATETIME     DEFAULT NOW(),
      archived_at    DATETIME     DEFAULT NULL
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
  `);

  await db.execute(`
    CREATE TABLE IF NOT EXISTS workspace_members (
      workspace_id   VARCHAR(36)  NOT NULL,
      user_id        VARCHAR(36)  NOT NULL,
      role           VARCHAR(32)  NOT NULL DEFAULT 'member',
      joined_at      DATETIME     DEFAULT NOW(),
      PRIMARY KEY (workspace_id, user_id)
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
  `);

  await db.execute(`
    CREATE TABLE IF NOT EXISTS channels (
      id               VARCHAR(36)  PRIMARY KEY,
      workspace_id     VARCHAR(36)  NOT NULL,
      name             VARCHAR(255) NOT NULL,
      type             VARCHAR(64)  NOT NULL,
      capability_set   JSON         NOT NULL,
      channel_agent_id VARCHAR(36)  DEFAULT NULL,
      daemon_id        VARCHAR(36)  DEFAULT NULL,
      status           VARCHAR(32)  NOT NULL DEFAULT 'created',
      created_at       DATETIME     DEFAULT NOW(),
      archived_at      DATETIME     DEFAULT NULL,
      KEY idx_workspace_status (workspace_id, status)
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
  `);

  await db.execute(`
    CREATE TABLE IF NOT EXISTS channel_members (
      channel_id      VARCHAR(36)  NOT NULL,
      member_type     VARCHAR(32)  NOT NULL,
      member_id       VARCHAR(36)  NOT NULL,
      joined_at       DATETIME     DEFAULT NOW(),
      PRIMARY KEY (channel_id, member_type, member_id)
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
  `);

  await db.execute(`
    CREATE TABLE IF NOT EXISTS task_counter (
      team_id        VARCHAR(36) NOT NULL PRIMARY KEY,
      current_number INT         NOT NULL DEFAULT 0
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
  `);

  await db.execute(`
    CREATE TABLE IF NOT EXISTS seq_counter (
      id          INT  PRIMARY KEY,
      current_seq BIGINT NOT NULL DEFAULT 0
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
  `);

  await db.execute(`
    CREATE TABLE IF NOT EXISTS agent_memory (
      agent_id   VARCHAR(36)  NOT NULL,
      team_id    VARCHAR(255) NOT NULL DEFAULT '',
      path       VARCHAR(500) NOT NULL,
      content    MEDIUMTEXT NOT NULL,
      updated_at BIGINT NOT NULL DEFAULT 0,
      PRIMARY KEY (agent_id, team_id, path(200))
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
  `);
  await ensureAgentMemoryScopedByTeam(db);

  // ── Seed default server / team / member ──────────────────────────────────────
  await db.execute(
    `INSERT IGNORE INTO servers (id, name, slug, owner_id, plan) VALUES (?,?,?,?,?)`,
    [sid, 'Demo', 'demo', uid, 'free']
  );

  // ── feishu bindings ───────────────────────────────────────────────────────────
  await db.execute(`
    CREATE TABLE IF NOT EXISTS feishu_team_bindings (
      chat_id    VARCHAR(255) PRIMARY KEY,
      team_id    VARCHAR(36)  NOT NULL,
      agent_id   VARCHAR(36)  NOT NULL,
      created_at DATETIME     DEFAULT NOW()
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
  `);
  await db.execute(`
    CREATE TABLE IF NOT EXISTS feishu_binding_codes (
      code       VARCHAR(10)  PRIMARY KEY,
      team_id    VARCHAR(36)  NOT NULL,
      agent_id   VARCHAR(36)  NOT NULL,
      expires_at DATETIME     NOT NULL,
      created_at DATETIME     DEFAULT NOW()
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
  `);

  // ── agent_team_sessions ───────────────────────────────────────────────────────
  await db.execute(`
    CREATE TABLE IF NOT EXISTS agent_team_sessions (
      agent_id   VARCHAR(36)  NOT NULL,
      team_id    VARCHAR(255) NOT NULL,
      session_id VARCHAR(255) NOT NULL,
      updated_at DATETIME     DEFAULT NOW(),
      PRIMARY KEY (agent_id, team_id)
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
  `);

  // ── fulltext index on messages.content ───────────────────────────────────────
  {
    const [indexes] = await db.execute(
      `SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS
       WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'messages' AND INDEX_NAME = 'ft_content'`
    );
    if (indexes.length === 0) {
      await db.execute(`ALTER TABLE messages ADD FULLTEXT INDEX ft_content (content) WITH PARSER ngram`);
      console.error('[DB] Added fulltext index ft_content on messages');
    }
  }

  // ── messages.deleted_at migration ───────────────────────────────────────────
  {
    const [cols] = await db.execute(
      `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'messages' AND COLUMN_NAME = 'deleted_at'`
    );
    if (cols.length === 0) {
      await db.execute(`ALTER TABLE messages ADD COLUMN deleted_at DATETIME DEFAULT NULL`);
      console.error('[DB] Added deleted_at column to messages');
    }
  }

  // ── messages.channel_id migration + view-cache support ─────────────────────
  {
    const [cols] = await db.execute(
      `SELECT COLUMN_NAME, IS_NULLABLE
       FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'messages' AND COLUMN_NAME IN ('team_id', 'channel_id')`
    );
    const columnMap = new Map(cols.map(row => [row.COLUMN_NAME, row]));
    if (!columnMap.has('channel_id')) {
      await db.execute(`ALTER TABLE messages ADD COLUMN channel_id VARCHAR(36) DEFAULT NULL AFTER team_id`);
      console.error('[DB] Added channel_id column to messages (view cache for channels)');
    }
    if (columnMap.get('team_id')?.IS_NULLABLE !== 'YES') {
      await db.execute(`ALTER TABLE messages MODIFY team_id VARCHAR(36) DEFAULT NULL`);
      console.error('[DB] Altered messages.team_id to allow NULL for channel-only view-cache rows');
    }
    const [indexes] = await db.execute(
      `SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS
       WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'messages' AND INDEX_NAME = 'idx_channel_id'`
    );
    if (indexes.length === 0) {
      await db.execute(`ALTER TABLE messages ADD INDEX idx_channel_id (channel_id, seq)`);
      console.error('[DB] Added idx_channel_id on messages(channel_id, seq)');
    }
  }
  await ensureMessageEnvelopeColumns(db);

  // ── team_members.role_prompt migration ──────────────────────────────────────
  {
    const [cols] = await db.execute(
      `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'team_members' AND COLUMN_NAME = 'role_prompt'`
    );
    if (cols.length === 0) {
      await db.execute(`ALTER TABLE team_members ADD COLUMN role_prompt TEXT DEFAULT NULL`);
      console.error('[DB] Added role_prompt column to team_members');
    }
  }

  // ── machines.deleted_at migration ────────────────────────────────────────────
  {
    const [cols] = await db.execute(
      `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'machines' AND COLUMN_NAME = 'deleted_at'`
    );
    if (cols.length === 0) {
      await db.execute(`ALTER TABLE machines ADD COLUMN deleted_at DATETIME DEFAULT NULL`);
      console.error('[DB] Added deleted_at column to machines');
    }
  }

  // ── agents.agent_api_key migration ──────────────────────────────────────────
  {
    const [cols] = await db.execute(
      `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'agents' AND COLUMN_NAME = 'agent_api_key'`
    );
    if (cols.length === 0) {
      await db.execute(`ALTER TABLE agents ADD COLUMN agent_api_key VARCHAR(80) DEFAULT NULL UNIQUE`);
      console.error('[DB] Added agent_api_key column to agents');
      // Backfill existing agents
      const { randomBytes } = await import('crypto');
      const [existing] = await db.execute(`SELECT id FROM agents WHERE deleted_at IS NULL`);
      for (const row of existing) {
        const key = 'sk_agent_' + randomBytes(32).toString('hex');
        await db.execute(`UPDATE agents SET agent_api_key = ? WHERE id = ?`, [key, row.id]);
      }
      console.error(`[DB] Backfilled agent_api_key for ${existing.length} agents`);
    }
  }

  // ── team_workspace table ──────────────────────────────────────────────────────
  await db.execute(`
    CREATE TABLE IF NOT EXISTS team_workspace (
      team_id    VARCHAR(36)  NOT NULL,
      path       VARCHAR(512) NOT NULL,
      content    MEDIUMTEXT,
      updated_at DATETIME     DEFAULT NOW(),
      PRIMARY KEY (team_id, path(200))
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
  `);

  // ── devices table (T73 / M1.2-T1) ────────────────────────────────────────────
  // T79 (M1.2-FIX-D, P1#6): `(daemon_id, device_id)` is UNIQUE *only* across
  // active rows. We can't write a partial unique index in MySQL directly, so we
  // emulate one with a virtual generated column — `active_device_id` is NULL
  // when the row is not active, and `(daemon_id, NULL)` does not collide with
  // anything in MySQL UNIQUE indexes. Result: at most one active device row
  // per (daemon_id, device_id), revoked rows can pile up freely.
  await db.execute(`
    CREATE TABLE IF NOT EXISTS devices (
      id                VARCHAR(36)  PRIMARY KEY,
      device_id         VARCHAR(64)  NOT NULL,
      api_key           VARCHAR(80)  NOT NULL UNIQUE,
      user_id           VARCHAR(36)  NOT NULL,
      channel_id        VARCHAR(36)  DEFAULT NULL,
      daemon_id         VARCHAR(36)  DEFAULT NULL,
      device_type       VARCHAR(32)  NOT NULL,
      status            VARCHAR(16)  NOT NULL DEFAULT 'active',
      created_at        DATETIME     DEFAULT NOW(),
      revoked_at        DATETIME     DEFAULT NULL,
      active_device_id  VARCHAR(64)  GENERATED ALWAYS AS
        (CASE WHEN status = 'active' THEN device_id ELSE NULL END) VIRTUAL,
      KEY idx_devices_daemon_status (daemon_id, status),
      KEY idx_devices_user (user_id),
      UNIQUE KEY uq_devices_active (daemon_id, active_device_id)
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
  `);

  // ── devices.active_device_id / uq_devices_active migration (T79 / M1.2-FIX-D)
  // For DBs that already have the legacy schema (no generated column, no
  // active-only unique index): add the column + index idempotently. When
  // adding the unique index we first dedupe any pre-existing duplicate active
  // rows by revoking all-but-the-newest within each (daemon_id, device_id)
  // bucket, otherwise the ALTER would fail.
  {
    const [activeCols] = await db.execute(
      `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'devices' AND COLUMN_NAME = 'active_device_id'`
    );
    if (activeCols.length === 0) {
      await db.execute(
        `ALTER TABLE devices ADD COLUMN active_device_id VARCHAR(64)
         GENERATED ALWAYS AS (CASE WHEN status = 'active' THEN device_id ELSE NULL END) VIRTUAL`
      );
      console.error('[DB] Added devices.active_device_id virtual column');
    }
    const [activeIdx] = await db.execute(
      `SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS
       WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'devices' AND INDEX_NAME = 'uq_devices_active'`
    );
    if (activeIdx.length === 0) {
      // Dedupe legacy duplicates: for every (daemon_id, device_id) bucket with
      // >1 active row, keep only the newest (by created_at desc, id desc as
      // tiebreaker) and mark the rest revoked. Without this the ALTER below
      // would fail with ER_DUP_ENTRY on existing data.
      await db.execute(`
        UPDATE devices d
        JOIN (
          SELECT id FROM (
            SELECT id,
                   ROW_NUMBER() OVER (
                     PARTITION BY daemon_id, device_id
                     ORDER BY created_at DESC, id DESC
                   ) AS rn
            FROM devices
            WHERE status = 'active'
          ) ranked
          WHERE ranked.rn > 1
        ) dups ON dups.id = d.id
        SET d.status = 'revoked',
            d.revoked_at = COALESCE(d.revoked_at, NOW())
      `);
      await db.execute(
        `ALTER TABLE devices ADD UNIQUE KEY uq_devices_active (daemon_id, active_device_id)`
      );
      console.error('[DB] Added uq_devices_active UNIQUE on devices(daemon_id, active_device_id)');
    }
  }

  // ── machines.daemon_host / daemon_port / capabilities / daemon_scheme migration
  // T77 (M1.2-FIX-B): daemon_scheme is the explicit public-URL scheme reported
  // by the daemon at register time (`http`/`https`/`ws`/`wss`). resolve uses
  // it to render `ws_url`/`http_url`. NULL keeps the legacy ws/http default
  // for dev clusters that don't sit behind a TLS proxy.
  for (const [col, ddl] of [
    ['daemon_host',   'ALTER TABLE machines ADD COLUMN daemon_host VARCHAR(255) DEFAULT NULL'],
    ['daemon_port',   'ALTER TABLE machines ADD COLUMN daemon_port INT          DEFAULT NULL'],
    ['capabilities',  'ALTER TABLE machines ADD COLUMN capabilities TEXT        DEFAULT NULL'],
    ['daemon_scheme', 'ALTER TABLE machines ADD COLUMN daemon_scheme VARCHAR(16) DEFAULT NULL'],
  ]) {
    const [cols] = await db.execute(
      `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'machines' AND COLUMN_NAME = ?`,
      [col]
    );
    if (cols.length === 0) {
      await db.execute(ddl);
      console.error(`[DB] Added ${col} column to machines`);
    }
  }

  // ── skills tables ─────────────────────────────────────────────────────────────
  await db.execute(`
    CREATE TABLE IF NOT EXISTS skills (
      id                  VARCHAR(36)  PRIMARY KEY,
      owner_id            VARCHAR(36),
      type                VARCHAR(16)  NOT NULL DEFAULT 'user',
      name                VARCHAR(255) NOT NULL,
      description         TEXT         NOT NULL DEFAULT (''),
      content             MEDIUMTEXT   NOT NULL DEFAULT (''),
      tags                VARCHAR(1024) DEFAULT '[]',
      mcp_config          TEXT         DEFAULT NULL,
      created_by_agent_id VARCHAR(36)  DEFAULT NULL,
      created_at          DATETIME     DEFAULT NOW(),
      updated_at          DATETIME     DEFAULT NOW(),
      deleted_at          DATETIME     DEFAULT NULL,
      UNIQUE KEY uq_owner_name (owner_id, name)
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
  `);

  await db.execute(`
    CREATE TABLE IF NOT EXISTS skill_bindings (
      id          VARCHAR(36)  PRIMARY KEY,
      skill_id    VARCHAR(36)  NOT NULL,
      target_type VARCHAR(16)  NOT NULL,
      target_id   VARCHAR(36)  NOT NULL,
      created_at  DATETIME     DEFAULT NOW(),
      UNIQUE KEY uq_binding (skill_id, target_type, target_id),
      KEY idx_target (target_type, target_id)
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
  `);

  await db.execute(`INSERT IGNORE INTO seq_counter (id, current_seq) VALUES (1, 0)`);

  // ── platform_credentials ──────────────────────────────────────────────────────
  await db.execute(`
    CREATE TABLE IF NOT EXISTS platform_credentials (
      id              VARCHAR(36)   PRIMARY KEY,
      server_id       VARCHAR(36)   NOT NULL,
      owner_id        VARCHAR(36)   NOT NULL,
      platform        VARCHAR(32)   NOT NULL,
      display_name    VARCHAR(255)  NOT NULL,
      credential_type VARCHAR(16)   NOT NULL,
      encrypted_data  TEXT          NOT NULL,
      iv              VARCHAR(64)   NOT NULL,
      scopes          VARCHAR(1024) DEFAULT '[]',
      expires_at      DATETIME      DEFAULT NULL,
      deleted_at      DATETIME      DEFAULT NULL,
      created_at      DATETIME      DEFAULT NOW(),
      updated_at      DATETIME      DEFAULT NOW(),
      KEY idx_owner (owner_id),
      KEY idx_server_platform (server_id, platform)
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
  `);

  // ── credential_grants ─────────────────────────────────────────────────────────
  await db.execute(`
    CREATE TABLE IF NOT EXISTS credential_grants (
      id            VARCHAR(36)  PRIMARY KEY,
      credential_id VARCHAR(36)  NOT NULL,
      grantee_type  VARCHAR(16)  NOT NULL,
      grantee_id    VARCHAR(36)  NOT NULL,
      granted_by    VARCHAR(36)  NOT NULL,
      revoked_at    DATETIME     DEFAULT NULL,
      created_at    DATETIME     DEFAULT NOW(),
      UNIQUE KEY uq_grant (credential_id, grantee_type, grantee_id),
      KEY idx_grantee (grantee_type, grantee_id)
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
  `);

  // ── platform_action_log ───────────────────────────────────────────────────────
  await db.execute(`
    CREATE TABLE IF NOT EXISTS platform_action_log (
      id            VARCHAR(36)   PRIMARY KEY,
      credential_id VARCHAR(36)   NOT NULL,
      agent_id      VARCHAR(36)   NOT NULL,
      team_id       VARCHAR(36)   DEFAULT NULL,
      platform      VARCHAR(32)   NOT NULL,
      action_type   VARCHAR(64)   NOT NULL,
      payload       TEXT          DEFAULT NULL,
      result        TEXT          DEFAULT NULL,
      status        VARCHAR(16)   NOT NULL,
      error         TEXT          DEFAULT NULL,
      executed_at   DATETIME      DEFAULT NOW(),
      KEY idx_credential (credential_id),
      KEY idx_agent (agent_id),
      KEY idx_executed (executed_at)
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
  `);

  // ── pending_actions ───────────────────────────────────────────────────────────
  await db.execute(`
    CREATE TABLE IF NOT EXISTS pending_actions (
      id              VARCHAR(36)   PRIMARY KEY,
      agent_id        VARCHAR(36)   NOT NULL,
      team_id         VARCHAR(36)   DEFAULT NULL,
      action_type     VARCHAR(64)   NOT NULL,
      platform        VARCHAR(32)   DEFAULT NULL,
      description     TEXT          NOT NULL,
      payload         MEDIUMTEXT    NOT NULL,
      credential_id   VARCHAR(36)   DEFAULT NULL,
      status          VARCHAR(16)   DEFAULT 'pending',
      idempotency_key VARCHAR(128)  DEFAULT NULL,
      decided_by      VARCHAR(36)   DEFAULT NULL,
      decided_at      DATETIME      DEFAULT NULL,
      executed_at     DATETIME      DEFAULT NULL,
      error           TEXT          DEFAULT NULL,
      created_at      DATETIME      DEFAULT NOW(),
      UNIQUE KEY uq_idempotency (idempotency_key),
      KEY idx_agent_team (agent_id, team_id),
      KEY idx_status (status),
      KEY idx_created (created_at)
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
  `);
  await ensureUtf8mb4UnicodeCollation(db);
  await ensureSoftDeleteColumns(db);

  // Cleanup: soft-delete agents owned by guest users
  {
    const [result] = await db.execute(`
      UPDATE agents
      SET is_del = 1, deleted_at = COALESCE(deleted_at, NOW()), status = 'inactive'
      WHERE owner_id IN (SELECT id FROM users WHERE is_guest = 1)
        AND is_del = 0
    `);
    if (result.affectedRows > 0)
      console.error(`[DB] Deleted ${result.affectedRows} guest-owned agent(s)`);
  }

  // Cleanup: soft-delete teams owned by guest users
  {
    const [guestTeams] = await db.execute(`
      SELECT id FROM teams
      WHERE type = 'team'
        AND owner_id IN (SELECT id FROM users WHERE is_guest = 1)
        AND is_del = 0
    `);
    for (const row of guestTeams) {
      await deleteTeamMemory(db, row.id);
      await deleteTeamSessions(db, row.id);
      await softDeleteRows(db, 'team_members', `team_id = ?`, [row.id]);
      await softDeleteRows(db, 'messages', `team_id = ?`, [row.id]);
      await softDeleteRows(db, 'team_workspace', `team_id = ?`, [row.id]);
      await softDeleteRows(db, 'task_counter', `team_id = ?`, [row.id]);
      await deleteFeishuBinding(db, row.id);
      await softDeleteRows(db, 'feishu_binding_codes', `team_id = ?`, [row.id]);
    }
    const [result] = await db.execute(`
      UPDATE teams
      SET is_del = 1, deleted_at = COALESCE(deleted_at, NOW())
      WHERE type = 'team'
        AND owner_id IN (SELECT id FROM users WHERE is_guest = 1)
        AND is_del = 0
    `);
    if (result.affectedRows > 0)
      console.error(`[DB] Deleted ${result.affectedRows} guest-owned team(s)`);
  }

  // Seed platform skills from src/skills/platform/
  await seedPlatformSkills(db);

  console.error('[DB] MySQL ready');
}

async function ensureSoftDeleteColumns(db) {
  const [tables] = await db.execute(`
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_TYPE = 'BASE TABLE'
  `);
  for (const row of tables) {
    const table = row.TABLE_NAME;
    const [cols] = await db.execute(
      `SELECT COLUMN_NAME
       FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?`,
      [table]
    );
    const names = new Set(cols.map(c => c.COLUMN_NAME));
    if (!names.has('is_del')) {
      await db.execute(`ALTER TABLE \`${table}\` ADD COLUMN is_del TINYINT NOT NULL DEFAULT 0`);
      console.error(`[DB] Added is_del column to ${table}`);
    }
    if (!names.has('deleted_at')) {
      await db.execute(`ALTER TABLE \`${table}\` ADD COLUMN deleted_at DATETIME DEFAULT NULL`);
      console.error(`[DB] Added deleted_at column to ${table}`);
    }
    await db.execute(`UPDATE \`${table}\` SET is_del = 1 WHERE deleted_at IS NOT NULL AND is_del = 0`);
  }
}

async function softDeleteRows(db, table, whereSql, params = []) {
  await db.execute(
    `UPDATE \`${table}\`
     SET is_del = 1, deleted_at = COALESCE(deleted_at, NOW())
     WHERE ${whereSql} AND is_del = 0`,
    params
  );
}

async function ensureUtf8mb4UnicodeCollation(db) {
  const tables = [
    'credential_grants',
    'pending_actions',
    'platform_action_log',
    'platform_credentials',
    'team_workspace',
  ];
  for (const table of tables) {
    const [cols] = await db.execute(
      `SELECT 1
       FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = DATABASE()
         AND TABLE_NAME = ?
         AND CHARACTER_SET_NAME = 'utf8mb4'
         AND COLLATION_NAME <> 'utf8mb4_unicode_ci'
       LIMIT 1`,
      [table]
    );
    if (cols.length === 0) continue;
    await db.execute(`ALTER TABLE \`${table}\` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci`);
    console.error(`[DB] Converted ${table} to utf8mb4_unicode_ci`);
  }
}

async function ensureAgentMemoryScopedByTeam(db) {
  const [stats] = await db.execute(`
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'agent_memory'
      AND INDEX_NAME = 'PRIMARY'
    ORDER BY SEQ_IN_INDEX
  `);
  const primaryColumns = stats.map(r => r.COLUMN_NAME).join(',');
  if (primaryColumns === 'agent_id,team_id,path') return;

  const [dupes] = await db.execute(`
    SELECT agent_id, team_id, path, COUNT(*) AS c
    FROM agent_memory
    GROUP BY agent_id, team_id, path
    HAVING c > 1
    LIMIT 1
  `);
  if (dupes.length > 0) {
    console.warn('[DB] agent_memory primary key migration skipped: duplicate scoped memory rows exist');
    return;
  }

  await db.execute(`
    ALTER TABLE agent_memory
      MODIFY path VARCHAR(500) NOT NULL,
      MODIFY content MEDIUMTEXT NOT NULL,
      MODIFY updated_at BIGINT NOT NULL DEFAULT 0,
      DROP PRIMARY KEY,
      ADD PRIMARY KEY (agent_id, team_id, path(200))
  `);
  console.error('[DB] Migrated agent_memory primary key to include team_id');
}

async function dropColumnIfExists(db, table, column) {
  const [cols] = await db.execute(
    `SELECT COLUMN_NAME
     FROM INFORMATION_SCHEMA.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?`,
    [table, column],
  );
  if (cols.length === 0) return;
  await db.execute(`ALTER TABLE \`${table}\` DROP COLUMN \`${column}\``);
  console.error(`[DB] Dropped ${table}.${column}`);
}

async function ensureMessageEnvelopeColumns(db) {
  const legacyColumns = [
    'sender_type',
    'sender_name',
    'message_type',
    'task_status',
    'task_number',
    'task_assignee_type',
    'task_assignee_id',
    'task_claimed_at',
    'task_completed_at',
    'mentions',
  ];
  for (const column of legacyColumns) {
    await dropColumnIfExists(db, 'messages', column);
  }

  const desired = [
    ['sender_kind', 'VARCHAR(16) NOT NULL'],
    ['payload_type', 'VARCHAR(128) NOT NULL'],
    ['payload_body', 'JSON DEFAULT NULL'],
    ['parent_id', 'VARCHAR(64) DEFAULT NULL'],
    ['correlation_id', 'VARCHAR(64) DEFAULT NULL'],
    ['task_id', 'VARCHAR(64) DEFAULT NULL'],
    ['audience', 'JSON DEFAULT NULL'],
    ['not_before', 'BIGINT DEFAULT NULL'],
    ['origin', 'VARCHAR(16) DEFAULT NULL'],
    ['expires_at', 'BIGINT DEFAULT NULL'],
    ['ts_received', 'BIGINT DEFAULT NULL'],
    ['envelope_json', 'JSON DEFAULT NULL'],
    ['daemon_request_id', 'VARCHAR(128) DEFAULT NULL'],
  ];
  const [cols] = await db.execute(
    `SELECT COLUMN_NAME
     FROM INFORMATION_SCHEMA.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'messages'`
  );
  const existing = new Set(cols.map((row) => row.COLUMN_NAME));
  for (const [name, definition] of desired) {
    if (existing.has(name)) continue;
    await db.execute(`ALTER TABLE messages ADD COLUMN ${name} ${definition}`);
    console.error(`[DB] Added messages.${name} envelope column`);
  }

  const indexes = [
    ['idx_messages_payload_type', 'payload_type, seq'],
    ['idx_messages_correlation', 'correlation_id, seq'],
    ['idx_messages_task', 'task_id, seq'],
    ['idx_messages_not_before', 'not_before, seq'],
    ['idx_messages_daemon_request_id', 'daemon_request_id', 'UNIQUE'],
  ];
  const [existingIndexes] = await db.execute(
    `SELECT INDEX_NAME
     FROM INFORMATION_SCHEMA.STATISTICS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'messages'`
  );
  const indexNames = new Set(existingIndexes.map((row) => row.INDEX_NAME));
  for (const [name, columns, kind] of indexes) {
    if (indexNames.has(name)) continue;
    const indexType = kind === 'UNIQUE' ? 'ADD UNIQUE KEY' : 'ADD INDEX';
    await db.execute(`ALTER TABLE messages ${indexType} ${name} (${columns})`);
    console.error(`[DB] Added ${name} on messages(${columns})`);
  }
}

async function seedPlatformSkills(db) {
  const __dirname = dirname(fileURLToPath(import.meta.url));
  const skillsDir = join(__dirname, '..', 'skills', 'platform');
  let files;
  try { files = readdirSync(skillsDir).filter(f => f.endsWith('.md')); }
  catch { return; } // directory doesn't exist yet

  for (const file of files) {
    const raw = readFileSync(join(skillsDir, file), 'utf-8');
    const fmMatch = raw.match(/^---\n([\s\S]*?)\n---\n([\s\S]*)$/);
    if (!fmMatch) continue;

    const meta = {};
    for (const line of fmMatch[1].split('\n')) {
      const idx = line.indexOf(':');
      if (idx === -1) continue;
      const key = line.slice(0, idx).trim();
      const val = line.slice(idx + 1).trim();
      meta[key] = val;
    }

    const name = meta.name ?? file.replace(/\.md$/, '');
    const description = meta.description ?? '';
    const tags = meta.tags ?? '[]';
    const mcpConfig = meta.mcp_config ?? null;
    const content = fmMatch[2].trim();

    await db.execute(
      `INSERT INTO skills (id, owner_id, type, name, description, content, tags, mcp_config)
       VALUES (?, NULL, 'platform', ?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE description = VALUES(description), content = VALUES(content),
         tags = VALUES(tags), mcp_config = VALUES(mcp_config), updated_at = NOW()`,
      [`platform:${name}`, name, description, content, tags, mcpConfig]
    );
  }
}

// ─── seq ─────────────────────────────────────────────────────────────────────

export async function maxSeq(db) {
  const [rows] = await db.execute(`SELECT MAX(seq) AS s FROM messages`);
  return rows[0].s ?? 0;
}

// ─── messages ─────────────────────────────────────────────────────────────────

function jsonOrNull(value) {
  if (value == null) return null;
  return typeof value === 'string' ? value : JSON.stringify(value);
}

function isDuplicateMessageError(err) {
  return err?.code === 'ER_DUP_ENTRY'
    || err?.errno === 1062
    || String(err?.message ?? '').includes('Duplicate entry');
}

function markDeduped(row) {
  if (row && typeof row === 'object') {
    Object.defineProperty(row, '__deduped', {
      value: true,
      enumerable: false,
      configurable: true,
    });
  }
  return row;
}

async function findExistingMessageForDuplicate(db, msg, daemonRequestId) {
  if (daemonRequestId) {
    const [rows] = await db.execute(
      `SELECT * FROM messages WHERE daemon_request_id = ? AND is_del = 0 LIMIT 1`,
      [daemonRequestId],
    );
    if (rows[0]) return markDeduped(rows[0]);
  }

  if (msg.id) {
    const [rows] = await db.execute(
      `SELECT * FROM messages WHERE id = ? AND is_del = 0 LIMIT 1`,
      [msg.id],
    );
    if (rows[0]) return markDeduped(rows[0]);
  }

  return null;
}

export async function insertMessage(db, msg) {
  const daemonRequestId = msg.daemonRequestId ?? msg.daemon_request_id ?? null;
  const senderKind = msg.senderKind ?? msg.sender_kind ?? msg.envelope?.sender?.kind;
  const senderId = msg.senderId ?? msg.sender_id ?? msg.envelope?.sender?.id;
  const payloadType = msg.payloadType ?? msg.payload_type ?? msg.payload?.type;
  if (!senderKind) throw new Error('sender_kind required');
  if (!senderId) throw new Error('sender_id required');
  if (!payloadType) throw new Error('payload_type required');
  let result;
  try {
    [result] = await db.execute(
      `INSERT INTO messages
        (id, team_id, channel_id, sender_id, sender_kind, payload_type, payload_body,
         content, parent_id, correlation_id, task_id, thread_id, audience, not_before,
         origin, expires_at, ts_received, envelope_json, daemon_request_id)
       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
      [msg.id, msg.teamId ?? null, msg.channelId ?? null, senderId, senderKind, payloadType,
       jsonOrNull(msg.payloadBody ?? msg.payload_body ?? msg.payload?.body),
       msg.content, msg.parentId ?? msg.parent_id ?? null, msg.correlationId ?? msg.correlation_id ?? null,
       msg.taskId ?? msg.task_id ?? null, msg.threadId ?? msg.thread_id ?? null,
       jsonOrNull(msg.audience ?? msg.envelope?.audience),
       msg.notBefore ?? msg.not_before ?? null,
       msg.origin ?? msg.envelope?.origin ?? null, msg.expiresAt ?? msg.expires_at ?? null,
       msg.tsReceived ?? msg.ts_received ?? msg.envelope?.ts_received ?? null,
       jsonOrNull(msg.envelope ?? msg.envelope_json), daemonRequestId]
    );
  } catch (err) {
    if (isDuplicateMessageError(err)) {
      const existing = await findExistingMessageForDuplicate(db, msg, daemonRequestId);
      if (existing) return existing;
    }
    throw err;
  }
  const [rows] = await db.execute(`SELECT * FROM messages WHERE seq = ?`, [result.insertId]);
  return rows[0];
}

export async function getMessages(db, teamId, { limit = 50, before, after } = {}) {
  const n = Math.max(1, Math.min(parseInt(limit) || 50, 500));
  if (after != null) {
    const [rows] = await db.execute(
      `SELECT * FROM messages WHERE team_id = ? AND seq > ? AND is_del = 0 ORDER BY seq ASC LIMIT ${n}`,
      [teamId, after]
    );
    return rows;
  }
  if (before != null) {
    const [rows] = await db.execute(
      `SELECT * FROM messages WHERE team_id = ? AND seq < ? AND is_del = 0 ORDER BY seq DESC LIMIT ${n}`,
      [teamId, before]
    );
    return rows.reverse();
  }
  const [rows] = await db.execute(
    `SELECT * FROM messages WHERE team_id = ? AND is_del = 0 ORDER BY seq DESC LIMIT ${n}`,
    [teamId]
  );
  return rows.reverse();
}

export async function getMessagesSince(db, sinceSeq, teamId) {
  if (teamId) {
    const [rows] = await db.execute(
      `SELECT * FROM messages WHERE seq > ? AND team_id = ? AND is_del = 0 ORDER BY seq ASC`,
      [sinceSeq, teamId]
    );
    return rows;
  }
  const [rows] = await db.execute(
    `SELECT * FROM messages WHERE seq > ? AND is_del = 0 ORDER BY seq ASC`,
    [sinceSeq]
  );
  return rows;
}

export async function getChannelMessages(db, channelId, { limit = 50, before, after } = {}) {
  const n = Math.max(1, Math.min(parseInt(limit) || 50, 500));
  if (after != null) {
    const [rows] = await db.execute(
      `SELECT * FROM messages WHERE channel_id = ? AND seq > ? AND is_del = 0 ORDER BY seq ASC LIMIT ${n}`,
      [channelId, after]
    );
    return rows;
  }
  if (before != null) {
    const [rows] = await db.execute(
      `SELECT * FROM messages WHERE channel_id = ? AND seq < ? AND is_del = 0 ORDER BY seq DESC LIMIT ${n}`,
      [channelId, before]
    );
    return rows.reverse();
  }
  const [rows] = await db.execute(
    `SELECT * FROM messages WHERE channel_id = ? AND is_del = 0 ORDER BY seq DESC LIMIT ${n}`,
    [channelId]
  );
  return rows.reverse();
}

export async function searchMessages(db, agentTeamIds, query, { teamId, limit = 10 } = {}) {
  const n = Math.min(limit, 20);
  if (teamId) {
    if (!agentTeamIds.includes(teamId)) return [];
    const [rows] = await db.execute(
      `SELECT m.*, c.name AS team_name, c.type AS team_type
       FROM messages m JOIN teams c ON c.id = m.team_id
       WHERE m.team_id = ? AND m.is_del = 0 AND MATCH(m.content) AGAINST(? IN BOOLEAN MODE)
       ORDER BY m.seq DESC LIMIT ${n}`,
      [teamId, query]
    );
    return rows;
  }
  const placeholders = agentTeamIds.map(() => '?').join(',');
  const [rows] = await db.execute(
    `SELECT m.*, c.name AS team_name, c.type AS team_type
     FROM messages m JOIN teams c ON c.id = m.team_id
     WHERE m.team_id IN (${placeholders}) AND m.is_del = 0 AND MATCH(m.content) AGAINST(? IN BOOLEAN MODE)
     ORDER BY m.seq DESC LIMIT ${n}`,
    [...agentTeamIds, query]
  );
  return rows;
}

export async function getMessageById(db, id) {
  const [rows] = await db.execute(`SELECT * FROM messages WHERE id = ? AND is_del = 0`, [id]);
  return rows[0] ?? null;
}

export async function updateMessage(db, id, fields) {
  const sets = Object.keys(fields).map(k => `${k} = ?`).join(', ');
  const vals = Object.values(fields);
  await db.execute(`UPDATE messages SET ${sets}, updated_at = NOW() WHERE id = ?`, [...vals, id]);
  const [rows] = await db.execute(`SELECT * FROM messages WHERE id = ?`, [id]);
  return rows[0] ?? null;
}

export async function deleteMessage(db, id) {
  await softDeleteRows(db, 'messages', `id = ?`, [id]);
}

// ─── teams ────────────────────────────────────────────────────────────────────

export async function getTeams(db, serverId) {
  const [rows] = await db.execute(
    `SELECT * FROM teams WHERE server_id = ? AND is_del = 0 AND deleted_at IS NULL ORDER BY created_at ASC`,
    [serverId]
  );
  return rows;
}

export async function getTeamById(db, id) {
  const [rows] = await db.execute(`SELECT * FROM teams WHERE id = ?`, [id]);
  return rows[0] ?? null;
}

export async function getTeamByName(db, serverId, name) {
  const [rows] = await db.execute(
    `SELECT * FROM teams WHERE server_id = ? AND name = ? AND is_del = 0 AND deleted_at IS NULL`,
    [serverId, name]
  );
  return rows[0] ?? null;
}

export async function getThreadByParent(db, serverId, parentMsgIdPrefix) {
  const [rows] = await db.execute(
    `SELECT * FROM teams WHERE server_id = ? AND type = 'thread' AND parent_message_id LIKE ? AND is_del = 0`,
    [serverId, `${parentMsgIdPrefix}%`]
  );
  return rows[0] ?? null;
}

export async function insertTeam(db, ch) {
  await db.execute(
    `INSERT INTO teams (id, server_id, owner_id, name, description, type, parent_message_id) VALUES (?,?,?,?,?,?,?)`,
    [ch.id, ch.serverId, ch.ownerId ?? null, ch.name, ch.description ?? '', ch.type ?? 'team', ch.parentMessageId ?? null]
  );
  const [rows] = await db.execute(`SELECT * FROM teams WHERE id = ?`, [ch.id]);
  return rows[0];
}

export async function getTeamMembers(db, teamId) {
  const [rows] = await db.execute(
    `SELECT * FROM team_members WHERE team_id = ? AND is_del = 0`, [teamId]
  );
  return rows;
}

export async function addTeamMember(db, teamId, memberId, memberType) {
  await db.execute(
    `INSERT INTO team_members (team_id, member_id, member_type)
     VALUES (?,?,?)
     ON DUPLICATE KEY UPDATE member_type = VALUES(member_type), is_del = 0, deleted_at = NULL`,
    [teamId, memberId, memberType]
  );
}

export async function updateTeamMemberRolePrompt(db, teamId, memberId, rolePrompt) {
  await db.execute(
    `UPDATE team_members SET role_prompt = ? WHERE team_id = ? AND member_id = ?`,
    [rolePrompt || null, teamId, memberId]
  );
}

export async function removeTeamMember(db, teamId, memberId) {
  await softDeleteRows(db, 'team_members', `team_id = ? AND member_id = ?`, [teamId, memberId]);
}

export async function getAgentTeams(db, agentId) {
  const [rows] = await db.execute(
    `SELECT c.* FROM teams c
     JOIN team_members cm ON cm.team_id = c.id
     WHERE cm.member_id = ? AND cm.member_type = 'agent'
       AND cm.is_del = 0 AND c.is_del = 0 AND c.deleted_at IS NULL`,
    [agentId]
  );
  return rows;
}

export async function isAgentInTeam(db, agentId, teamId) {
  const [rows] = await db.execute(
    `SELECT 1 FROM team_members WHERE team_id = ? AND member_id = ? AND member_type = 'agent' AND is_del = 0`,
    [teamId, agentId]
  );
  return rows.length > 0;
}

export async function getTeamAgentMembers(db, teamId) {
  const [memberRows] = await db.execute(
    `SELECT cm.member_id FROM team_members cm WHERE cm.team_id = ? AND cm.member_type = 'agent' AND cm.is_del = 0`,
    [teamId]
  );
  if (memberRows.length === 0) return [];
  const ids = memberRows.map(r => r.member_id);
  const placeholders = ids.map(() => '?').join(',');
  const [agentRows] = await db.execute(
    `SELECT * FROM agents WHERE id IN (${placeholders}) AND is_del = 0 AND deleted_at IS NULL AND status = 'active'`,
    ids
  );
  return agentRows;
}

export async function getMemberTeamIds(db, memberId) {
  const [rows] = await db.execute(
    `SELECT team_id FROM team_members WHERE member_id = ? AND member_type = 'agent' AND is_del = 0`,
    [memberId]
  );
  return rows.map(r => r.team_id);
}

// ─── workspaces & channels ────────────────────────────────────────────────────

export async function insertWorkspace(db, workspace) {
  await db.execute(
    `INSERT INTO workspaces (id, name, owner_user_id) VALUES (?,?,?)`,
    [workspace.id, workspace.name, workspace.ownerUserId]
  );
  const [rows] = await db.execute(`SELECT * FROM workspaces WHERE id = ?`, [workspace.id]);
  return rows[0] ?? null;
}

export async function getWorkspaceById(db, id) {
  const [rows] = await db.execute(`SELECT * FROM workspaces WHERE id = ?`, [id]);
  return rows[0] ?? null;
}

export async function getWorkspaceByName(db, name) {
  const [rows] = await db.execute(
    `SELECT * FROM workspaces WHERE name = ? AND is_del = 0 AND deleted_at IS NULL LIMIT 1`,
    [name]
  );
  return rows[0] ?? null;
}

export async function getUserWorkspaces(db, userId, { includeArchived = false } = {}) {
  const [rows] = await db.execute(
    `SELECT w.* FROM workspaces w
     JOIN workspace_members wm ON wm.workspace_id = w.id
     WHERE wm.user_id = ? AND wm.is_del = 0
       AND w.is_del = 0 AND w.deleted_at IS NULL
       ${includeArchived ? '' : 'AND w.archived_at IS NULL'}
     ORDER BY w.created_at ASC`,
    [userId]
  );
  return rows;
}

export async function addWorkspaceMember(db, workspaceId, userId, role = 'member') {
  await db.execute(
    `INSERT INTO workspace_members (workspace_id, user_id, role)
     VALUES (?,?,?)
     ON DUPLICATE KEY UPDATE role = VALUES(role), joined_at = NOW(), is_del = 0, deleted_at = NULL`,
    [workspaceId, userId, role]
  );
}

export async function getWorkspaceMembers(db, workspaceId) {
  const [rows] = await db.execute(
    `SELECT wm.*, u.name AS user_name, u.avatar AS user_avatar
     FROM workspace_members wm
     LEFT JOIN users u ON u.id = wm.user_id
     WHERE wm.workspace_id = ? AND wm.is_del = 0
     ORDER BY wm.joined_at ASC`,
    [workspaceId]
  );
  return rows;
}

export async function isWorkspaceMember(db, workspaceId, userId) {
  const [rows] = await db.execute(
    `SELECT 1 FROM workspace_members
     WHERE workspace_id = ? AND user_id = ? AND is_del = 0`,
    [workspaceId, userId]
  );
  return rows.length > 0;
}

export async function isWorkspaceOwner(db, workspaceId, userId) {
  const [rows] = await db.execute(
    `SELECT 1 FROM workspaces
     WHERE id = ? AND owner_user_id = ? AND is_del = 0 AND deleted_at IS NULL`,
    [workspaceId, userId]
  );
  return rows.length > 0;
}

export async function updateWorkspace(db, id, fields) {
  const allowed = ['name', 'archived_at'];
  const updates = {};
  for (const [key, value] of Object.entries(fields)) {
    if (allowed.includes(key)) updates[key] = value;
  }
  if (Object.keys(updates).length === 0) return getWorkspaceById(db, id);
  const sets = Object.keys(updates).map(key => `${key} = ?`).join(', ');
  await db.execute(`UPDATE workspaces SET ${sets} WHERE id = ?`, [...Object.values(updates), id]);
  return getWorkspaceById(db, id);
}

export async function insertChannel(db, channel) {
  await db.execute(
    `INSERT INTO channels
      (id, workspace_id, name, type, capability_set, channel_agent_id, daemon_id, status)
     VALUES (?,?,?,?,?,?,?,?)`,
    [
      channel.id,
      channel.workspaceId,
      channel.name,
      channel.type,
      JSON.stringify(channel.capabilitySet ?? { cli_binaries: [] }),
      channel.channelAgentId ?? null,
      channel.daemonId ?? null,
      channel.status ?? 'created',
    ]
  );
  const [rows] = await db.execute(`SELECT * FROM channels WHERE id = ?`, [channel.id]);
  return rows[0] ?? null;
}

export async function getChannelById(db, id) {
  const [rows] = await db.execute(`SELECT * FROM channels WHERE id = ?`, [id]);
  return rows[0] ?? null;
}

export async function getWorkspaceChannels(db, workspaceId, { includeArchived = false } = {}) {
  const [rows] = await db.execute(
    `SELECT * FROM channels
     WHERE workspace_id = ? AND is_del = 0 AND deleted_at IS NULL
       ${includeArchived ? '' : "AND archived_at IS NULL AND status IN ('created','active','paused','failed')"}
     ORDER BY created_at ASC`,
    [workspaceId]
  );
  return rows;
}

export async function addChannelMember(db, channelId, memberType, memberId) {
  await db.execute(
    `INSERT INTO channel_members (channel_id, member_type, member_id)
     VALUES (?,?,?)
     ON DUPLICATE KEY UPDATE joined_at = NOW(), is_del = 0, deleted_at = NULL`,
    [channelId, memberType, memberId]
  );
}

export async function getChannelMembers(db, channelId) {
  const [rows] = await db.execute(
    `SELECT cm.*,
            u.name AS human_name,
            u.avatar AS human_avatar,
            a.name AS agent_name,
            a.display_name AS agent_display_name
     FROM channel_members cm
     LEFT JOIN users u
       ON cm.member_type = 'human' AND u.id = cm.member_id
     LEFT JOIN agents a
       ON cm.member_type IN ('channel_agent', 'sub_agent', 'worker') AND a.id = cm.member_id
     WHERE cm.channel_id = ? AND cm.is_del = 0
     ORDER BY cm.joined_at ASC`,
    [channelId]
  );
  return rows;
}

export async function isChannelMember(db, channelId, memberType, memberId) {
  const [rows] = await db.execute(
    `SELECT 1 FROM channel_members
     WHERE channel_id = ? AND member_type = ? AND member_id = ? AND is_del = 0`,
    [channelId, memberType, memberId]
  );
  return rows.length > 0;
}

export async function updateChannel(db, id, fields) {
  const allowed = ['name', 'type', 'capability_set', 'channel_agent_id', 'daemon_id', 'status', 'archived_at'];
  const updates = {};
  for (const [key, value] of Object.entries(fields)) {
    if (!allowed.includes(key)) continue;
    updates[key] = key === 'capability_set' ? JSON.stringify(value) : value;
  }
  if (Object.keys(updates).length === 0) return getChannelById(db, id);
  const sets = Object.keys(updates).map(key => `${key} = ?`).join(', ');
  await db.execute(`UPDATE channels SET ${sets} WHERE id = ?`, [...Object.values(updates), id]);
  return getChannelById(db, id);
}

// ─── agents ───────────────────────────────────────────────────────────────────

export async function getAgents(db, serverId, ownerId = null) {
  if (ownerId) {
    const [rows] = await db.execute(
      `SELECT * FROM agents WHERE server_id = ? AND owner_id = ? AND is_del = 0 ORDER BY created_at ASC`,
      [serverId, ownerId]
    );
    return rows;
  }
  const [rows] = await db.execute(
    `SELECT * FROM agents WHERE server_id = ? AND is_del = 0 ORDER BY created_at ASC`,
    [serverId]
  );
  return rows;
}

export async function getAgentById(db, id) {
  const [rows] = await db.execute(`SELECT * FROM agents WHERE id = ?`, [id]);
  return rows[0] ?? null;
}

export async function getAgentByName(db, serverId, name) {
  const [rows] = await db.execute(
    `SELECT * FROM agents WHERE server_id = ? AND name = ? AND is_del = 0 AND deleted_at IS NULL`,
    [serverId, name]
  );
  return rows[0] ?? null;
}

export async function getDmTeamForAgent(db, serverId, agentId) {
  const [rows] = await db.execute(
    `SELECT c.* FROM teams c
     JOIN team_members cm ON cm.team_id = c.id
     WHERE c.server_id = ? AND c.type = 'dm' AND cm.member_id = ?
       AND c.is_del = 0 AND c.deleted_at IS NULL AND cm.is_del = 0`,
    [serverId, agentId]
  );
  return rows[0] ?? null;
}

export async function insertAgent(db, agent) {
  const { randomBytes } = await import('crypto');
  const agentApiKey = 'sk_agent_' + randomBytes(32).toString('hex');
  await db.execute(
    `INSERT INTO agents
      (id, server_id, owner_id, name, display_name, description, model, runtime,
       reasoning_effort, machine_id, env_vars, hosted, agent_api_key)
     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
    [agent.id, agent.serverId, agent.ownerId ?? null,
     agent.name, agent.displayName, agent.description ?? '',
     agent.model ?? null, agent.runtime ?? 'claude', agent.reasoningEffort ?? null,
     agent.machineId ?? null,
     agent.envVars ? JSON.stringify(agent.envVars) : null,
     agent.hosted ? 1 : 0,
     agentApiKey]
  );
  const [rows] = await db.execute(`SELECT * FROM agents WHERE id = ?`, [agent.id]);
  return rows[0];
}

export async function getAgentByApiKey(db, apiKey) {
  const [rows] = await db.execute(`SELECT * FROM agents WHERE agent_api_key = ? AND is_del = 0 AND deleted_at IS NULL`, [apiKey]);
  return rows[0] ?? null;
}

export async function updateAgent(db, id, fields) {
  const allowed = ['name','display_name','description','status','session_id','model','runtime',
    'reasoning_effort','machine_id','env_vars','activity','activity_detail','is_del','deleted_at','hosted',
    'feishu_app_id','feishu_app_secret','feishu_verification_token','feishu_team_id','feishu_bot_name'];
  const updates = {};
  for (const [k, v] of Object.entries(fields)) {
    if (allowed.includes(k)) updates[k] = v;
  }
  if (Object.keys(updates).length === 0) return getAgentById(db, id);
  const sets = Object.keys(updates).map(k => `${k} = ?`).join(', ');
  await db.execute(`UPDATE agents SET ${sets}, updated_at = NOW() WHERE id = ?`, [...Object.values(updates), id]);
  const [rows] = await db.execute(`SELECT * FROM agents WHERE id = ?`, [id]);
  return rows[0] ?? null;
}

// ─── agent memory ─────────────────────────────────────────────────────────────

export async function listMemoryFiles(db, agentId, teamId = '') {
  const [rows] = await db.execute(
    `SELECT path, updated_at FROM agent_memory WHERE agent_id = ? AND team_id = ? AND is_del = 0 ORDER BY path ASC`,
    [agentId, teamId]
  );
  return rows;
}

export async function getMemoryFile(db, agentId, teamId = '', path) {
  const [rows] = await db.execute(
    `SELECT content FROM agent_memory WHERE agent_id = ? AND team_id = ? AND path = ? AND is_del = 0`,
    [agentId, teamId, path]
  );
  return rows[0] ?? null;
}

export async function upsertMemoryFile(db, agentId, teamId = '', path, content) {
  await db.execute(
    `INSERT INTO agent_memory (agent_id, team_id, path, content, updated_at)
     VALUES (?,?,?,?,UNIX_TIMESTAMP())
     ON DUPLICATE KEY UPDATE team_id = VALUES(team_id), content = VALUES(content),
       updated_at = UNIX_TIMESTAMP(), is_del = 0, deleted_at = NULL`,
    [agentId, teamId, path, content]
  );
}

export async function deleteTeamMemory(db, teamId) {
  await softDeleteRows(db, 'agent_memory', `team_id = ?`, [teamId]);
}

// ─── team workspace ───────────────────────────────────────────────────────────

export async function listTeamWorkspaceFiles(db, teamId) {
  const [rows] = await db.execute(
    `SELECT path, updated_at FROM team_workspace WHERE team_id = ? AND is_del = 0 ORDER BY path ASC`,
    [teamId]
  );
  return rows;
}

export async function getTeamWorkspaceFile(db, teamId, path) {
  const [rows] = await db.execute(
    `SELECT content FROM team_workspace WHERE team_id = ? AND path = ? AND is_del = 0`,
    [teamId, path]
  );
  return rows[0]?.content ?? null;
}

export async function upsertTeamWorkspaceFile(db, teamId, path, content) {
  await db.execute(
    `INSERT INTO team_workspace (team_id, path, content, updated_at)
     VALUES (?, ?, ?, NOW())
     ON DUPLICATE KEY UPDATE content = VALUES(content), updated_at = NOW(), is_del = 0, deleted_at = NULL`,
    [teamId, path, content]
  );
}

export async function deleteTeamWorkspaceFile(db, teamId, path) {
  const [result] = await db.execute(
    `UPDATE team_workspace
     SET is_del = 1, deleted_at = COALESCE(deleted_at, NOW())
     WHERE team_id = ? AND path = ? AND is_del = 0`,
    [teamId, path]
  );
  return result.affectedRows > 0;
}

export async function deleteTeamWorkspace(db, teamId) {
  await softDeleteRows(db, 'team_workspace', `team_id = ?`, [teamId]);
}

// ─── feishu bindings ──────────────────────────────────────────────────────────

export async function getFeishuBinding(db, chatId) {
  const [rows] = await db.execute(
    `SELECT * FROM feishu_team_bindings WHERE chat_id = ? AND is_del = 0`, [chatId]
  );
  if (!rows[0]) return null;
  return rows[0];
}

export async function getFeishuBindingByTeam(db, teamId) {
  const [rows] = await db.execute(
    `SELECT * FROM feishu_team_bindings WHERE team_id = ? AND is_del = 0`, [teamId]
  );
  return rows[0] ?? null;
}

export async function insertFeishuBinding(db, chatId, teamId, agentId) {
  await db.execute(
    `INSERT INTO feishu_team_bindings (chat_id, team_id, agent_id)
     VALUES (?,?,?) ON DUPLICATE KEY UPDATE team_id=VALUES(team_id), agent_id=VALUES(agent_id),
       is_del = 0, deleted_at = NULL`,
    [chatId, teamId, agentId]
  );
}

export async function deleteFeishuBinding(db, teamId) {
  await softDeleteRows(db, 'feishu_team_bindings', `team_id = ?`, [teamId]);
}

export async function createBindingCode(db, teamId, agentId) {
  await db.execute(
    `UPDATE feishu_binding_codes
     SET is_del = 1, deleted_at = COALESCE(deleted_at, NOW())
     WHERE is_del = 0 AND (team_id = ? OR expires_at < NOW())`,
    [teamId]
  );
  const code = Math.random().toString(36).slice(2,8).toUpperCase();
  await db.execute(
    `INSERT INTO feishu_binding_codes (code, team_id, agent_id, expires_at)
     VALUES (?,?,?, DATE_ADD(NOW(), INTERVAL 15 MINUTE))
     ON DUPLICATE KEY UPDATE team_id = VALUES(team_id), agent_id = VALUES(agent_id),
       expires_at = VALUES(expires_at), is_del = 0, deleted_at = NULL`,
    [code, teamId, agentId]
  );
  return code;
}

export async function getBindingCode(db, teamId) {
  const [rows] = await db.execute(
    `SELECT code, expires_at FROM feishu_binding_codes
     WHERE team_id = ? AND expires_at > NOW() AND is_del = 0`, [teamId]
  );
  return rows[0] ?? null;
}

export async function consumeBindingCode(db, code) {
  const [rows] = await db.execute(
    `SELECT * FROM feishu_binding_codes WHERE code = ? AND expires_at > NOW() AND is_del = 0`, [code]
  );
  if (!rows[0]) return null;
  await softDeleteRows(db, 'feishu_binding_codes', `code = ?`, [code]);
  return rows[0];
}

// ─── agent_team_sessions ──────────────────────────────────────────────────────

export async function getTeamSession(db, agentId, teamId) {
  const [rows] = await db.execute(
    `SELECT session_id FROM agent_team_sessions WHERE agent_id = ? AND team_id = ? AND is_del = 0`,
    [agentId, teamId]
  );
  return rows[0]?.session_id ?? null;
}

export async function upsertTeamSession(db, agentId, teamId, sessionId) {
  await db.execute(
    `INSERT INTO agent_team_sessions (agent_id, team_id, session_id, updated_at)
     VALUES (?,?,?,NOW())
     ON DUPLICATE KEY UPDATE session_id = VALUES(session_id), updated_at = NOW(), is_del = 0, deleted_at = NULL`,
    [agentId, teamId, sessionId]
  );
}

export async function deleteAgentTeamSessions(db, agentId) {
  await softDeleteRows(db, 'agent_team_sessions', `agent_id = ?`, [agentId]);
}

export async function deleteTeamSessions(db, teamId) {
  await softDeleteRows(db, 'agent_team_sessions', `team_id = ?`, [teamId]);
}

// ─── skills ───────────────────────────────────────────────────────────────────

export async function insertSkill(db, s) {
  await db.execute(
    `INSERT INTO skills (id, owner_id, type, name, description, content, tags, created_by_agent_id)
     VALUES (?,?,?,?,?,?,?,?)`,
    [s.id, s.ownerId ?? null, s.type ?? 'user', s.name, s.description ?? '',
     s.content ?? '', JSON.stringify(s.tags ?? []), s.createdByAgentId ?? null]
  );
  return getSkillById(db, s.id);
}

export async function updateSkill(db, id, fields) {
  const allowed = ['name', 'description', 'content', 'tags', 'is_del', 'deleted_at'];
  const updates = {};
  for (const [k, v] of Object.entries(fields)) {
    if (allowed.includes(k)) updates[k] = k === 'tags' ? JSON.stringify(v) : v;
  }
  if (Object.keys(updates).length === 0) return getSkillById(db, id);
  const sets = Object.keys(updates).map(k => `${k} = ?`).join(', ');
  await db.execute(`UPDATE skills SET ${sets}, updated_at = NOW() WHERE id = ?`, [...Object.values(updates), id]);
  return getSkillById(db, id);
}

export async function getSkillById(db, id) {
  const [rows] = await db.execute(`SELECT * FROM skills WHERE id = ? AND is_del = 0 AND deleted_at IS NULL`, [id]);
  return rows[0] ?? null;
}

export async function getSkillByName(db, ownerId, name) {
  const [rows] = await db.execute(
    `SELECT * FROM skills WHERE owner_id = ? AND name = ? AND is_del = 0 AND deleted_at IS NULL`,
    [ownerId, name]
  );
  if (rows[0]) return rows[0];
  const [platform] = await db.execute(
    `SELECT * FROM skills WHERE owner_id IS NULL AND name = ? AND is_del = 0 AND deleted_at IS NULL`, [name]
  );
  return platform[0] ?? null;
}

export async function getSkillsByOwner(db, ownerId, { type, search } = {}) {
  let sql = `SELECT * FROM skills WHERE is_del = 0 AND deleted_at IS NULL AND (owner_id = ? OR owner_id IS NULL)`;
  const params = [ownerId];
  if (type) { sql += ` AND type = ?`; params.push(type); }
  if (search) { sql += ` AND (name LIKE ? OR description LIKE ?)`; params.push(`%${search}%`, `%${search}%`); }
  sql += ` ORDER BY type ASC, name ASC`;
  const [rows] = await db.execute(sql, params);
  return rows;
}

export async function getPlatformSkills(db) {
  const [rows] = await db.execute(
    `SELECT * FROM skills WHERE type = 'platform' AND is_del = 0 AND deleted_at IS NULL ORDER BY name ASC`
  );
  return rows;
}

export async function getSkillsForAgent(db, agentId, ownerId) {
  const [rows] = await db.execute(
    `SELECT DISTINCT s.* FROM skills s
     JOIN skill_bindings sb ON sb.skill_id = s.id
     WHERE s.is_del = 0 AND s.deleted_at IS NULL
       AND sb.is_del = 0 AND sb.target_type = 'agent' AND sb.target_id = ?`,
    [agentId]
  );
  return rows;
}

export async function searchSkills(db, ownerId, query) {
  const like = `%${query}%`;
  const [rows] = await db.execute(
    `SELECT * FROM skills WHERE is_del = 0 AND deleted_at IS NULL
     AND (owner_id = ? OR owner_id IS NULL)
     AND (name LIKE ? OR description LIKE ? OR content LIKE ?)
     ORDER BY type ASC, name ASC`,
    [ownerId, like, like, like]
  );
  return rows;
}

export async function insertSkillBinding(db, b) {
  await db.execute(
    `INSERT INTO skill_bindings (id, skill_id, target_type, target_id)
     VALUES (?,?,?,?)
     ON DUPLICATE KEY UPDATE id = VALUES(id), is_del = 0, deleted_at = NULL`,
    [b.id, b.skillId, b.targetType, b.targetId]
  );
}

export async function deleteSkillBinding(db, skillId, targetType, targetId) {
  await softDeleteRows(db, 'skill_bindings', `skill_id = ? AND target_type = ? AND target_id = ?`, [skillId, targetType, targetId]);
}

export async function getSkillBindings(db, skillId) {
  const [rows] = await db.execute(
    `SELECT * FROM skill_bindings WHERE skill_id = ? AND is_del = 0 ORDER BY created_at ASC`, [skillId]
  );
  return rows;
}

// ─── machines ─────────────────────────────────────────────────────────────────

export async function getMachines(db, serverId, ownerId = null) {
  if (ownerId) {
    const [rows] = await db.execute(
      `SELECT * FROM machines WHERE server_id = ? AND owner_id = ? AND is_del = 0 AND deleted_at IS NULL ORDER BY created_at ASC`,
      [serverId, ownerId]
    );
    return rows;
  }
  const [rows] = await db.execute(
    `SELECT * FROM machines WHERE server_id = ? AND is_del = 0 AND deleted_at IS NULL ORDER BY created_at ASC`,
    [serverId]
  );
  return rows;
}

export async function getMachineById(db, id) {
  const [rows] = await db.execute(`SELECT * FROM machines WHERE id = ? AND is_del = 0 AND deleted_at IS NULL`, [id]);
  return rows[0] ?? null;
}

export async function getMachineByApiKey(db, apiKey) {
  const [rows] = await db.execute(`SELECT * FROM machines WHERE api_key = ? AND is_del = 0 AND deleted_at IS NULL`, [apiKey]);
  return rows[0] ?? null;
}

export async function insertMachine(db, m) {
  await db.execute(
    `INSERT INTO machines (id, server_id, owner_id, name, api_key, api_key_prefix, is_platform)
     VALUES (?,?,?,?,?,?,?)`,
    [m.id, m.serverId, m.ownerId ?? null, m.name, m.apiKey, m.apiKeyPrefix, m.isPlatform ? 1 : 0]
  );
  const [rows] = await db.execute(`SELECT * FROM machines WHERE id = ?`, [m.id]);
  return rows[0];
}

export async function updateMachine(db, id, fields) {
  const allowed = ['name','api_key','api_key_prefix','hostname','os','runtimes','models_by_runtime',
    'status','daemon_version','last_heartbeat'];
  const updates = {};
  for (const [k, v] of Object.entries(fields)) {
    if (allowed.includes(k)) updates[k] = v;
  }
  if (Object.keys(updates).length === 0) return getMachineById(db, id);
  const sets = Object.keys(updates).map(k => `${k} = ?`).join(', ');
  await db.execute(`UPDATE machines SET ${sets} WHERE id = ?`, [...Object.values(updates), id]);
  const [rows] = await db.execute(`SELECT * FROM machines WHERE id = ?`, [id]);
  return rows[0] ?? null;
}

export async function deleteMachine(db, id) {
  await db.execute(`UPDATE machines SET is_del = 1, deleted_at = COALESCE(deleted_at, NOW()), status = 'offline' WHERE id = ?`, [id]);
}

// ─── users & auth ─────────────────────────────────────────────────────────────

export async function getUserById(db, userId) {
  const [rows] = await db.execute(
    `SELECT * FROM users WHERE id = ? AND is_del = 0 AND deleted_at IS NULL`,
    [userId]
  );
  return rows[0] ?? null;
}

export async function findUserByIdOrName(db, identifier) {
  const [rows] = await db.execute(
    `SELECT * FROM users
     WHERE (id = ? OR name = ?)
       AND is_del = 0 AND deleted_at IS NULL
     ORDER BY created_at ASC
     LIMIT 1`,
    [identifier, identifier]
  );
  return rows[0] ?? null;
}

export async function getUsers(db, { query = '', limit = 20 } = {}) {
  const n = Math.max(1, Math.min(parseInt(limit) || 20, 100));
  const trimmed = query.trim();
  if (trimmed) {
    const [rows] = await db.execute(
      `SELECT * FROM users
       WHERE is_del = 0 AND deleted_at IS NULL
         AND (id = ? OR name LIKE ?)
       ORDER BY created_at ASC
       LIMIT ${n}`,
      [trimmed, `%${trimmed}%`]
    );
    return rows;
  }
  const [rows] = await db.execute(
    `SELECT * FROM users
     WHERE is_del = 0 AND deleted_at IS NULL
     ORDER BY created_at ASC
     LIMIT ${n}`
  );
  return rows;
}

export async function findUserByIdentity(db, provider, providerUid) {
  const [rows] = await db.execute(
    `SELECT u.* FROM users u
     JOIN user_identities ui ON ui.user_id = u.id
     WHERE ui.provider = ? AND ui.provider_uid = ? AND ui.is_del = 0 AND u.is_del = 0`,
    [provider, providerUid]
  );
  return rows[0] ?? null;
}

export async function createUserWithIdentity(db, { name, avatar, isGuest }, { provider, providerUid, meta }) {
  const { v4: uuidv4 } = await import('uuid');
  const userId = uuidv4();
  const identityId = uuidv4();
  const sid = process.env.DEFAULT_SERVER_ID ?? 'server-001';
  await db.execute(
    `INSERT INTO users (id, name, avatar, is_guest) VALUES (?,?,?,?)`,
    [userId, name ?? '', avatar ?? null, isGuest ? 1 : 0]
  );
  await db.execute(
    `INSERT INTO user_identities (id, user_id, provider, provider_uid, meta_json) VALUES (?,?,?,?,?)`,
    [identityId, userId, provider, providerUid, meta ? JSON.stringify(meta) : null]
  );
  // Create default team for new (non-guest) user
  if (!isGuest) {
    const teamId = uuidv4();
    await db.execute(
      `INSERT INTO teams (id, server_id, owner_id, name, description, type) VALUES (?,?,?,?,?,?)`,
      [teamId, sid, userId, 'default', '', 'team']
    );
    await db.execute(
      `INSERT INTO team_members (team_id, member_id, member_type) VALUES (?,?,?)`,
      [teamId, userId, 'user']
    );
  }
  const [rows] = await db.execute(`SELECT * FROM users WHERE id = ?`, [userId]);
  return rows[0];
}

export async function convertGuestToUser(db, userId) {
  await db.execute(`UPDATE users SET is_guest = 0 WHERE id = ?`, [userId]);
  await softDeleteRows(db, 'user_identities', `user_id = ? AND provider = 'guest'`, [userId]);
}

export async function updateIdentityMeta(db, provider, providerUid, meta) {
  await db.execute(
    `UPDATE user_identities SET meta_json = ?, provider_uid = ?
     WHERE provider = ? AND provider_uid = ?`,
    [JSON.stringify(meta), meta.union_id ?? providerUid, provider, providerUid]
  );
}

export async function createSession(db, userId, ttlMs = 30 * 24 * 60 * 60 * 1000) {
  const { randomBytes } = await import('crypto');
  const token = randomBytes(32).toString('hex');
  const expiresAt = new Date(Date.now() + ttlMs);
  await db.execute(
    `INSERT INTO sessions (token, user_id, expires_at) VALUES (?,?,?)`,
    [token, userId, expiresAt]
  );
  return token;
}

export async function getSessionUser(db, token) {
  const [rows] = await db.execute(
    `SELECT u.* FROM users u
     JOIN sessions s ON s.user_id = u.id
     WHERE s.token = ? AND s.expires_at > NOW()
       AND s.is_del = 0 AND u.is_del = 0 AND u.deleted_at IS NULL`,
    [token]
  );
  return rows[0] ?? null;
}

export async function deleteSession(db, token) {
  await softDeleteRows(db, 'sessions', `token = ?`, [token]);
}

// ─── account linking & merging ────────────────────────────────────────────────

export async function getUserIdentities(db, userId) {
  const [rows] = await db.execute(
    `SELECT id, user_id, provider, provider_uid, meta_json, created_at
     FROM user_identities WHERE user_id = ? AND is_del = 0 ORDER BY created_at ASC`,
    [userId]
  );
  return rows;
}

export async function addIdentityToUser(db, userId, { provider, providerUid, credential, meta }) {
  const { v4: uuidv4 } = await import('uuid');
  const id = uuidv4();
  await db.execute(
    `INSERT INTO user_identities (id, user_id, provider, provider_uid, credential, meta_json)
     VALUES (?,?,?,?,?,?)
     ON DUPLICATE KEY UPDATE
       user_id = IF(is_del = 1, VALUES(user_id), user_id),
       credential = IF(is_del = 1, VALUES(credential), credential),
       meta_json = IF(is_del = 1, VALUES(meta_json), meta_json),
       is_del = IF(is_del = 1, 0, is_del),
       deleted_at = IF(is_del = 0, NULL, deleted_at)`,
    [id, userId, provider, providerUid, credential ?? null, meta ? JSON.stringify(meta) : null]
  );
  const [rows] = await db.execute(
    `SELECT id, user_id, provider, provider_uid
     FROM user_identities
     WHERE provider = ? AND provider_uid = ? AND user_id = ? AND is_del = 0`,
    [provider, providerUid, userId]
  );
  return rows[0] ?? { id, user_id: userId, provider, provider_uid: providerUid };
}

export async function removeIdentity(db, identityId, userId) {
  const [countRows] = await db.execute(
    `SELECT COUNT(*) as cnt FROM user_identities WHERE user_id = ? AND is_del = 0`, [userId]
  );
  if (countRows[0].cnt <= 1) {
    return { removed: false, reason: 'last_identity' };
  }
  await softDeleteRows(db, 'user_identities', `id = ? AND user_id = ?`, [identityId, userId]);
  return { removed: true };
}

export async function mergeUsers(db, keepUserId, removeUserId) {
  if (keepUserId === removeUserId) throw new Error('Cannot merge user into itself');

  const conn = await db.getConnection();
  try {
    await conn.beginTransaction();

    const [agentResult] = await conn.execute(
      `UPDATE agents SET owner_id = ? WHERE owner_id = ?`, [keepUserId, removeUserId]
    );
    const [machineResult] = await conn.execute(
      `UPDATE machines SET owner_id = ? WHERE owner_id = ?`, [keepUserId, removeUserId]
    );
    const [memberRows] = await conn.execute(
      `SELECT team_id, member_type FROM team_members WHERE member_id = ? AND is_del = 0`, [removeUserId]
    );
    for (const row of memberRows) {
      await conn.execute(
        `INSERT INTO team_members (team_id, member_id, member_type)
         VALUES (?,?,?)
         ON DUPLICATE KEY UPDATE member_type = VALUES(member_type), is_del = 0, deleted_at = NULL`,
        [row.team_id, keepUserId, row.member_type]
      );
    }
    await conn.execute(
      `UPDATE team_members SET is_del = 1, deleted_at = COALESCE(deleted_at, NOW())
       WHERE member_id = ? AND is_del = 0`,
      [removeUserId]
    );

    const [msgResult] = await conn.execute(
      `UPDATE messages SET sender_id = ? WHERE sender_id = ? AND sender_kind = 'human'`,
      [keepUserId, removeUserId]
    );
    const [idResult] = await conn.execute(
      `UPDATE user_identities SET user_id = ? WHERE user_id = ?`, [keepUserId, removeUserId]
    );
    await conn.execute(
      `UPDATE sessions SET is_del = 1, deleted_at = COALESCE(deleted_at, NOW())
       WHERE user_id = ? AND is_del = 0`,
      [removeUserId]
    );

    await conn.execute(
      `UPDATE users SET is_del = 1, deleted_at = COALESCE(deleted_at, NOW()), merged_into = ? WHERE id = ?`,
      [keepUserId, removeUserId]
    );
    await conn.execute(
      `UPDATE users SET merged_into = ? WHERE merged_into = ?`,
      [keepUserId, removeUserId]
    );

    await conn.commit();
    return {
      merged: true,
      transferred: {
        agents: agentResult.affectedRows,
        machines: machineResult.affectedRows,
        teams: memberRows.length,
        messages: msgResult.affectedRows,
        identities: idResult.affectedRows,
      },
    };
  } catch (err) {
    await conn.rollback();
    throw err;
  } finally {
    conn.release();
  }
}

export async function resolveUser(db, userId) {
  const [rows] = await db.execute(`SELECT * FROM users WHERE id = ?`, [userId]);
  const user = rows[0];
  if (!user) return null;
  if (user.merged_into) {
    const [canonical] = await db.execute(`SELECT * FROM users WHERE id = ?`, [user.merged_into]);
    return canonical[0] ?? user;
  }
  return user;
}

// ─── platform_credentials ─────────────────────────────────────────────────────

export async function insertCredential(db, cred) {
  await db.execute(
    `INSERT INTO platform_credentials (id, server_id, owner_id, platform, display_name, credential_type, encrypted_data, iv, scopes, expires_at)
     VALUES (?,?,?,?,?,?,?,?,?,?)`,
    [cred.id, cred.serverId, cred.ownerId, cred.platform, cred.displayName,
     cred.credentialType, cred.encryptedData, cred.iv,
     JSON.stringify(cred.scopes ?? []), cred.expiresAt ?? null]
  );
  const [rows] = await db.execute(`SELECT * FROM platform_credentials WHERE id = ?`, [cred.id]);
  return rows[0];
}

export async function getCredentialsByOwner(db, ownerId) {
  const [rows] = await db.execute(
    `SELECT * FROM platform_credentials WHERE owner_id = ? AND is_del = 0 AND deleted_at IS NULL ORDER BY created_at DESC`,
    [ownerId]
  );
  return rows;
}

export async function getCredentialById(db, id) {
  const [rows] = await db.execute(
    `SELECT * FROM platform_credentials WHERE id = ? AND is_del = 0 AND deleted_at IS NULL`, [id]
  );
  return rows[0] ?? null;
}

export async function deleteCredential(db, id) {
  await db.execute(
    `UPDATE platform_credentials SET is_del = 1, deleted_at = COALESCE(deleted_at, NOW()) WHERE id = ?`, [id]
  );
}

// ─── credential_grants ────────────────────────────────────────────────────────

export async function insertCredentialGrant(db, grant) {
  await db.execute(
    `INSERT INTO credential_grants (id, credential_id, grantee_type, grantee_id, granted_by)
     VALUES (?,?,?,?,?)
     ON DUPLICATE KEY UPDATE revoked_at = NULL, granted_by = VALUES(granted_by),
       is_del = 0, deleted_at = NULL`,
    [grant.id, grant.credentialId, grant.granteeType, grant.granteeId, grant.grantedBy]
  );
}

export async function revokeCredentialGrant(db, credentialId, granteeType, granteeId) {
  await db.execute(
    `UPDATE credential_grants SET revoked_at = NOW()
     WHERE credential_id = ? AND grantee_type = ? AND grantee_id = ? AND revoked_at IS NULL`,
    [credentialId, granteeType, granteeId]
  );
}

export async function getGrantsByCredential(db, credentialId) {
  const [rows] = await db.execute(
    `SELECT * FROM credential_grants WHERE credential_id = ? AND revoked_at IS NULL AND is_del = 0`,
    [credentialId]
  );
  return rows;
}

// 查询 agent 被授权的所有有效凭证（含解密所需字段）
export async function getCredentialGrantsForAgent(db, agentId) {
  const [rows] = await db.execute(
    `SELECT pc.id, pc.platform, pc.credential_type, pc.encrypted_data, pc.iv, pc.scopes, pc.expires_at
     FROM credential_grants cg
     JOIN platform_credentials pc ON pc.id = cg.credential_id
     WHERE cg.grantee_type = 'agent' AND cg.grantee_id = ?
       AND cg.revoked_at IS NULL AND cg.is_del = 0
       AND pc.is_del = 0 AND pc.deleted_at IS NULL
     ORDER BY cg.created_at DESC`,
    [agentId]
  );
  return rows;
}

// ─── pending_actions ──────────────────────────────────────────────────────────

export async function insertPendingAction(db, action) {
  await db.execute(
    `INSERT INTO pending_actions (id, agent_id, team_id, action_type, platform, description, payload, credential_id, idempotency_key)
     VALUES (?,?,?,?,?,?,?,?,?)`,
    [action.id, action.agentId, action.teamId ?? null, action.actionType,
     action.platform ?? null, action.description, action.payload,
     action.credentialId ?? null, action.idempotencyKey ?? null]
  );
  const [rows] = await db.execute(`SELECT * FROM pending_actions WHERE id = ?`, [action.id]);
  return rows[0];
}

export async function getPendingActionById(db, id) {
  const [rows] = await db.execute(`SELECT * FROM pending_actions WHERE id = ? AND is_del = 0`, [id]);
  return rows[0] ?? null;
}

export async function updatePendingAction(db, id, fields) {
  const sets = Object.keys(fields).map(k => `${k} = ?`).join(', ');
  const vals = Object.values(fields);
  await db.execute(`UPDATE pending_actions SET ${sets} WHERE id = ?`, [...vals, id]);
}

export async function getPendingActionsByTeam(db, teamId, { status } = {}) {
  const where = status ? `AND status = ?` : `AND status = 'pending'`;
  const params = status ? [teamId, status] : [teamId];
  const [rows] = await db.execute(
    `SELECT * FROM pending_actions WHERE team_id = ? AND is_del = 0 ${where} ORDER BY created_at DESC`,
    params
  );
  return rows;
}

// ─── platform_action_log ──────────────────────────────────────────────────────

export async function insertActionLog(db, log) {
  await db.execute(
    `INSERT INTO platform_action_log (id, credential_id, agent_id, team_id, platform, action_type, payload, result, status, error)
     VALUES (?,?,?,?,?,?,?,?,?,?)`,
    [log.id, log.credentialId, log.agentId, log.teamId ?? null, log.platform,
     log.actionType, log.payload ?? null, log.result ?? null, log.status, log.error ?? null]
  );
}

// ─── quota / plan ─────────────────────────────────────────────────────────────

// ─── devices (T73 / M1.2-T1) ─────────────────────────────────────────────────

function rowToDevice(row) {
  if (!row) return null;
  return {
    id: row.id,
    device_id: row.device_id,
    api_key: row.api_key,
    user_id: row.user_id,
    channel_id: row.channel_id ?? null,
    daemon_id: row.daemon_id ?? null,
    device_type: row.device_type,
    status: row.status,
    created_at: row.created_at,
    revoked_at: row.revoked_at ?? null,
  };
}

/**
 * Thrown by `insertDevice` when the (daemon_id, device_id) active-only unique
 * index rejects the row — i.e. an active row with the same daemon_id + device_id
 * already exists. The route layer catches this and turns it into a 409.
 *
 * T79 (M1.2-FIX-D, P1#6): without this the second POST would silently insert a
 * second active row, daemon's in-memory DeviceStore would overwrite by
 * device_id, and the older extension key would 401 on next reconnect.
 */
export class DeviceConflictError extends Error {
  constructor(message, { daemon_id, device_id } = {}) {
    super(message);
    this.name = 'DeviceConflictError';
    this.code = 'DEVICE_CONFLICT';
    this.daemon_id = daemon_id ?? null;
    this.device_id = device_id ?? null;
  }
}

function isDuplicateEntryError(err) {
  return err?.code === 'ER_DUP_ENTRY'
    || err?.errno === 1062
    || String(err?.message ?? '').includes('Duplicate entry');
}

export async function insertDevice(db, device) {
  try {
    await db.execute(
      `INSERT INTO devices
         (id, device_id, api_key, user_id, channel_id, daemon_id, device_type, status)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        device.id,
        device.device_id,
        device.api_key,
        device.user_id,
        device.channel_id ?? null,
        device.daemon_id ?? null,
        device.device_type,
        device.status ?? 'active',
      ]
    );
  } catch (err) {
    // Distinguish active-only (daemon_id, device_id) collision from the
    // separate api_key UNIQUE — both surface as ER_DUP_ENTRY but only the
    // former is a caller-fixable 409 ("device already exists"). The api_key
    // UNIQUE collision is an internal RNG miss and we let it bubble as 500.
    if (isDuplicateEntryError(err) && /uq_devices_active|active_device_id/i.test(String(err?.message ?? ''))) {
      throw new DeviceConflictError(
        `Active device already exists for daemon_id=${device.daemon_id ?? '∅'} device_id=${device.device_id ?? '∅'}`,
        { daemon_id: device.daemon_id ?? null, device_id: device.device_id ?? null }
      );
    }
    throw err;
  }
  const [rows] = await db.execute(`SELECT * FROM devices WHERE id = ?`, [device.id]);
  return rowToDevice(rows[0]);
}

export async function getDeviceById(db, id) {
  const [rows] = await db.execute(`SELECT * FROM devices WHERE id = ?`, [id]);
  return rowToDevice(rows[0] ?? null);
}

export async function getDeviceByApiKey(db, apiKey) {
  const [rows] = await db.execute(`SELECT * FROM devices WHERE api_key = ?`, [apiKey]);
  return rowToDevice(rows[0] ?? null);
}

export async function getDevices(db, filters = {}) {
  const where = [];
  const args = [];
  if (filters.user_id)    { where.push('user_id = ?');    args.push(filters.user_id); }
  if (filters.channel_id) { where.push('channel_id = ?'); args.push(filters.channel_id); }
  if (filters.daemon_id)  { where.push('daemon_id = ?');  args.push(filters.daemon_id); }
  if (filters.status)     { where.push('status = ?');     args.push(filters.status); }
  if (filters.device_type){ where.push('device_type = ?');args.push(filters.device_type); }
  const sql = `SELECT * FROM devices${where.length ? ` WHERE ${where.join(' AND ')}` : ''} ORDER BY created_at DESC`;
  const [rows] = await db.execute(sql, args);
  return rows.map(rowToDevice);
}

export async function getDevicesByDaemonId(db, daemonId) {
  return getDevices(db, { daemon_id: daemonId, status: 'active' });
}

/**
 * Recently-revoked device_ids for a daemon, scoped to a retention window.
 *
 * T82 (M1.2-FIX-G) — fresh-boot tombstone seed source for the daemon-side
 * DeviceStore. After a daemon restart its in-memory `serverManagedIds` /
 * `revokedServerIds` sets are empty; if the operator's env still carries a
 * stale key for a server-revoked device_id the verifyKey() path would fall
 * back to env and re-authenticate the device. Returning the recent
 * revoke-set on the boot pull lets the daemon seed tombstones deterministically
 * without relying on push-event delivery across the restart.
 *
 * Default window: 30 days. Older revokes are assumed to have been operator-
 * rotated already; the cap also bounds payload size for daemons that have
 * been managing devices for years. Pass `{ sinceDays: null }` to disable.
 *
 * @param {*} db — mysql2 connection
 * @param {string} daemonId
 * @param {{ sinceDays?: number|null }} [opts]
 * @returns {Promise<string[]>} unique device_id strings
 */
export async function getRevokedDeviceIdsByDaemonId(db, daemonId, { sinceDays = 30 } = {}) {
  if (!daemonId) return [];
  const params = [daemonId];
  let where = `daemon_id = ? AND status = 'revoked'`;
  if (sinceDays != null && Number(sinceDays) > 0) {
    // INTERVAL ? DAY does not bind through `?` portably across mysql drivers,
    // but the value is a Number we control above (clamped to integer below).
    const days = Math.floor(Number(sinceDays));
    where += ` AND revoked_at IS NOT NULL AND revoked_at >= (NOW() - INTERVAL ${days} DAY)`;
  }
  const sql = `SELECT DISTINCT device_id FROM devices WHERE ${where}`;
  const [rows] = await db.execute(sql, params);
  const out = [];
  const seen = new Set();
  for (const row of rows) {
    const id = row?.device_id;
    if (typeof id === 'string' && id && !seen.has(id)) {
      seen.add(id);
      out.push(id);
    }
  }
  return out;
}

export async function revokeDevice(db, id) {
  await db.execute(
    `UPDATE devices SET status = 'revoked', revoked_at = COALESCE(revoked_at, NOW()) WHERE id = ?`,
    [id]
  );
  return getDeviceById(db, id);
}

export async function updateMachineDaemonInfo(db, id, fields) {
  const updates = {};
  if ('daemon_host' in fields)   updates.daemon_host = fields.daemon_host ?? null;
  if ('daemon_port' in fields)   updates.daemon_port = fields.daemon_port ?? null;
  if ('daemon_scheme' in fields) updates.daemon_scheme = fields.daemon_scheme ?? null;
  if ('capabilities' in fields) updates.capabilities = fields.capabilities == null
    ? null
    : (typeof fields.capabilities === 'string' ? fields.capabilities : JSON.stringify(fields.capabilities));
  if ('status' in fields)         updates.status = fields.status;
  if ('last_heartbeat' in fields) updates.last_heartbeat = fields.last_heartbeat;
  if (Object.keys(updates).length === 0) return getMachineById(db, id);
  const sets = Object.keys(updates).map(k => `${k} = ?`).join(', ');
  await db.execute(`UPDATE machines SET ${sets} WHERE id = ?`, [...Object.values(updates), id]);
  const [rows] = await db.execute(`SELECT * FROM machines WHERE id = ?`, [id]);
  return rows[0] ?? null;
}

export async function checkQuota(db, serverId, resource) {
  const [serverRows] = await db.execute(`SELECT plan FROM servers WHERE id = ?`, [serverId]);
  const plan = serverRows[0]?.plan ?? 'free';
  const limits = { free: { agents: 5, machines: 3, teams: 10 } };
  const planLimits = limits[plan] ?? limits.free;
  const limit = planLimits[resource] ?? Infinity;

  let current = 0;
  if (resource === 'agents') {
    const [r] = await db.execute(
      `SELECT COUNT(*) as c FROM agents WHERE server_id = ? AND is_del = 0 AND deleted_at IS NULL`, [serverId]
    );
    current = r[0].c;
  } else if (resource === 'machines') {
    const [r] = await db.execute(
      `SELECT COUNT(*) as c FROM machines WHERE server_id = ? AND is_del = 0`, [serverId]
    );
    current = r[0].c;
  } else if (resource === 'teams') {
    const [r] = await db.execute(
      `SELECT COUNT(*) as c FROM teams WHERE server_id = ? AND type = 'team' AND is_del = 0 AND deleted_at IS NULL`, [serverId]
    );
    current = r[0].c;
  }
  return { allowed: current < limit, current, limit };
}
