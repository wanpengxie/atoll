// Package store holds the v4 channel-local + daemon-level SQLite layer.
//
// Authoritative spec references:
//
//   - L2 §1.4.1  messages
//   - L2 §1.4.2  type_registry
//   - L2 §1.4.3  actor_cursors
//   - L2 §1.4.6  actor_registry
//   - L2 §1.4.7  bootstrap_registry (daemon-level)
//   - L2 §1.4.9  worker_locks
//   - L2 §1.4.10.1 action_ledger
//
// The DDL constants below are normative — they must stay byte-equivalent to
// the spec. The only acceptable diffs are whitespace and comment edits.
// Bumping the schema requires a dedicated ticket + version bump because
// M1.3 locks the protocol baseline.
package store

// ChannelLocalDDL builds the 6 channel-local tables + their indexes inside
// one `messages.sqlite` file. Step 2 of the channel bootstrap saga
// (L2 §1.4.7) calls into this DDL.
//
// The string is split into multiple `CREATE` statements; `database/sql`'s
// `Exec` accepts multi-statement input on the modernc sqlite driver.
const ChannelLocalDDL = `
-- =============================================================
-- 1) messages  (L2 §1.4.1)
-- =============================================================
CREATE TABLE IF NOT EXISTS messages (
  seq                  INTEGER PRIMARY KEY AUTOINCREMENT,

  id                   TEXT NOT NULL UNIQUE,
  ts                   INTEGER NOT NULL,
  ts_received          INTEGER NOT NULL,
  channel_id           TEXT NOT NULL,
  sender_kind          TEXT NOT NULL CHECK (sender_kind IN ('human','agent','system','tool')),
  sender_id            TEXT NOT NULL,
  sender_name          TEXT,
  kind                 TEXT NOT NULL CHECK (kind IN ('event','request','response')),
  type                 TEXT NOT NULL,
  payload              TEXT NOT NULL,
  parent_id            TEXT,
  correlation_id       TEXT,
  doc_refs             TEXT,
  visibility           TEXT NOT NULL CHECK (visibility IN ('public','private','system')),
  audience             TEXT NOT NULL,
  not_before           INTEGER,
  expires_at           INTEGER,

  delivered_at         INTEGER,
  delivery_failed_at   INTEGER,
  last_error           TEXT,
  attempts             INTEGER NOT NULL DEFAULT 0,

  -- Future-scheduler in-flight claim (R2-FIX-3). claim_owner holds the
  -- daemon instance id that is currently dispatching; claimed_at is the
  -- unix-ms timestamp when the claim was taken. Both are NULL while the
  -- row is dispatchable. They are decoupled from delivered_at so a
  -- crash between claim and dispatch never silently drops the row —
  -- scan picks the row up again once claimed_at < now - claim_ttl_ms.
  claim_owner          TEXT,
  claimed_at           INTEGER,

  is_terminal          INTEGER NOT NULL DEFAULT 0 CHECK (is_terminal IN (0,1))
);

CREATE INDEX IF NOT EXISTS ix_messages_correlation_ts ON messages(correlation_id, ts_received);
CREATE INDEX IF NOT EXISTS ix_messages_parent         ON messages(parent_id);
CREATE INDEX IF NOT EXISTS ix_messages_type_kind      ON messages(type, kind);
CREATE INDEX IF NOT EXISTS ix_messages_not_before     ON messages(not_before) WHERE not_before IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_messages_expires        ON messages(expires_at) WHERE expires_at IS NOT NULL AND kind='request';

CREATE UNIQUE INDEX IF NOT EXISTS ux_terminal_response_per_request
  ON messages(parent_id)
  WHERE kind = 'response' AND is_terminal = 1;

-- =============================================================
-- 2) type_registry  (L2 §1.4.2)
-- =============================================================
CREATE TABLE IF NOT EXISTS type_registry (
  type                 TEXT PRIMARY KEY,
  allowed_kinds        TEXT NOT NULL,
  schemas_by_kind      TEXT NOT NULL,
  handler_binding      TEXT NOT NULL
                       CHECK (handler_binding IN ('daemon_rpc','in_worker_bus')),
  terminal_convention  TEXT NOT NULL DEFAULT 'payload_status'
                       CHECK (terminal_convention IN ('payload_status','single-response')),
  max_pending_ms       INTEGER,
  handler_actor_id     TEXT,
  domain               TEXT,
  created_at           INTEGER NOT NULL
);

-- =============================================================
-- 3) actor_cursors  (L2 §1.4.3)
-- =============================================================
CREATE TABLE IF NOT EXISTS actor_cursors (
  actor_id             TEXT PRIMARY KEY,
  last_consumed_seq    INTEGER NOT NULL DEFAULT 0,
  last_consumed_id     TEXT,
  updated_at           INTEGER NOT NULL
);

-- =============================================================
-- 4) actor_registry  (L2 §1.4.6)
-- =============================================================
CREATE TABLE IF NOT EXISTS actor_registry (
  actor_id           TEXT PRIMARY KEY,
  actor_kind         TEXT NOT NULL
                     CHECK (actor_kind IN ('human','agent','system','tool')),
  actor_binding      TEXT
                     CHECK (actor_binding IS NULL
                            OR actor_binding IN ('daemon_rpc','in_worker_bus')),
  created_at         INTEGER NOT NULL,
  deregistered_at    INTEGER
);

CREATE INDEX IF NOT EXISTS ix_actor_registry_active
  ON actor_registry(actor_kind)
  WHERE deregistered_at IS NULL;

-- =============================================================
-- 5) worker_locks  (L2 §1.4.9)
-- =============================================================
CREATE TABLE IF NOT EXISTS worker_locks (
  agent_id           TEXT PRIMARY KEY,
  worker_id          TEXT NOT NULL,
  fencing_token      INTEGER NOT NULL,
  lease_expires_at   INTEGER NOT NULL,
  acquired_at        INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_worker_locks_expires ON worker_locks(lease_expires_at);

-- =============================================================
-- 6) action_ledger  (L2 §1.4.10.1)
-- =============================================================
CREATE TABLE IF NOT EXISTS action_ledger (
  ledger_key         TEXT PRIMARY KEY,
  turn_id            TEXT NOT NULL,
  actor_id           TEXT NOT NULL,
  envelope_id        TEXT NOT NULL,
  status             TEXT NOT NULL
                     CHECK (status IN ('reserved','committed')),
  reserved_at        INTEGER NOT NULL,
  committed_at       INTEGER
);

CREATE INDEX IF NOT EXISTS ix_action_ledger_turn ON action_ledger(turn_id);
`

// DaemonLevelDDL builds the daemon-level sqlite tables. There is exactly
// one bootstrap_registry row per channel-create attempt; the file lives
// outside any channel workdir (typically `~/.coagent/daemon.sqlite`).
const DaemonLevelDDL = `
-- =============================================================
-- bootstrap_registry  (L2 §1.4.7)
-- =============================================================
CREATE TABLE IF NOT EXISTS bootstrap_registry (
  create_request_id  TEXT PRIMARY KEY,
  channel_id         TEXT NOT NULL UNIQUE,
  status             TEXT NOT NULL
                     CHECK (status IN ('in_progress','completed','rolled_back')),
  workdir_path       TEXT NOT NULL,
  started_at         INTEGER NOT NULL,
  completed_at       INTEGER,
  rollback_reason    TEXT
);

CREATE INDEX IF NOT EXISTS ix_bootstrap_status ON bootstrap_registry(status);
`

// ChannelLocalTables enumerates the channel-local table names in
// initialization order. Tests assert that every name exists in
// `sqlite_master` after `OpenChannel`.
var ChannelLocalTables = []string{
	"messages",
	"type_registry",
	"actor_cursors",
	"actor_registry",
	"worker_locks",
	"action_ledger",
}

// ChannelLocalIndexes enumerates the channel-local non-PK indexes,
// including the partial UNIQUE INDEX. Tests assert each is reachable
// via `sqlite_master`.
var ChannelLocalIndexes = []string{
	"ix_messages_correlation_ts",
	"ix_messages_parent",
	"ix_messages_type_kind",
	"ix_messages_not_before",
	"ix_messages_expires",
	"ux_terminal_response_per_request",
	"ix_actor_registry_active",
	"ix_worker_locks_expires",
	"ix_action_ledger_turn",
}

// DaemonLevelTables enumerates daemon-level table names.
var DaemonLevelTables = []string{"bootstrap_registry"}

// DaemonLevelIndexes enumerates daemon-level non-PK indexes.
var DaemonLevelIndexes = []string{"ix_bootstrap_status"}
