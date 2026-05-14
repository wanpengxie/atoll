package store

// ChannelLocalDDL creates all channel-local tables inside one
// channel sqlite (`messages.sqlite`).
//
// Authoritative spec references:
//
//   - L2 §1.4.1  messages
//   - L2 §1.4.2  type_registry
//   - L2 §1.4.3  actor_cursors
//   - L2 §1.4.6  actor_registry
//   - L2 §1.4.9  worker_locks
//   - L2 §1.4.10.1 action_ledger
//   - L1 §8.6 / T3 view_sync_outbox  (M1.5 new)
//   - L2 §1.4.11 / T3 channel_lock   (M1.5 new — daemon-side mirror
//     of channel_placements fencing fields)
//
// The DDL string is split into multiple CREATE statements; the
// modernc.org/sqlite driver accepts multi-statement input via Exec.
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
                       CHECK (handler_binding IN ('in_process','outbound_http','via_server_transit')),
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
                            OR actor_binding IN ('in_process','outbound_http','via_server_transit')),
  display_name       TEXT,
  created_at         INTEGER NOT NULL,
  deregistered_at    INTEGER
);

CREATE INDEX IF NOT EXISTS ix_actor_registry_active
  ON actor_registry(actor_kind)
  WHERE deregistered_at IS NULL;

-- =============================================================
-- 5) worker_locks  (L2 §1.4.9 — daemon-side authoritative lease)
-- =============================================================
CREATE TABLE IF NOT EXISTS worker_locks (
  agent_id           TEXT PRIMARY KEY,
  worker_id          TEXT NOT NULL,
  fencing_token      INTEGER NOT NULL,
  daemon_epoch       INTEGER NOT NULL,
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

-- =============================================================
-- 7) view_sync_outbox  (L1 §8.6 — daemon persistent outbox)
-- =============================================================
-- One row per messages.seq enqueued for view-sync push. Inserted in
-- the same transaction as messages append. Status transitions:
--   pending  -> pushed   (transit.client sent the frame)
--   pushed   -> acked    (server returned ack with last_received_seq
--                         >= this row's seq)  -- soft-state; rows
--                         are deleted on ack (no 'acked' row state
--                         is persisted; deletion = acked).
CREATE TABLE IF NOT EXISTS view_sync_outbox (
  seq                INTEGER PRIMARY KEY,
  message_id         TEXT NOT NULL UNIQUE,
  envelope_json      TEXT NOT NULL,
  enqueued_at        INTEGER NOT NULL,
  pushed_at          INTEGER,
  status             TEXT NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending','pushed')),
  FOREIGN KEY (seq) REFERENCES messages(seq) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_view_sync_outbox_status_seq
  ON view_sync_outbox(status, seq);

-- =============================================================
-- 8) channel_lock  (L2 §1.4.11 + T1.4 — daemon-side fencing mirror)
-- =============================================================
-- Single row (channel_id = 'self'). Holds the fencing fields the
-- daemon process must satisfy to write into this channel sqlite.
-- daemon_epoch is the daemon process counter — bumped on every
-- daemon restart so stale worker IPC after restart fails fence_check.
CREATE TABLE IF NOT EXISTS channel_lock (
  channel_id         TEXT PRIMARY KEY,
  fencing_token      INTEGER NOT NULL,
  owner_epoch        INTEGER NOT NULL,
  daemon_id          TEXT NOT NULL,
  daemon_epoch       INTEGER NOT NULL,
  acquired_at        INTEGER NOT NULL,
  refreshed_at       INTEGER NOT NULL
);
`

// DaemonLocalDDL builds the daemon-level sqlite tables. There is exactly
// one bootstrap_registry row per channel-create attempt.
const DaemonLocalDDL = `
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
// `sqlite_master` after OpenChannel.
var ChannelLocalTables = []string{
	"messages",
	"type_registry",
	"actor_cursors",
	"actor_registry",
	"worker_locks",
	"action_ledger",
	"view_sync_outbox",
	"channel_lock",
}

// DaemonLocalTables enumerates daemon-level table names.
var DaemonLocalTables = []string{"bootstrap_registry"}
