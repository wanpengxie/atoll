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
  cross_channel_refs   TEXT,
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
  is_terminal          INTEGER NOT NULL DEFAULT 0 CHECK (is_terminal IN (0,1)),
  canonical_hash       TEXT NOT NULL DEFAULT ''
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
  type                     TEXT PRIMARY KEY,
  allowed_kinds            TEXT NOT NULL,
  handler_binding          TEXT NOT NULL
                           CHECK (handler_binding IN ('embedded','runtime_outbound','runtime_inbound_via_relay')),
  max_pending_ms           INTEGER,
	  handler_actor_id         TEXT,
	  domain                   TEXT,
	  install_status           TEXT NOT NULL DEFAULT 'installed'
	                           CHECK (install_status IN ('installing','installed','failed')),
		  install_error            TEXT NOT NULL DEFAULT '',
		  created_at               INTEGER NOT NULL
		);

	CREATE TABLE IF NOT EXISTS type_registry_pending (
	  install_attempt_id      TEXT PRIMARY KEY,
	  type                    TEXT NOT NULL UNIQUE,
	  allowed_kinds           TEXT NOT NULL,
	  handler_binding         TEXT NOT NULL
	                          CHECK (handler_binding IN ('embedded','runtime_outbound','runtime_inbound_via_relay')),
	  max_pending_ms          INTEGER,
	  handler_actor_id        TEXT,
	  install_status          TEXT NOT NULL DEFAULT 'installing'
	                          CHECK (install_status IN ('installing','failed')),
	  install_error           TEXT NOT NULL DEFAULT '',
	  created_at              INTEGER NOT NULL
	);

	CREATE INDEX IF NOT EXISTS ix_type_registry_pending_type_status
	  ON type_registry_pending(type, install_status);

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
                            OR actor_binding IN ('embedded','runtime_outbound','runtime_inbound_via_relay')),
  display_name       TEXT,
  created_at         INTEGER NOT NULL,
  deregistered_at    INTEGER,
  ready_state        TEXT NOT NULL DEFAULT 'unknown'
                     CHECK (ready_state IN ('ready','not_ready','unknown')),
  ready_reason       TEXT NOT NULL DEFAULT 'unknown',
  ready_detail       TEXT NOT NULL DEFAULT '{}',
  last_ready_at      INTEGER NOT NULL DEFAULT 0,
  last_state_change_at INTEGER NOT NULL DEFAULT 0
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
  -- fencing_token is an opaque unguessable string (proto-foundation
  -- §3.6.1). Decoupled from owner_epoch / daemon_epoch.
  fencing_token      TEXT NOT NULL,
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
	-- 7) adapter_state  (L2 §8 F4 — framework StateStore)
	-- =============================================================
	CREATE TABLE IF NOT EXISTS adapter_state (
	  key                TEXT PRIMARY KEY,
	  value              BLOB NOT NULL,
	  updated_at         INTEGER NOT NULL
	);

	-- =============================================================
	-- 8) adapter_credentials  (L2 §8 F8 — framework CredentialStore)
	-- =============================================================
	CREATE TABLE IF NOT EXISTS adapter_credentials (
	  key                TEXT PRIMARY KEY,
	  value              TEXT NOT NULL,
	  updated_at         INTEGER NOT NULL
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
	  phase              TEXT NOT NULL DEFAULT 'sent'
	                     CHECK (phase IN ('sent','awaiting_ack','partial_takeover','completed','abandoned')),
	  workdir_path       TEXT NOT NULL,
	  sent_at            INTEGER NOT NULL DEFAULT 0,
	  expected_ack_frame_kind TEXT NOT NULL DEFAULT 'control.create_channel_ack',
	  terminal_status    TEXT NOT NULL DEFAULT '',
	  abandonment_reason TEXT NOT NULL DEFAULT '',
	  attempt_count      INTEGER NOT NULL DEFAULT 0,
	  last_attempt_at    INTEGER NOT NULL DEFAULT 0,
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
	"adapter_state",
	"adapter_credentials",
}

// DaemonLocalTables enumerates daemon-level table names.
var DaemonLocalTables = []string{"bootstrap_registry"}
