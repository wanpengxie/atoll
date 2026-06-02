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
  kind                 TEXT NOT NULL CHECK (kind IN ('event','request','response')),
  type                 TEXT NOT NULL,
  payload              TEXT NOT NULL,
  parent_id            TEXT,
  correlation_id       TEXT,
  doc_refs             TEXT,
  cross_channel_refs   TEXT,
  visibility           TEXT NOT NULL CHECK (visibility IN ('public','private','system')),
  audience             TEXT NOT NULL,
  expires_at           INTEGER,
  is_terminal          INTEGER NOT NULL DEFAULT 0 CHECK (is_terminal IN (0,1))
);

CREATE INDEX IF NOT EXISTS ix_messages_correlation_ts ON messages(correlation_id, ts_received);
CREATE INDEX IF NOT EXISTS ix_messages_parent         ON messages(parent_id);
CREATE INDEX IF NOT EXISTS ix_messages_type_kind      ON messages(type, kind);
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
  created_at         INTEGER NOT NULL,
  deregistered_at    INTEGER
);

CREATE INDEX IF NOT EXISTS ix_actor_registry_active
  ON actor_registry(actor_kind)
  WHERE deregistered_at IS NULL;

-- (v2: worker_locks table removed. The v1 daemon-side channel-write lease is
-- obsolete — the channel has a single writer, server harness, by construction
-- (proto-v2-physical §4); worker-instance fencing is a volatile compute-side
-- lease, not a channel-sqlite row.)

-- (v2: action_ledger table removed. Turn-replay idempotency (L2 §1.4.10.1) had
-- no triggering scenario left — P-A1 retired at-least-once redelivery and the
-- no-transparent-respawn收口 means substrate never replays a turn — and its key
-- is derived from a domain-supplied semantic_action_key, so idempotency is an
-- application/stdlib concern, not substrate truth. Additive re-add when a real
-- exactly-once-external-effect use case demands it.)
`

// ChannelLocalTables enumerates the channel-local table names in
// initialization order. Tests assert that every name exists in
// `sqlite_master` after OpenChannel.
//
// v2: there is NO daemon-level persistent store. The v1 daemon-local
// bootstrap_registry contradicted the v2 topology (daemon = attached compute,
// no truth — proto-v2-physical §4); channel-create + its crash-recovery state
// is server-side truth, not a daemon-local sqlite. Removed.
var ChannelLocalTables = []string{
	"messages",
	"type_registry",
	"actor_cursors",
	"actor_registry",
}
