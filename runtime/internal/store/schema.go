package store

// ChannelLocalDDL creates all channel-local tables inside one
// channel sqlite (`messages.sqlite`).
//
// The DDL string is split into multiple CREATE statements; the
// modernc.org/sqlite driver accepts multi-statement input via Exec.
//
// Vocabulary closed sets (sender_kind / kind / visibility / actor_kind)
// carry NO value-set `CHECK (... IN (...))` clause. Those sets
// are authoritatively enforced in Go — write path: harness stepEnvelopeShape
// (kind, visibility) + stepSenderConsistent (sender.kind force-overwritten
// from the pen-WELDED caller kind — the registry lookup was retired by the
// incarnation rework, Mint is the single truth source) + the actor record
// store's validateDraft (actor_kind gated by ParseKind before insert); read
// path: store scan via ParseKind / ParseVisibility (out-of-set values fail
// loud). A DB CHECK
// would be a redundant SECOND enforcer that also welds an append-only DB to a
// frozen vocabulary: extending a pre-launch closed set (e.g. a new sender_kind)
// would make every existing channel sqlite reject inserts AND forbid recreation
// against the old file. The set is closed by the Go ADT; the DDL must not
// foreclose its evolution. is_terminal KEEPS its CHECK (0,1) — that is a
// structural boolean integrity constraint, not an evolving vocabulary.
const ChannelLocalDDL = `
-- =============================================================
-- 1) messages
-- =============================================================
CREATE TABLE IF NOT EXISTS messages (
  seq                  INTEGER PRIMARY KEY AUTOINCREMENT,
  id                   TEXT NOT NULL UNIQUE,
  ts                   INTEGER NOT NULL,
  ts_received          INTEGER NOT NULL,
  channel_id           TEXT NOT NULL,
  sender_kind          TEXT NOT NULL,
  sender_id            TEXT NOT NULL,
  kind                 TEXT NOT NULL,
  type                 TEXT NOT NULL,
  payload              TEXT NOT NULL,
  parent_id            TEXT,
  correlation_id       TEXT,
  visibility           TEXT NOT NULL,
  audience             TEXT NOT NULL,
  expires_at           INTEGER,
  client_fingerprint   TEXT,
  is_terminal          INTEGER NOT NULL DEFAULT 0 CHECK (is_terminal IN (0,1))
);

CREATE INDEX IF NOT EXISTS ix_messages_parent         ON messages(parent_id);
CREATE INDEX IF NOT EXISTS ix_messages_expires        ON messages(expires_at) WHERE expires_at IS NOT NULL AND kind='request';
CREATE INDEX IF NOT EXISTS ix_messages_type_sender_seq ON messages(type, sender_id, seq);

CREATE UNIQUE INDEX IF NOT EXISTS ux_terminal_response_per_request
  ON messages(parent_id)
  WHERE kind = 'response' AND is_terminal = 1;

CREATE UNIQUE INDEX IF NOT EXISTS ux_sysop_completed_correlation
  ON messages(correlation_id)
  WHERE kind = 'event' AND type = 'sysop_completed';

-- (v2: actor_cursors table removed. A per-actor durable consumption offset is
-- NOT substrate truth: only a log-PULL consumer that must resume gap-free needs
-- one, and that offset is the consumer's own bookkeeping (it knows where it left
-- off), persisted by the consumer itself outside the substrate. The substrate's universal log
-- primitive is the seq-ordered message log + ReadAfterSeq; v2 actors are
-- push-mailbox consumers (death-closure, not replay), so no actor
-- pulls-and-resumes. Additive re-add when a real durable-pull actor demands it.)

-- =============================================================
-- 3) actor_registry
-- =============================================================
-- One row per managed actor record. The record answers "who is it / what is
-- it"; the definition columns hold the CURRENT value only. There is no version
-- history table and no current-version pointer: the registry stores what is,
-- the audit narration stores what happened.
CREATE TABLE IF NOT EXISTS actor_registry (
  actor_id           TEXT PRIMARY KEY,
  actor_kind         TEXT NOT NULL,
  principal          TEXT NOT NULL DEFAULT '', -- login identity only; declaration-backed actors normally leave it empty
  source_decl_id     TEXT NOT NULL DEFAULT '', -- immutable declaration provenance; never an operation identity
  class              TEXT NOT NULL,
  config_json        TEXT,
  placement          TEXT NOT NULL CHECK(placement IN ('server','daemon')),
  desired_host       TEXT NOT NULL DEFAULT '' CHECK(placement='daemon' OR desired_host=''),
  created_at         INTEGER NOT NULL,
  deregistered_at    INTEGER
);

CREATE INDEX IF NOT EXISTS ix_actor_registry_active
  ON actor_registry(actor_kind)
  WHERE deregistered_at IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS ux_actor_registry_active_principal
  ON actor_registry(actor_kind, principal)
  WHERE deregistered_at IS NULL AND principal <> '';
CREATE UNIQUE INDEX IF NOT EXISTS ux_actor_registry_active_source_decl
  ON actor_registry(source_decl_id)
  WHERE deregistered_at IS NULL AND source_decl_id <> '';

-- Immutable self-truth written exactly once during ChannelHost provisioning.
CREATE TABLE IF NOT EXISTS channel_genesis (
  channel_id         TEXT PRIMARY KEY,
  type               TEXT NOT NULL,
  owner_principal    TEXT NOT NULL,
  parent_channel_id  TEXT,
  initiator_principal TEXT,
  created_at         INTEGER NOT NULL
);

-- Channel-local daemon binding truth. Live link attachment is deliberately a
-- separate observation maintained by the link acceptor.
CREATE TABLE IF NOT EXISTS channel_daemon_bindings (
  daemon_id   TEXT PRIMARY KEY,
  attached_at INTEGER NOT NULL
);

-- (v2: worker_locks table removed. channel-sqlite is append-only truth;
-- write-path exclusivity is a structural invariant of the single write path,
-- not a per-row lease.)

-- (v2: action_ledger table removed. Turn-replay idempotency had no triggering
-- scenario left — at-least-once redelivery was retired and substrate never
-- transparently respawns and replays a turn — and its key is derived from a
-- domain-supplied semantic_action_key, so idempotency is an application/stdlib
-- concern, not substrate truth. Additive re-add when a real
-- exactly-once-external-effect use case demands it.)

-- =============================================================
-- 4) resources  (access plane)
-- =============================================================
-- The plane-2 object-lifecycle truth: existence + inline bytes. Same channel
-- sqlite as the message log — access is channel-scoped.
--
-- There is NO authorization relation table: the membrane is a uniform trust
-- phase (PM-D1) — read/write authorization is channel membership itself
-- (owner root ∪ active member), judged at the door from membership facts;
-- delete additionally distinguishes the creator via created_by (PM-D3).
-- Per-object grants structurally cannot exist.
--
-- No scope column on resources, ever (owner-pinned): actor-scoped objects
-- live in a SEPARATE storage locus (the actor_state table below), so
-- scope is expressed by the STRUCTURE an object lives in, not a column (Unix:
-- an anonymous mapping is not a file tagged "anonymous"). This table holds only
-- channel-scoped objects.
CREATE TABLE IF NOT EXISTS resources (
  resource_id           TEXT PRIMARY KEY,
  kind                  TEXT NOT NULL,
  bytes                 BLOB,                     -- KindKV driver's inline bytes; NULL for kv = resolved-but-empty, ALWAYS NULL for file (its bytes live at placement_coord, never inline)
  placement_daemon_id   TEXT NOT NULL DEFAULT '', -- explicit routing column: which daemon's Streamer holds the bytes; '' for kv
  placement_coord       TEXT NOT NULL DEFAULT '', -- opaque storage handle, server-registry-generated (§1.6); '' for kv; NEVER crosses Stat/List to a caller (§3.6 red line, enforced one layer up)
  created_by            TEXT NOT NULL DEFAULT '', -- durable creator actor id; AUTHORIZATION PREDICATE since PM-D3 (op=delete = creator ∨ channel owner root, judged at the door) and the audit record it always was; read/write never consult it (membrane-uniform, PM-D1)
  source_channel_id     TEXT,
  source_resource_id    TEXT,
  created_at            INTEGER NOT NULL,
  is_dir                INTEGER NOT NULL DEFAULT 0 CHECK (is_dir IN (0,1)) -- file BYTE-SHAPE bit (the inode's S_IFDIR analogue): 1 = directory-shaped file resource (workspace, bytes = a whole tree委托真fs, Open→os.Root lease句柄), 0 = regular blob (Open→single-file staging句柄) / kv (always 0). Structural boolean integrity, KEEPS its CHECK (same discipline as is_terminal); this is the door's Open ROUTING truth, read at resolve, never a leaf the daemon re-derives from disk
);

-- =============================================================
-- 4b) resource_reservations + resource_tombstones  (create/delete outbox)
-- =============================================================
-- The create-outbox's two durable halves, both server-side (期11 spec §1.3 —
-- v1.1's corrected home: the daemon holds no truth, so BOTH the create
-- write-ahead half and the delete collection-pending half live in THIS
-- channel sqlite, never on the daemon). True mirrors of each other: create's
-- authorization-first-mover is the door (server) writing resource_reservations
-- before any byte moves; delete's is the door (server) writing
-- resource_tombstones before the Reclaimer collects bytes. Neither table is
-- message-log truth (append-only) — both are mutable control-plane outboxes,
-- rows created and later DELETED as their event closes, same non-truth
-- discipline as the timers table above.
CREATE TABLE IF NOT EXISTS resource_reservations (
  reservation_id       TEXT PRIMARY KEY,        -- fresh uuid per ReserveCreate call
  resource_id          TEXT NOT NULL,
  kind                 TEXT NOT NULL,
  placement_daemon_id  TEXT NOT NULL DEFAULT '',
  placement_coord      TEXT NOT NULL DEFAULT '',
  created_by           TEXT NOT NULL,           -- door-authenticated creator (never daemon-reported)
  source_channel_id    TEXT,
  source_resource_id   TEXT,
  reserved_at          INTEGER NOT NULL,
  is_dir               INTEGER NOT NULL DEFAULT 0 CHECK (is_dir IN (0,1)), -- carried write-ahead so CommitReservation lands the resources row with the correct byte-shape bit (a content-less dir create's shape must survive the ReserveCreate→AllocRequest→Committed round trip; daemon reports no truth, §1.3)
  last_progress_at     INTEGER NOT NULL DEFAULT 0 -- most-recent activity stamp for the in-flight transfer
);
CREATE INDEX IF NOT EXISTS ix_resource_reservations_daemon ON resource_reservations(placement_daemon_id);
CREATE INDEX IF NOT EXISTS ix_resource_reservations_reserved_at ON resource_reservations(reserved_at);
CREATE INDEX IF NOT EXISTS ix_resource_reservations_last_progress_at ON resource_reservations(last_progress_at);

CREATE TABLE IF NOT EXISTS resource_tombstones (
  tombstone_id    TEXT PRIMARY KEY,       -- fresh uuid per Delete(file) call
  resource_id     TEXT NOT NULL,          -- NON-unique index: same-name delete/recreate can leave multiple tombstones co-existing without colliding on the primary key
  daemon_id       TEXT NOT NULL DEFAULT '',
  placement_coord TEXT NOT NULL DEFAULT '',
  kind            TEXT NOT NULL,
  deleted_at      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_resource_tombstones_daemon ON resource_tombstones(daemon_id);

-- =============================================================
-- 5) actor_state  (access plane)
-- =============================================================
-- The ACTOR-SCOPED storage locus: the second, structurally separate home of
-- objects, dual to the channel-scoped resources table. Same channel sqlite as
-- everything else — access is channel-scoped — but a SEPARATE table because scope is
-- expressed by WHICH structure an object lives in, never by a column (Unix: an
-- anonymous mapping is not a file tagged "anonymous", it simply is not in the fs
-- namespace). The collapsed authorization (reachable set ≡ {owner}) means the
-- byte row IS the whole object.
-- Keyed (owner_id, resource_id) — the door welds owner at handle mint, so owner
-- is a coordinate, not a per-call arg. Rows of a dead owner are NOT cleared on
-- deregister: ActorIDs are never reused and every belonging is keyed by
-- ActorID, so a dead owner's rows are unreachable inert data. Correctness lives
-- at the admission gate, never in a delete; reclaiming disk is an explicit
-- batch management action, not lifecycle logic.
CREATE TABLE IF NOT EXISTS actor_state (
  owner_id    TEXT NOT NULL,             -- identity level (ActorID); incarnation NEVER persisted
  resource_id TEXT NOT NULL,
  bytes       BLOB,                      -- inline small bytes, plaintext (at-rest encryption deferred); NULL = resolved-but-empty
  created_at  INTEGER NOT NULL,
  PRIMARY KEY (owner_id, resource_id)
  -- No kind column (day-1 single mechanical shape; a second actor-scoped variant
  -- adds one additively — day-1 it would be a dead tag).
  -- No scope column, ever (this whole table IS the actor-scoped locus, so the
  -- STRUCTURE is the scope — a column would be redundant and never read).
  -- No version column (per-key fence / CAS deferred; day-1 natural single
  -- writer — reachable set ≡ {owner} + serial gift — so nothing to fence yet).
);

-- =============================================================
-- 6) timers  (time axis)
-- =============================================================
-- The IDENTITY-level pending-timer CONTROL PLANE: future intent keyed by
-- author ActorID, mutable (cancellable), NEVER truth (pending in the
-- append-only log would be unretractable). Same channel sqlite as the
-- messages/registry/state tables.
--
-- This table is the Durable Scheduler home. Memory-home timers live only in
-- the current Scheduler instance and vanish with it. Both homes are owned by
-- ActorID and cross actor replacement; storage home is not an
-- AttemptKey/Incarnation coordinate. So: no per-row home/generation column.
-- No target column, ever (timers are always self-targeted). No recurrence
-- column (one-shot is the complete primitive; recurrence is domain re-arm).
CREATE TABLE IF NOT EXISTS timers (
  timer_id       TEXT PRIMARY KEY,
  author_id      TEXT NOT NULL,     -- identity; the actor that scheduled the timer = the fire author (welded, never freely reassignable)
  fire_at        INTEGER NOT NULL,  -- UnixMilli
  type           TEXT NOT NULL,
  payload        BLOB,
  correlation_id TEXT,              -- captured at schedule time; inherited by the fire envelope
  created_at     INTEGER NOT NULL,
  state          TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','fired'))
);
CREATE INDEX IF NOT EXISTS ix_timers_fire_at ON timers(fire_at);
CREATE INDEX IF NOT EXISTS ix_timers_author  ON timers(author_id);

CREATE TABLE IF NOT EXISTS timer_dead (
  dead_seq       INTEGER PRIMARY KEY AUTOINCREMENT,
  timer_id       TEXT NOT NULL UNIQUE,
  author_id      TEXT NOT NULL,
  fire_at        INTEGER NOT NULL,
  type           TEXT NOT NULL,
  payload        BLOB,
  correlation_id TEXT,
  created_at     INTEGER NOT NULL,
  death_class    TEXT NOT NULL,
  reason         TEXT NOT NULL,
  detail         TEXT NOT NULL,
  died_at        INTEGER NOT NULL
);
`

// ChannelLocalTables returns the channel-local table names in initialization
// order — a fresh copy per call, so no caller can mutate the canonical list.
// Tests assert that every name exists in `sqlite_master` after OpenChannel.
//
// The list contains only channel-local truth tables. The former
// bootstrap_registry table was not channel-local truth and has been removed.
func ChannelLocalTables() []string {
	return []string{
		"messages",
		"actor_registry",
		"channel_genesis",
		"channel_daemon_bindings",
		"resources",
		"resource_reservations",
		"resource_tombstones",
		"actor_state",
		"timers",
		"timer_dead",
	}
}
