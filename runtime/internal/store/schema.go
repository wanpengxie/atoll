package store

// ChannelLocalDDL creates all channel-local tables inside one
// channel sqlite (`messages.sqlite`).
//
// Authoritative spec references:
//
//   - L2 §1.4.1  messages
//   - L2 §1.4.6  actor_registry
//
// The DDL string is split into multiple CREATE statements; the
// modernc.org/sqlite driver accepts multi-statement input via Exec.
//
// Vocabulary closed sets (sender_kind / kind / visibility / actor_kind /
// actor_binding) carry NO value-set `CHECK (... IN (...))` clause. Those sets
// are authoritatively enforced in Go — write path: harness stepEnvelopeShape
// (kind, visibility) + stepSenderConsistent (sender.kind overwritten from the
// registry's parsed truth); read path: store scan via ParseKind /
// ParseVisibility / ParseBinding (out-of-set values fail loud). A DB CHECK
// would be a redundant SECOND enforcer that also welds an append-only DB to a
// frozen vocabulary: extending a pre-launch closed set (e.g. a new sender_kind)
// would make every existing channel sqlite reject inserts AND forbid recreation
// against the old file. The set is closed by the Go ADT; the DDL must not
// foreclose its evolution. is_terminal KEEPS its CHECK (0,1) — that is a
// structural boolean integrity constraint, not an evolving vocabulary.
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
  is_terminal          INTEGER NOT NULL DEFAULT 0 CHECK (is_terminal IN (0,1))
);

CREATE INDEX IF NOT EXISTS ix_messages_correlation_ts ON messages(correlation_id, ts_received);
CREATE INDEX IF NOT EXISTS ix_messages_parent         ON messages(parent_id);
CREATE INDEX IF NOT EXISTS ix_messages_type_kind      ON messages(type, kind);
CREATE INDEX IF NOT EXISTS ix_messages_expires        ON messages(expires_at) WHERE expires_at IS NOT NULL AND kind='request';

CREATE UNIQUE INDEX IF NOT EXISTS ux_terminal_response_per_request
  ON messages(parent_id)
  WHERE kind = 'response' AND is_terminal = 1;

-- (v2: actor_cursors table removed. A per-actor durable consumption offset is
-- NOT substrate truth: only a log-PULL consumer that must resume gap-free needs
-- one, and that offset is the consumer's own bookkeeping (it knows where it left
-- off), persisted by the consumer itself outside the substrate. The substrate's universal log
-- primitive is the seq-ordered message log + ReadAfterSeq; v2 actors are
-- push-mailbox consumers (death-closure, not replay), so no actor
-- pulls-and-resumes. Additive re-add when a real durable-pull actor demands it.)

-- =============================================================
-- 3) actor_registry  (L2 §1.4.6)
-- =============================================================
CREATE TABLE IF NOT EXISTS actor_registry (
  actor_id           TEXT PRIMARY KEY,
  actor_kind         TEXT NOT NULL,
  actor_binding      TEXT,
  created_at         INTEGER NOT NULL,
  deregistered_at    INTEGER
);

CREATE INDEX IF NOT EXISTS ix_actor_registry_active
  ON actor_registry(actor_kind)
  WHERE deregistered_at IS NULL;

-- (v2: worker_locks table removed. channel-sqlite is append-only truth;
-- write-path exclusivity is a structural invariant of the single write path
-- (proto-v2-physical §4), not a per-row lease.)

-- (v2: action_ledger table removed. Turn-replay idempotency (L2 §1.4.10.1) had
-- no triggering scenario left — P-A1 retired at-least-once redelivery and the
-- no-transparent-respawn收口 means substrate never replays a turn — and its key
-- is derived from a domain-supplied semantic_action_key, so idempotency is an
-- application/stdlib concern, not substrate truth. Additive re-add when a real
-- exactly-once-external-effect use case demands it.)

-- =============================================================
-- 4) resources + resource_grants  (access plane / forward §12)
-- =============================================================
-- The plane-2 object-lifecycle truth: existence + inline bytes (resources) and
-- the authorization relation R (resource_grants). Same channel sqlite as the
-- message log — access is channel-封 (forward §12.5), so R and resource bytes
-- share the one DB as sibling tables, never a separate library.
--
-- No scope column on resources, ever (§8.1, owner-pinned): actor-scoped objects
-- live in a SEPARATE storage locus (an actor_state-shaped table, deferred), so
-- scope is expressed by the STRUCTURE an object lives in, not a column (Unix:
-- an anonymous mapping is not a file tagged "anonymous"). This table holds only
-- channel-scoped objects.
--
-- No CHECK on grantee_kind: it is a Go-enforced closed set (access.GranteeKind,
-- validated by the door's ValidateGrant on the write path), same discipline as
-- sender_kind / actor_kind above — a DB CHECK would weld an evolving vocabulary
-- to every existing channel file.
CREATE TABLE IF NOT EXISTS resources (
  resource_id TEXT PRIMARY KEY,
  kind        TEXT NOT NULL,
  bytes       BLOB,               -- KindKV driver's inline bytes (day-1's only driver); NULL = resolved-but-empty
  created_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS resource_grants (
  resource_id  TEXT NOT NULL,
  grantee_kind TEXT NOT NULL,             -- access.GranteeKind closed set (A8)
  grantee      TEXT NOT NULL DEFAULT '',  -- actor id when kind=actor; '' when kind=members (sum form persisted in full)
  ops          TEXT NOT NULL,             -- JSON array of access.Operation
  PRIMARY KEY (resource_id, grantee_kind, grantee),
  FOREIGN KEY (resource_id) REFERENCES resources(resource_id)
);

-- =============================================================
-- 5) actor_state  (access plane / forward §6 · §12.9 拍点 8.1)
-- =============================================================
-- The ACTOR-SCOPED storage locus: the second, structurally separate home of
-- objects, dual to the channel-scoped resources table. Same channel sqlite as
-- everything else — access is channel-封 — but a SEPARATE table because scope is
-- expressed by WHICH structure an object lives in, never by a column (Unix: an
-- anonymous mapping is not a file tagged "anonymous", it simply is not in the fs
-- namespace). The collapsed authorization (reachable set ≡ {owner}) means there
-- is no R here — no resource_grants sibling: the byte row IS the whole object.
-- Keyed (owner_id, resource_id) — the door welds owner at handle mint, so owner
-- is a coordinate, not a per-call arg. Cascade-cleared with actor_registry on
-- deregister (§10.12 row 3 = the scope law: owner 亡 ⟹ its state 亡, Erlang ETS
-- private).
CREATE TABLE IF NOT EXISTS actor_state (
  owner_id    TEXT NOT NULL,             -- identity level (ActorID); incarnation NEVER persisted (§5.3)
  resource_id TEXT NOT NULL,
  bytes       BLOB,                      -- inline small bytes, plaintext (at-rest encryption deferred, 拍点 8.6); NULL = resolved-but-empty
  created_at  INTEGER NOT NULL,
  PRIMARY KEY (owner_id, resource_id)
  -- No kind column (day-1 single mechanical shape; a second actor-scoped variant
  -- adds one additively, 拍点 8.4 — day-1 it would be a dead tag).
  -- No scope column, ever (this whole table IS the actor-scoped locus, so the
  -- STRUCTURE is the scope — a column would be redundant and never read).
  -- No version column (per-key fence / CAS deferred, §1.5; day-1 natural single
  -- writer — reachable set ≡ {owner} + serial gift — so nothing to fence yet).
);
`

// ChannelLocalTables enumerates the channel-local table names in
// initialization order. Tests assert that every name exists in
// `sqlite_master` after OpenChannel.
//
// ChannelLocalTables contains only channel-local truth tables. The former
// bootstrap_registry table was not channel-local truth and has been removed.
var ChannelLocalTables = []string{
	"messages",
	"actor_registry",
	"resources",
	"resource_grants",
	"actor_state",
}
