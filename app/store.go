package app

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

// OpenDB opens the app-level SQLite database (identity, workspace, channel
// catalog, daemon registry). This is NOT channel truth -- each channel's
// message log lives in its own DB managed by platform.Home.
func OpenDB(path string) (*sql.DB, error) {
	// modernc.org/sqlite reads pragmas via the _pragma= DSN form (applied per
	// connection, so they hold across the database/sql pool). foreign_keys(1) is
	// required for the ON DELETE CASCADE on daemon_channels / channel_actors to
	// actually fire — SQLite leaves FK enforcement OFF by default.
	db, err := sql.Open("sqlite", path+"?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		return nil, fmt.Errorf("app: open db: %w", err)
	}
	if err := migrate(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("app: migrate: %w", err)
	}
	return db, nil
}

// migrateDeclTableName renames the legacy `agents` table to `actor_decls`
// BEFORE the canonical DDL runs. Order is load-bearing: if the canonical
// `CREATE TABLE IF NOT EXISTS actor_decls` ran first against an old DB, it
// would create an EMPTY actor_decls, the rename would then fail (target
// exists) and every existing declaration row would become invisible. Running
// the rename first means the canonical DDL no-ops on the renamed table.
// Both tables present = a half-migrated / hand-touched DB: fail loud (refuse
// to open) rather than silently picking one and shadowing the other's rows.
func migrateDeclTableName(db *sql.DB) error {
	has := func(name string) (bool, error) {
		var n int
		err := db.QueryRow(
			`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, name).Scan(&n)
		return n > 0, err
	}
	hasOld, err := has("agents")
	if err != nil {
		return err
	}
	hasNew, err := has("actor_decls")
	if err != nil {
		return err
	}
	switch {
	case hasOld && hasNew:
		return fmt.Errorf("both 'agents' and 'actor_decls' tables exist — refusing to guess; resolve the duplicate manually")
	case hasOld && !hasNew:
		if _, err := db.Exec(`ALTER TABLE agents RENAME TO actor_decls`); err != nil {
			return fmt.Errorf("rename agents -> actor_decls: %w", err)
		}
	}
	return nil
}

func migrate(db *sql.DB) error {
	if err := migrateDeclTableName(db); err != nil {
		return err
	}
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id TEXT PRIMARY KEY,
			email TEXT UNIQUE NOT NULL,
			password TEXT NOT NULL,
			display_name TEXT,
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS sessions (
			token TEXT PRIMARY KEY,
			user_id TEXT NOT NULL REFERENCES users(id),
			created_at INTEGER NOT NULL,
			expires_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS workspaces (
			id TEXT PRIMARY KEY,
			owner_id TEXT NOT NULL REFERENCES users(id),
			name TEXT NOT NULL,
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS workspace_members (
			workspace_id TEXT NOT NULL REFERENCES workspaces(id),
			user_id TEXT NOT NULL REFERENCES users(id),
			role TEXT NOT NULL DEFAULT 'member',
			PRIMARY KEY(workspace_id, user_id)
		);
		CREATE TABLE IF NOT EXISTS channels (
			id TEXT PRIMARY KEY,
			workspace_id TEXT NOT NULL REFERENCES workspaces(id),
			name TEXT NOT NULL,
			type TEXT NOT NULL DEFAULT 'group',
			db_path TEXT NOT NULL,
			default_agent TEXT,
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS daemons (
			id TEXT PRIMARY KEY,
			owner_id TEXT NOT NULL REFERENCES users(id),
			name TEXT NOT NULL,
			api_key_hash TEXT NOT NULL,
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS daemon_channels (
			daemon_id TEXT NOT NULL REFERENCES daemons(id) ON DELETE CASCADE,
			channel_id TEXT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
			PRIMARY KEY(daemon_id, channel_id)
		);
		-- channel_actors: a channel's DESIRED actor-instance set (composition /
		-- "spec"), the canonical writer for what a channel should run.
		-- One row = one instance = (class) + spec.
		-- This is INTENT, never live truth: "who is actually running" is the
		-- substrate's actor_registry (read via Home.View().ListActors), never this
		-- table. default_agent is a name-agnostic pointer INTO this set.
		--
		-- placement = which host CLASS runs the instance ('server' = server-embedded
		-- cell, 'daemon' = a connected daemon). desired_host = which specific
		-- daemon INSTANCE claims a 'daemon' row (''=unassigned pool). Two-level
		-- invariant (enforced at the write face): placement='server' ⟹
		-- desired_host=''; placement='daemon' AND desired_host='' = a legal
		-- unassigned pool row (delivered to NO daemon). /compute/plan filters a
		-- daemon's assignment on desired_host = its own id (G4: two daemons on one
		-- channel each pull only their own rows). desired_host (intent) is a
		-- separate plane from the embodiment fact (membership Host); never join
		-- them to judge liveness.
		CREATE TABLE IF NOT EXISTS channel_actors (
			channel_id  TEXT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
			instance_id TEXT NOT NULL,
			principal   TEXT NOT NULL DEFAULT '',
			class       TEXT NOT NULL,
			config_json TEXT,
			placement   TEXT NOT NULL DEFAULT 'server',
			desired_host TEXT NOT NULL DEFAULT '',
			PRIMARY KEY(channel_id, instance_id)
		);
		-- actor_decls: global actor-instance declarations. One row per declared
		-- instance, cross-channel (key = id): identity + class + config + owner +
		-- visibility. This is the declaration layer EVERY declared actor instance
		-- (agent, tool, ...) passes through to enter a channel — the mechanism is
		-- kind-neutral ("kind 卸重: 法只认 id+凭据+门"). 'default_class' = the
		-- instance's DEFAULT engine class (a create-time preference, NOT runtime
		-- truth): the per-channel concrete engine is channel_actors.class
		-- (= override ?? default_class). The engine IS the actor class
		-- (claude/go-kimi/echo are flat registry classes; there is NO umbrella
		-- "agent" class). config_json = the global identity body (persona/skills +
		-- engine knobs), layered UNDER channel_actors' per-channel config_json.
		-- Distinct from users (responsibility owner, never a declaration).
		--
		-- No "scope" column (cognitive-state scope) BY DESIGN — v1 is implicitly
		-- channel-scoped: each instance's cognitive state is per-channel isolated,
		-- NOT shared across channels. entity-scoped (one
		-- shared memory/persona across an instance's channels = "unified") is v2,
		-- added additively with the memory subsystem (then: ALTER TABLE ADD COLUMN
		-- scope). Do NOT read per-channel as permanent truth: the declared IDENTITY
		-- is global (one row here spans every channel) — only the cognitive STATE is
		-- isolated in v1. A scope column now would be a single-valued placeholder
		-- pointing at an unbuilt subsystem, so it is deliberately omitted until
		-- needed.
		--
		-- Instance ids keep their historical 'agent:' namespace prefix in
		-- channel_actors / membership / truth logs (persistent names stay constant;
		-- the prefix carries no classification weight). Renaming the prefix would be
		-- a truth-data migration — deliberately NOT done here.
		CREATE TABLE IF NOT EXISTS actor_decls (
			id          TEXT PRIMARY KEY,
			name        TEXT NOT NULL,
			owner          TEXT NOT NULL REFERENCES users(id),
			default_class  TEXT NOT NULL,
			config_json TEXT,
			deleted_at  INTEGER,
			created_at  INTEGER NOT NULL,
			updated_at  INTEGER NOT NULL,
			visibility  TEXT NOT NULL DEFAULT 'private'
		);
	`)
	if err != nil {
		return err
	}
	// Drop the dead daemon liveness columns (status/hostname/platform/
	// last_heartbeat): attachment is volatile link state, read live from the
	// platform View — never a persisted directory column (it only ever lied).
	// Best-effort per column: a fresh DB created above never had them (no such
	// column → ignore); an existing dev DB gets them dropped, rows preserved.
	for _, col := range []string{"status", "hostname", "platform", "last_heartbeat"} {
		_, _ = db.Exec(`ALTER TABLE daemons DROP COLUMN ` + col)
	}
	// channels.default_agent: a name-agnostic pointer into the channel's
	// composition (channel_actors), defaulting to the agent:boost fallback
	// instance. Best-effort add for an existing dev DB.
	_, _ = db.Exec(`ALTER TABLE channels ADD COLUMN default_agent TEXT`)

	// channel_actors.placement: best-effort add for a DB whose channel_actors was
	// created before the column existed (a fresh CREATE above already has it).
	_, _ = db.Exec(`ALTER TABLE channel_actors ADD COLUMN placement TEXT NOT NULL DEFAULT 'server'`)
	_, _ = db.Exec(`ALTER TABLE channel_actors ADD COLUMN principal TEXT NOT NULL DEFAULT ''`)

	// channel_actors.desired_host: which specific daemon instance claims a
	// 'daemon' row (''=unassigned pool). Two-level invariant with placement,
	// enforced at the write face. additive best-effort add for a dev DB.
	_, _ = db.Exec(`ALTER TABLE channel_actors ADD COLUMN desired_host TEXT NOT NULL DEFAULT ''`)

	// Column-rename chain, ordered oldest-vintage first (each step best-effort:
	// a fresh CREATE above already has default_class, so both renames no-op on
	// the missing column):
	//   looper → default_looper (pre-class-split vintage)
	//   default_looper → default_class (the looper→class vocabulary rename)
	_, _ = db.Exec(`ALTER TABLE actor_decls RENAME COLUMN looper TO default_looper`)
	_, _ = db.Exec(`ALTER TABLE actor_decls RENAME COLUMN default_looper TO default_class`)

	// actor_decls.visibility: reference-eligibility axis, orthogonal to owner
	// (management authority). private = only owner may introduce; public = any
	// member. additive best-effort add for a dev DB.
	_, _ = db.Exec(`ALTER TABLE actor_decls ADD COLUMN visibility TEXT NOT NULL DEFAULT 'private'`)

	// Backfill channel_actors.class from the old placeholder shell value 'agent' to
	// the REAL engine class (engine = class now; there is no "agent" class):
	//   1. instances with a declaration → their actor_decls.default_class.
	//   2. remaining 'agent' shells (e.g. agent:boost, no declaration) → boost engine.
	_, _ = db.Exec(`UPDATE channel_actors
		SET class = (SELECT default_class FROM actor_decls WHERE 'agent:' || actor_decls.id = channel_actors.instance_id)
		WHERE class = 'agent' AND instance_id IN (SELECT 'agent:' || id FROM actor_decls)`)
	_, _ = db.Exec(`UPDATE channel_actors SET class = 'go-kimi' WHERE class = 'agent'`)

	// Backfill existing channels to the composition model (don't clear data):
	//   1. seed an agent:boost row for any channel that lacks ONE (not just
	//      channels with no rows at all — a channel with other composition but no
	//      boost would otherwise get default_agent=agent:boost with no matching
	//      instance to spawn). placement='server' = the server-embedded fallback.
	//      class = 'go-kimi' = the boost engine (engine IS the class).
	//   2. migrate the old hardcoded pointer agent:main → agent:boost (and fill a
	//      NULL pointer) so default_agent points at the seeded instance.
	_, _ = db.Exec(`INSERT OR IGNORE INTO channel_actors (channel_id, instance_id, principal, class, placement)
		SELECT id, 'agent:boost', 'boost', 'go-kimi', 'server' FROM channels
		WHERE id NOT IN (SELECT channel_id FROM channel_actors WHERE instance_id = 'agent:boost')`)
	_, _ = db.Exec(`UPDATE channels SET default_agent = 'agent:boost'
		WHERE default_agent IS NULL OR default_agent = 'agent:main'`)
	return nil
}
