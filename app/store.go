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

func migrate(db *sql.DB) error {
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
		-- "spec"), the canonical writer for what a channel should run
		-- (actor-instance-model §3/§8). One row = one instance = (class) + spec.
		-- This is INTENT, never live truth: "who is actually running" is the
		-- substrate's actor_registry (read via Home.View().ListActors), never this
		-- table. default_agent is a name-agnostic pointer INTO this set.
		--
		-- placement = which host runs the instance ('server' = server-embedded
		-- cell, 'daemon' = a connected daemon). The server spawns ONLY its
		-- 'server' rows (spawnComposition filters on it); 'daemon' rows are for
		-- daemon hosts to claim via server→daemon composition delivery (that
		-- delivery is the additive next step — the column completes the data shape
		-- now so adding it needs no migration).
		CREATE TABLE IF NOT EXISTS channel_actors (
			channel_id  TEXT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
			instance_id TEXT NOT NULL,
			class       TEXT NOT NULL,
			config_json TEXT,
			placement   TEXT NOT NULL DEFAULT 'server',
			state       TEXT,
			PRIMARY KEY(channel_id, instance_id)
		);
		-- agents: global agent IDENTITY declarations (agent-spec §二). One row per
		-- agent, cross-channel (key = id). 'default_looper' = the agent's DEFAULT
		-- engine (a create-time preference, NOT runtime truth): the per-channel
		-- concrete engine is channel_actors.class (= override ?? default_looper).
		-- The engine IS the actor class (claude/go-kimi are flat registry classes,
		-- kind=agent; there is NO umbrella "agent" class). config_json = the global
		-- identity body (persona/skills + engine knobs), layered UNDER
		-- channel_actors' per-channel config_json. Distinct from users
		-- (responsibility owner, never an agent). See agent-kind-vs-class §7.
		--
		-- No "scope" column (cognitive-state scope) BY DESIGN — v1 is implicitly
		-- channel-scoped: each agent's state is per-channel isolated
		-- (channel_actors.state), NOT shared across channels. entity-scoped (one
		-- shared memory/persona across an agent's channels = "unified") is v2, added
		-- additively with the memory subsystem (then: ALTER TABLE ADD COLUMN scope).
		-- Do NOT read per-channel as permanent truth: the agent IDENTITY is global
		-- (one row here spans every channel) — only the cognitive STATE is isolated
		-- in v1. A scope column now would be a single-valued placeholder pointing at
		-- an unbuilt subsystem (零预留 #4). See actor-instance-model §8.4 (state-root
		-- scope-key) + agent-spec §二.
		CREATE TABLE IF NOT EXISTS agents (
			id          TEXT PRIMARY KEY,
			name        TEXT NOT NULL,
			owner          TEXT NOT NULL REFERENCES users(id),
			default_looper TEXT NOT NULL,
			config_json TEXT,
			deleted_at  INTEGER,
			created_at  INTEGER NOT NULL,
			updated_at  INTEGER NOT NULL
		);
	`)
	if err != nil {
		return err
	}
	// Drop the dead daemon liveness columns (status/hostname/platform/
	// last_heartbeat): presence is volatile L1 link state, read live from the
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

	// channel_actors.state: per-instance looper-opaque checkpoint slot (durable
	// resume; agent-spec §二/§三). The looper is its only author — external
	// control never writes it directly. additive best-effort add for a dev DB.
	_, _ = db.Exec(`ALTER TABLE channel_actors ADD COLUMN state TEXT`)

	// agents.looper → default_looper (agent-kind-vs-class §7): the engine is now a
	// per-channel concrete actor class (channel_actors.class); agents keeps only the
	// create-time DEFAULT. Best-effort rename for an existing dev DB (a fresh CREATE
	// above already has default_looper; the rename then no-ops on the missing column).
	_, _ = db.Exec(`ALTER TABLE agents RENAME COLUMN looper TO default_looper`)

	// Backfill channel_actors.class from the old placeholder shell value 'agent' to
	// the REAL engine class (engine = class now; there is no "agent" class):
	//   1. agent instances with a declaration → their agents.default_looper.
	//   2. remaining 'agent' shells (e.g. agent:boost, no declaration) → boost engine.
	_, _ = db.Exec(`UPDATE channel_actors
		SET class = (SELECT default_looper FROM agents WHERE 'agent:' || agents.id = channel_actors.instance_id)
		WHERE class = 'agent' AND instance_id IN (SELECT 'agent:' || id FROM agents)`)
	_, _ = db.Exec(`UPDATE channel_actors SET class = 'go-kimi' WHERE class = 'agent'`)

	// Backfill existing channels to the composition model (don't clear data):
	//   1. seed an agent:boost row for any channel that lacks ONE (not just
	//      channels with no rows at all — a channel with other composition but no
	//      boost would otherwise get default_agent=agent:boost with no matching
	//      instance to spawn). placement='server' = the server-embedded fallback.
	//      class = 'go-kimi' = the boost engine (engine IS the class).
	//   2. migrate the old hardcoded pointer agent:main → agent:boost (and fill a
	//      NULL pointer) so default_agent points at the seeded instance.
	_, _ = db.Exec(`INSERT OR IGNORE INTO channel_actors (channel_id, instance_id, class, placement)
		SELECT id, 'agent:boost', 'go-kimi', 'server' FROM channels
		WHERE id NOT IN (SELECT channel_id FROM channel_actors WHERE instance_id = 'agent:boost')`)
	_, _ = db.Exec(`UPDATE channels SET default_agent = 'agent:boost'
		WHERE default_agent IS NULL OR default_agent = 'agent:main'`)
	return nil
}
