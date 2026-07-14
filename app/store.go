package app

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

// OpenDB opens the app-level SQLite database (identity, workspace, channel
// catalog, daemon registry). This is NOT channel truth -- each channel's
// message log lives in its own DB managed by home.Home.
func OpenDB(path string) (*sql.DB, error) {
	// modernc.org/sqlite reads pragmas via the _pragma= DSN form (applied per
	// connection, so they hold across the database/sql pool). foreign_keys(1) is
	// required for the ON DELETE CASCADE on daemon_channels and other app tables to
	// actually fire — SQLite leaves FK enforcement OFF by default.
	db, err := sql.Open("sqlite", path+"?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		return nil, fmt.Errorf("app: open db: %w", err)
	}
	if err := initializeSchema(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("app: initialize schema: %w", err)
	}
	return db, nil
}

// initializeSchema installs the one supported app schema. The product has not
// shipped and has no legacy databases, so startup deliberately performs no
// rename, ALTER, backfill, or data-copy migration. CREATE IF NOT EXISTS keeps
// ordinary same-version restarts idempotent; schema evolution begins only once
// a released database version actually exists.
func initializeSchema(db *sql.DB) error {
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
		CREATE TABLE IF NOT EXISTS decl_fanout_jobs (
			job_id       INTEGER PRIMARY KEY AUTOINCREMENT,
			decl_id      TEXT NOT NULL,
			op           TEXT NOT NULL CHECK(op IN ('delete','restart')),
			initiator    TEXT NOT NULL,
			targets_json TEXT NOT NULL,
			attempt      INTEGER NOT NULL DEFAULT 0,
			last_error   TEXT,
			created_at   INTEGER NOT NULL,
			done_at      INTEGER
		);
		CREATE UNIQUE INDEX IF NOT EXISTS ux_decl_jobs_dedup
			ON decl_fanout_jobs(decl_id, op) WHERE done_at IS NULL AND op='delete';
		CREATE INDEX IF NOT EXISTS ix_decl_jobs_pending
			ON decl_fanout_jobs(decl_id) WHERE done_at IS NULL;

		CREATE TABLE IF NOT EXISTS daemon_revoke_jobs (
			job_id       INTEGER PRIMARY KEY AUTOINCREMENT,
			daemon_id    TEXT NOT NULL,
			op           TEXT NOT NULL CHECK(op IN ('delete','detach')),
			targets_json TEXT NOT NULL,
			attempt      INTEGER NOT NULL DEFAULT 0,
			last_error   TEXT,
			created_at   INTEGER NOT NULL,
			done_at      INTEGER
		);
		CREATE INDEX IF NOT EXISTS ix_daemon_jobs_pending
			ON daemon_revoke_jobs(daemon_id) WHERE done_at IS NULL;
		-- actor_decls: global actor-instance declarations. One row per declared
		-- instance, cross-channel (key = id): identity + class + config + owner +
		-- visibility. This is the declaration layer EVERY declared actor instance
		-- (agent, tool, ...) passes through to enter a channel — the mechanism is
		-- kind-neutral ("kind 卸重: 法只认 id+凭据+门"). 'default_class' = the
		-- instance's DEFAULT engine class (a create-time preference, NOT runtime
		-- truth): the per-channel concrete engine lives in channel-local composition
		-- (= override ?? default_class). The engine IS the actor class
		-- (claude/go-kimi/echo are flat registry classes; there is NO umbrella
		-- "agent" class). config_json = the global identity body (persona/skills +
		-- engine knobs), layered UNDER the channel-local composition config.
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
		-- channel composition / membership / truth logs (persistent names stay constant;
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
	return err
}
