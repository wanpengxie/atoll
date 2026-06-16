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
	db, err := sql.Open("sqlite", path+"?_journal_mode=WAL&_busy_timeout=5000")
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
	// channels.default_agent: set only when an agent is assembled into the channel
	// (see app.spawnBuiltinAgent). Empty = no designated brain = the channel runs
	// the group-chat routing policy. Best-effort add for an existing dev DB.
	_, _ = db.Exec(`ALTER TABLE channels ADD COLUMN default_agent TEXT`)
	return nil
}
