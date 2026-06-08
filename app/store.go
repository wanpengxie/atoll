package app

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

// OpenDB opens the app-level SQLite database (identity, workspace, channel
// catalog, daemon registry). This is NOT channel truth -- each channel's
// message log lives in its own DB managed by platform.ChannelHome.
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
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS daemons (
			id TEXT PRIMARY KEY,
			owner_id TEXT NOT NULL REFERENCES users(id),
			name TEXT NOT NULL,
			api_key_hash TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'offline',
			hostname TEXT,
			platform TEXT,
			last_heartbeat INTEGER,
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS daemon_channels (
			daemon_id TEXT NOT NULL REFERENCES daemons(id) ON DELETE CASCADE,
			channel_id TEXT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
			PRIMARY KEY(daemon_id, channel_id)
		);
	`)
	return err
}
