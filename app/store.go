package app

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	_ "modernc.org/sqlite"
)

func openDB(path string, initialize bool) (*sql.DB, error) {
	// modernc.org/sqlite reads pragmas via the _pragma= DSN form (applied per
	// connection, so they hold across the database/sql pool). foreign_keys(1) is
	// required for the ON DELETE CASCADE on daemon_channels and other app tables to
	// actually fire — SQLite leaves FK enforcement OFF by default.
	dsn := (&url.URL{Scheme: "file", Path: path}).String()
	if !initialize {
		dsn += "?mode=rw&"
	} else {
		dsn += "?"
	}
	dsn += "_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"
	if initialize {
		dsn += "&_pragma=journal_mode(WAL)"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("app: open db: %w", err)
	}
	if initialize {
		err = initializeSchema(db)
	}
	if err == nil {
		err = verifySchema(db)
	}
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("app: verify schema: %w", err)
	}
	return db, nil
}

var appSchemaColumns = map[string][]string{
	"users":              {"id:TEXT", "email:TEXT", "password:TEXT", "display_name:TEXT", "created_at:INTEGER"},
	"sessions":           {"token:TEXT", "user_id:TEXT", "created_at:INTEGER", "expires_at:INTEGER"},
	"workspaces":         {"id:TEXT", "owner_id:TEXT", "name:TEXT", "created_at:INTEGER"},
	"workspace_members":  {"workspace_id:TEXT", "user_id:TEXT", "role:TEXT"},
	"channels":           {"id:TEXT", "workspace_id:TEXT", "name:TEXT", "type:TEXT", "db_path:TEXT", "created_at:INTEGER"},
	"daemons":            {"id:TEXT", "owner_id:TEXT", "name:TEXT", "api_key_hash:TEXT", "created_at:INTEGER"},
	"daemon_channels":    {"daemon_id:TEXT", "channel_id:TEXT"},
	"decl_fanout_jobs":   {"job_id:INTEGER", "decl_id:TEXT", "op:TEXT", "initiator:TEXT", "targets_json:TEXT", "attempt:INTEGER", "last_error:TEXT", "created_at:INTEGER", "done_at:INTEGER"},
	"daemon_revoke_jobs": {"job_id:INTEGER", "daemon_id:TEXT", "op:TEXT", "targets_json:TEXT", "attempt:INTEGER", "last_error:TEXT", "created_at:INTEGER", "done_at:INTEGER"},
	"actor_decls":        {"id:TEXT", "name:TEXT", "owner:TEXT", "default_class:TEXT", "config_json:TEXT", "deleted_at:INTEGER", "created_at:INTEGER", "updated_at:INTEGER", "visibility:TEXT"},
}

func verifySchema(db *sql.DB) error {
	requiredIndexes := map[string]bool{
		"ux_decl_jobs_dedup":     false,
		"ix_decl_jobs_pending":   false,
		"ix_daemon_jobs_pending": false,
	}
	rows, err := db.Query(`SELECT type,name FROM sqlite_master WHERE type IN ('table','index')`)
	if err != nil {
		return err
	}
	defer rows.Close()
	seenTables := map[string]bool{}
	for rows.Next() {
		var typ, name string
		if err := rows.Scan(&typ, &name); err != nil {
			return err
		}
		if strings.HasPrefix(name, "sqlite_autoindex_") || name == "sqlite_sequence" {
			continue
		}
		switch typ {
		case "table":
			if _, ok := appSchemaColumns[name]; !ok {
				return fmt.Errorf("unexpected table %q", name)
			}
			seenTables[name] = true
		case "index":
			if _, ok := requiredIndexes[name]; !ok {
				return fmt.Errorf("unexpected index %q", name)
			}
			requiredIndexes[name] = true
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for table, want := range appSchemaColumns {
		if !seenTables[table] {
			return fmt.Errorf("missing table %q", table)
		}
		got, err := schemaColumns(db, table)
		if err != nil {
			return err
		}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			return fmt.Errorf("table %s columns = %v, want %v", table, got, want)
		}
	}
	for name, seen := range requiredIndexes {
		if !seen {
			return fmt.Errorf("missing index %q", name)
		}
	}
	return nil
}

func schemaColumns(db *sql.DB, table string) ([]string, error) {
	rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var cid, notnull, pk int
		var name, typ string
		var def any
		if err := rows.Scan(&cid, &name, &typ, &notnull, &def, &pk); err != nil {
			return nil, err
		}
		out = append(out, name+":"+strings.ToUpper(typ))
	}
	return out, rows.Err()
}

// initializeSchema installs the one supported app schema into a path that
// OpenProcessDB has just created exclusively. Reopen never calls this function.
func initializeSchema(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			email TEXT UNIQUE NOT NULL,
			password TEXT NOT NULL,
			display_name TEXT,
			created_at INTEGER NOT NULL
		);
		CREATE TABLE sessions (
			token TEXT PRIMARY KEY,
			user_id TEXT NOT NULL REFERENCES users(id),
			created_at INTEGER NOT NULL,
			expires_at INTEGER NOT NULL
		);
		CREATE TABLE workspaces (
			id TEXT PRIMARY KEY,
			owner_id TEXT NOT NULL REFERENCES users(id),
			name TEXT NOT NULL,
			created_at INTEGER NOT NULL
		);
		CREATE TABLE workspace_members (
			workspace_id TEXT NOT NULL REFERENCES workspaces(id),
			user_id TEXT NOT NULL REFERENCES users(id),
			role TEXT NOT NULL DEFAULT 'member',
			PRIMARY KEY(workspace_id, user_id)
		);
		CREATE TABLE channels (
			id TEXT PRIMARY KEY,
			workspace_id TEXT NOT NULL REFERENCES workspaces(id),
			name TEXT NOT NULL,
			type TEXT NOT NULL DEFAULT 'group',
			db_path TEXT NOT NULL,
			created_at INTEGER NOT NULL
		);
		CREATE TABLE daemons (
			id TEXT PRIMARY KEY,
			owner_id TEXT NOT NULL REFERENCES users(id),
			name TEXT NOT NULL,
			api_key_hash TEXT NOT NULL,
			created_at INTEGER NOT NULL
		);
		CREATE TABLE daemon_channels (
			daemon_id TEXT NOT NULL REFERENCES daemons(id) ON DELETE CASCADE,
			channel_id TEXT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
			PRIMARY KEY(daemon_id, channel_id)
		);
		CREATE TABLE decl_fanout_jobs (
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
		CREATE UNIQUE INDEX ux_decl_jobs_dedup
			ON decl_fanout_jobs(decl_id, op) WHERE done_at IS NULL AND op='delete';
		CREATE INDEX ix_decl_jobs_pending
			ON decl_fanout_jobs(decl_id) WHERE done_at IS NULL;

		CREATE TABLE daemon_revoke_jobs (
			job_id       INTEGER PRIMARY KEY AUTOINCREMENT,
			daemon_id    TEXT NOT NULL,
			op           TEXT NOT NULL CHECK(op IN ('delete','detach')),
			targets_json TEXT NOT NULL,
			attempt      INTEGER NOT NULL DEFAULT 0,
			last_error   TEXT,
			created_at   INTEGER NOT NULL,
			done_at      INTEGER
		);
		CREATE INDEX ix_daemon_jobs_pending
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
		CREATE TABLE actor_decls (
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
