package app

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

func openDB(path string, initialize bool) (*sql.DB, error) {
	// modernc.org/sqlite reads pragmas via the _pragma= DSN form (applied per
	// connection, so they hold across the database/sql pool). foreign_keys(1) is
	// required for the ON DELETE CASCADE on app projection tables to
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

type schemaObject struct {
	typ  string
	name string
	sql  string
}

// appSchema is the single schema authority used by both exclusive init and
// strict reopen verification. Exact object SQL covers column order/types,
// PK/FK/NOT NULL/default/CHECK constraints, index uniqueness and predicates.
var appSchema = []schemaObject{
	{"table", "users", `CREATE TABLE users (
		id TEXT PRIMARY KEY,
		email TEXT UNIQUE NOT NULL,
		password TEXT NOT NULL,
		display_name TEXT,
		created_at INTEGER NOT NULL
	)`},
	{"table", "sessions", `CREATE TABLE sessions (
		token TEXT PRIMARY KEY,
		user_id TEXT NOT NULL REFERENCES users(id),
		created_at INTEGER NOT NULL,
		expires_at INTEGER NOT NULL
	)`},
	{"table", "channels", `CREATE TABLE channels (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		type TEXT NOT NULL,
		status TEXT NOT NULL CHECK(status IN ('present','retiring')),
		owner_principal TEXT NOT NULL,
		spec_json TEXT NOT NULL,
		created_at INTEGER NOT NULL,
		parent_id TEXT
	)`},
	{"index", "ux_channels_present_name", `CREATE UNIQUE INDEX ux_channels_present_name
		ON channels(name) WHERE status='present'`},
	{"table", "principal_channels", `CREATE TABLE principal_channels (
		principal TEXT NOT NULL,
		channel_id TEXT NOT NULL,
		actor_id TEXT NOT NULL,
		updated_at INTEGER NOT NULL,
		PRIMARY KEY (principal, channel_id)
	)`},
	{"index", "ix_principal_channels_channel", `CREATE INDEX ix_principal_channels_channel
		ON principal_channels(channel_id)`},
	{"table", "channel_decl_instances", `CREATE TABLE channel_decl_instances (
		channel_id TEXT NOT NULL,
		decl_id TEXT NOT NULL,
		actor_id TEXT NOT NULL,
		updated_at INTEGER NOT NULL,
		PRIMARY KEY (channel_id, actor_id)
	)`},
	{"index", "ix_channel_decl_instances_decl", `CREATE INDEX ix_channel_decl_instances_decl
		ON channel_decl_instances(decl_id)`},
	{"table", "daemon_channels", `CREATE TABLE daemon_channels (
		channel_id TEXT NOT NULL,
		daemon_id TEXT NOT NULL,
		updated_at INTEGER NOT NULL,
		PRIMARY KEY (channel_id, daemon_id)
	)`},
	{"index", "ix_daemon_channels_daemon", `CREATE INDEX ix_daemon_channels_daemon
		ON daemon_channels(daemon_id)`},
	{"table", "channel_decl_overlays", `CREATE TABLE channel_decl_overlays (
		channel_id TEXT NOT NULL,
		decl_id TEXT NOT NULL,
		config_json TEXT,
		updated_at INTEGER NOT NULL,
		PRIMARY KEY (channel_id, decl_id)
	)`},
	{"table", "daemons", `CREATE TABLE daemons (
		id TEXT PRIMARY KEY,
		owner_id TEXT NOT NULL REFERENCES users(id),
		name TEXT NOT NULL,
		api_key TEXT NOT NULL,
		deleted_at INTEGER,
		created_at INTEGER NOT NULL
	)`},
	{"table", "actor_decls", `CREATE TABLE actor_decls (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		owner TEXT NOT NULL,
		default_class TEXT NOT NULL,
		config_json TEXT,
		deleted_at INTEGER,
		created_at INTEGER NOT NULL,
		updated_at INTEGER NOT NULL,
		visibility TEXT NOT NULL DEFAULT 'private'
	)`},
}

func verifySchema(db *sql.DB) error {
	want := make(map[string]schemaObject, len(appSchema))
	for _, object := range appSchema {
		want[object.name] = object
	}
	rows, err := db.Query(`SELECT type,name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'`)
	if err != nil {
		return err
	}
	defer rows.Close()
	seen := make(map[string]bool, len(want))
	for rows.Next() {
		var typ, name, ddl string
		if err := rows.Scan(&typ, &name, &ddl); err != nil {
			return err
		}
		expected, ok := want[name]
		if !ok {
			return fmt.Errorf("unexpected %s %q", typ, name)
		}
		if typ != expected.typ {
			return fmt.Errorf("object %q type = %s, want %s", name, typ, expected.typ)
		}
		if normalizeSchemaSQL(ddl) != normalizeSchemaSQL(expected.sql) {
			return fmt.Errorf("object %q definition does not match current schema", name)
		}
		seen[name] = true
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for name := range want {
		if !seen[name] {
			return fmt.Errorf("missing schema object %q", name)
		}
	}
	return nil
}

func normalizeSchemaSQL(ddl string) string {
	return strings.TrimSuffix(strings.Join(strings.Fields(ddl), " "), ";")
}

// initializeSchema installs the one supported app schema into a path that
// OpenProcessDB has just created exclusively. Reopen never calls this function.
func initializeSchema(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, object := range appSchema {
		if _, err := tx.Exec(object.sql); err != nil {
			return fmt.Errorf("create %s %s: %w", object.typ, object.name, err)
		}
	}
	now := time.Now().UnixMilli()
	if _, err := tx.Exec(`INSERT INTO actor_decls(id,name,owner,default_class,config_json,created_at,updated_at,visibility)
		VALUES (?,?,?,?,?,?,?,'public')`, realmToolDeclID, "Realm Tool", "system", realmToolClass, `{}`, now, now); err != nil {
		return fmt.Errorf("seed realm tool declaration: %w", err)
	}
	return tx.Commit()
}
