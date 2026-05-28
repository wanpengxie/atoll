package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite" // register driver
)

// OpenOptions tunes DSN-level pragmas. Zero value is fine for production.
type OpenOptions struct {
	// ReadOnly opens the database in read-only mode (mode=ro). Used by
	// scheduler / trigger gateways that only need to scan.
	ReadOnly bool

	// SkipDDL skips the schema bootstrap step. Useful for tests that
	// install custom DDL or open an existing file.
	SkipDDL bool

	// ExtraPragmas appends additional `PRAGMA key=val` statements after
	// the standard WAL + foreign_keys + busy_timeout block. Each entry is
	// a full SQL statement WITHOUT a trailing semicolon. Order is preserved.
	ExtraPragmas []string
}

// OpenChannel opens (creating if absent) the per-channel messages.sqlite
// at dbPath, runs ChannelLocalDDL, and returns *sql.DB ready for use.
//
// Pragmas applied (per L2 §1.4 sqlite tuning):
//
//	PRAGMA journal_mode=WAL
//	PRAGMA synchronous=NORMAL
//	PRAGMA foreign_keys=ON
//	PRAGMA busy_timeout=5000
func OpenChannel(ctx context.Context, dbPath string, opts OpenOptions) (*sql.DB, error) {
	db, err := openSqlite(ctx, dbPath, opts, ChannelLocalDDL)
	if err != nil {
		return nil, err
	}
	// Schema migrations are idempotent (column-add guarded by columnExists)
	// and MUST run on every writable open — including SkipDDL=true callers
	// (runtime/daemon.go caches existing channel sqlites with SkipDDL=true
	// to skip the CREATE TABLE baseline, but those caches need new columns
	// like actor_registry.ready_state to materialise on old channels).
	// Only ReadOnly skips, since migrations write DDL.
	if !opts.ReadOnly {
		if err := ensureChannelLocalSchema(ctx, db); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	return db, nil
}

// OpenDaemon opens (creating if absent) the daemon-level sqlite at
// dbPath, runs DaemonLocalDDL, and returns *sql.DB.
func OpenDaemon(ctx context.Context, dbPath string, opts OpenOptions) (*sql.DB, error) {
	db, err := openSqlite(ctx, dbPath, opts, DaemonLocalDDL)
	if err != nil {
		return nil, err
	}
	if !opts.SkipDDL && !opts.ReadOnly {
		if err := ensureDaemonLocalSchema(ctx, db); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	return db, nil
}

func openSqlite(ctx context.Context, dbPath string, opts OpenOptions, ddl string) (*sql.DB, error) {
	if dbPath == "" {
		return nil, errors.New("store: dbPath empty")
	}
	if dir := filepath.Dir(dbPath); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("store: mkdir %q: %w", dir, err)
		}
	}

	dsn := dbPath
	if opts.ReadOnly {
		dsn = fmt.Sprintf("file:%s?mode=ro&_pragma=busy_timeout(5000)", dbPath)
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open %q: %w", dbPath, err)
	}

	// modernc.org/sqlite is single-connection-safe; cap pool to 1 for
	// channel sqlite so WAL writers/readers don't fight pragma state.
	// Daemon-level sqlite has the same property.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := applyPragmas(ctx, db, opts); err != nil {
		_ = db.Close()
		return nil, err
	}

	if !opts.SkipDDL && !opts.ReadOnly {
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("store: exec DDL on %q: %w", dbPath, err)
		}
	}

	return db, nil
}

func applyPragmas(ctx context.Context, db *sql.DB, opts OpenOptions) error {
	base := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000",
	}
	if opts.ReadOnly {
		// WAL pragma not applicable to read-only; foreign_keys still useful.
		base = []string{
			"PRAGMA foreign_keys=ON",
			"PRAGMA busy_timeout=5000",
		}
	}
	all := make([]string, 0, len(base)+len(opts.ExtraPragmas))
	all = append(all, base...)
	all = append(all, opts.ExtraPragmas...)
	for _, p := range all {
		if _, err := db.ExecContext(ctx, p); err != nil {
			return fmt.Errorf("store: %s: %w", p, err)
		}
	}
	return nil
}

func ensureDaemonLocalSchema(ctx context.Context, db *sql.DB) error {
	adds := map[string]string{
		"phase":                   "phase TEXT NOT NULL DEFAULT 'sent' CHECK (phase IN ('sent','awaiting_ack','partial_takeover','completed','abandoned'))",
		"sent_at":                 "sent_at INTEGER NOT NULL DEFAULT 0",
		"expected_ack_frame_kind": "expected_ack_frame_kind TEXT NOT NULL DEFAULT 'control.create_channel_ack'",
		"terminal_status":         "terminal_status TEXT NOT NULL DEFAULT ''",
		"abandonment_reason":      "abandonment_reason TEXT NOT NULL DEFAULT ''",
		"attempt_count":           "attempt_count INTEGER NOT NULL DEFAULT 0",
		"last_attempt_at":         "last_attempt_at INTEGER NOT NULL DEFAULT 0",
	}
	for col, ddl := range adds {
		ok, err := columnExists(ctx, db, "bootstrap_registry", col)
		if err != nil {
			return err
		}
		if ok {
			continue
		}
		if _, err := db.ExecContext(ctx, "ALTER TABLE bootstrap_registry ADD COLUMN "+ddl); err != nil {
			return fmt.Errorf("store: add bootstrap_registry.%s: %w", col, err)
		}
	}
	return nil
}

func ensureChannelLocalSchema(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS type_registry_pending (
		  install_attempt_id      TEXT PRIMARY KEY,
		  type                    TEXT NOT NULL UNIQUE,
		  allowed_kinds           TEXT NOT NULL,
		  handler_binding         TEXT NOT NULL
		                          CHECK (handler_binding IN ('embedded','runtime_outbound','runtime_inbound_via_relay')),
		  terminal_convention     TEXT NOT NULL DEFAULT 'payload_status'
		                          CHECK (terminal_convention IN ('payload_status','single-response')),
		  max_pending_ms          INTEGER,
		  handler_actor_id        TEXT,
		  install_status          TEXT NOT NULL DEFAULT 'installing'
		                          CHECK (install_status IN ('installing','failed')),
		  install_error           TEXT NOT NULL DEFAULT '',
		  created_at              INTEGER NOT NULL
		);
		CREATE INDEX IF NOT EXISTS ix_type_registry_pending_type_status
		  ON type_registry_pending(type, install_status);
	`); err != nil {
		return fmt.Errorf("store: ensure type_registry_pending: %w", err)
	}
	messageAdds := map[string]string{
		"cross_channel_refs": "cross_channel_refs TEXT",
	}
	for col, ddl := range messageAdds {
		ok, err := columnExists(ctx, db, "messages", col)
		if err != nil {
			return err
		}
		if ok {
			continue
		}
		if _, err := db.ExecContext(ctx, "ALTER TABLE messages ADD COLUMN "+ddl); err != nil {
			return fmt.Errorf("store: add messages.%s: %w", col, err)
		}
	}
	adds := map[string]string{
		"install_status": "install_status TEXT NOT NULL DEFAULT 'installed' CHECK (install_status IN ('installing','installed','failed'))",
		"install_error":  "install_error TEXT NOT NULL DEFAULT ''",
	}
	for col, ddl := range adds {
		ok, err := columnExists(ctx, db, "type_registry", col)
		if err != nil {
			return err
		}
		if ok {
			continue
		}
		if _, err := db.ExecContext(ctx, "ALTER TABLE type_registry ADD COLUMN "+ddl); err != nil {
			return fmt.Errorf("store: add type_registry.%s: %w", col, err)
		}
	}
	actorAdds := map[string]string{
		"ready_state":          "ready_state TEXT NOT NULL DEFAULT 'unknown' CHECK (ready_state IN ('ready','not_ready','unknown'))",
		"ready_reason":         "ready_reason TEXT NOT NULL DEFAULT 'unknown'",
		"ready_detail":         "ready_detail TEXT NOT NULL DEFAULT '{}'",
		"last_ready_at":        "last_ready_at INTEGER NOT NULL DEFAULT 0",
		"last_state_change_at": "last_state_change_at INTEGER NOT NULL DEFAULT 0",
	}
	for col, ddl := range actorAdds {
		ok, err := columnExists(ctx, db, "actor_registry", col)
		if err != nil {
			return err
		}
		if ok {
			continue
		}
		if _, err := db.ExecContext(ctx, "ALTER TABLE actor_registry ADD COLUMN "+ddl); err != nil {
			return fmt.Errorf("store: add actor_registry.%s: %w", col, err)
		}
	}
	return nil
}

func columnExists(ctx context.Context, db *sql.DB, table, column string) (bool, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA table_info("+table+")")
	if err != nil {
		return false, fmt.Errorf("store: pragma table_info(%s): %w", table, err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue any
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return false, fmt.Errorf("store: scan table_info(%s): %w", table, err)
		}
		if name == column {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("store: table_info(%s) rows: %w", table, err)
	}
	return false, nil
}
