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

// openChannelDB opens (creating if absent) the per-channel messages.sqlite at
// dbPath, runs ChannelLocalDDL, and returns the raw *sql.DB. UNEXPORTED by
// design (§4.5): the raw handle must never cross the store boundary — only the
// OpenChannel assembly (channel.go) may hold it, exposing storespec interfaces.
//
// Pragmas applied (per L2 §1.4 sqlite tuning):
//
//	PRAGMA journal_mode=WAL
//	PRAGMA synchronous=NORMAL
//	PRAGMA foreign_keys=ON
//	PRAGMA busy_timeout=5000
func openChannelDB(ctx context.Context, dbPath string, opts OpenOptions) (*sql.DB, error) {
	db, err := openSqlite(ctx, dbPath, opts, ChannelLocalDDL)
	if err != nil {
		return nil, err
	}
	// Single authoritative schema: ChannelLocalDDL (schema.go) is the only
	// place the channel-local shape is defined. There is no in-code
	// migration path. A fresh open (SkipDDL=false) installs the full
	// baseline; an existing open (SkipDDL=true, used by the daemon channel
	// cache) MUST already match the baseline. We never ALTER or drop —
	// channel sqlite holds the append-only message-log truth (INVARIANT-2 /
	// INVARIANT-12); a stale DB is recreated by a human, not silently
	// migrated. Validate shape on every open and fail-fast on mismatch.
	if err := verifyChannelLocalSchema(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
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

	// modernc.org/sqlite is single-connection-safe; cap pool to 1 for the
	// channel sqlite so WAL writers/readers don't fight pragma state.
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

// channelLocalSchemaShape is the authoritative set of (table -> required
// columns) that ChannelLocalDDL guarantees. verifyChannelLocalSchema checks
// an opened DB against it. Keep in lockstep with schema.go ChannelLocalDDL —
// the DDL is the single source of truth; this map is just the fail-fast
// guard that an opened file actually carries that shape.
var channelLocalSchemaShape = map[string][]string{
	"messages":              {"seq", "id", "type", "kind", "cross_channel_refs"},
	"type_registry":         {"type", "handler_binding", "install_status", "install_error"},
	"type_registry_pending": {"install_attempt_id", "type", "install_status", "install_error"},
	"actor_cursors":         {"actor_id", "last_consumed_seq"},
	"actor_registry":        {"actor_id", "actor_kind", "deregistered_at"},
	"action_ledger":         {"ledger_key", "turn_id", "status"},
}

func verifyChannelLocalSchema(ctx context.Context, db *sql.DB) error {
	return verifySchema(ctx, db, "channel", channelLocalSchemaShape)
}

// verifySchema fail-fast-validates an opened sqlite against the authoritative
// baseline shape (schema.go). On any missing table or column it returns a
// clear "stale <kind> DB" error instructing recreation. It NEVER mutates the
// DB — no ALTER, no DROP, no silent migration. Channel/daemon sqlite holds
// append-only truth; a shape mismatch means a human must recreate the DB.
func verifySchema(ctx context.Context, db *sql.DB, kind string, shape map[string][]string) error {
	for table, cols := range shape {
		present, err := tableColumns(ctx, db, table)
		if err != nil {
			return err
		}
		if len(present) == 0 {
			return fmt.Errorf("store: stale %s DB, recreate: missing table %q (schema does not match baseline schema.go)", kind, table)
		}
		for _, col := range cols {
			if _, ok := present[col]; !ok {
				return fmt.Errorf("store: stale %s DB, recreate: table %q missing column %q (schema does not match baseline schema.go)", kind, table, col)
			}
		}
	}
	return nil
}

// tableColumns returns the set of column names for table. An empty result
// (no rows) means the table does not exist.
func tableColumns(ctx context.Context, db *sql.DB, table string) (map[string]struct{}, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA table_info("+table+")")
	if err != nil {
		return nil, fmt.Errorf("store: pragma table_info(%s): %w", table, err)
	}
	defer func() { _ = rows.Close() }()
	cols := map[string]struct{}{}
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue any
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return nil, fmt.Errorf("store: scan table_info(%s): %w", table, err)
		}
		cols[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: table_info(%s) rows: %w", table, err)
	}
	return cols, nil
}
