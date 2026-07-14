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
//
// NOTE (schema baseline): a create open installs ChannelLocalDDL once. A
// MustExist, ReadOnly, or SkipDDL open never installs or repairs schema; the
// verifier requires the file to carry the current baseline already. The
// product has no released legacy DB, so there is deliberately no in-code
// channel schema migration.
type OpenOptions struct {
	// ReadOnly opens the database in read-only mode (mode=ro); no filesystem
	// write side-effects.
	ReadOnly bool

	// MustExist opens in SQLite mode=rw and performs no parent-directory or DDL
	// creation. It is the production reopen mode for a channel already listed in
	// the app directory.
	MustExist bool

	// SkipDDL skips the schema bootstrap step. Useful for tests that
	// install custom DDL or open an existing file.
	SkipDDL bool
}

// openChannelDB opens the per-channel messages.sqlite and returns the raw
// *sql.DB. A create open installs ChannelLocalDDL; MustExist only verifies the
// current schema. Unexported by
// design: the raw handle must never cross the store boundary — only the
// OpenChannel assembly (channel.go) may hold it, exposing storespec interfaces.
//
// Pragmas applied:
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
	// Single authoritative schema: ChannelLocalDDL (schema.go) is the fresh-
	// install baseline. Every reopen validates the complete shape and fails fast
	// on a missing or incomplete file; it never mutates an old shape into a new
	// one.
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
	// Existing and read-only opens must have NO filesystem creation side effect.
	// Missing paths surface as open errors instead of becoming replacement DBs.
	if !opts.ReadOnly && !opts.MustExist {
		if dir := filepath.Dir(dbPath); dir != "" {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, fmt.Errorf("store: mkdir %q: %w", dir, err)
			}
		}
	}

	// Per-CONNECTION pragmas ride the DSN, never a one-shot ExecContext:
	// database/sql may retire and reopen the underlying connection (bad-conn
	// recycle), and a pragma applied imperatively to the first connection
	// silently vanishes on its replacement — foreign-key enforcement and
	// busy_timeout would be OFF exactly on the recovery path. The modernc
	// driver applies each _pragma= to EVERY connection it opens, so the DSN
	// is the per-connection geometry. (journal_mode is persisted in the file
	// itself; synchronous / foreign_keys / busy_timeout are per-connection.)
	dsn := fmt.Sprintf(
		"file:%s?mode=rwc&_pragma=journal_mode(WAL)&_pragma=synchronous(%s)&_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)",
		dbPath, syncPragma)
	if opts.ReadOnly {
		// WAL/synchronous not applicable to a read-only open; FK still guards
		// any PRAGMA-sensitive read semantics, busy_timeout still applies.
		dsn = fmt.Sprintf("file:%s?mode=ro&_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)", dbPath)
	} else if opts.MustExist {
		dsn = fmt.Sprintf(
			"file:%s?mode=rw&_pragma=journal_mode(WAL)&_pragma=synchronous(%s)&_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)",
			dbPath, syncPragma)
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open %q: %w", dbPath, err)
	}

	// modernc.org/sqlite is single-connection-safe; cap pool to 1 for the
	// channel sqlite so WAL writers/readers don't fight pragma state. This
	// pin is also LOAD-BEARING for messages.go's in-tx provisional-after-
	// final re-check (single serialized connection = the re-check and the
	// INSERT are atomic); TestChannelDBPoolPinnedToOneConnection anchors it.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if !opts.SkipDDL && !opts.ReadOnly && !opts.MustExist {
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("store: exec DDL on %q: %w", dbPath, err)
		}
	}

	return db, nil
}

// channelLocalSchemaShape is the authoritative set of (table -> columns) that
// ChannelLocalDDL guarantees. verifyChannelLocalSchema checks an opened DB
// against it. Keep in lockstep with schema.go ChannelLocalDDL — the DDL is the
// single source of truth; this map is the fail-fast guard that an opened file
// actually carries that shape. Every table below MIRRORS its DDL column set in
// full, EXCEPT messages, whose entry is a deliberate representative probe (the
// four load-bearing columns), not the whole 16-column row — the append-only
// truth core needs only a shape sniff, not a per-column mirror.
// TestChannelLocalSchemaShapeMatchesDDL machine-reconciles the two so the
// mirror can never silently drift again.
var channelLocalSchemaShape = map[string][]string{
	"messages":       {"seq", "id", "type", "kind"}, // intentional subset — see doc above
	"actor_registry": {"actor_id", "actor_kind", "principal", "actor_binding", "host", "created_at", "deregistered_at"},
	"channel_composition": {
		"instance_id", "decl_id", "principal", "class", "config_json", "placement",
		"desired_host", "is_default", "restart_epoch",
	},
	"restart_applied": {"job_id", "instance_id", "applied_at"},
	"resources": {
		"resource_id", "kind", "bytes",
		"placement_kind", "placement_daemon_id", "placement_coord", "provenance", "created_by",
		"created_at", "is_dir",
	},
	"resource_grants": {"resource_id", "grantee_kind", "grantee", "ops"},
	"resource_reservations": {
		"reservation_id", "resource_id", "kind",
		"placement_daemon_id", "placement_coord", "created_by", "reserved_at", "is_dir", "last_progress_at",
	},
	"resource_tombstones": {
		"tombstone_id", "resource_id", "daemon_id", "placement_coord", "provenance", "kind", "deleted_at",
	},
	"actor_state": {"owner_id", "resource_id", "bytes", "created_at"},
	"timers":      {"timer_id", "author_id", "fire_at", "type", "payload", "correlation_id", "created_at"},
	"timer_dead":  {"dead_seq", "timer_id", "author_id", "fire_at", "type", "payload", "correlation_id", "created_at", "death_class", "reason", "detail", "died_at"},
}

func verifyChannelLocalSchema(ctx context.Context, db *sql.DB) error {
	return verifySchema(ctx, db, "channel", channelLocalSchemaShape)
}

// verifySchema fail-fast-validates an opened sqlite against the authoritative
// baseline shape (schema.go). On any missing table or column it returns a
// clear "stale <kind> DB" error instructing recreation. It NEVER mutates the
// DB — no ALTER, no DROP, no silent migration. Channel sqlite holds
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
