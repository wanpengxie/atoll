package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	_ "modernc.org/sqlite" // register driver
)

// OpenOptions tunes DSN-level pragmas. Zero value is fine for production.
//
// NOTE (schema baseline): a create open installs ChannelLocalDDL once. A
// MustExist or ReadOnly open never installs or repairs schema; the
// verifier requires the file to carry the current baseline already. The
// product has no released legacy DB, so there is deliberately no in-code
// channel schema migration.
type OpenOptions struct {
	// ReadOnly opens the database in read-only mode (mode=ro); no filesystem
	// write side-effects.
	ReadOnly bool

	// MustExist opens in SQLite mode=rw and performs no parent-directory or DDL
	// creation. It is the production reopen mode for a channel already listed in
	// the process data directory.
	MustExist bool
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
	// ChannelLocalDDL is the fresh-install baseline. Reopens validate the
	// complete shape and never mutate an old shape into a new one.
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
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("store: open %q: %w", dbPath, err)
	}

	if !opts.ReadOnly && !opts.MustExist {
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("store: exec DDL on %q: %w", dbPath, err)
		}
	}

	return db, nil
}

type sqliteSchemaObject struct {
	typ string
	sql string
}

var (
	channelSchemaOnce sync.Once
	channelSchema     map[string]sqliteSchemaObject
	channelSchemaErr  error
)

// verifyChannelLocalSchema compares the entire schema catalog with the catalog
// produced by executing ChannelLocalDDL in a private database. SQLite, not a
// hand-maintained column mirror, parses the one schema authority. This covers
// every column, constraint and index while rejecting extra objects.
func verifyChannelLocalSchema(ctx context.Context, db *sql.DB) error {
	want, err := expectedChannelSchema()
	if err != nil {
		return fmt.Errorf("store: build channel schema baseline: %w", err)
	}
	got, err := readSchemaObjects(ctx, db)
	if err != nil {
		return err
	}
	for name, actual := range got {
		expected, ok := want[name]
		if !ok {
			return fmt.Errorf("store: stale channel DB, recreate: unexpected %s %q", actual.typ, name)
		}
		if actual.typ != expected.typ {
			return fmt.Errorf("store: stale channel DB, recreate: object %q type = %s, want %s", name, actual.typ, expected.typ)
		}
		if normalizeSchemaSQL(actual.sql) != normalizeSchemaSQL(expected.sql) {
			return fmt.Errorf("store: stale channel DB, recreate: object %q definition does not match current schema", name)
		}
	}
	for name := range want {
		if _, ok := got[name]; !ok {
			return fmt.Errorf("store: stale channel DB, recreate: missing schema object %q", name)
		}
	}
	return nil
}

func expectedChannelSchema() (map[string]sqliteSchemaObject, error) {
	channelSchemaOnce.Do(func() {
		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			channelSchemaErr = err
			return
		}
		defer func() { _ = db.Close() }()
		db.SetMaxOpenConns(1)
		if _, err := db.ExecContext(context.Background(), ChannelLocalDDL); err != nil {
			channelSchemaErr = err
			return
		}
		channelSchema, channelSchemaErr = readSchemaObjects(context.Background(), db)
	})
	return channelSchema, channelSchemaErr
}

func readSchemaObjects(ctx context.Context, db *sql.DB) (map[string]sqliteSchemaObject, error) {
	rows, err := db.QueryContext(ctx, `SELECT type,name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'`)
	if err != nil {
		return nil, fmt.Errorf("store: read schema catalog: %w", err)
	}
	defer func() { _ = rows.Close() }()
	objects := map[string]sqliteSchemaObject{}
	for rows.Next() {
		var typ, name, ddl string
		if err := rows.Scan(&typ, &name, &ddl); err != nil {
			return nil, fmt.Errorf("store: scan schema catalog: %w", err)
		}
		objects[name] = sqliteSchemaObject{typ: typ, sql: ddl}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate schema catalog: %w", err)
	}
	return objects, nil
}

func normalizeSchemaSQL(ddl string) string {
	return strings.TrimSuffix(strings.Join(strings.Fields(ddl), " "), ";")
}
