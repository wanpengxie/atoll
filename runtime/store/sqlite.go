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
	return openSqlite(ctx, dbPath, opts, ChannelLocalDDL)
}

// OpenDaemon opens (creating if absent) the daemon-level sqlite at
// dbPath, runs DaemonLocalDDL, and returns *sql.DB.
func OpenDaemon(ctx context.Context, dbPath string, opts OpenOptions) (*sql.DB, error) {
	return openSqlite(ctx, dbPath, opts, DaemonLocalDDL)
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
	all := append(base, opts.ExtraPragmas...)
	for _, p := range all {
		if _, err := db.ExecContext(ctx, p); err != nil {
			return fmt.Errorf("store: %s: %w", p, err)
		}
	}
	return nil
}
