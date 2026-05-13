package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"

	// Register the modernc.org/sqlite database/sql driver under the name
	// "sqlite". Imported for side-effect only — no symbol from this
	// package is referenced directly.
	_ "modernc.org/sqlite"
)

// driverName is the database/sql driver name registered by the modernc
// sqlite blank-import above. Centralized so the rest of the package and
// downstream callers stay decoupled from the driver choice.
const driverName = "sqlite"

// DSN-level pragmas applied to every connection in the pool. WAL +
// busy_timeout matter most for concurrent writers; the others lock down
// foreign keys + recursive triggers consistently with the L2 contract.
//
// busy_timeout is set generously (5s) so that test goroutines competing
// for the partial UNIQUE INDEX don't trip SQLITE_BUSY; production
// callers can override via OpenOptions if 5s is too high.
var defaultPragmas = []string{
	"_pragma=journal_mode(WAL)",
	"_pragma=busy_timeout(5000)",
	"_pragma=foreign_keys(ON)",
	"_pragma=synchronous(NORMAL)",
}

// OpenOptions tunes Open / OpenChannel / OpenDaemon behaviour. Zero
// value is fine for production callers.
type OpenOptions struct {
	// ReadOnly opens the file in read-only mode. Useful for tooling
	// like `migrate from-node --src` which must not mutate the Node
	// daemon sqlite even if pragmas would otherwise upgrade it.
	ReadOnly bool

	// ExtraPragmas appends additional `_pragma=...` query fragments to
	// the DSN. Each entry is the *value* part — e.g. "cache_size(-2000)".
	ExtraPragmas []string

	// SkipDDL when true opens the file without running the channel/daemon
	// DDL. Used by `migrate from-node --src` (source file is foreign
	// Node schema) and by tests that need to inspect a freshly opened
	// empty file.
	SkipDDL bool
}

// OpenChannel opens (or creates) a channel-local `messages.sqlite` file at
// path and applies the 6-table v4 channel schema via ChannelLocalDDL.
//
// The returned *sql.DB pool has WAL + busy_timeout + foreign_keys pragmas
// applied to every connection. Callers MUST `defer db.Close()`.
func OpenChannel(ctx context.Context, path string, opts OpenOptions) (*sql.DB, error) {
	db, err := open(path, opts)
	if err != nil {
		return nil, err
	}
	if opts.SkipDDL {
		return db, nil
	}
	if err := applyDDL(ctx, db, ChannelLocalDDL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("store: apply channel DDL: %w", err)
	}
	return db, nil
}

// OpenDaemon opens (or creates) the daemon-level sqlite file at path and
// applies the bootstrap_registry DDL.
func OpenDaemon(ctx context.Context, path string, opts OpenOptions) (*sql.DB, error) {
	db, err := open(path, opts)
	if err != nil {
		return nil, err
	}
	if opts.SkipDDL {
		return db, nil
	}
	if err := applyDDL(ctx, db, DaemonLevelDDL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("store: apply daemon DDL: %w", err)
	}
	return db, nil
}

// open is the shared sql.Open + pragma wiring. It does NOT apply DDL —
// callers (OpenChannel / OpenDaemon / migrate tool) decide that.
func open(path string, opts OpenOptions) (*sql.DB, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("store: open path is required")
	}

	dsn := buildDSN(path, opts)
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("store: sql.Open: %w", err)
	}

	// Force a single connection so that WAL mode pragma applied below
	// stays sticky and the partial UNIQUE INDEX semantics are observed
	// consistently across goroutines. SQLite's writer is single-threaded
	// anyway, so pool size > 1 does not buy concurrency for writes; it
	// only confuses the journal_mode setting. Tests and migration tool
	// behave identically.
	//
	// L2 callers needing reader concurrency can open a second *sql.DB
	// pointing at the same file (WAL allows N readers + 1 writer).
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if opts.ReadOnly {
		// `mode=ro` already enforces read-only; pragma still useful in
		// case some downstream call tries journal_mode=WAL on a ro
		// connection. WAL needs write access; suppress it.
		db.SetMaxOpenConns(0) // unlimited readers, no contention
		db.SetMaxIdleConns(2)
	}

	return db, nil
}

// buildDSN composes the modernc sqlite DSN with the default + extra pragmas.
// Read-only mode appends `mode=ro` and drops the journal_mode pragma since
// WAL requires write access.
func buildDSN(path string, opts OpenOptions) string {
	q := url.Values{}
	pragmas := defaultPragmas
	if opts.ReadOnly {
		// Drop journal_mode(WAL) for read-only opens — the pragma is
		// a no-op on ro files and emits a warning on some sqlite
		// builds. Keep the rest.
		filtered := make([]string, 0, len(defaultPragmas))
		for _, p := range defaultPragmas {
			if strings.Contains(p, "journal_mode") {
				continue
			}
			filtered = append(filtered, p)
		}
		pragmas = filtered
		q.Set("mode", "ro")
	}
	for _, p := range pragmas {
		// `_pragma=journal_mode(WAL)` → ("_pragma", "journal_mode(WAL)")
		eq := strings.IndexByte(p, '=')
		if eq < 0 {
			continue
		}
		q.Add(p[:eq], p[eq+1:])
	}
	for _, p := range opts.ExtraPragmas {
		q.Add("_pragma", p)
	}

	// modernc.org/sqlite parses `file:` URIs natively; bare paths also
	// work but URI form lets us encode pragmas safely.
	return "file:" + path + "?" + q.Encode()
}

// applyDDL runs the given DDL string under a single ExecContext call.
// modernc.org/sqlite accepts multi-statement input; the underlying call
// drives sqlite3_exec which splits at `;` boundaries.
func applyDDL(ctx context.Context, db *sql.DB, ddl string) error {
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return err
	}
	return nil
}
