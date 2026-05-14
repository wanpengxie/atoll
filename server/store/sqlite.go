// Package store owns the server-side sqlite database — connection
// pool, pragmas (WAL + busy_timeout + foreign_keys), and the
// idempotent migration runner that applies the .up.sql files embedded
// alongside this package.
//
// Authoritative spec: .dalek/pm/m1.5-tickets.md §T6 (server-side sqlite
// schema 精简 + WAL 模式).
//
// Driver: modernc.org/sqlite (pure Go, no cgo). Registered under name
// "sqlite" via the blank import below.
//
// Migration philosophy: T6 demo-period uses `golang-migrate`-style files
// (0001_*.up.sql / 0002_*.up.sql / …) but we DO NOT depend on the
// `migrate` CLI at runtime. Migrate is applied in-process via Apply via
// an idempotent CREATE TABLE IF NOT EXISTS pattern + a schema_migrations
// bookkeeping table. The .up.sql files are also compatible with the
// `migrate` CLI for ops-side tooling (golang-migrate is in go.mod for
// future programmatic use; the in-process Apply path is what cmd/server
// actually calls).
package store

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"sort"
	"strings"

	// Register the modernc.org/sqlite database/sql driver under the
	// name "sqlite". Imported for side-effect only.
	_ "modernc.org/sqlite"
)

// driverName matches the modernc.org/sqlite registration above.
const driverName = "sqlite"

// defaultPragmas are applied to every connection via DSN query
// fragments. WAL + busy_timeout + foreign_keys is the spec-baseline
// (server.sqlite is single-writer per process; WAL gives concurrent
// reads; busy_timeout protects against transient contention in tests).
var defaultPragmas = []string{
	"_pragma=journal_mode(WAL)",
	"_pragma=busy_timeout(5000)",
	"_pragma=foreign_keys(ON)",
	"_pragma=synchronous(NORMAL)",
}

// MigrationsFS embeds the migration .sql files alongside this package
// so `Apply` can run them without external file-system access.
//
//go:embed migrations/*.up.sql
var MigrationsFS embed.FS

// OpenOptions tunes Open behaviour. Zero value is fine for production.
type OpenOptions struct {
	// ReadOnly opens the database in read-only mode.
	ReadOnly bool
	// SkipMigrate when true skips automatic migration on Open. Useful
	// for tests that want to inspect the empty database first.
	SkipMigrate bool
	// ExtraPragmas appends to the DSN — each entry is the value part
	// (e.g. "cache_size(-2000)").
	ExtraPragmas []string
}

// Open opens (or creates) the server-side sqlite file at path and
// applies the embedded migrations (unless OpenOptions.SkipMigrate).
//
// Special case: path == ":memory:" opens an anonymous private in-memory
// database (one per call). For shared-memory in-memory databases the
// caller should pass the explicit file::memory: form.
func Open(ctx context.Context, path string, opts OpenOptions) (*sql.DB, error) {
	dsn, err := buildDSN(path, opts)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("server/store: open %q: %w", path, err)
	}

	// modernc.org/sqlite handles concurrent reads but serializes writes;
	// limit the pool so we don't open many file handles.
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("server/store: ping %q: %w", path, err)
	}

	if opts.SkipMigrate || opts.ReadOnly {
		return db, nil
	}

	if err := Apply(ctx, db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("server/store: migrate: %w", err)
	}

	return db, nil
}

// buildDSN composes the DSN string passed to sql.Open. The modernc
// driver accepts file paths directly and reads `_pragma=` query
// fragments to apply per-connection pragmas.
func buildDSN(path string, opts OpenOptions) (string, error) {
	if path == "" {
		return "", errors.New("server/store: empty path")
	}

	q := url.Values{}
	for _, p := range defaultPragmas {
		appendDSNPragma(&q, p)
	}
	for _, p := range opts.ExtraPragmas {
		appendDSNPragma(&q, p)
	}
	if opts.ReadOnly {
		q.Set("mode", "ro")
	}

	// Special-case the bare ":memory:" form — keep it readable, modernc
	// accepts it verbatim.
	base := path
	if path == ":memory:" {
		base = ":memory:"
	}
	enc := q.Encode()
	if enc == "" {
		return base, nil
	}
	return base + "?" + enc, nil
}

// appendDSNPragma normalises an "_pragma=..." entry — defaultPragmas
// uses the encoded "_pragma=journal_mode(WAL)" form (the equals sign
// after _pragma is a literal); url.Values.Add takes (key, value) so we
// split on the first '=' to keep encoding correct.
func appendDSNPragma(q *url.Values, raw string) {
	const prefix = "_pragma="
	if strings.HasPrefix(raw, prefix) {
		q.Add("_pragma", raw[len(prefix):])
		return
	}
	if idx := strings.Index(raw, "="); idx > 0 {
		q.Add(raw[:idx], raw[idx+1:])
		return
	}
	q.Add(raw, "")
}

// Apply runs the embedded migrations in lexical order under a
// schema_migrations bookkeeping table. Each migration runs in a
// transaction; if one fails the partial migration is rolled back and
// the error is returned.
//
// Idempotent: re-running Apply after a previous successful run is a
// no-op (schema_migrations rows skip already-applied versions).
func Apply(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    TEXT PRIMARY KEY,
			applied_at INTEGER NOT NULL
		)
	`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	entries, err := fs.ReadDir(MigrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("read embedded migrations: %w", err)
	}

	files := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".up.sql") {
			continue
		}
		files = append(files, name)
	}
	sort.Strings(files)

	for _, name := range files {
		version := strings.TrimSuffix(name, ".up.sql")

		var applied int
		if err := db.QueryRowContext(
			ctx,
			"SELECT COUNT(*) FROM schema_migrations WHERE version = ?",
			version,
		).Scan(&applied); err != nil {
			return fmt.Errorf("check version %q: %w", version, err)
		}
		if applied > 0 {
			continue
		}

		body, err := fs.ReadFile(MigrationsFS, "migrations/"+name)
		if err != nil {
			return fmt.Errorf("read %q: %w", name, err)
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin tx %q: %w", name, err)
		}

		if _, err := tx.ExecContext(ctx, string(body)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("apply %q: %w", name, err)
		}
		if _, err := tx.ExecContext(
			ctx,
			"INSERT INTO schema_migrations (version, applied_at) VALUES (?, strftime('%s','now') * 1000)",
			version,
		); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("record version %q: %w", version, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit %q: %w", name, err)
		}
	}

	return nil
}

// MigrationNames returns the list of embedded migration file names
// (sorted lexically). Useful for tests + ops tooling.
func MigrationNames() ([]string, error) {
	entries, err := fs.ReadDir(MigrationsFS, "migrations")
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".up.sql") {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names, nil
}
