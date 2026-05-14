package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// BeginDeferred opens a DEFERRED transaction — sqlite's default and the
// right pick for the majority of harness writes (read-then-maybe-write
// turns lock acquisition into a no-op when the path is read-only).
//
// Per L2 §1.4: keep DEFERRED for non-critical writes; reserve
// IMMEDIATE (see WithImmediate) for paths that must lock the file.
func BeginDeferred(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	// database/sql BeginTx with nil opts maps to BEGIN, which on
	// sqlite is BEGIN DEFERRED.
	return db.BeginTx(ctx, nil)
}

// WithImmediate runs fn inside an IMMEDIATE transaction. The reserved
// (writer) lock is grabbed up-front so concurrent writers fail fast
// instead of upgrading mid-transaction (which would surface
// SQLITE_BUSY half-way through the body).
//
// Use this for:
//
//   - Channel bootstrap saga (L2 §1.4.7)
//   - worker_locks acquire / steal CAS (L2 §1.4.9)
//   - action_ledger reserve/commit (L2 §1.4.10.1)
//   - Engine message append (L2 §1.4.5 atomic primitives table)
//
// fn receives the dedicated *sql.Conn that owns the BEGIN — all
// statements inside fn MUST execute against this conn (or a
// *sql.Tx derived from it via conn.BeginTx). Executing on the
// parent *sql.DB pool from inside fn risks deadlocking against the
// single-conn pool we configure in `open()`.
func WithImmediate(ctx context.Context, db *sql.DB, fn func(context.Context, *sql.Conn) error) (err error) {
	if db == nil {
		return errors.New("store: WithImmediate db is nil")
	}
	if fn == nil {
		return errors.New("store: WithImmediate fn is nil")
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("store: acquire conn: %w", err)
	}
	defer func() {
		// Always release the conn back to the pool. Close() returns
		// no useful error for our purposes (the underlying conn is
		// just put back); swallow it.
		_ = conn.Close()
	}()

	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return fmt.Errorf("store: begin immediate: %w", err)
	}

	defer func() {
		// Panic path: recover so we can run ROLLBACK against the
		// dedicated conn before re-raising. Previously the panic
		// bypassed the err != nil branch below, leaving the conn
		// returning to the pool with an open IMMEDIATE transaction;
		// the next acquirer would crash on "cannot start a
		// transaction within a transaction" (claude 85-2 major:
		// T103 / FIX E). We rebind ctx to context.Background() so
		// shutdown / cancellation does not also skip the rollback.
		if r := recover(); r != nil {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
			panic(r)
		}
		// If the body errored, roll back. Otherwise the COMMIT below
		// already ran. Best-effort: if sqlite reports "no transaction
		// active" because COMMIT half-succeeded, we still want the
		// original err to bubble up.
		if err != nil {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		}
	}()

	if err = fn(ctx, conn); err != nil {
		return err
	}

	if _, err = conn.ExecContext(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("store: commit immediate: %w", err)
	}
	return nil
}
