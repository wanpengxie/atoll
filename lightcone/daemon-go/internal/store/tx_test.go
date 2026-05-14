package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
)

// TestWithImmediate_HappyPath confirms the baseline behaviour: fn
// runs, COMMIT lands, the conn returns to the pool clean (a follow-up
// WithImmediate succeeds).
func TestWithImmediate_HappyPath(t *testing.T) {
	t.Parallel()
	db := openTxTestDB(t)

	called := false
	if err := WithImmediate(context.Background(), db, func(ctx context.Context, conn *sql.Conn) error {
		called = true
		if _, err := conn.ExecContext(ctx,
			`INSERT INTO action_ledger (ledger_key, turn_id, actor_id, envelope_id, status, reserved_at, committed_at)
			 VALUES ('k-1','t-1','a-1','e-1','reserved',1700000000,NULL)`); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("WithImmediate: %v", err)
	}
	if !called {
		t.Fatalf("fn was not invoked")
	}
	// Sanity: a second WithImmediate works (conn not held captive by
	// a leftover transaction).
	if err := WithImmediate(context.Background(), db, func(ctx context.Context, conn *sql.Conn) error {
		_, err := conn.ExecContext(ctx, `DELETE FROM action_ledger WHERE ledger_key = 'k-1'`)
		return err
	}); err != nil {
		t.Fatalf("follow-up WithImmediate after happy commit: %v", err)
	}
}

// TestWithImmediate_ErrorRollsBack confirms an fn that returns an
// error leaves no half-committed work and the conn pool stays usable.
func TestWithImmediate_ErrorRollsBack(t *testing.T) {
	t.Parallel()
	db := openTxTestDB(t)

	bodyErr := errors.New("synthetic body failure")
	err := WithImmediate(context.Background(), db, func(ctx context.Context, conn *sql.Conn) error {
		if _, err := conn.ExecContext(ctx,
			`INSERT INTO action_ledger (ledger_key, turn_id, actor_id, envelope_id, status, reserved_at, committed_at)
			 VALUES ('k-err','t-1','a-1','e-1','reserved',1700000000,NULL)`); err != nil {
			return err
		}
		return bodyErr
	})
	if !errors.Is(err, bodyErr) {
		t.Fatalf("expected body error to surface, got %v", err)
	}

	// Insert should have rolled back — no row visible.
	row := db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM action_ledger WHERE ledger_key = 'k-err'`)
	var n int
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 0 {
		t.Fatalf("rolled-back row leaked into table: count=%d", n)
	}

	// Conn must not be poisoned — the next BEGIN IMMEDIATE works.
	if err := WithImmediate(context.Background(), db, func(_ context.Context, _ *sql.Conn) error {
		return nil
	}); err != nil {
		t.Fatalf("follow-up WithImmediate after error path: %v", err)
	}
}

// TestWithImmediate_PanicRollsBack is the FIX-3 R1 regression
// (T103 / claude 85-2 major): a panic inside fn MUST run ROLLBACK
// on the dedicated conn before re-raising. Prior implementation
// only rolled back on `err != nil`; the panic skipped over the
// named return assignment, the conn went back to the pool with
// an open IMMEDIATE tx, and the next acquirer crashed with
// "cannot start a transaction within a transaction".
//
// Assertions:
//
//   - the panic value propagates out of WithImmediate unchanged;
//   - a follow-up WithImmediate against the SAME db succeeds —
//     proving the conn pool was not poisoned by a leftover tx.
func TestWithImmediate_PanicRollsBack(t *testing.T) {
	t.Parallel()
	db := openTxTestDB(t)

	const panicMsg = "T103-FIX-E synthetic panic"

	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatalf("expected panic to propagate, got none")
			}
			if msg, ok := r.(string); !ok || msg != panicMsg {
				t.Fatalf("panic value = %v, want %q", r, panicMsg)
			}
		}()
		_ = WithImmediate(context.Background(), db, func(ctx context.Context, conn *sql.Conn) error {
			// Make sure the body actually grabbed the writer lock — a
			// silent fast-path that bypasses BEGIN would mask the bug.
			if _, err := conn.ExecContext(ctx,
				`INSERT INTO action_ledger (ledger_key, turn_id, actor_id, envelope_id, status, reserved_at, committed_at)
				 VALUES ('k-panic','t-1','a-1','e-1','reserved',1700000000,NULL)`); err != nil {
				t.Fatalf("insert before panic: %v", err)
			}
			panic(panicMsg)
		})
	}()

	// The inserted row must not be visible — ROLLBACK ran.
	row := db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM action_ledger WHERE ledger_key = 'k-panic'`)
	var n int
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan after panic recovery: %v", err)
	}
	if n != 0 {
		t.Fatalf("panic-rolled-back row leaked: count=%d", n)
	}

	// The conn pool must not be poisoned. Pre-fix, the next
	// BEGIN IMMEDIATE would error with "cannot start a transaction
	// within a transaction".
	if err := WithImmediate(context.Background(), db, func(_ context.Context, _ *sql.Conn) error {
		return nil
	}); err != nil {
		t.Fatalf("follow-up WithImmediate after panic: %v (conn pool poisoned — FIX E regression)", err)
	}
}

// openTxTestDB opens a fresh channel sqlite under t.TempDir() and
// registers cleanup. Shared by every test in this file.
func openTxTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := OpenChannel(context.Background(),
		filepath.Join(t.TempDir(), "messages.sqlite"), OpenOptions{})
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}
