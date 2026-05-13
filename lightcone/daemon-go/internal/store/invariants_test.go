package store

import (
	"context"
	"database/sql"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// TestPartialUniqueIndex_TerminalResponseRace asserts that the partial
// UNIQUE INDEX `ux_terminal_response_per_request` (L2 §1.4.1) only
// admits one terminal response per parent_id.
//
// Two goroutines race to INSERT `kind='response', is_terminal=1` rows
// pointing at the same parent_id; exactly one MUST succeed, the other
// MUST surface a UNIQUE constraint error.
//
// This is The One Law (L1 §10.2 step 8) enforced at the storage layer.
// Drift here = silent terminal duplication = downstream invariants
// (long-pending scheduler, harness fallback emit) break.
func TestPartialUniqueIndex_TerminalResponseRace(t *testing.T) {
	ctx := context.Background()
	db := openTempChannel(t, ctx)

	// Seed the request the terminals point at (parent_id = req-1).
	mustInsertMessage(t, ctx, db, messageRow{
		id: "req-1", kind: "request", typ: "xhs.publish", parentID: "",
		isTerminal: 0,
	})

	const racers = 2
	var (
		wg     sync.WaitGroup
		ok     int32 // successful inserts
		unique int32 // inserts that hit UNIQUE constraint
		other  int32 // unexpected errors
		start  = make(chan struct{})
	)

	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			runtime.Gosched()
			err := insertResponse(ctx, db, responseRow{
				id:         respID(idx),
				parentID:   "req-1",
				typ:        "xhs.publish",
				isTerminal: 1,
			})
			switch {
			case err == nil:
				atomic.AddInt32(&ok, 1)
			case isUniqueError(err):
				atomic.AddInt32(&unique, 1)
			default:
				atomic.AddInt32(&other, 1)
				t.Errorf("racer %d: unexpected error: %v", idx, err)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if ok != 1 {
		t.Errorf("ok inserts = %d, want exactly 1", ok)
	}
	if unique != racers-1 {
		t.Errorf("UNIQUE-error inserts = %d, want %d", unique, racers-1)
	}
	if other != 0 {
		t.Errorf("unexpected errors = %d", other)
	}

	// Sanity: only one terminal response row stored.
	var n int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM messages
		 WHERE parent_id='req-1' AND kind='response' AND is_terminal=1`,
	).Scan(&n); err != nil {
		t.Fatalf("count terminals: %v", err)
	}
	if n != 1 {
		t.Errorf("stored terminal count = %d, want 1", n)
	}
}

// TestPartialUniqueIndex_NonTerminalResponsesAllowed verifies the partial
// UNIQUE INDEX is partial — non-terminal responses on the same parent_id
// MUST be allowed (e.g. progress / accepted intermediate emits).
func TestPartialUniqueIndex_NonTerminalResponsesAllowed(t *testing.T) {
	ctx := context.Background()
	db := openTempChannel(t, ctx)

	mustInsertMessage(t, ctx, db, messageRow{
		id: "req-2", kind: "request", typ: "xhs.publish",
	})

	for i := 0; i < 3; i++ {
		err := insertResponse(ctx, db, responseRow{
			id:         respID(i),
			parentID:   "req-2",
			typ:        "xhs.publish",
			isTerminal: 0, // non-terminal — partial UNIQUE should ignore
		})
		if err != nil {
			t.Fatalf("non-terminal response %d unexpectedly rejected: %v", i, err)
		}
	}
}

// TestActorCursors_MonotonicCAS verifies cursor advancement is strictly
// monotonic (L1 §6.3.4.3, L2 §1.4.3).
//
//   - first WRITE 10 → succeeds
//   - then attempt 5 → 0 rows affected (5 < 10)
//   - then 15      → succeeds (15 > 10)
//   - finally 15 again → 0 rows affected (15 NOT < 15)
func TestActorCursors_MonotonicCAS(t *testing.T) {
	ctx := context.Background()
	db := openTempChannel(t, ctx)

	// Seed the cursor row (actor_registry seeding does this in the
	// real bootstrap saga; here we go direct).
	if _, err := db.ExecContext(ctx,
		`INSERT INTO actor_cursors (actor_id, last_consumed_seq, last_consumed_id, updated_at)
		 VALUES ('agent-1', 0, NULL, 0)`,
	); err != nil {
		t.Fatalf("seed cursor: %v", err)
	}

	cases := []struct {
		name      string
		nextSeq   int64
		wantRows  int64
		expectSeq int64
	}{
		{"advance 0→10", 10, 1, 10},
		{"reject 10→5 (smaller)", 5, 0, 10},
		{"advance 10→15", 15, 1, 15},
		{"reject 15→15 (equal not allowed)", 15, 0, 15},
		{"advance 15→16", 16, 1, 16},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := db.ExecContext(ctx,
				`UPDATE actor_cursors
				 SET last_consumed_seq=?, last_consumed_id=?, updated_at=?
				 WHERE actor_id=? AND last_consumed_seq < ?`,
				tc.nextSeq, "msg-x", 1, "agent-1", tc.nextSeq,
			)
			if err != nil {
				t.Fatalf("UPDATE: %v", err)
			}
			n, err := res.RowsAffected()
			if err != nil {
				t.Fatalf("RowsAffected: %v", err)
			}
			if n != tc.wantRows {
				t.Errorf("RowsAffected = %d, want %d", n, tc.wantRows)
			}

			var got int64
			if err := db.QueryRowContext(ctx,
				`SELECT last_consumed_seq FROM actor_cursors WHERE actor_id='agent-1'`,
			).Scan(&got); err != nil {
				t.Fatalf("read cursor: %v", err)
			}
			if got != tc.expectSeq {
				t.Errorf("stored seq = %d, want %d", got, tc.expectSeq)
			}
		})
	}
}

// TestMessagesAttempts_AtomicIncrement verifies that the
// `UPDATE messages SET attempts = attempts + 1 WHERE id = ?` primitive
// (L2 §1.4.4) is atomic under concurrent callers — final value MUST
// equal the number of UPDATEs.
//
// Because we configure max_open_conns=1 + busy_timeout=5000, concurrent
// goroutines serialize at the conn level, but the SQL itself is the
// authoritative atomic step. Test asserts the contract holds.
func TestMessagesAttempts_AtomicIncrement(t *testing.T) {
	ctx := context.Background()
	db := openTempChannel(t, ctx)

	mustInsertMessage(t, ctx, db, messageRow{
		id: "m-attempts", kind: "event", typ: "agent.text",
	})

	const updates = 50
	var wg sync.WaitGroup
	for i := 0; i < updates; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := db.ExecContext(ctx,
				`UPDATE messages SET attempts = attempts + 1 WHERE id = ?`,
				"m-attempts",
			); err != nil {
				t.Errorf("increment: %v", err)
			}
		}()
	}
	wg.Wait()

	var got int64
	if err := db.QueryRowContext(ctx,
		`SELECT attempts FROM messages WHERE id = ?`, "m-attempts",
	).Scan(&got); err != nil {
		t.Fatalf("read attempts: %v", err)
	}
	if got != updates {
		t.Errorf("attempts = %d, want %d", got, updates)
	}
}

// TestMessagesDelivered_TerminalOnceCAS verifies that delivered_at MUST
// only be set once (L0 §2.5 / L2 §1.4.4 atomic primitives table).
//
// Two UPDATEs of the form `... WHERE delivered_at IS NULL` against the
// same row — first succeeds with rows_affected=1, second returns
// rows_affected=0 because the predicate no longer holds. The stored
// value MUST stay at the first writer's timestamp.
func TestMessagesDelivered_TerminalOnceCAS(t *testing.T) {
	ctx := context.Background()
	db := openTempChannel(t, ctx)

	mustInsertMessage(t, ctx, db, messageRow{
		id: "m-deliver", kind: "event", typ: "agent.text",
	})

	first, err := db.ExecContext(ctx,
		`UPDATE messages SET delivered_at = ? WHERE id = ? AND delivered_at IS NULL`,
		111, "m-deliver",
	)
	if err != nil {
		t.Fatalf("first deliver UPDATE: %v", err)
	}
	if n, _ := first.RowsAffected(); n != 1 {
		t.Errorf("first UPDATE rows=%d, want 1", n)
	}

	second, err := db.ExecContext(ctx,
		`UPDATE messages SET delivered_at = ? WHERE id = ? AND delivered_at IS NULL`,
		222, "m-deliver",
	)
	if err != nil {
		t.Fatalf("second deliver UPDATE: %v", err)
	}
	if n, _ := second.RowsAffected(); n != 0 {
		t.Errorf("second UPDATE rows=%d, want 0 (terminal-once violated)", n)
	}

	var got sql.NullInt64
	if err := db.QueryRowContext(ctx,
		`SELECT delivered_at FROM messages WHERE id = ?`, "m-deliver",
	).Scan(&got); err != nil {
		t.Fatalf("read delivered_at: %v", err)
	}
	if !got.Valid || got.Int64 != 111 {
		t.Errorf("delivered_at = %+v, want 111 (first writer wins)", got)
	}

	// Symmetric check: delivery_failed_at + last_error CAS.
	failFirst, err := db.ExecContext(ctx,
		`UPDATE messages SET delivery_failed_at = ?, last_error = ?
		 WHERE id = ? AND delivery_failed_at IS NULL`,
		333, "boom", "m-deliver",
	)
	if err != nil {
		t.Fatalf("first fail UPDATE: %v", err)
	}
	if n, _ := failFirst.RowsAffected(); n != 1 {
		t.Errorf("first fail rows=%d, want 1", n)
	}

	failSecond, err := db.ExecContext(ctx,
		`UPDATE messages SET delivery_failed_at = ?, last_error = ?
		 WHERE id = ? AND delivery_failed_at IS NULL`,
		444, "later", "m-deliver",
	)
	if err != nil {
		t.Fatalf("second fail UPDATE: %v", err)
	}
	if n, _ := failSecond.RowsAffected(); n != 0 {
		t.Errorf("second fail rows=%d, want 0", n)
	}
}

// --- test fixtures ---------------------------------------------------------

type messageRow struct {
	id         string
	kind       string
	typ        string
	parentID   string
	isTerminal int
}

type responseRow struct {
	id         string
	parentID   string
	typ        string
	isTerminal int // 0 = non-terminal, 1 = terminal — caller MUST set explicitly
}

func mustInsertMessage(t *testing.T, ctx context.Context, db *sql.DB, r messageRow) {
	t.Helper()
	if _, err := db.ExecContext(ctx,
		`INSERT INTO messages
		 (id, ts, ts_received, channel_id, sender_kind, sender_id,
		  kind, type, payload, parent_id, visibility, audience, is_terminal)
		 VALUES (?, 1, 1, 'c', 'human', 'u', ?, ?, '{}', ?, 'public', '["*"]', ?)`,
		r.id, r.kind, r.typ, nullIfEmpty(r.parentID), r.isTerminal,
	); err != nil {
		t.Fatalf("insert message %q: %v", r.id, err)
	}
}

// insertResponse INSERTs a single response message verbatim. is_terminal
// is taken from the caller — no defaulting; tests asserting terminal vs
// non-terminal behaviour MUST be explicit.
func insertResponse(ctx context.Context, db *sql.DB, r responseRow) error {
	_, err := db.ExecContext(ctx,
		`INSERT INTO messages
		 (id, ts, ts_received, channel_id, sender_kind, sender_id,
		  kind, type, payload, parent_id, visibility, audience, is_terminal)
		 VALUES (?, 1, 1, 'c', 'agent', 'bot', 'response', ?, '{}', ?, 'public', '["*"]', ?)`,
		r.id, r.typ, nullIfEmpty(r.parentID), r.isTerminal,
	)
	return err
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func respID(idx int) string {
	return "resp-" + itoa(idx)
}

func itoa(i int) string {
	const digits = "0123456789"
	if i == 0 {
		return "0"
	}
	negative := i < 0
	if negative {
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = digits[i%10]
		i /= 10
	}
	if negative {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}

func isUniqueError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	// modernc.org/sqlite surfaces "constraint failed: UNIQUE constraint failed: messages.parent_id"
	// or "UNIQUE constraint failed". Match the substring "UNIQUE" — robust to
	// future driver text drift.
	return strings.Contains(strings.ToUpper(msg), "UNIQUE")
}
