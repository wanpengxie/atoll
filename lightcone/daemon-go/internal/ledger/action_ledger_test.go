package ledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/coagent-ai/daemon-go/internal/store"
)

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

func openChannel(t *testing.T) *sql.DB {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "messages.sqlite")
	db, err := store.OpenChannel(ctx, path, store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// counterEnvelopeID returns a NewEnvelopeIDFunc that emits deterministic
// "env-<n>" strings — keeps the replay assertions readable.
func counterEnvelopeID() (NewEnvelopeIDFunc, *int64) {
	var n int64
	return func() string {
		v := atomic.AddInt64(&n, 1)
		return fmt.Sprintf("env-%d", v)
	}, &n
}

// ---------------------------------------------------------------------------
// 1. Reserve happy path — INSERT new row, return generated envelope_id.
// ---------------------------------------------------------------------------

func TestReserve_NewRow_InsertsReserved(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	gen, _ := counterEnvelopeID()

	res, err := Reserve(ctx, db,
		"ledger-key-1", "turn-1", "agent:writer", 1_700_000_000,
		Options{NewEnvelopeID: gen},
	)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if res.EnvelopeID != "env-1" {
		t.Errorf("EnvelopeID = %q, want env-1", res.EnvelopeID)
	}
	if res.Status != StatusReserved {
		t.Errorf("Status = %q, want reserved", res.Status)
	}
	if res.Replayed {
		t.Errorf("Replayed = true, want false on first insert")
	}

	got, err := Get(ctx, db, "ledger-key-1")
	if err != nil {
		t.Fatalf("Get after Reserve: %v", err)
	}
	if got.EnvelopeID != "env-1" {
		t.Errorf("persisted EnvelopeID = %q, want env-1", got.EnvelopeID)
	}
	if got.Status != StatusReserved {
		t.Errorf("persisted Status = %q, want reserved", got.Status)
	}
	if got.CommittedAt != nil {
		t.Errorf("CommittedAt should be NULL pre-commit, got %v", *got.CommittedAt)
	}
	if got.ReservedAt != 1_700_000_000 {
		t.Errorf("ReservedAt = %d, want 1_700_000_000", got.ReservedAt)
	}
}

// ---------------------------------------------------------------------------
// 2. Reserve replay — same ledger_key returns same envelope_id (the
//    headline acceptance criterion for turn replay).
// ---------------------------------------------------------------------------

func TestReserve_SameKey_ReturnsSameEnvelopeID(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	gen, counter := counterEnvelopeID()

	first, err := Reserve(ctx, db,
		"ledger-key-2", "turn-1", "agent:writer", 1_700_000_000,
		Options{NewEnvelopeID: gen},
	)
	if err != nil {
		t.Fatalf("Reserve #1: %v", err)
	}

	// Simulate a crash before Commit — caller re-enters with same key.
	second, err := Reserve(ctx, db,
		"ledger-key-2", "turn-1", "agent:writer", 1_700_000_100,
		Options{NewEnvelopeID: gen},
	)
	if err != nil {
		t.Fatalf("Reserve #2: %v", err)
	}

	if second.EnvelopeID != first.EnvelopeID {
		t.Errorf("replay envelope_id mismatch: %q vs %q", second.EnvelopeID, first.EnvelopeID)
	}
	if !second.Replayed {
		t.Errorf("Replayed = false, want true on second Reserve")
	}
	if second.Status != StatusReserved {
		t.Errorf("Replayed Status = %q, want reserved (Commit was skipped)", second.Status)
	}
	if atomic.LoadInt64(counter) != 1 {
		t.Errorf("generator called %d times, want 1 (second Reserve must not consume a new id)",
			atomic.LoadInt64(counter))
	}

	// ReservedAt MUST remain the first call's timestamp (audit signal).
	got, _ := Get(ctx, db, "ledger-key-2")
	if got.ReservedAt != 1_700_000_000 {
		t.Errorf("ReservedAt drifted: %d, want 1_700_000_000", got.ReservedAt)
	}
}

// ---------------------------------------------------------------------------
// 3. Commit flips status to 'committed' + sets committed_at.
// ---------------------------------------------------------------------------

func TestCommit_FlipsStatus(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	gen, _ := counterEnvelopeID()

	if _, err := Reserve(ctx, db,
		"ledger-key-3", "turn-1", "agent:writer", 1_700_000_000,
		Options{NewEnvelopeID: gen},
	); err != nil {
		t.Fatalf("Reserve: %v", err)
	}

	if err := Commit(ctx, db, "ledger-key-3", 1_700_000_050); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	got, _ := Get(ctx, db, "ledger-key-3")
	if got.Status != StatusCommitted {
		t.Errorf("Status = %q, want committed", got.Status)
	}
	if got.CommittedAt == nil || *got.CommittedAt != 1_700_000_050 {
		t.Errorf("CommittedAt = %v, want 1_700_000_050", got.CommittedAt)
	}
	if got.ReservedAt != 1_700_000_000 {
		t.Errorf("ReservedAt mutated by Commit: %d", got.ReservedAt)
	}
}

// ---------------------------------------------------------------------------
// 4. Commit is idempotent — second Commit on the same key is a no-op
//    that does NOT rewrite committed_at.
// ---------------------------------------------------------------------------

func TestCommit_Idempotent(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	gen, _ := counterEnvelopeID()

	if _, err := Reserve(ctx, db,
		"ledger-key-4", "turn-1", "agent:writer", 1_700_000_000,
		Options{NewEnvelopeID: gen},
	); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if err := Commit(ctx, db, "ledger-key-4", 1_700_000_050); err != nil {
		t.Fatalf("Commit #1: %v", err)
	}

	// Second Commit MUST return nil (idempotent) without touching
	// committed_at.
	if err := Commit(ctx, db, "ledger-key-4", 1_700_000_999); err != nil {
		t.Fatalf("Commit #2: %v", err)
	}
	got, _ := Get(ctx, db, "ledger-key-4")
	if *got.CommittedAt != 1_700_000_050 {
		t.Errorf("CommittedAt rewritten on idempotent Commit: %d", *got.CommittedAt)
	}
}

// ---------------------------------------------------------------------------
// 5. Commit on a never-reserved key returns ErrLedgerMissing.
// ---------------------------------------------------------------------------

func TestCommit_MissingKey(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)

	err := Commit(ctx, db, "ledger-key-nope", 1_700_000_000)
	if !errors.Is(err, ErrLedgerMissing) {
		t.Fatalf("err = %v, want ErrLedgerMissing", err)
	}
}

// ---------------------------------------------------------------------------
// 6. Reserve replay survives a Commit — second Reserve still surfaces
//    the original envelope_id (now Status='committed').
// ---------------------------------------------------------------------------

func TestReserve_ReplayAfterCommit(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	gen, _ := counterEnvelopeID()

	first, err := Reserve(ctx, db,
		"ledger-key-5", "turn-1", "agent:writer", 1_700_000_000,
		Options{NewEnvelopeID: gen},
	)
	if err != nil {
		t.Fatalf("Reserve #1: %v", err)
	}
	if err := Commit(ctx, db, "ledger-key-5", 1_700_000_050); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	second, err := Reserve(ctx, db,
		"ledger-key-5", "turn-1", "agent:writer", 1_700_000_100,
		Options{NewEnvelopeID: gen},
	)
	if err != nil {
		t.Fatalf("Reserve #2: %v", err)
	}
	if second.EnvelopeID != first.EnvelopeID {
		t.Errorf("envelope_id mismatch after replay: %q vs %q", second.EnvelopeID, first.EnvelopeID)
	}
	if !second.Replayed {
		t.Errorf("Replayed = false, want true")
	}
	if second.Status != StatusCommitted {
		t.Errorf("Status = %q, want committed (the prior Commit ran)", second.Status)
	}
}

// ---------------------------------------------------------------------------
// 7. Reserve under store.WithImmediate composes — the L2 §1.4.10.1
//    suggestion "same tx" is honoured without special casing.
// ---------------------------------------------------------------------------

func TestReserve_InsideImmediateTx(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	gen, _ := counterEnvelopeID()

	var result ReserveResult
	if err := store.WithImmediate(ctx, db, func(ctx context.Context, conn *sql.Conn) error {
		r, rerr := Reserve(ctx, conn,
			"ledger-key-tx", "turn-1", "agent:writer", 1_700_000_000,
			Options{NewEnvelopeID: gen},
		)
		if rerr != nil {
			return rerr
		}
		result = r
		return Commit(ctx, conn, "ledger-key-tx", 1_700_000_050)
	}); err != nil {
		t.Fatalf("WithImmediate: %v", err)
	}

	got, _ := Get(ctx, db, "ledger-key-tx")
	if got.Status != StatusCommitted {
		t.Errorf("Status = %q, want committed", got.Status)
	}
	if got.EnvelopeID != result.EnvelopeID {
		t.Errorf("EnvelopeID drifted across tx boundary: %q vs %q", got.EnvelopeID, result.EnvelopeID)
	}
}

// ---------------------------------------------------------------------------
// 8. Default generator returns a non-empty UUID v4 (smoke).
// ---------------------------------------------------------------------------

func TestReserve_DefaultGenerator_NonEmpty(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)

	res, err := Reserve(ctx, db,
		"ledger-key-uuid", "turn-1", "agent:writer", 1_700_000_000,
		Options{}, // no generator → defaults to UUID
	)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if len(res.EnvelopeID) < 32 {
		t.Errorf("UUID looks too short: %q", res.EnvelopeID)
	}
	if res.Replayed {
		t.Errorf("Replayed = true on first Reserve")
	}
}

// ---------------------------------------------------------------------------
// 9. Input validation.
// ---------------------------------------------------------------------------

func TestReserveCommit_InputValidation(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	gen, _ := counterEnvelopeID()

	cases := []struct {
		name string
		run  func() error
	}{
		{"reserve empty key", func() error {
			_, err := Reserve(ctx, db, "", "turn", "agent", 1, Options{NewEnvelopeID: gen})
			return err
		}},
		{"reserve empty turn", func() error {
			_, err := Reserve(ctx, db, "k", "", "agent", 1, Options{NewEnvelopeID: gen})
			return err
		}},
		{"reserve empty actor", func() error {
			_, err := Reserve(ctx, db, "k", "turn", "", 1, Options{NewEnvelopeID: gen})
			return err
		}},
		{"reserve zero now", func() error {
			_, err := Reserve(ctx, db, "k", "turn", "agent", 0, Options{NewEnvelopeID: gen})
			return err
		}},
		{"reserve empty envelope id", func() error {
			_, err := Reserve(ctx, db, "k", "turn", "agent", 1,
				Options{NewEnvelopeID: func() string { return "" }})
			return err
		}},
		{"commit empty key", func() error {
			return Commit(ctx, db, "", 1)
		}},
		{"commit zero now", func() error {
			return Commit(ctx, db, "k", 0)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.run(); !errors.Is(err, ErrInvalidInput) {
				t.Errorf("err = %v, want ErrInvalidInput", err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 10. Concurrent Reservers on the same key all observe the same
//     envelope_id — the race recovery branch (UNIQUE collision →
//     re-read) is exercised.
// ---------------------------------------------------------------------------

func TestReserve_ConcurrentSameKey(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)

	const racers = 8
	// Each goroutine proposes its own envelope_id; only one INSERT
	// wins, everyone else must surface the winner's id.
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		seen  = make(map[string]struct{})
		errs  int32
		ids   = make([]string, racers)
		ready = make(chan struct{})
	)

	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-ready
			res, err := Reserve(ctx, db,
				"shared-key", "turn-1", "agent:writer", 1_700_000_000+int64(idx),
				Options{NewEnvelopeID: func() string {
					return fmt.Sprintf("env-%d", idx)
				}},
			)
			if err != nil {
				atomic.AddInt32(&errs, 1)
				t.Errorf("racer %d: %v", idx, err)
				return
			}
			ids[idx] = res.EnvelopeID
			mu.Lock()
			seen[res.EnvelopeID] = struct{}{}
			mu.Unlock()
		}(i)
	}
	close(ready)
	wg.Wait()

	if errs != 0 {
		t.Fatalf("racers errored = %d", errs)
	}
	if len(seen) != 1 {
		t.Errorf("distinct envelope_ids observed = %d, want 1 (got %v)", len(seen), seen)
	}
	winner := ids[0]
	for i, id := range ids {
		if id != winner {
			t.Errorf("racer %d saw id %q, want %q (all racers must agree)", i, id, winner)
		}
	}
}
