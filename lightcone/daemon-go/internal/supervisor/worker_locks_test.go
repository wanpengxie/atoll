package supervisor

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/coagent-ai/daemon-go/internal/store"
)

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const (
	testAgentID   = "agent:writer"
	testWorker1ID = "worker-1"
	testWorker2ID = "worker-2"
	testTTL       = int64(60)
)

// openChannel mirrors the registry test helper — fresh channel sqlite
// under t.TempDir() with full L2 channel DDL applied.
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

// fixedClock builds a now-func that returns a controlled timestamp.
// Tests advance the clock by reassigning *t.
func fixedClock(ts *int64) func() int64 {
	return func() int64 { return *ts }
}

// ---------------------------------------------------------------------------
// 1. Acquire happy path — first-ever spawn writes fencing_token=1.
// ---------------------------------------------------------------------------

func TestAcquire_FirstEver_InsertsTokenOne(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	now := int64(1_700_000_000)

	lock, err := Acquire(ctx, db, testAgentID, testWorker1ID, testTTL, fixedClock(&now))
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if lock.AgentID != testAgentID || lock.WorkerID != testWorker1ID {
		t.Errorf("Lock identity mismatch: %+v", lock)
	}
	if lock.FencingToken != 1 {
		t.Errorf("FencingToken = %d, want 1", lock.FencingToken)
	}
	if lock.LeaseExpiresAt != now+testTTL {
		t.Errorf("LeaseExpiresAt = %d, want %d", lock.LeaseExpiresAt, now+testTTL)
	}
	if lock.AcquiredAt != now {
		t.Errorf("AcquiredAt = %d, want %d", lock.AcquiredAt, now)
	}

	// Row really persisted.
	got, err := Get(ctx, db, testAgentID)
	if err != nil {
		t.Fatalf("Get after Acquire: %v", err)
	}
	if *got != *lock {
		t.Errorf("Get mismatch: got %+v want %+v", got, lock)
	}
}

// ---------------------------------------------------------------------------
// 2. Acquire rejects when an existing lease is still valid.
// ---------------------------------------------------------------------------

func TestAcquire_LockHeld_RejectsWhileLeaseValid(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	now := int64(1_700_000_000)

	if _, err := Acquire(ctx, db, testAgentID, testWorker1ID, testTTL, fixedClock(&now)); err != nil {
		t.Fatalf("Acquire #1: %v", err)
	}

	// Even mid-lease (now+1, well below now+TTL), Acquire MUST refuse.
	mid := now + 1
	_, err := Acquire(ctx, db, testAgentID, testWorker2ID, testTTL, fixedClock(&mid))
	if !errors.Is(err, ErrLockHeld) {
		t.Fatalf("Acquire #2 err = %v, want ErrLockHeld", err)
	}

	// Lock identity unchanged.
	got, _ := Get(ctx, db, testAgentID)
	if got.WorkerID != testWorker1ID {
		t.Errorf("Lock taken over while held: worker_id = %q", got.WorkerID)
	}
	if got.FencingToken != 1 {
		t.Errorf("FencingToken bumped while lock held: got %d", got.FencingToken)
	}
}

// ---------------------------------------------------------------------------
// 3. Acquire steals an expired lease and bumps fencing_token.
// ---------------------------------------------------------------------------

func TestAcquire_StealsExpiredLease_BumpsFencingToken(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	now := int64(1_700_000_000)

	lock1, err := Acquire(ctx, db, testAgentID, testWorker1ID, testTTL, fixedClock(&now))
	if err != nil {
		t.Fatalf("Acquire #1: %v", err)
	}

	// Jump past the lease (== expired, the spec predicate uses `<=`).
	stealAt := lock1.LeaseExpiresAt
	lock2, err := Acquire(ctx, db, testAgentID, testWorker2ID, testTTL, fixedClock(&stealAt))
	if err != nil {
		t.Fatalf("Acquire #2 (steal): %v", err)
	}
	if lock2.WorkerID != testWorker2ID {
		t.Errorf("Stolen lock worker_id = %q, want %q", lock2.WorkerID, testWorker2ID)
	}
	if lock2.FencingToken != lock1.FencingToken+1 {
		t.Errorf("FencingToken did not bump: %d -> %d", lock1.FencingToken, lock2.FencingToken)
	}
	if lock2.LeaseExpiresAt != stealAt+testTTL {
		t.Errorf("LeaseExpiresAt = %d, want %d", lock2.LeaseExpiresAt, stealAt+testTTL)
	}
	if lock2.AcquiredAt != stealAt {
		t.Errorf("AcquiredAt = %d, want %d", lock2.AcquiredAt, stealAt)
	}

	// Second steal at the same wall clock with same token MUST also
	// succeed (the worker died, supervisor pre-checked Expired()).
	stealAgain := stealAt + testTTL
	lock3, err := Acquire(ctx, db, testAgentID, "worker-3", testTTL, fixedClock(&stealAgain))
	if err != nil {
		t.Fatalf("Acquire #3 (re-steal): %v", err)
	}
	if lock3.FencingToken != 3 {
		t.Errorf("Re-steal FencingToken = %d, want 3", lock3.FencingToken)
	}
}

// ---------------------------------------------------------------------------
// 4. Heartbeat happy path — extends lease, leaves fencing_token alone.
// ---------------------------------------------------------------------------

func TestHeartbeat_ExtendsLease(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	now := int64(1_700_000_000)

	lock, err := Acquire(ctx, db, testAgentID, testWorker1ID, testTTL, fixedClock(&now))
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	hbAt := now + 30
	if err := Heartbeat(ctx, db, testAgentID, testWorker1ID, lock.FencingToken, testTTL, fixedClock(&hbAt)); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	got, _ := Get(ctx, db, testAgentID)
	if got.LeaseExpiresAt != hbAt+testTTL {
		t.Errorf("LeaseExpiresAt = %d, want %d", got.LeaseExpiresAt, hbAt+testTTL)
	}
	if got.FencingToken != lock.FencingToken {
		t.Errorf("FencingToken changed during heartbeat: %d -> %d", lock.FencingToken, got.FencingToken)
	}
	if got.WorkerID != testWorker1ID {
		t.Errorf("Heartbeat altered worker_id: %q", got.WorkerID)
	}
}

// ---------------------------------------------------------------------------
// 5. Heartbeat returns ErrFencingStale after the lock was stolen.
// ---------------------------------------------------------------------------

func TestHeartbeat_StaleAfterSteal(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	now := int64(1_700_000_000)

	lock1, err := Acquire(ctx, db, testAgentID, testWorker1ID, testTTL, fixedClock(&now))
	if err != nil {
		t.Fatalf("Acquire #1: %v", err)
	}

	stealAt := lock1.LeaseExpiresAt + 1
	if _, err := Acquire(ctx, db, testAgentID, testWorker2ID, testTTL, fixedClock(&stealAt)); err != nil {
		t.Fatalf("Acquire #2 (steal): %v", err)
	}

	// Old worker tries to heartbeat with its old fencing_token.
	hbAt := stealAt + 5
	err = Heartbeat(ctx, db, testAgentID, testWorker1ID, lock1.FencingToken, testTTL, fixedClock(&hbAt))
	if !errors.Is(err, ErrFencingStale) {
		t.Fatalf("Heartbeat err = %v, want ErrFencingStale", err)
	}

	// The lease the new owner set MUST stay intact.
	got, _ := Get(ctx, db, testAgentID)
	if got.WorkerID != testWorker2ID || got.FencingToken != lock1.FencingToken+1 {
		t.Errorf("New owner state corrupted by stale heartbeat: %+v", got)
	}
	if got.LeaseExpiresAt != stealAt+testTTL {
		t.Errorf("Stale heartbeat extended someone else's lease: %d", got.LeaseExpiresAt)
	}
}

// ---------------------------------------------------------------------------
// 6. Heartbeat on a never-acquired agent returns ErrLockMissing.
// ---------------------------------------------------------------------------

func TestHeartbeat_NoRow_Missing(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	now := int64(1_700_000_000)

	err := Heartbeat(ctx, db, "agent:nope", "worker-x", 1, testTTL, fixedClock(&now))
	if !errors.Is(err, ErrLockMissing) {
		t.Fatalf("Heartbeat err = %v, want ErrLockMissing", err)
	}
}

// ---------------------------------------------------------------------------
// 7. Release deletes own row; double-release is ErrLockMissing.
// ---------------------------------------------------------------------------

func TestRelease_SelfDelete(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	now := int64(1_700_000_000)

	if _, err := Acquire(ctx, db, testAgentID, testWorker1ID, testTTL, fixedClock(&now)); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	if err := Release(ctx, db, testAgentID, testWorker1ID); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if _, err := Get(ctx, db, testAgentID); !errors.Is(err, ErrLockMissing) {
		t.Errorf("Get after Release err = %v, want ErrLockMissing", err)
	}

	// Idempotent double-release surfaces missing rather than ambiguity.
	if err := Release(ctx, db, testAgentID, testWorker1ID); !errors.Is(err, ErrLockMissing) {
		t.Errorf("second Release err = %v, want ErrLockMissing", err)
	}
}

// ---------------------------------------------------------------------------
// 8. Release does NOT delete a row owned by someone else.
// ---------------------------------------------------------------------------

func TestRelease_DoesNotTouchOtherWorker(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	now := int64(1_700_000_000)

	if _, err := Acquire(ctx, db, testAgentID, testWorker1ID, testTTL, fixedClock(&now)); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	// Worker-2 (the zombie) tries to Release worker-1's row.
	if err := Release(ctx, db, testAgentID, testWorker2ID); !errors.Is(err, ErrLockMissing) {
		t.Errorf("Stale Release err = %v, want ErrLockMissing", err)
	}

	// Row still there.
	got, err := Get(ctx, db, testAgentID)
	if err != nil {
		t.Fatalf("Get after stale Release: %v", err)
	}
	if got.WorkerID != testWorker1ID {
		t.Errorf("Stale Release deleted real owner: %+v", got)
	}
}

// ---------------------------------------------------------------------------
// 9. Acquire / Heartbeat / Release input validation.
// ---------------------------------------------------------------------------

func TestInputValidation(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	now := int64(1_700_000_000)
	clk := fixedClock(&now)

	cases := []struct {
		name string
		run  func() error
	}{
		{"acquire empty agent", func() error {
			_, err := Acquire(ctx, db, "", testWorker1ID, testTTL, clk)
			return err
		}},
		{"acquire empty worker", func() error {
			_, err := Acquire(ctx, db, testAgentID, "", testTTL, clk)
			return err
		}},
		{"acquire zero ttl", func() error {
			_, err := Acquire(ctx, db, testAgentID, testWorker1ID, 0, clk)
			return err
		}},
		{"acquire nil clock", func() error {
			_, err := Acquire(ctx, db, testAgentID, testWorker1ID, testTTL, nil)
			return err
		}},
		{"heartbeat empty agent", func() error {
			return Heartbeat(ctx, db, "", testWorker1ID, 1, testTTL, clk)
		}},
		{"heartbeat zero token", func() error {
			return Heartbeat(ctx, db, testAgentID, testWorker1ID, 0, testTTL, clk)
		}},
		{"release empty agent", func() error {
			return Release(ctx, db, "", testWorker1ID)
		}},
		{"release empty worker", func() error {
			return Release(ctx, db, testAgentID, "")
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
// 10. Lock.Expired predicate sanity (matches L2 §1.4.9 `<=` semantics).
// ---------------------------------------------------------------------------

func TestLock_Expired(t *testing.T) {
	l := Lock{LeaseExpiresAt: 100}
	cases := []struct {
		now  int64
		want bool
	}{
		{99, false},
		{100, true}, // boundary is expired per spec `<=`
		{101, true},
	}
	for _, tc := range cases {
		if got := l.Expired(tc.now); got != tc.want {
			t.Errorf("Expired(%d) = %v, want %v", tc.now, got, tc.want)
		}
	}
}
