package store_test

import (
	"context"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/runtime/internal/store"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// seedActor registers an actor (which seeds its actor_cursors row in the same
// tx per L2 §1.4.6) and returns its id.
func seedActor(t *testing.T, cs *store.ChannelStores, id actor.ActorID) {
	t.Helper()
	ctx := context.Background()
	if err := cs.Membership.Insert(ctx, storespec.Record{
		ID: id, Kind: actor.KindAgent, CreatedAt: 1000,
	}); err != nil {
		t.Fatalf("Insert %s: %v", id, err)
	}
}

// Insert seeds the cursor at seq 0 in the same transaction as registration.
func TestCursor_SeededByRegistration(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	seedActor(t, cs, "planner")

	cur, ok, err := cs.Cursors.Get(ctx, "planner")
	if err != nil || !ok {
		t.Fatalf("Get ok=%v err=%v (cursor must be seeded on Insert)", ok, err)
	}
	if cur.ActorID != "planner" || cur.LastConsumedSeq != 0 {
		t.Errorf("seeded cursor=%+v want {planner 0 ...}", cur)
	}
}

// Get on an unregistered actor returns ok=false (no row), not an error.
func TestCursor_GetMissing(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	_, ok, err := cs.Cursors.Get(ctx, "ghost")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if ok {
		t.Error("unregistered actor cursor ok=true want false")
	}
}

// Advance is a monotonic compare-and-set: forward moves persist and report
// ok=true; equal or backward moves are silent no-ops (ok=false, err=nil) and
// leave the stored position untouched.
func TestCursor_AdvanceMonotonicCAS(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	seedActor(t, cs, "planner")

	cases := []struct {
		name    string
		seq     storespec.Seq
		wantOK  bool
		wantPos storespec.Seq
	}{
		{"forward 0->5", 5, true, 5},
		{"forward 5->9", 9, true, 9},
		{"equal 9->9 no-op", 9, false, 9},
		{"backward 9->3 no-op", 3, false, 9},
		{"forward 9->10", 10, true, 10},
	}
	now := int64(2000)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			now++
			ok, err := cs.Cursors.Advance(ctx, "planner", tc.seq, now)
			if err != nil {
				t.Fatalf("Advance: %v", err)
			}
			if ok != tc.wantOK {
				t.Errorf("Advance ok=%v want %v", ok, tc.wantOK)
			}
			cur, _, err := cs.Cursors.Get(ctx, "planner")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if cur.LastConsumedSeq != tc.wantPos {
				t.Errorf("position=%d want %d", cur.LastConsumedSeq, tc.wantPos)
			}
		})
	}
}

// A successful Advance records the updated_at timestamp; a no-op does not
// bump it (the UPDATE matched zero rows).
func TestCursor_AdvanceUpdatesTimestampOnlyOnMove(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	seedActor(t, cs, "planner")

	if _, err := cs.Cursors.Advance(ctx, "planner", 5, 3000); err != nil {
		t.Fatalf("Advance forward: %v", err)
	}
	cur, _, _ := cs.Cursors.Get(ctx, "planner")
	if cur.UpdatedAt != 3000 {
		t.Fatalf("updated_at=%d want 3000 after move", cur.UpdatedAt)
	}
	// Backward no-op must not touch updated_at.
	if ok, _ := cs.Cursors.Advance(ctx, "planner", 2, 4000); ok {
		t.Fatal("backward Advance reported ok=true")
	}
	cur, _, _ = cs.Cursors.Get(ctx, "planner")
	if cur.UpdatedAt != 3000 {
		t.Errorf("updated_at=%d want unchanged 3000 after no-op", cur.UpdatedAt)
	}
}
