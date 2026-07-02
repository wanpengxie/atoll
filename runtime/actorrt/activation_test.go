package actorrt

import (
	"context"
	"testing"
	"time"
)

// TestLiveIDs_MatchesCurrentlySpawnedSet: LiveIDs is the map-key snapshot of
// r.embodiments — spawning two and despawning one must leave exactly the
// one still-live id in the result.
func TestLiveIDs_MatchesCurrentlySpawnedSet(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()

	a := rt.Spawn("a", static(newRecordActor()))
	rt.Spawn("b", static(newRecordActor()))
	rt.Despawn(a)

	ids := rt.LiveIDs()
	if len(ids) != 1 {
		t.Fatalf("LiveIDs() = %v, want exactly 1 entry", ids)
	}
	if ids[0] != "b" {
		t.Fatalf("LiveIDs() = %v, want [b]", ids)
	}
}

// TestSpawnIfAbsent_EmptyIDSucceeds: on an unoccupied id, SpawnIfAbsent mints
// and goes live, same as Spawn would.
func TestSpawnIfAbsent_EmptyIDSucceeds(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()

	ra := newRecordActor()
	inc, ok := rt.SpawnIfAbsent("fresh", static(ra))
	if !ok {
		t.Fatal("SpawnIfAbsent(absent id) returned ok=false")
	}
	if !rt.IsLive(inc) {
		t.Fatal("SpawnIfAbsent(absent id) did not go live")
	}
	select {
	case <-ra.startedCh:
	case <-time.After(time.Second):
		t.Fatal("SpawnIfAbsent(absent id) never started the actor")
	}
	ids := rt.LiveIDs()
	if len(ids) != 1 || ids[0] != "fresh" {
		t.Fatalf("LiveIDs() = %v, want [fresh]", ids)
	}
}

// TestSpawnIfAbsent_OccupiedIDNeverReplaces: SpawnIfAbsent is a CAS mint, NOT
// Spawn's replace semantics — on an already-occupied id it must return
// ok=false, discard the freshly-built shell WITHOUT ever calling c.start() on
// it (the discarded actor's startedCh never fires), and leave the
// pre-existing embodiment completely untouched (still the SAME incarnation,
// still live).
func TestSpawnIfAbsent_OccupiedIDNeverReplaces(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()

	original := newRecordActor()
	originalInc := rt.Spawn("taken", static(original))
	select {
	case <-original.startedCh:
	case <-time.After(time.Second):
		t.Fatal("original instance never started")
	}

	discarded := newRecordActor()
	_, ok := rt.SpawnIfAbsent("taken", static(discarded))
	if ok {
		t.Fatal("SpawnIfAbsent(occupied id) returned ok=true, want false")
	}

	// The discarded shell's build closure ran (outside the lock, same
	// discipline as Spawn/Fork), but c.start() must never have been called on
	// it — its actor's Start hook must never fire.
	select {
	case <-discarded.startedCh:
		t.Fatal("SpawnIfAbsent(occupied id) started the discarded shell")
	case <-time.After(100 * time.Millisecond):
	}

	// The pre-existing embodiment is untouched: same incarnation pointer, still
	// live, never stopped.
	if !rt.IsLive(originalInc) {
		t.Fatal("SpawnIfAbsent(occupied id) disturbed the pre-existing embodiment's liveness")
	}
	select {
	case <-original.stoppedCh:
		t.Fatal("SpawnIfAbsent(occupied id) stopped the pre-existing embodiment")
	case <-time.After(100 * time.Millisecond):
	}
	rt.mu.RLock()
	cur := rt.embodiments["taken"]
	rt.mu.RUnlock()
	if cur != originalInc.p {
		t.Fatal("SpawnIfAbsent(occupied id) replaced the pre-existing embodiment pointer")
	}
}
