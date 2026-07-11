package actorrt

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
)

func TestSpawnIfAbsent_BuilderFailuresAreTypedAndLeaveNoEmbodiment(t *testing.T) {
	for _, tc := range []struct {
		name  string
		build func(Incarnation) Actor
		nil   bool
	}{
		{"nil", func(Incarnation) Actor { return nil }, true},
		{"panic", func(Incarnation) Actor { panic("boom") }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rt, _ := New(Config{Parent: context.Background()})
			_, built, err := rt.SpawnIfAbsent("failed", actor.KindAgent, tc.build)
			var failure *BuildFailure
			if built || !errors.As(err, &failure) || failure.NilActor != tc.nil {
				t.Fatalf("built=%v err=%v failure=%+v", built, err, failure)
			}
			if _, live := rt.Stat("failed"); live || len(rt.LiveIDs()) != 0 {
				t.Fatal("failed builder left a live or registered embodiment")
			}
		})
	}
}

// TestLiveIDs_MatchesCurrentlySpawnedSet: LiveIDs is the map-key snapshot of
// r.embodiments — spawning two and despawning one must leave exactly the
// one still-live id in the result.
func TestLiveIDs_MatchesCurrentlySpawnedSet(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()

	a, _, _ := rt.SpawnIfAbsent("a", actor.KindAgent, static(newRecordActor()))
	_, _, _ = rt.SpawnIfAbsent("b", actor.KindAgent, static(newRecordActor()))
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
	inc, ok, err := rt.SpawnIfAbsent("fresh", actor.KindAgent, static(ra))
	if err != nil {
		t.Fatal(err)
	}
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
	originalInc, _, _ := rt.SpawnIfAbsent("taken", actor.KindAgent, static(original))
	select {
	case <-original.startedCh:
	case <-time.After(time.Second):
		t.Fatal("original instance never started")
	}

	discarded := newRecordActor()
	_, ok, err := rt.SpawnIfAbsent("taken", actor.KindAgent, static(discarded))
	if err != nil {
		t.Fatal(err)
	}
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
