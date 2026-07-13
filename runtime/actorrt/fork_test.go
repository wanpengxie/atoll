package actorrt

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
)

// TestFork_ChildLiveAndOwned is the base fork happy path: a live parent
// forks a child, the child is immediately live and addressable, and the
// ownership edge (r.owned) records it.
func TestFork_ChildLiveAndOwned(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()

	parentInc, _, _ := rt.SpawnIfAbsent("parent", actor.KindAgent, static(newRecordActor()))
	childInc, err := rt.Fork(parentInc, "parent/child", actor.KindAgent, static(newRecordActor()))
	if err != nil {
		t.Fatalf("Fork: %v", err)
	}
	if !rt.IsLive(childInc) {
		t.Fatal("child not live immediately after Fork")
	}

	rt.mu.RLock()
	children := append([]embodiment(nil), rt.owned[parentInc.p]...)
	rt.mu.RUnlock()
	found := false
	for _, ch := range children {
		if ch == childInc.p {
			found = true
		}
	}
	if !found {
		t.Fatalf("r.owned[parent] = %v, does not contain forked child", children)
	}
}

// TestFork_ParentNotLive_FastPath: Fork on an already-dead parent fails fast
// with ErrParentNotLive (the lock-free entry check) — no child embodiment
// is ever inserted.
func TestFork_ParentNotLive_FastPath(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()

	parentInc, _, _ := rt.SpawnIfAbsent("parent", actor.KindAgent, static(newRecordActor()))
	rt.Despawn(parentInc)

	_, err := rt.Fork(parentInc, "parent/child", actor.KindAgent, static(newRecordActor()))
	if !errors.Is(err, ErrParentNotLive) {
		t.Fatalf("Fork(dead parent) = %v, want ErrParentNotLive", err)
	}
	if _, ok := rt.embodiments["parent/child"]; ok {
		t.Fatal("Fork on a dead parent inserted an embodiment anyway")
	}
}

// TestFork_ChildIDCollision_HardFail: a childID that already names a live
// embodiment is a HARD failure — NOT Attach's last-go-live-wins replace
// semantics. The pre-existing embodiment must be untouched.
func TestFork_ChildIDCollision_HardFail(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()

	parentInc, _, _ := rt.SpawnIfAbsent("parent", actor.KindAgent, static(newRecordActor()))
	existing, _, _ := rt.SpawnIfAbsent("parent/child", actor.KindAgent, static(newRecordActor()))

	_, err := rt.Fork(parentInc, "parent/child", actor.KindAgent, static(newRecordActor()))
	if !errors.Is(err, ErrChildIDCollision) {
		t.Fatalf("Fork(colliding id) = %v, want ErrChildIDCollision", err)
	}
	if !rt.IsLive(existing) {
		t.Fatal("colliding Fork killed/replaced the pre-existing embodiment")
	}
	rt.mu.RLock()
	cur := rt.embodiments["parent/child"]
	rt.mu.RUnlock()
	if cur != existing.p {
		t.Fatal("embodiments[childID] pointer changed after a colliding Fork — must not replace")
	}
}

// TestFork_PrunesDeadChildrenOnNextFork ensures r.owned does not grow
// unbounded on a long-lived parent that forks many short-lived children —
// each Fork call prunes already-not-live entries before appending the new
// one.
func TestFork_PrunesDeadChildrenOnNextFork(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()

	parentInc, _, _ := rt.SpawnIfAbsent("parent", actor.KindAgent, static(newRecordActor()))

	c1, err := rt.Fork(parentInc, "parent/c1", actor.KindAgent, static(newRecordActor()))
	if err != nil {
		t.Fatalf("Fork c1: %v", err)
	}
	rt.Despawn(c1) // c1 dies independently of parent — a stale r.owned entry.

	c2, err := rt.Fork(parentInc, "parent/c2", actor.KindAgent, static(newRecordActor()))
	if err != nil {
		t.Fatalf("Fork c2: %v", err)
	}

	rt.mu.RLock()
	children := append([]embodiment(nil), rt.owned[parentInc.p]...)
	rt.mu.RUnlock()
	if len(children) != 1 || children[0] != c2.p {
		t.Fatalf("r.owned[parent] = %v (len %d), want exactly [c2] — dead c1 must be pruned on the next Fork", children, len(children))
	}
}

// TestFork_CascadeOnParentDeath: killing the parent must signal-cascade the
// still-live child to death WITHOUT the parent's own teardown blocking on
// the child's goroutine — Despawn(parent) must return promptly, and the
// child must already be non-live and unaddressable by the time it does.
func TestFork_CascadeOnParentDeath(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()

	parentInc, _, _ := rt.SpawnIfAbsent("parent", actor.KindAgent, static(newRecordActor()))
	childInc, err := rt.Fork(parentInc, "parent/child", actor.KindAgent, static(newRecordActor()))
	if err != nil {
		t.Fatalf("Fork: %v", err)
	}
	if !rt.IsLive(childInc) {
		t.Fatal("child not live after Fork")
	}

	done := make(chan struct{})
	start := time.Now()
	go func() {
		rt.Despawn(parentInc)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Despawn(parent) hung — teardown must be O(judge-dead), never join a child")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("Despawn(parent) took %v — teardown must not block on the cascade", elapsed)
	}

	// The cascade now runs on the parent's escort goroutine (the teardown signal
	// is fired there, off the entry path), so the child dies WITHIN grace rather
	// than synchronously with Despawn's return. Poll for it.
	deadline := time.Now().Add(2 * time.Second)
	for rt.IsLive(childInc) && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if rt.IsLive(childInc) {
		t.Fatal("child still live after parent death — cascade did not fire")
	}
	for {
		rt.mu.RLock()
		_, hosted := rt.embodiments["parent/child"]
		rt.mu.RUnlock()
		if !hosted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("child still addressable in r.embodiments after cascade — must be evicted within grace")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestDespawnChild_AuthorityCheck exercises SpawnHandle.Despawn's underlying
// authority gate: a by-id request only succeeds if childID is owned by the
// incarnation presenting the request; a mismatched incarnation is rejected
// and must NOT be able to kill someone else's child.
func TestDespawnChild_AuthorityCheck(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()

	parentInc, _, _ := rt.SpawnIfAbsent("parent", actor.KindAgent, static(newRecordActor()))
	otherInc, _, _ := rt.SpawnIfAbsent("other", actor.KindAgent, static(newRecordActor()))
	childInc, err := rt.Fork(parentInc, "parent/child", actor.KindAgent, static(newRecordActor()))
	if err != nil {
		t.Fatalf("Fork: %v", err)
	}

	if err := rt.DespawnChild(otherInc, "parent/child"); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("DespawnChild(non-owner) = %v, want ErrNotOwner", err)
	}
	if !rt.IsLive(childInc) {
		t.Fatal("non-owner DespawnChild killed a child it does not own")
	}

	if err := rt.DespawnChild(parentInc, "parent/child"); err != nil {
		t.Fatalf("DespawnChild(owner): %v", err)
	}
	if rt.IsLive(childInc) {
		t.Fatal("child still live after owner DespawnChild")
	}
}
