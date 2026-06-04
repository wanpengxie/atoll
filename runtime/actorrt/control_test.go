package actorrt

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// controllableActor records control signals delivered to its control lane.
type controllableActor struct {
	mu     sync.Mutex
	got    []Signal
	notify chan struct{}
}

func (c *controllableActor) Receive(context.Context, *message.Envelope) error { return nil }
func (c *controllableActor) OnControl(_ context.Context, sig Signal) {
	c.mu.Lock()
	c.got = append(c.got, sig)
	c.mu.Unlock()
	if c.notify != nil {
		c.notify <- struct{}{}
	}
}

// TestRaiseUnknownKindRejected: the control vocabulary is a substrate-owned
// closed set, enforced at Raise — an unknown kind is rejected, never delivered.
func TestRaiseUnknownKindRejected(t *testing.T) {
	t.Parallel()
	rt, _, ctrl := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	rt.Spawn("a", &controllableActor{})
	if err := ctrl.Raise("a", Signal{Kind: SignalKind("adapter.bogus")}); err != ErrUnknownSignal {
		t.Fatalf("Raise(unknown kind) err = %v, want ErrUnknownSignal", err)
	}
}

// TestRaiseNotHosted: raising at an unhosted id is reported, not silently
// swallowed.
func TestRaiseNotHosted(t *testing.T) {
	t.Parallel()
	rt, _, ctrl := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	if err := ctrl.Raise("ghost", Signal{Kind: SignalReload}); err != ErrNotHosted {
		t.Fatalf("Raise(unhosted) err = %v, want ErrNotHosted", err)
	}
}

// TestControllableReceivesSignal: a Controllable actor gets the signal on its
// control lane, dispatched on its own goroutine (serial with Receive).
func TestControllableReceivesSignal(t *testing.T) {
	t.Parallel()
	ca := &controllableActor{notify: make(chan struct{}, 1)}
	rt, _, ctrl := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	rt.Spawn("a", ca)

	if err := ctrl.Raise("a", Signal{Kind: SignalReload, Payload: []byte("cfg")}); err != nil {
		t.Fatalf("Raise: %v", err)
	}
	select {
	case <-ca.notify:
	case <-time.After(2 * time.Second):
		t.Fatal("control signal never reached OnControl")
	}
	ca.mu.Lock()
	defer ca.mu.Unlock()
	if len(ca.got) != 1 || ca.got[0].Kind != SignalReload || string(ca.got[0].Payload) != "cfg" {
		t.Fatalf("OnControl got %+v, want one reload with payload cfg", ca.got)
	}
}

// TestDefaultStopCancelsCell: an actor that does NOT implement Controllable gets
// the runtime default disposition — SignalStop self-cancels the cell (clean exit,
// no death edge), making it unaddressable.
func TestDefaultStopCancelsCell(t *testing.T) {
	t.Parallel()
	w := &recordingWatcher{notify: make(chan struct{}, 1)}
	rt, _, ctrl := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	rt.WatchPresence(w)
	rt.Spawn("a", newRecordActor()) // not Controllable

	if err := ctrl.Raise("a", Signal{Kind: SignalStop}); err != nil {
		t.Fatalf("Raise stop: %v", err)
	}
	// The cell self-cancels and self-evicts.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := rt.Stat("a"); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("SignalStop default disposition did not stop the cell")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// A clean default-stop is NOT a death — no presence-down edge.
	select {
	case <-w.notify:
		t.Fatal("default SignalStop published a death edge — clean stop must be silent")
	case <-time.After(150 * time.Millisecond):
	}
}

// TestSpawnDespawnRaceNoDeadlock locks the replacement/teardown window with TWO
// concurrent goroutines hammering the SAME id — one replacing (Spawn), one
// despawning. This actually enters the "map-insert unlocked, start() not yet
// run, another goroutine tears down" interleaving (not a single-goroutine
// sequence) and must neither deadlock nor (under -race) race.
func TestSpawnDespawnRaceNoDeadlock(t *testing.T) {
	t.Parallel()
	rt, _, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	const id = actor.ActorID("x")
	done := make(chan struct{}, 2)
	go func() {
		for i := 0; i < 500; i++ {
			rt.Spawn(id, newRecordActor())
		}
		done <- struct{}{}
	}()
	go func() {
		for i := 0; i < 500; i++ {
			rt.Despawn(id)
		}
		done <- struct{}{}
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Spawn/Despawn race deadlocked")
		}
	}
}
