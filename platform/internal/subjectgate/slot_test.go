package subjectgate

import (
	"sync"
	"testing"
	"time"
)

const testID = "human:alice"

// TestPublishLevelEdgeSeqDedup pins same-epoch strictly-increasing dedup
// (build spec §2 pair C×E / DoD-4 edgeSeq去重).
func TestPublishLevelEdgeSeqDedup(t *testing.T) {
	s := newSlot(testID)
	var got []PresenceUpdate
	s.RegisterObserver("tok", func(u PresenceUpdate) { got = append(got, u) })
	if !s.PublishLevel(1, 1, LevelOnline) {
		t.Fatal("first edge should apply")
	}
	if s.PublishLevel(1, 1, LevelOffline) {
		t.Fatal("duplicate edgeSeq must be dropped")
	}
	if s.PublishLevel(1, 0, LevelOffline) {
		t.Fatal("lower edgeSeq must be dropped")
	}
	if !s.PublishLevel(1, 2, LevelOffline) {
		t.Fatal("higher edgeSeq should apply")
	}
	if len(got) != 2 {
		t.Fatalf("want 2 delivered edges, got %d: %+v", len(got), got)
	}
}

// TestPublishLevelNewEpochRevokeThenSnapshot pins new-epoch = revoke old then
// snapshot new (build spec §2 pair C×E / DoD-4).
func TestPublishLevelNewEpochRevokeThenSnapshot(t *testing.T) {
	s := newSlot(testID)
	var got []PresenceUpdate
	s.RegisterObserver("tok", func(u PresenceUpdate) { got = append(got, u) })
	s.PublishLevel(1, 5, LevelOnline)
	got = nil
	if !s.PublishLevel(2, 1, LevelOnline) {
		t.Fatal("new epoch should apply")
	}
	if len(got) != 2 || got[0].Live || got[0].Epoch != 1 || !got[1].Live || got[1].Epoch != 2 {
		t.Fatalf("want revoke(epoch1) then snapshot(epoch2), got %+v", got)
	}
	// A lesser epoch is stale.
	if s.PublishLevel(1, 99, LevelOffline) {
		t.Fatal("stale (lesser) epoch must be dropped")
	}
}

// TestIndependenceInvariant pins that a layer-2 rebind produces ZERO presence
// side effect (build spec §2 pair B×E / design §5.2 独立性不变式).
func TestIndependenceInvariant(t *testing.T) {
	s := newSlot(testID)
	fired := false
	s.RegisterObserver("tok", func(PresenceUpdate) { fired = true })
	s.PublishLevel(1, 1, LevelOnline)
	fired = false
	s.SetBinding(2)
	s.SetBinding(3)
	if fired {
		t.Fatal("SetBinding must not touch the presence register (禁互训)")
	}
	if level, _, _, ok := s.Snapshot(); !ok || level != LevelOnline {
		t.Fatalf("rebind altered presence snapshot: %v %v", level, ok)
	}
	if s.BindingGen() != 3 {
		t.Fatalf("bindingGen not updated: %d", s.BindingGen())
	}
}

// TestObserverPointerGenerationRemoval pins that an old cell's摘除 (its token)
// cannot remove a newer cell's registration (build spec §2 pair C×B / DoD-4
// 旧观察者摘不掉新登记).
func TestObserverPointerGenerationRemoval(t *testing.T) {
	s := newSlot(testID)
	var newFired int
	s.RegisterObserver("old", func(PresenceUpdate) {})
	s.RegisterObserver("new", func(PresenceUpdate) { newFired++ })
	// The old cell tears down and摘除 with ITS token.
	s.RemoveObserver("old")
	s.PublishLevel(1, 1, LevelOnline)
	if newFired != 1 {
		t.Fatalf("new observer should still receive edges after old摘除: %d", newFired)
	}
}

// TestForgetRevokesAndFoldsUnknown pins Forget = revoke + unknown (design §5.4).
func TestForgetRevokesAndFoldsUnknown(t *testing.T) {
	s := newSlot(testID)
	var last PresenceUpdate
	s.RegisterObserver("tok", func(u PresenceUpdate) { last = u })
	s.PublishLevel(1, 1, LevelOnline)
	s.Forget()
	if last.Live {
		t.Fatal("Forget should deliver a revocation")
	}
	if _, _, _, ok := s.Snapshot(); ok {
		t.Fatal("Forget should fold the register to unknown (no value)")
	}
}

// TestDeliverSyncAndUnblock pins the帧递交端 synchronous request/reply +解阻:
// an attached interpreter answers Deliver synchronously; after release a blocked
// Deliver returns ErrNoOccupant.
func TestDeliverSyncAndUnblock(t *testing.T) {
	s := newSlot(testID)
	ch, release := s.AttachInterpreter()

	var wg sync.WaitGroup
	wg.Add(1)
	stop := make(chan struct{})
	go func() {
		defer wg.Done()
		for {
			select {
			case job := <-ch:
				r, _ := NewFrame(FrameReceipt, 0, job.Frame.Ref, SubmitReceipt{MessageID: "m1", Seq: 1})
				job.Reply(FrameResult{Frame: r})
			case <-stop:
				return
			}
		}
	}()

	f, _ := NewFrame(FrameSubmit, 0, "ref-9", SubmitPayload{MsgType: "m"})
	res, err := s.Deliver(f)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if res.Frame.Type != FrameReceipt || res.Frame.Ref != "ref-9" {
		t.Fatalf("unexpected receipt: %+v", res.Frame)
	}

	// Tear down: stop the loop, release the slot; a subsequent Deliver refuses.
	close(stop)
	wg.Wait()
	release()
	if _, err := s.Deliver(f); err != ErrNoOccupant {
		t.Fatalf("Deliver after release want ErrNoOccupant, got %v", err)
	}
}

// TestDeliverBlockedUnblocksOnRelease pins解阻 for a Deliver already blocked on
// an idle interpreter when the interpreter detaches.
func TestDeliverBlockedUnblocksOnRelease(t *testing.T) {
	s := newSlot(testID)
	_, release := s.AttachInterpreter() // attached but nobody reads the channel

	done := make(chan error, 1)
	go func() {
		f, _ := NewFrame(FrameSubmit, 0, "r", SubmitPayload{MsgType: "m"})
		_, err := s.Deliver(f)
		done <- err
	}()

	// Give the Deliver goroutine time to block on the send, then release.
	time.Sleep(20 * time.Millisecond)
	release()
	select {
	case err := <-done:
		if err != ErrNoOccupant {
			t.Fatalf("want ErrNoOccupant on release, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Deliver did not unblock on release (解阻 broken)")
	}
}

// TestRegistryEnsureIdempotent pins EnsureSlot idempotency + Slot/Remove.
func TestRegistryEnsureIdempotent(t *testing.T) {
	r := NewRegistry()
	s1 := r.EnsureSlot(testID)
	s2 := r.EnsureSlot(testID)
	if s1 != s2 {
		t.Fatal("EnsureSlot must be idempotent (same slot)")
	}
	if got, ok := r.Slot(testID); !ok || got != s1 {
		t.Fatal("Slot lookup mismatch")
	}
	r.Remove(testID)
	if _, ok := r.Slot(testID); ok {
		t.Fatal("Remove should drop the slot")
	}
}
