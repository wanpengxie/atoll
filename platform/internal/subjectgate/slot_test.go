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
	ch, _, release := s.AttachInterpreter()

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
	res, err := s.Deliver(f, 0)
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
	if _, err := s.Deliver(f, 0); err != ErrNoOccupant {
		t.Fatalf("Deliver after release want ErrNoOccupant, got %v", err)
	}
}

// TestDeliverBlockedUnblocksOnRelease pins解阻 for a Deliver already blocked on
// an idle interpreter when the interpreter detaches.
func TestDeliverBlockedUnblocksOnRelease(t *testing.T) {
	s := newSlot(testID)
	_, _, release := s.AttachInterpreter() // attached but nobody reads the channel

	done := make(chan error, 1)
	go func() {
		f, _ := NewFrame(FrameSubmit, 0, "r", SubmitPayload{MsgType: "m"})
		_, err := s.Deliver(f, 0)
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

// TestAttachInterpreterIncarnationGate: an old incarnation's delayed release must
// not摘掉 the successor that already took over (C×interpreter straddle). A attaches,
// B attaches (覆盖), A releases late — B is still live and a Deliver reaches B; only
// once B releases does the slot refuse (P0-5 incarnation gate).
func TestAttachInterpreterIncarnationGate(t *testing.T) {
	s := newSlot(testID)
	_, tokA, relA := s.AttachInterpreter()
	framesB, tokB, relB := s.AttachInterpreter()
	if tokA == tokB {
		t.Fatal("each AttachInterpreter must mint a distinct incarnation token")
	}

	// B answers one job.
	go func() {
		job := <-framesB
		r, _ := NewFrame(FrameReceipt, 0, job.Frame.Ref, SubmitReceipt{MessageID: "mb", Seq: 1})
		job.Reply(FrameResult{Frame: r})
	}()

	// A's stale release: it must NOT flip B's liveness.
	relA()

	f, _ := NewFrame(FrameSubmit, 0, "ref-b", SubmitPayload{MsgType: "m"})
	res, err := s.Deliver(f, 0)
	if err != nil {
		t.Fatalf("Deliver after stale release of A must reach live B, got %v", err)
	}
	if res.Frame.Ref != "ref-b" {
		t.Fatalf("unexpected receipt from B: %+v", res.Frame)
	}

	// Now B releases (the current incarnation) → the slot refuses.
	relB()
	if _, err := s.Deliver(f, 0); err != ErrNoOccupant {
		t.Fatalf("Deliver after current release want ErrNoOccupant, got %v", err)
	}
}

// TestDeliverStaleBindingRefused pins the递交线性化点世代 gate (P0-1 下half): a
// frame carrying a superseded binding_gen is refused ErrStaleBinding UNDER the slot
// lock (atomic with the live check), while the current gen delivers. This is the
// slot-side half of the双向世代 gate that closes the初验→seal→rebind→Deliver window.
func TestDeliverStaleBindingRefused(t *testing.T) {
	s := newSlot(testID)
	ch, _, release := s.AttachInterpreter()
	defer release()
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case job := <-ch:
				r, _ := NewFrame(FrameReceipt, 0, job.Frame.Ref, SubmitReceipt{MessageID: "m", Seq: 1})
				job.Reply(FrameResult{Frame: r})
			case <-stop:
				return
			}
		}
	}()
	defer close(stop)

	s.SetBinding(1) // first binding
	s.SetBinding(2) // rebind (seal → fresh arm → SetBinding) supersedes gen 1

	f, _ := NewFrame(FrameSubmit, 1, "ref-stale", SubmitPayload{MsgType: "m"})
	if _, err := s.Deliver(f, 1); err != ErrStaleBinding {
		t.Fatalf("a stale binding_gen must be refused ErrStaleBinding at the linearization point, got %v", err)
	}
	// The current binding gen delivers.
	if _, err := s.Deliver(f, 2); err != nil {
		t.Fatalf("current binding_gen must deliver, got %v", err)
	}
	// DeliverAnyGen bypasses the assertion (trusted internal path).
	if _, err := s.Deliver(f, DeliverAnyGen); err != nil {
		t.Fatalf("DeliverAnyGen must bypass the gen gate, got %v", err)
	}
}

// TestBindingGuardSerializesRebindAfterCommit pins the五轮 P0 fix: a SetBinding
// (rebind) that races an in-flight interpreter commit is held by the绑定世代提交守卫's
// exclusive Lock until the commit's落账动词 has run — the复核↔落账 third window (gen-read
// → verb) is closed. The commit ran under the still-valid old binding, so its
// serialization序 is commit ≺ rebind (legal), and the rebind returns only afterwards.
func TestBindingGuardSerializesRebindAfterCommit(t *testing.T) {
	s := newSlot(testID)
	s.SetBinding(1)

	entered := make(chan struct{})
	proceed := make(chan struct{})
	committed := make(chan bool, 1)
	go func() {
		_, ok := s.WithBindingGuard(1, func(int64) {
			close(entered) // recheck passed — inside the commit临界区 (RLock held)
			<-proceed      // hold the落账动词 open so a rebind must wait
		})
		committed <- ok
	}()
	<-entered // the commit is inside the guard with the verb not yet done

	// A concurrent rebind must NOT return while the commit still holds the RLock.
	rebindDone := make(chan struct{})
	go func() { s.SetBinding(2); close(rebindDone) }()
	select {
	case <-rebindDone:
		t.Fatal("SetBinding returned while an in-flight commit still held the guard (window open)")
	case <-time.After(50 * time.Millisecond):
		// expected: the rebind is parked on the exclusive Lock.
	}

	// Release the commit: it lands legally under the old binding, THEN the rebind returns.
	close(proceed)
	if ok := <-committed; !ok {
		t.Fatal("commit under the still-current binding must succeed (串行化序 commit ≺ rebind)")
	}
	select {
	case <-rebindDone:
	case <-time.After(2 * time.Second):
		t.Fatal("SetBinding did not return after the commit released the guard")
	}
	if s.BindingGen() != 2 {
		t.Fatalf("rebind must have advanced the binding to 2, got %d", s.BindingGen())
	}
}

// TestBindingGuardRefusesCommitAfterRebind pins the reverse serialization: a rebind
// that完成 BEFORE the commit's recheck → the commit sees the advanced gen and is refused
// (ok=false), and its落账动词 NEVER runs (the frame never lands in the successor binding).
func TestBindingGuardRefusesCommitAfterRebind(t *testing.T) {
	s := newSlot(testID)
	s.SetBinding(1)
	s.SetBinding(2) // rebind completes first

	ran := false
	if _, ok := s.WithBindingGuard(1, func(int64) { ran = true }); ok {
		t.Fatal("a commit carrying the stale gen 1 must be refused after the rebind to 2")
	}
	if ran {
		t.Fatal("the落账动词 must NOT run for a refused (stale) commit")
	}
	// DeliverAnyGen bypasses the gen comparison but still commits under the guard.
	anyRan := false
	if _, ok := s.WithBindingGuard(DeliverAnyGen, func(int64) { anyRan = true }); !ok || !anyRan {
		t.Fatal("DeliverAnyGen must commit under the guard regardless of gen")
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
