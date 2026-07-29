package subjectgate

import (
	"context"
	"sync"
	"testing"
	"time"
)

const testID = "human:alice"

// TestPublishLevelMintsMonotonicEdgeSeq pins the连接模型勘误期 form: edgeSeq is
// slot-minted (not client-supplied), so every same-or-greater-epoch PublishLevel
// applies with a strictly-increasing seq; a lesser epoch is stale-dropped.
func TestPublishLevelMintsMonotonicEdgeSeq(t *testing.T) {
	s := newSlot()
	var got []PresenceUpdate
	s.RegisterObserver("tok", func(u PresenceUpdate) { got = append(got, u) })
	if !s.PublishLevel(1, LevelOnline) {
		t.Fatal("first edge should apply")
	}
	if !s.PublishLevel(1, LevelOffline) {
		t.Fatal("a same-epoch edge applies with a slot-minted greater seq")
	}
	if len(got) != 2 {
		t.Fatalf("want 2 delivered edges, got %d: %+v", len(got), got)
	}
	if got[1].EdgeSeq <= got[0].EdgeSeq {
		t.Fatalf("slot-minted edgeSeq must strictly increase: %d then %d", got[0].EdgeSeq, got[1].EdgeSeq)
	}
	if s.PublishLevel(0, LevelOnline) {
		t.Fatal("a lesser epoch must be dropped (stale gateway)")
	}
}

// TestPublishCurrentIdempotent pins the§3.2 幂等补发: re-publishing the current
// (epoch, level) is a no-op (zero notify), while a changed value publishes.
func TestPublishCurrentIdempotent(t *testing.T) {
	s := newSlot()
	var got []PresenceUpdate
	s.RegisterObserver("tok", func(u PresenceUpdate) { got = append(got, u) })
	if !s.PublishCurrent(1, LevelOnline) {
		t.Fatal("first PublishCurrent should apply")
	}
	if s.PublishCurrent(1, LevelOnline) {
		t.Fatal("re-publishing the same (epoch,level) must be a no-op")
	}
	if len(got) != 1 {
		t.Fatalf("idempotent re-publish must not notify: got %d edges", len(got))
	}
	if !s.PublishCurrent(1, LevelOffline) {
		t.Fatal("a changed level must publish")
	}
	if len(got) != 2 {
		t.Fatalf("a changed level must notify: got %d edges", len(got))
	}
}

// TestRegisterObserverDeliversCurrentFirst pins the出生握手 (§3.2 六轮 P0-2): the
// current value (if any) is delivered as the observer's FIRST callback, under the
// slot lock, in-order with every subsequent edge.
func TestRegisterObserverDeliversCurrentFirst(t *testing.T) {
	s := newSlot()
	s.PublishLevel(3, LevelOnline)
	var got []PresenceUpdate
	s.RegisterObserver("tok", func(u PresenceUpdate) { got = append(got, u) })
	if len(got) != 1 || !got[0].Live || got[0].Level != LevelOnline || got[0].Epoch != 3 {
		t.Fatalf("RegisterObserver must deliver the current value as its first callback, got %+v", got)
	}
	// A new epoch follows in order: revoke(epoch3) then snapshot(epoch4).
	s.PublishLevel(4, LevelOffline)
	if len(got) != 3 || got[1].Live || got[1].Epoch != 3 || !got[2].Live || got[2].Epoch != 4 {
		t.Fatalf("want snapshot then revoke(3)+snapshot(4), got %+v", got)
	}
}

// TestRegisterObserverNoCurrentNoFirst: with nothing published, RegisterObserver
// delivers no first edge (unknown 诚实默认).
func TestRegisterObserverNoCurrentNoFirst(t *testing.T) {
	s := newSlot()
	fired := 0
	s.RegisterObserver("tok", func(PresenceUpdate) { fired++ })
	if fired != 0 {
		t.Fatalf("no testimony → no first callback, got %d", fired)
	}
}

// TestPublishLevelNewEpochRevokeThenSnapshot pins new-epoch = revoke old then
// snapshot new (build spec §2 pair C×E / DoD-4).
func TestPublishLevelNewEpochRevokeThenSnapshot(t *testing.T) {
	s := newSlot()
	var got []PresenceUpdate
	s.RegisterObserver("tok", func(u PresenceUpdate) { got = append(got, u) })
	s.PublishLevel(1, LevelOnline)
	got = nil
	if !s.PublishLevel(2, LevelOnline) {
		t.Fatal("new epoch should apply")
	}
	if len(got) != 2 || got[0].Live || got[0].Epoch != 1 || !got[1].Live || got[1].Epoch != 2 {
		t.Fatalf("want revoke(epoch1) then snapshot(epoch2), got %+v", got)
	}
	// A lesser epoch is stale.
	if s.PublishLevel(1, LevelOffline) {
		t.Fatal("stale (lesser) epoch must be dropped")
	}
}

// TestObserverPointerGenerationRemoval pins that an old cell's摘除 (its token)
// cannot remove a newer cell's registration (build spec §2 pair C×B / DoD-4
// 旧观察者摘不掉新登记).
func TestObserverPointerGenerationRemoval(t *testing.T) {
	s := newSlot()
	var newFired int
	s.RegisterObserver("old", func(PresenceUpdate) {})
	s.RegisterObserver("new", func(PresenceUpdate) { newFired++ })
	// The old cell tears down and摘除 with ITS token.
	s.RemoveObserver("old")
	s.PublishLevel(1, LevelOnline)
	if newFired != 1 {
		t.Fatalf("new observer should still receive edges after old摘除: %d", newFired)
	}
}

// TestForgetRevokesAndFoldsUnknown pins Forget = revoke + unknown (design §5.4).
func TestForgetRevokesAndFoldsUnknown(t *testing.T) {
	s := newSlot()
	var last PresenceUpdate
	s.RegisterObserver("tok", func(u PresenceUpdate) { last = u })
	s.PublishLevel(1, LevelOnline)
	s.Forget()
	if last.Live {
		t.Fatal("Forget should deliver a revocation")
	}
	if _, _, _, ok := s.Snapshot(); ok {
		t.Fatal("Forget should fold the register to unknown (no value)")
	}
}

// TestForgetEpochConditional pins the CAS 清账 (§3.2/§3.4): ForgetEpoch clears
// ONLY when the current testimony belongs to the given epoch — a stale epoch is a
// no-op, so a late Close never抹 a newer epoch's testimony.
func TestForgetEpochConditional(t *testing.T) {
	s := newSlot()
	fired := 0
	var last PresenceUpdate
	s.RegisterObserver("tok", func(u PresenceUpdate) { last = u; fired++ })
	s.PublishLevel(5, LevelOnline)
	before := fired
	s.ForgetEpoch(4) // different epoch → no-op
	if fired != before {
		t.Fatal("ForgetEpoch for a different epoch must be a no-op")
	}
	if _, _, _, ok := s.Snapshot(); !ok {
		t.Fatal("testimony must survive a non-matching ForgetEpoch")
	}
	s.ForgetEpoch(5) // matching epoch → clears
	if last.Live {
		t.Fatal("matching ForgetEpoch must revoke")
	}
	if _, _, _, ok := s.Snapshot(); ok {
		t.Fatal("matching ForgetEpoch must fold to unknown")
	}
}

// TestDeliverSyncAndUnblock pins the帧递交端 synchronous request/reply +解阻:
// an attached interpreter answers Deliver synchronously; after release a blocked
// Deliver returns ErrNoOccupant.
func TestDeliverSyncAndUnblock(t *testing.T) {
	s := newSlot()
	ch, _, release := s.AttachInterpreter()

	var wg sync.WaitGroup
	wg.Add(1)
	stop := make(chan struct{})
	go func() {
		defer wg.Done()
		for {
			select {
			case job := <-ch:
				r, _ := NewFrame(FrameReceipt, job.Frame.Ref, SubmitReceipt{MessageID: "m1"})
				job.Reply(FrameResult{Frame: r})
			case <-stop:
				return
			}
		}
	}()

	f, _ := NewFrame(FrameSubmit, "ref-9", SubmitPayload{ChannelID: "c1", MsgType: "m"})
	res, err := s.Deliver(context.Background(), f)
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
	if _, err := s.Deliver(context.Background(), f); err != ErrNoOccupant {
		t.Fatalf("Deliver after release want ErrNoOccupant, got %v", err)
	}
}

// TestDeliverBlockedUnblocksOnRelease pins解阻 for a Deliver already blocked on
// an idle interpreter when the interpreter detaches.
func TestDeliverBlockedUnblocksOnRelease(t *testing.T) {
	s := newSlot()
	_, _, release := s.AttachInterpreter() // attached but nobody reads the channel

	done := make(chan error, 1)
	go func() {
		f, _ := NewFrame(FrameSubmit, "r", SubmitPayload{ChannelID: "c1", MsgType: "m"})
		_, err := s.Deliver(context.Background(), f)
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
	s := newSlot()
	_, tokA, relA := s.AttachInterpreter()
	framesB, tokB, relB := s.AttachInterpreter()
	if tokA == tokB {
		t.Fatal("each AttachInterpreter must mint a distinct incarnation token")
	}

	// B answers one job.
	go func() {
		job := <-framesB
		r, _ := NewFrame(FrameReceipt, job.Frame.Ref, SubmitReceipt{MessageID: "mb"})
		job.Reply(FrameResult{Frame: r})
	}()

	// A's stale release: it must NOT flip B's liveness.
	relA()

	f, _ := NewFrame(FrameSubmit, "ref-b", SubmitPayload{ChannelID: "c1", MsgType: "m"})
	res, err := s.Deliver(context.Background(), f)
	if err != nil {
		t.Fatalf("Deliver after stale release of A must reach live B, got %v", err)
	}
	if res.Frame.Ref != "ref-b" {
		t.Fatalf("unexpected receipt from B: %+v", res.Frame)
	}

	// Now B releases (the current incarnation) → the slot refuses.
	relB()
	if _, err := s.Deliver(context.Background(), f); err != ErrNoOccupant {
		t.Fatalf("Deliver after current release want ErrNoOccupant, got %v", err)
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

// TestDeliverCtxExit (法典纪律④三路出口): a caller whose own life ends (dead ws
// connector, cancelled session ctx) must unblock its Deliver wait via ctx — not
// park until cell death — while an interpreter is attached but never replies.
func TestDeliverCtxExit(t *testing.T) {
	s := newSlot()
	_, _, release := s.AttachInterpreter() // attached, but nobody drains frames
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		f, _ := NewFrame(FrameSubmit, "ref", SubmitPayload{ChannelID: "c1", MsgType: "m"})
		_, err := s.Deliver(ctx, f)
		done <- err
	}()
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("cancelled caller must get ctx.Err(), got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Deliver did not unblock on ctx cancel (纪律④出口缺格)")
	}
}
