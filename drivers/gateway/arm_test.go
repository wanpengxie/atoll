package gateway

import (
	"sync"
	"testing"
	"time"
)

// TestArmDetachSealBidirectional: after seal no direction produces an observable
// success — the shared世代 admission gate refuses (admit=false) and the arm reports
// sealed (DoD-5 双向零可观察成功). Seal is idempotent.
func TestArmDetachSealBidirectional(t *testing.T) {
	a := newChannelArm(nil, "chan", "subj", nil, nil) // tail-form arm (nil slot) exercises the seal mechanics
	if !a.admit() {
		t.Fatal("a fresh arm should admit")
	}
	a.seal()
	if a.admit() {
		t.Fatal("a sealed arm must refuse admission (upstream write half)")
	}
	if !a.isSealed() {
		t.Fatal("a sealed arm must report sealed (downstream read half)")
	}
	select {
	case <-a.context().Done():
	default:
		t.Fatal("seal must cancel the arm ctx (every pump/reader unblocks)")
	}
	a.seal() // idempotent, no panic
}

// TestArmGenerationGate (DoD-5 pair B×A 越代写/读): the shared世代 admission gate
// admits ONLY the current binding_gen on an unsealed arm, and refuses every gen once
// sealed — a real admission decision, not a counter increment (假核销修正). The真投
// stale-帧-through-a-Session assertion lives in gateway_test TestUpstreamStaleGenRefused.
func TestArmGenerationGate(t *testing.T) {
	a := newChannelArm(nil, "chan", "subj", nil, nil)
	g1 := a.nextGen()
	g2 := a.nextGen()
	if g1 == g2 || g2 != g1+1 {
		t.Fatalf("binding generations must strictly increase: %d then %d", g1, g2)
	}
	// Current gen admitted; a stale (earlier) gen refused (not sealed).
	if ok, sealed := a.admitUpstream(g2); !ok || sealed {
		t.Fatalf("current gen must be admitted: ok=%v sealed=%v", ok, sealed)
	}
	if ok, sealed := a.admitUpstream(g1); ok || sealed {
		t.Fatalf("stale gen must be refused (not sealed): ok=%v sealed=%v", ok, sealed)
	}
	// After seal, even the current gen is refused, flagged sealed.
	a.seal()
	if ok, sealed := a.admitUpstream(g2); ok || !sealed {
		t.Fatalf("sealed arm must refuse+flag sealed: ok=%v sealed=%v", ok, sealed)
	}
}

// TestArmSealJoinsPumps: seal joins the tracked read pumps; a pump that stops
// promptly is joined with zero leak (DoD-5 有界 join, fast path).
func TestArmSealJoinsPumps(t *testing.T) {
	var leaked int64
	var mu sync.Mutex
	a := newChannelArm(nil, "chan", "subj", nil, nil)
	a.leaked = &leaked
	a.leakMu = &mu

	stopped := make(chan struct{})
	a.track()
	go func() {
		defer a.untrack()
		<-a.context().Done() // a well-behaved pump stops on seal
		close(stopped)
	}()

	a.seal()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("pump did not stop after seal")
	}
	if leaked != 0 {
		t.Fatalf("a promptly-stopping pump must not leak; leaked=%d", leaked)
	}
}

// TestArmSealLeakOnTimeout: a pump that ignores the cancel is abandoned响亮 at the
// join budget and counted (DoD-5 超时响亮弃 + 泄漏计数). The join timeout is tuned
// tiny here so the deterministic leak path runs without the 15s wall-clock wait.
func TestArmSealLeakOnTimeout(t *testing.T) {
	var leaked int64
	var mu sync.Mutex
	a := newChannelArm(nil, "chan", "subj", nil, nil)
	a.leaked = &leaked
	a.leakMu = &mu
	a.joinTimeout = 30 * time.Millisecond

	release := make(chan struct{})
	a.track()
	go func() {
		defer a.untrack()
		<-release // a stuck pump that outlives the join budget
	}()

	a.seal() // blocks up to joinTimeout, then abandons响亮
	mu.Lock()
	got := leaked
	mu.Unlock()
	if got != 1 {
		t.Fatalf("a stuck pump past the join budget must count 1 leak; got %d", got)
	}
	close(release)
}

// TestArmSealJoinBudgetOrdering pins the法条 F ordering: the lane write timeout is
// strictly less than the arm seal join budget, so a slow lane always dies BEFORE
// it can drag out a seal (DoD-5 join 预算推导注).
func TestArmSealJoinBudgetOrdering(t *testing.T) {
	if !(LaneWriteTimeoutMs < int(ArmSealJoinTimeout/time.Millisecond)) {
		t.Fatalf("lane write timeout %dms must be < arm seal join budget %dms (法条 F)",
			LaneWriteTimeoutMs, ArmSealJoinTimeout/time.Millisecond)
	}
}

// The A×L "stuck writer × seal" interleave is proven by the REAL feed-pump path in
// TestSealJoinIsolatesStuckWriter (attach_integration_test.go): a REAL writer draining
// the REAL lane and parked in a blocking write on a REAL net.Pipe whose far end is never
// read (modelling writerPump stuck on a dead peer), + the real runFeed pump over a real
// Home — asserting 满→断连 tears the pump down, seal joins ONLY that pump within the real
// 15s budget (returning in ms, not waiting the parked writer), and the writer's own write
// deadline unwinds it independently. The former channel-blocked goroutine stand-in
// modelled nothing load-bearing (blocked on an unrelated channel, no lane/connection).
