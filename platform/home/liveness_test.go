package home

import (
	"errors"
	"sync"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// noInc is the zero incarnation token ledger-level tests publish and observe
// with — matching publish/down tokens keep the write self-validation neutral
// here; the stale-token rejection itself is covered with REAL incarnations in
// TestLateDownEdgeFromReplacedBodyCannotWipeSuccessor.
var noInc = actorrt.Incarnation{}

type testCarrier struct {
	mu   sync.Mutex
	envs []*message.Envelope
	err  error
}

func (c *testCarrier) Enqueue(env *message.Envelope) error {
	c.mu.Lock()
	c.envs = append(c.envs, env)
	c.mu.Unlock()
	return c.err
}

func TestLivenessTicketAndDeliveryTransitions(t *testing.T) {
	l := newLivenessLedger()
	if l.Bootstrap([]actor.ActorID{"a"}) != transitionApplied {
		t.Fatal("bootstrap")
	}
	event := &message.Envelope{Kind: message.KindEvent}
	_, _ = l.AcceptDelivery("a", event)
	if s, _ := l.stateForTest("a"); s.dirty {
		t.Fatal("dormant event set dirty")
	}
	req := &message.Envelope{Kind: message.KindRequest}
	_, _ = l.AcceptDelivery("a", req)
	if s, _ := l.stateForTest("a"); !s.dirty {
		t.Fatal("dormant request did not set dirty")
	}
	ticket, _ := l.BeginEnsure("a", 1)
	if again, _ := l.BeginEnsure("a", 1); again != ticket {
		t.Fatal("ensure changed live ticket")
	}
	q := &testCarrier{}
	if l.PublishLocal("a", EnsureTicket("stale"), noInc, q) != transitionStaleTicket {
		t.Fatal("stale publish accepted")
	}
	if l.PublishLocal("a", ticket, noInc, q) != transitionApplied {
		t.Fatal("publish")
	}
	if s, _ := l.stateForTest("a"); s.dirty || s.restart || s.occ != occRunning {
		t.Fatalf("state=%+v", s)
	}
	_, err := l.AcceptDelivery("a", req)
	if err != nil || len(q.envs) != 1 {
		t.Fatalf("enqueue err=%v count=%d", err, len(q.envs))
	}
}

func TestConcurrentBeginEnsureMintsExactlyOneTicket(t *testing.T) {
	l := newLivenessLedger()
	if l.Bootstrap([]actor.ActorID{"a"}) != transitionApplied {
		t.Fatal("bootstrap")
	}

	const callers = 64
	tickets := make(chan EnsureTicket, callers)
	verdicts := make(chan transitionVerdict, callers)
	var wg sync.WaitGroup
	wg.Add(callers)
	for range callers {
		go func() {
			defer wg.Done()
			ticket, verdict := l.BeginEnsure("a", 1)
			tickets <- ticket
			verdicts <- verdict
		}()
	}
	wg.Wait()
	close(tickets)
	close(verdicts)

	var ticket EnsureTicket
	for got := range tickets {
		if got == "" {
			t.Fatal("BeginEnsure returned an empty ticket")
		}
		if ticket == "" {
			ticket = got
		} else if got != ticket {
			t.Fatalf("concurrent BeginEnsure minted two tickets: %q and %q", ticket, got)
		}
	}
	applied := 0
	for verdict := range verdicts {
		switch verdict {
		case transitionApplied:
			applied++
		case transitionInFlight:
		default:
			t.Fatalf("unexpected verdict %v", verdict)
		}
	}
	if applied != 1 {
		t.Fatalf("transitionApplied count = %d, want exactly 1", applied)
	}
	state, ok := l.stateForTest("a")
	if !ok || state.ticket != ticket || state.occ != occStarting {
		t.Fatalf("final state = %+v, ok=%v", state, ok)
	}
}

func TestLivenessFullDoesNotBufferOrDirty(t *testing.T) {
	l := newLivenessLedger()
	l.Bootstrap([]actor.ActorID{"a"})
	ticket, _ := l.BeginEnsure("a", 1)
	q := &testCarrier{err: errors.New("full")}
	l.PublishLocal("a", ticket, noInc, q)
	_, err := l.AcceptDelivery("a", &message.Envelope{Kind: message.KindRequest})
	if err == nil {
		t.Fatal("full not surfaced")
	}
	if s, _ := l.stateForTest("a"); s.dirty || s.occ != occRunning {
		t.Fatalf("full mutated ledger: %+v", s)
	}
}

func TestFiredTimerIsTheOnlyEventThatCreatesDormantWakeDebt(t *testing.T) {
	var mu sync.Mutex
	var drops []deliveryDropReason
	l := newLivenessLedger(func(_ actor.ActorID, _ *message.Envelope, reason deliveryDropReason, _ error) {
		mu.Lock()
		drops = append(drops, reason)
		mu.Unlock()
	})
	l.Bootstrap([]actor.ActorID{"a"})
	env := &message.Envelope{Kind: message.KindEvent}
	_, _ = l.AcceptDelivery("a", env)
	if state, _ := l.stateForTest("a"); state.dirty {
		t.Fatal("ordinary dormant event created wake debt")
	}
	_, _ = l.AcceptFiredDelivery("a", env)
	if state, _ := l.stateForTest("a"); !state.dirty {
		t.Fatal("fired timer did not create wake debt")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(drops) != 1 || drops[0] != deliveryDropNoCarrier {
		t.Fatalf("dormant ordinary event observations = %v", drops)
	}
}

func TestLivenessIdleDeliveryRaceSerializes(t *testing.T) {
	for n := 0; n < 100; n++ {
		l := newLivenessLedger()
		l.Bootstrap([]actor.ActorID{"a"})
		q := &testCarrier{}
		ticket, _ := l.BeginEnsure("a", 1)
		l.PublishLocal("a", ticket, noInc, q)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); l.ApproveIdle("a") }()
		go func() { defer wg.Done(); _, _ = l.AcceptDelivery("a", &message.Envelope{Kind: message.KindRequest}) }()
		wg.Wait()
		s, _ := l.stateForTest("a")
		q.mu.Lock()
		queued := len(q.envs)
		q.mu.Unlock()
		if s.occ == occNone && !s.dirty && queued == 0 {
			t.Fatal("idle race lost wake")
		}
	}
}

func TestDaemonIdleDownRebindAndLeaseEndpointTransitions(t *testing.T) {
	l := newLivenessLedger()
	l.Bootstrap([]actor.ActorID{"idle", "rebind"})
	q := &testCarrier{}

	idleTicket, _ := l.BeginEnsure("idle", 1)
	if l.Attach("idle", idleTicket, 1, noInc, q) != transitionApplied {
		t.Fatal("attach idle actor")
	}
	if _, verdict := l.ApproveIdle("idle"); verdict != transitionApplied {
		t.Fatalf("approve idle=%v", verdict)
	}
	if verdict := l.ObserveDown("idle", noInc, true, false); verdict != transitionInvalid {
		t.Fatalf("late port down after idle=%v", verdict)
	}
	if _, verdict := l.Retire("idle", true); verdict != transitionApplied {
		t.Fatalf("late lease endpoint after idle=%v", verdict)
	}
	if state, _ := l.stateForTest("idle"); state.occ != occNone || state.restart || state.ticket != "" {
		t.Fatalf("idle resource-tail edges changed L: %+v", state)
	}

	ticket, _ := l.BeginEnsure("rebind", 1)
	if l.Attach("rebind", ticket, 1, noInc, q) != transitionApplied {
		t.Fatal("attach rebind actor")
	}
	if l.ObserveDown("rebind", noInc, true, false) != transitionApplied {
		t.Fatal("ordinary port down")
	}
	if state, _ := l.stateForTest("rebind"); state.occ != occDetached || state.ticket != ticket || state.restart {
		t.Fatalf("ordinary down state=%+v", state)
	}
	if l.Attach("rebind", ticket, 1, noInc, q) != transitionApplied {
		t.Fatal("same-ticket rebind")
	}
	if l.ObserveDown("rebind", noInc, true, false) != transitionApplied {
		t.Fatal("second ordinary port down")
	}
	if _, verdict := l.Retire("rebind", true); verdict != transitionApplied {
		t.Fatalf("lease endpoint=%v", verdict)
	}
	if state, _ := l.stateForTest("rebind"); state.occ != occNone || !state.restart || state.ticket != "" {
		t.Fatalf("lease endpoint state=%+v", state)
	}
	if got := l.Attach("rebind", ticket, 1, noInc, q); got != transitionStaleTicket {
		t.Fatalf("expired ticket reattached=%v", got)
	}
}

func TestAttachmentFenceInvalidatesBeforeIntentDestruction(t *testing.T) {
	makeFence := func(t *testing.T) (*livenessLedger, attachmentFence, EnsureTicket) {
		t.Helper()
		l := newLivenessLedger()
		l.Bootstrap([]actor.ActorID{"a"})
		ticket, verdict := l.BeginEnsure("a", 1)
		if verdict != transitionApplied {
			t.Fatalf("BeginEnsure=%v", verdict)
		}
		fence, verdict := l.prepareAttachmentFence("a", ticket, 1)
		if verdict != transitionApplied || !fence.Valid() {
			t.Fatalf("prepare fence=(%v,%v)", verdict, fence.Valid())
		}
		return l, fence, ticket
	}

	t.Run("retire", func(t *testing.T) {
		l, fence, _ := makeFence(t)
		_, _ = l.Retire("a", true)
		if fence.Valid() {
			t.Fatal("retired intent left in-flight attachment fence valid")
		}
	})
	t.Run("end", func(t *testing.T) {
		l, fence, _ := makeFence(t)
		_, _ = l.EndIdentity("a")
		if fence.Valid() {
			t.Fatal("ended identity left in-flight attachment fence valid")
		}
	})
	t.Run("bootstrap", func(t *testing.T) {
		l, fence, _ := makeFence(t)
		_ = l.Bootstrap([]actor.ActorID{"a"})
		if fence.Valid() {
			t.Fatal("bootstrap row replacement revived an old fence generation")
		}
	})
	t.Run("close", func(t *testing.T) {
		l, fence, _ := makeFence(t)
		_ = l.Close()
		if fence.Valid() {
			t.Fatal("closed ledger left in-flight attachment fence valid")
		}
	})
	t.Run("version", func(t *testing.T) {
		l, fence, _ := makeFence(t)
		if _, retired := l.RetireIfVersionSkew("a", 2); !retired {
			t.Fatal("version skew did not retire old attempt")
		}
		if fence.Valid() {
			t.Fatal("version migration left old version fence valid")
		}
	})
	// The three events below are the ones the spec calls out by name
	// ("fence 生命周期三事件——ApproveIdle、本地 ObserveDown、AbortEnsure 各自使
	// 原子票据字失效") that the original five subtests above did not cover.
	t.Run("approve_idle", func(t *testing.T) {
		l, fence, ticket := makeFence(t)
		q := &testCarrier{}
		if l.PublishLocal("a", ticket, noInc, q) != transitionApplied {
			t.Fatal("publish local before idle")
		}
		if !fence.Valid() {
			t.Fatal("publishing under the SAME ticket must not invalidate the captured fence")
		}
		if _, verdict := l.ApproveIdle("a"); verdict != transitionApplied {
			t.Fatalf("ApproveIdle=%v", verdict)
		}
		if fence.Valid() {
			t.Fatal("idle-approved (occRunning -> occNone) left in-flight attachment fence valid")
		}
	})
	t.Run("observe_down_local", func(t *testing.T) {
		l, fence, ticket := makeFence(t)
		q := &testCarrier{}
		if l.PublishLocal("a", ticket, noInc, q) != transitionApplied {
			t.Fatal("publish local before local down")
		}
		if !fence.Valid() {
			t.Fatal("publishing under the SAME ticket must not invalidate the captured fence")
		}
		if l.ObserveDown("a", noInc, false, false) != transitionApplied {
			t.Fatal("local (non-port) down")
		}
		if fence.Valid() {
			t.Fatal("local cell down (occRunning -> occNone, non-voluntary) left in-flight attachment fence valid")
		}
	})
	t.Run("observe_down_port_does_not_invalidate", func(t *testing.T) {
		// Contrast case (not one of the three fence-invalidating events): a PORT
		// down (occRunning -> occDetached) is the A2b "carrier removed, body not yet
		// judged dead" state — the same incarnation is expected to rebind with the
		// SAME ticket, so the fence must survive it (§2.6 occDetached semantics).
		l, fence, ticket := makeFence(t)
		q := &testCarrier{}
		if l.PublishLocal("a", ticket, noInc, q) != transitionApplied {
			t.Fatal("publish local before port down")
		}
		if l.ObserveDown("a", noInc, true, false) != transitionApplied {
			t.Fatal("port down")
		}
		if !fence.Valid() {
			t.Fatal("port down (detach-for-rebind) must NOT invalidate the same-ticket fence")
		}
	})
	t.Run("abort_ensure", func(t *testing.T) {
		l, fence, ticket := makeFence(t)
		if l.AbortEnsure("a", ticket) != transitionApplied {
			t.Fatal("AbortEnsure")
		}
		if fence.Valid() {
			t.Fatal("aborted ensure left in-flight attachment fence valid")
		}
	})
}
