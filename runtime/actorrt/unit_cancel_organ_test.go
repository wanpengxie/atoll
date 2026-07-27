package actorrt

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// cancelOrganWait is deliberately generous: these tests run under concurrent
// build load and only ever wait for an event that is already in flight.
const cancelOrganWait = 5 * time.Second

// lockedLogWriter is a concurrency-safe slog sink. Cancel warnings are emitted
// on the caller goroutine while the Unit goroutine may concurrently log its own
// stop events, so a plain bytes.Buffer would be a data race.
type lockedLogWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *lockedLogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *lockedLogWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// cancelOrganProbe is a RequestCanceller actor whose Receive and CancelRequest
// hooks can each be held open, so the cancel lane can be observed while the
// work lane is blocked (and vice versa).
type cancelOrganProbe struct {
	receiveEntered chan struct{}
	receiveGate    chan struct{}

	cancels       chan message.ID
	cancelEntered chan struct{}
	cancelOnce    sync.Once
	cancelGate    chan struct{}

	inflight atomic.Int32
	overlap  atomic.Int64
}

func newCancelOrganProbe(cancelBuffer int) *cancelOrganProbe {
	return &cancelOrganProbe{
		receiveEntered: make(chan struct{}, 8),
		cancels:        make(chan message.ID, cancelBuffer),
	}
}

func (a *cancelOrganProbe) Receive(_ context.Context, _ *message.Envelope) error {
	select {
	case a.receiveEntered <- struct{}{}:
	default:
	}
	if a.receiveGate != nil {
		<-a.receiveGate
	}
	return nil
}

func (a *cancelOrganProbe) CancelRequest(id message.ID) {
	if a.inflight.Add(1) > 1 {
		a.overlap.Add(1)
	}
	// Widen the overlap window without using sleep as synchronisation: a second
	// drainer, if one existed, would be observed by the counter above.
	for i := 0; i < 32; i++ {
		runtime.Gosched()
	}
	if a.cancelEntered != nil {
		a.cancelOnce.Do(func() { close(a.cancelEntered) })
	}
	if a.cancelGate != nil {
		<-a.cancelGate
	}
	a.inflight.Add(-1)
	a.cancels <- id
}

func waitCancel(t *testing.T, ch <-chan message.ID) message.ID {
	t.Helper()
	select {
	case id := <-ch:
		return id
	case <-time.After(cancelOrganWait):
		t.Fatal("cancel organ did not forward the request")
		return ""
	}
}

func waitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(cancelOrganWait):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// TestCancelOrganForwardsRequestWhileReceiveIsBlocked pins the reason the
// cancel lane is a separate organ at all: a request cancel must reach the actor
// while its single work goroutine is still inside a long Receive.
func TestCancelOrganForwardsRequestWhileReceiveIsBlocked(t *testing.T) {
	t.Parallel()

	impl := newCancelOrganProbe(4)
	impl.receiveGate = make(chan struct{})
	u, _ := prepareProbe(t, "agent:cancel-blocked-receive", impl, nil, nil)
	if err := u.Start(); err != nil {
		t.Fatal(err)
	}
	if err := u.Deliver(&message.Envelope{ID: "work-1"}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	select {
	case <-impl.receiveEntered:
	case <-time.After(cancelOrganWait):
		t.Fatal("Receive was not entered")
	}

	u.CancelRequest("req-1")
	if got := waitCancel(t, impl.cancels); got != "req-1" {
		t.Fatalf("forwarded cancel = %q, want req-1", got)
	}

	close(impl.receiveGate)
	u.Stop()
	waitDone(t, u)
}

// TestCancelOrganIsTheSoleDrainerUnderConcurrentRequests asserts the dispatch
// side is fan-in-only: many callers, exactly one drainer, so the actor's
// CancelRequest never runs concurrently with itself.
func TestCancelOrganIsTheSoleDrainerUnderConcurrentRequests(t *testing.T) {
	t.Parallel()

	const callers = 8
	const perCaller = 24
	const total = callers * perCaller

	impl := newCancelOrganProbe(total + 8)
	u, _ := prepareProbe(t, "agent:cancel-concurrent", impl, nil, nil)
	if err := u.Start(); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for c := 0; c < callers; c++ {
		wg.Add(1)
		go func(caller int) {
			defer wg.Done()
			for i := 0; i < perCaller; i++ {
				u.CancelRequest(message.ID(fmt.Sprintf("req-%d-%d", caller, i)))
			}
		}(c)
	}
	wg.Wait()

	// The queue capacity exceeds the number of requests, so every request is
	// admitted and must eventually be handed to the actor exactly once.
	seen := make(map[message.ID]int, total)
	for i := 0; i < total; i++ {
		seen[waitCancel(t, impl.cancels)]++
	}
	if len(seen) != total {
		t.Fatalf("distinct forwarded cancels = %d, want %d", len(seen), total)
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("cancel %q forwarded %d times", id, count)
		}
	}
	if overlaps := impl.overlap.Load(); overlaps != 0 {
		t.Fatalf("overlapping CancelRequest invocations = %d, want 0", overlaps)
	}

	u.Stop()
	waitDone(t, u)
}

// TestCancelRequestDropsAndWarnsWhenOrganQueueIsFull pins the best-effort
// contract: a full cancel queue is dropped with a warning, never blocking the
// caller and never growing without bound.
func TestCancelRequestDropsAndWarnsWhenOrganQueueIsFull(t *testing.T) {
	t.Parallel()

	logs := &lockedLogWriter{}
	impl := newCancelOrganProbe(cancelSetCap + 8)
	impl.cancelEntered = make(chan struct{})
	impl.cancelGate = make(chan struct{})
	u, _ := prepareProbe(t, "agent:cancel-queue-full", impl, nil, slog.New(slog.NewTextHandler(logs, nil)))
	if err := u.Start(); err != nil {
		t.Fatal(err)
	}

	// Park the single drainer inside the actor so the queue can be filled to
	// its exact capacity.
	u.CancelRequest("held")
	waitClosed(t, impl.cancelEntered, "cancel organ to enter the actor")

	for i := 0; i < cancelSetCap; i++ {
		u.CancelRequest(message.ID(fmt.Sprintf("queued-%d", i)))
	}
	if got := len(u.cancelQ); got != cancelSetCap {
		t.Fatalf("queued cancels = %d, want %d", got, cancelSetCap)
	}
	u.CancelRequest("overflow")
	if got := len(u.cancelQ); got != cancelSetCap {
		t.Fatalf("queue grew past capacity: %d", got)
	}
	if !strings.Contains(logs.String(), "actorrt.cancel_queue_full") {
		t.Fatalf("missing queue-full warning: %s", logs.String())
	}

	close(impl.cancelGate)
	u.Stop()
	waitDone(t, u)
}

// TestUnitDoneWaitsForBlockedCancelOrgan pins §13.2's "Done joins all organs":
// the cancel organ is Unit-owned, so Done cannot close while it is still inside
// actor code.
func TestUnitDoneWaitsForBlockedCancelOrgan(t *testing.T) {
	t.Parallel()

	impl := newCancelOrganProbe(4)
	impl.cancelEntered = make(chan struct{})
	impl.cancelGate = make(chan struct{})
	u, _ := prepareProbe(t, "agent:cancel-organ-join", impl, nil, nil)
	if err := u.Start(); err != nil {
		t.Fatal(err)
	}
	u.CancelRequest("held")
	waitClosed(t, impl.cancelEntered, "cancel organ to enter the actor")

	u.Stop()
	select {
	case <-u.Done():
		t.Fatal("Done closed while the cancel organ was still running")
	case <-time.After(50 * time.Millisecond):
	}
	close(impl.cancelGate)
	waitDone(t, u)
}

// TestCancelRequestIsDroppedOutsideTheLiveWindow pins the liveness gate: the
// cancel lane exists only while this exact Unit is alive.
func TestCancelRequestIsDroppedOutsideTheLiveWindow(t *testing.T) {
	t.Parallel()

	impl := newCancelOrganProbe(8)
	u, _ := prepareProbe(t, "agent:cancel-window", impl, nil, nil)

	u.CancelRequest("before-start")
	if got := len(u.cancelQ); got != 0 {
		t.Fatalf("prepared unit admitted %d cancels", got)
	}

	if err := u.Start(); err != nil {
		t.Fatal(err)
	}
	u.CancelRequest("live")
	if got := waitCancel(t, impl.cancels); got != "live" {
		t.Fatalf("forwarded cancel = %q, want live", got)
	}

	u.Stop()
	waitDone(t, u)
	u.CancelRequest("after-stop")
	select {
	case got := <-impl.cancels:
		t.Fatalf("cancel %q was forwarded after the unit stopped", got)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestCancelRequestOnActorWithoutCancellerIsNoOp pins that an actor which does
// not implement RequestCanceller neither starts an organ nor accumulates queued
// cancel work.
func TestCancelRequestOnActorWithoutCancellerIsNoOp(t *testing.T) {
	t.Parallel()

	impl := newUnitProbeActor()
	u, _ := prepareProbe(t, actor.ActorID("agent:no-canceller"), impl, nil, nil)
	if err := u.Start(); err != nil {
		t.Fatal(err)
	}
	<-impl.started
	u.CancelRequest("ignored")
	if got := len(u.cancelQ); got != 0 {
		t.Fatalf("non-canceller actor queued %d cancels", got)
	}
	u.Stop()
	waitDone(t, u)
}
