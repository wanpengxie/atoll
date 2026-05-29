// Package contract is the transport-agnostic contract harness for the
// framework sync/async caller core (futurereg). It holds ONE shared scenario
// table that asserts the futurereg semantics — register-before-deliver,
// final-before-await buffering, provisional-vs-final routing, atomic M2
// disposition, and abandon → NoActiveWaiter — and is run against multiple
// transport bindings so they provably never drift (spec §7).
//
// Two transports bind to it today:
//   - in-daemon: the framework response router path, exercised directly over a
//     futurereg.FutureRegistry (kernel/adapter/futurereg).
//   - kimi worker helper: the IPC-fed worker-side caller (adapters/llm/kimi),
//     driven through its Submit / Await / Deliver / Abandon surface with a
//     simulated Triggers stream.
//
// INVARIANT-1: this package imports ONLY the standard library (incl. testing,
// the standard test-helper dependency cf. testing/quick) + kernel/message +
// kernel/adapter/futurereg. It is NOT imported by any futurereg production
// code, so the futurereg production dependency set is unchanged. Each consuming
// package supplies a Transport adapter from within its own test, keeping any
// heavier transport dependency on the consumer side.
package contract

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/adapter/futurereg"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// Transport is the minimal caller-side surface every binding exposes over the
// shared futurereg core. Each scenario in the table drives only these
// operations; the concrete binding (direct registry / kimi worker helper)
// supplies the implementation.
//
// Semantics each implementation MUST honour (this is the contract):
//   - Register(id): subscribe a waiter for id (subscribe-before-send). For the
//     direct registry this is FutureRegistry.Register; for the kimi helper it
//     is Submit (register-then-IPC-write), which registers before the request
//     envelope leaves the worker.
//   - Deliver(env): feed one inbound response envelope and return the
//     single-lock-atomic disposition (M2). This is the response-receive path
//     (router ObserveResponse / worker Triggers).
//   - Await(ctx,id,timeout): block for the final. ok=false,err=nil means the
//     window elapsed with no final (not a hard error); err!=nil is a hard wait
//     error (ctx cancel / closed-abandoned).
//   - Watch(id): a provisional+final stream over the same future.
//   - Abandon(id): drop the local waiter without touching the substrate.
//   - Pending(): the in-flight (registered, not-yet-final) id list.
type Transport interface {
	Register(id message.ID)
	Deliver(env *message.Envelope) futurereg.Disposition
	Await(ctx context.Context, id message.ID, timeout time.Duration) (env *message.Envelope, ok bool, err error)
	Watch(id message.ID) (futurereg.Watcher, error)
	Abandon(id message.ID)
	Pending() []message.ID
}

// Resp builds a minimal response envelope for parent with payload.status.
// Exported so consuming tests can reuse the exact same envelope shape the
// scenarios assert on.
func Resp(parent message.ID, status string) *message.Envelope {
	return &message.Envelope{
		ID:       message.ID("resp:" + string(parent) + ":" + status),
		ParentID: parent,
		Kind:     message.KindResponse,
		Payload:  json.RawMessage(`{"status":"` + status + `"}`),
	}
}

// Scenario is one named contract check run against a freshly built transport.
type Scenario struct {
	Name string
	Run  func(t *testing.T, tr Transport)
}

// Scenarios is the single shared scenario table. Every transport binding runs
// this exact slice so the semantics cannot drift between them.
func Scenarios() []Scenario {
	return []Scenario{
		{"RegisterBeforeDeliver_FinalReachesAwait", scenarioRegisterBeforeDeliver},
		{"FinalBeforeAwait_Buffered_NotLost", scenarioFinalBeforeAwait},
		{"Provisional_WatchReceives_AwaitUnresolved", scenarioProvisionalWatchVsAwait},
		{"Final_ResolvesAwait_AndClears", scenarioFinalResolvesAndClears},
		{"M2_AtomicDisposition_AwaitTimeoutVsFinal", scenarioAtomicDisposition},
		{"AbandonThenFinal_IsNoActiveWaiter", scenarioAbandonThenFinal},
	}
}

// Run executes the full shared scenario table against the transport produced by
// newTransport (a fresh transport per scenario, so state never leaks between
// scenarios). Both the in-daemon and kimi worker tests call this with their own
// constructor, proving cross-transport semantic equivalence.
func Run(t *testing.T, newTransport func(t *testing.T) Transport) {
	t.Helper()
	for _, sc := range Scenarios() {
		sc := sc
		t.Run(sc.Name, func(t *testing.T) {
			tr := newTransport(t)
			sc.Run(t, tr)
		})
	}
}

// ── scenarios ───────────────────────────────────────────────────────────────

// register-before-deliver: a final delivered while an Await is parked must be
// handed to that Await (DeliveredToAwait), and Await returns it.
func scenarioRegisterBeforeDeliver(t *testing.T, tr Transport) {
	const id message.ID = "R-rbd"
	tr.Register(id)

	done := make(chan struct{})
	var got *message.Envelope
	var ok bool
	var awaitErr error
	go func() {
		got, ok, awaitErr = tr.Await(context.Background(), id, 2*time.Second)
		close(done)
	}()
	// let the Await goroutine park
	time.Sleep(30 * time.Millisecond)

	if disp := tr.Deliver(Resp(id, "completed")); disp != futurereg.DeliveredToAwait {
		t.Fatalf("disposition=%v want DeliveredToAwait", disp)
	}
	<-done
	if awaitErr != nil {
		t.Fatalf("await err: %v", awaitErr)
	}
	if !ok || got == nil || got.ParentID != id {
		t.Fatalf("await ok=%v got=%v", ok, got)
	}
}

// final-before-await buffer: final arrives before anyone awaits and is not lost
// — a subsequent Await returns it. Deliver with no parked waiter is
// NoActiveWaiter (the buffered slot keeps the final).
func scenarioFinalBeforeAwait(t *testing.T, tr Transport) {
	const id message.ID = "R-fba"
	tr.Register(id)

	if disp := tr.Deliver(Resp(id, "completed")); disp != futurereg.NoActiveWaiter {
		t.Fatalf("disposition=%v want NoActiveWaiter (buffered, no waiter)", disp)
	}
	got, ok, err := tr.Await(context.Background(), id, time.Second)
	if err != nil {
		t.Fatalf("await err: %v", err)
	}
	if !ok || got == nil || got.ParentID != id {
		t.Fatalf("await ok=%v got=%v (buffered final must be recovered)", ok, got)
	}
}

// provisional is received by Watch but does NOT resolve Await; a parked Await
// over a provisional-only request times out (ok=false, err=nil).
func scenarioProvisionalWatchVsAwait(t *testing.T, tr Transport) {
	const id message.ID = "R-prov"
	tr.Register(id)

	w, err := tr.Watch(id)
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	if disp := tr.Deliver(Resp(id, "processing")); disp != futurereg.DeliveredToWatch {
		t.Fatalf("provisional disp=%v want DeliveredToWatch", disp)
	}

	// Await over the still-pending (provisional-only) request must NOT resolve.
	got, ok, err := tr.Await(context.Background(), id, 150*time.Millisecond)
	if err != nil {
		t.Fatalf("await over provisional-only returned hard err: %v", err)
	}
	if ok || got != nil {
		t.Fatalf("provisional must not resolve Await; got ok=%v env=%v", ok, got)
	}

	// The Watch stream saw the provisional.
	select {
	case ev, open := <-w.Events():
		if !open {
			t.Fatalf("watch stream closed before delivering provisional")
		}
		if ev.IsFinal {
			t.Fatalf("watch event marked final for a provisional")
		}
	case <-time.After(time.Second):
		t.Fatalf("watch stream did not deliver the provisional")
	}
}

// final resolves a parked Await AND clears the future: after the final the id is
// no longer pending and a re-Await reports no further final (no double-deliver).
func scenarioFinalResolvesAndClears(t *testing.T, tr Transport) {
	const id message.ID = "R-final"
	tr.Register(id)

	if pending := containsID(tr.Pending(), id); !pending {
		t.Fatalf("id %s must be pending after Register; pending=%v", id, tr.Pending())
	}

	done := make(chan struct{})
	var got *message.Envelope
	var ok bool
	go func() {
		got, ok, _ = tr.Await(context.Background(), id, 2*time.Second)
		close(done)
	}()
	time.Sleep(30 * time.Millisecond)

	if disp := tr.Deliver(Resp(id, "completed")); disp != futurereg.DeliveredToAwait {
		t.Fatalf("final disp=%v want DeliveredToAwait", disp)
	}
	<-done
	if !ok || got == nil {
		t.Fatalf("await did not resolve on final: ok=%v got=%v", ok, got)
	}

	// Cleared: no longer pending.
	if containsID(tr.Pending(), id) {
		t.Fatalf("id %s must be cleared from pending after final; pending=%v", id, tr.Pending())
	}
	// No double-deliver: a fresh Await must NOT see the same final again.
	env2, ok2, _ := tr.Await(context.Background(), id, 100*time.Millisecond)
	if ok2 && env2 != nil {
		t.Fatalf("final observed twice (double-deliver)")
	}
}

// M2 atomic disposition (F2-strict): many trials where an Await's timeout
// fires roughly when the final lands. The single-lock state machine must make
// the two outcomes MUTUALLY EXCLUSIVE AND CONSISTENT:
//
//   - Deliver returns exactly one disposition (never both DeliveredToAwait and
//     NoActiveWaiter, never neither).
//   - If Deliver == DeliveredToAwait, the racing Await MUST have returned the
//     final (it cannot also report a timeout — that is the F2 double-loss the
//     fix forbids).
//   - If Deliver == NoActiveWaiter, the racing Await must NOT have returned the
//     final, and the buffered final must be recoverable by a fresh Await.
//
// A final is never lost and never double-delivered. Both transports run this.
func scenarioAtomicDisposition(t *testing.T, tr Transport) {
	const trials = 120
	for trial := 0; trial < trials; trial++ {
		id := message.ID("R-race-" + itoa(trial))
		tr.Register(id)

		var awaitGotFinal int32
		var awaitTimedOut int32
		var toAwait int32
		var noWaiter int32

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			env, ok, err := tr.Await(context.Background(), id, 5*time.Millisecond)
			if err == nil && ok && env != nil {
				atomic.StoreInt32(&awaitGotFinal, 1)
			} else if err == nil && !ok {
				atomic.StoreInt32(&awaitTimedOut, 1)
			}
		}()
		go func() {
			defer wg.Done()
			time.Sleep(5 * time.Millisecond)
			switch tr.Deliver(Resp(id, "completed")) {
			case futurereg.DeliveredToAwait:
				atomic.StoreInt32(&toAwait, 1)
			case futurereg.NoActiveWaiter:
				atomic.StoreInt32(&noWaiter, 1)
			}
		}()
		wg.Wait()

		gotFinal := atomic.LoadInt32(&awaitGotFinal) == 1
		timedOut := atomic.LoadInt32(&awaitTimedOut) == 1

		if atomic.LoadInt32(&toAwait) == 1 && atomic.LoadInt32(&noWaiter) == 1 {
			t.Fatalf("trial %d: both DeliveredToAwait and NoActiveWaiter (double)", trial)
		}
		if atomic.LoadInt32(&toAwait) == 0 && atomic.LoadInt32(&noWaiter) == 0 {
			t.Fatalf("trial %d: no disposition recorded (lost)", trial)
		}

		if atomic.LoadInt32(&toAwait) == 1 {
			// STRICT: Deliver handed the final to the awaiter → the awaiter MUST
			// have returned it, never timed out.
			if !gotFinal || timedOut {
				t.Fatalf("trial %d: disp=DeliveredToAwait but Await final=%v timedOut=%v (F2 double-loss)",
					trial, gotFinal, timedOut)
			}
			env, ok, _ := tr.Await(context.Background(), id, 50*time.Millisecond)
			if ok && env != nil {
				t.Fatalf("trial %d: final observed twice (double-deliver)", trial)
			}
		} else {
			// NoActiveWaiter: the Await must not have gotten the final, and the
			// buffered final must be recoverable.
			if gotFinal {
				t.Fatalf("trial %d: disp=NoActiveWaiter but Await also got the final (double-deliver)", trial)
			}
			env, ok, err := tr.Await(context.Background(), id, 300*time.Millisecond)
			if err != nil || !ok || env == nil {
				t.Fatalf("trial %d: final neither observed nor recoverable (lost): ok=%v err=%v", trial, ok, err)
			}
		}
	}
}

// abandon → NoActiveWaiter: a parked Await wakes (hard wait error), and a final
// arriving after Abandon routes as NoActiveWaiter (the caller's follow-up
// decision — e.g. kimi surfaces it as a new turn trigger). Abandon does not
// touch the substrate.
func scenarioAbandonThenFinal(t *testing.T, tr Transport) {
	const id message.ID = "R-abandon"
	tr.Register(id)

	done := make(chan error, 1)
	go func() {
		_, ok, err := tr.Await(context.Background(), id, 2*time.Second)
		if err == nil && ok {
			done <- errAwaitResolved
			return
		}
		done <- err
	}()
	time.Sleep(30 * time.Millisecond)
	tr.Abandon(id)

	select {
	case err := <-done:
		if err == errAwaitResolved {
			t.Fatalf("await must not resolve after abandon")
		}
		// err may be ErrClosed (futurereg) or any hard wait error — either is a
		// woken-not-resolved outcome, which is the contract.
	case <-time.After(time.Second):
		t.Fatalf("await did not wake on abandon")
	}

	if disp := tr.Deliver(Resp(id, "completed")); disp != futurereg.NoActiveWaiter {
		t.Fatalf("post-abandon final disp=%v want NoActiveWaiter", disp)
	}
}

// errAwaitResolved is a sentinel used only inside scenarioAbandonThenFinal to
// signal (over a channel) that an Await unexpectedly resolved.
var errAwaitResolved = &contractError{"await unexpectedly resolved"}

type contractError struct{ msg string }

func (e *contractError) Error() string { return e.msg }

func containsID(ids []message.ID, want message.ID) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

// itoa is a tiny stdlib-free-of-strconv helper kept inline to avoid extra
// imports churn; small non-negative ints only.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [12]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
