package channelkit_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/channelkit"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// panicActor panics on Receive — simulates a cell dying abnormally.
type panicActor struct{}

func (panicActor) Receive(context.Context, *message.Envelope) error { panic("boom") }

// closedForeverVerdict wires the closure triple's monotone predicate with a fixed
// verdict: gone=true → deregistered / never a member → closure owed; gone=false →
// still a registered member → no closure (its callers wait for the deadline).
func closedForeverVerdict(gone bool) func(context.Context, actor.ActorID) (bool, error) {
	return func(context.Context, actor.ActorID) (bool, error) { return gone, nil }
}

// closedForeverVerdictNotify is closedForeverVerdict plus a "called" signal: it
// pushes the queried id onto called (non-blocking, so a small buffer must be
// sized by the caller) every time the predicate is consulted. Tests that assert
// an ABSENCE of a terminal must first wait on called — otherwise a consumer that
// never even reached the predicate (e.g. the edge was dropped, or consumeDown
// hadn't scheduled yet) would pass the same assertion vacuously.
func closedForeverVerdictNotify(gone bool, called chan actor.ActorID) func(context.Context, actor.ActorID) (bool, error) {
	return func(_ context.Context, id actor.ActorID) (bool, error) {
		select {
		case called <- id:
		default:
		}
		return gone, nil
	}
}

// liveActor is a healthy no-op cell — stands in for a same-id successor.
func newChannel(cfg channelkit.Config) *channelkit.Channel {
	c, err := channelkit.New(cfg)
	if err != nil {
		panic(err)
	}
	if err := c.Start(); err != nil {
		panic(err)
	}
	return c
}

func TestChannel_StopAfterFailedStartReturns(t *testing.T) {
	c, err := channelkit.New(channelkit.Config{System: func(*actorrt.Runtime, actorrt.Incarnation) actorrt.Actor { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err == nil {
		t.Fatal("Start with nil system actor succeeded")
	}
	done := make(chan struct{})
	go func() { c.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close blocked after failed Start")
	}
}

type liveActor struct{}

func (liveActor) Receive(context.Context, *message.Envelope) error { return nil }

// TestOnDown_DoesNotDespawnSuccessor locks the DownWatcher contract: a late
// death edge for an id must NOT despawn whatever currently occupies that id. A
// dead predecessor self-evicts via the runtime's pointer-identity removeIf; if
// OnDown also despawned, a same-id successor would be wrongly killed (Despawn is
// not pointer-checked).
func TestOnDown_DoesNotDespawnSuccessor(t *testing.T) {
	ch := newChannel(channelkit.Config{ChannelID: "ch", Clock: time.Now})
	defer ch.Close()
	_, _, _ = ch.Cells().SpawnIfAbsent("worker", actor.KindTool, func(actorrt.Incarnation) actorrt.Actor { return liveActor{} })
	if _, ok := ch.Cells().Stat("worker"); !ok {
		t.Fatal("successor not hosted after Spawn")
	}
	// A late down edge for "worker" (e.g. from a replaced predecessor).
	ch.OnDown(context.Background(), "worker", actorrt.Incarnation{}, errors.New("late predecessor death"))
	if _, ok := ch.Cells().Stat("worker"); !ok {
		t.Fatal("OnDown despawned the live successor — DownWatcher contract violated")
	}
}

// fakeWriter is the test stand-in for the SYSTEM Pen the composition root injects
// (Mint(SystemActorID, chID)). channelkit no longer passes a sender — the system
// identity is welded INTO the pen — so this fake welds sender==SystemActorID the
// way the real system pen does, then records the terminal. (A real boundPen would
// also fail-fast on a pre-filled sender, but author#3's behavior builders leave
// it empty, so the weld is the only relevant half.)
type fakeWriter struct {
	mu      sync.Mutex
	written []*message.Envelope
}

func (f *fakeWriter) Write(_ context.Context, env *message.Envelope) (harness.WriteResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	env.Sender = message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID}
	f.written = append(f.written, env)
	return harness.WriteResult{MessageID: env.ID}, nil
}
func (f *fakeWriter) count() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.written) }

// fakeQuery satisfies storespec.MessageQuery for tests.
type fakeQuery struct {
	reqs      []storespec.StoredRow
	receivers []actor.ActorID
}

func (f fakeQuery) MaxSeq(context.Context) (int64, error) { return 0, nil }
func (f fakeQuery) ReadAfterSeq(context.Context, int64, int) ([]storespec.StoredRow, error) {
	return nil, nil
}
func (f fakeQuery) OpenRequestsForActor(context.Context, actor.ActorID) ([]storespec.StoredRow, error) {
	return f.reqs, nil
}
func (f fakeQuery) DistinctOpenRequestReceivers(context.Context) ([]actor.ActorID, error) {
	return f.receivers, nil
}

// TestOnDown_MaterialisesReceiverUnavailable proves closure author #3 is WIRED:
// a cell dying → watcher writes a system-authored receiver_unavailable
// terminal for the dead actor's in-flight request (NOT just Despawn).
func TestOnDown_MaterialisesReceiverUnavailable(t *testing.T) {
	req := message.Envelope{
		ID:        "req-1",
		ChannelID: "ch",
		Kind:      message.KindRequest,
		Type:      "x.do",
		Sender:    message.Sender{Kind: actor.KindAgent, ID: "caller"},
		Audience:  message.Audience{"worker"},
	}
	fc := &fakeWriter{}
	ch := newChannel(channelkit.Config{
		ChannelID:     "ch",
		SystemPen:     fc,
		OpenRequests:  fakeQuery{reqs: []storespec.StoredRow{{Envelope: req}}},
		ClosedForever: closedForeverVerdict(true), // worker is deregistered → closure owed
		Clock:         time.Now,
	})
	defer ch.Close()
	_, _, _ = ch.Cells().SpawnIfAbsent("worker", actor.KindTool, func(actorrt.Incarnation) actorrt.Actor { return panicActor{} })

	// Deliver a request → Receive panics → cell death → OnDown. Delivery goes
	// through the confined Deliverer (the post-harness fanout's capability), not
	// the broadly-shared Cells() handle.
	_, _ = ch.Deliverer().Deliver([]actor.ActorID{"worker"},
		&message.Envelope{ID: "trigger", ChannelID: "ch", Kind: message.KindRequest, Type: "x.do",
			Sender: message.Sender{Kind: actor.KindAgent, ID: "caller"}, Audience: message.Audience{"worker"}})

	// Wait for OnDown to materialise the terminal.
	deadline := time.Now().Add(2 * time.Second)
	for fc.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if fc.count() == 0 {
		t.Fatal("death-edge closure NOT materialised — OnDown wrote no terminal (black hole regression)")
	}
	term := fc.written[0]
	if term.Kind != message.KindResponse {
		t.Fatalf("terminal kind=%s, want response", term.Kind)
	}
	if term.ParentID != "req-1" {
		t.Fatalf("terminal parent_id=%s, want req-1", term.ParentID)
	}
	if term.Sender.ID != actor.SystemActorID {
		t.Fatalf("terminal sender=%s, want system (substrate-death author)", term.Sender.ID)
	}
	// payload must carry status=failed + reason=receiver_unavailable
	if !contains(string(term.Payload), "receiver_unavailable") || !contains(string(term.Payload), "failed") {
		t.Fatalf("terminal payload=%s, want status=failed reason=receiver_unavailable", term.Payload)
	}
}

// TestReconcile_ClosesDeregisteredReceiver proves the closure reconciler (the
// level scan) closes a DEREGISTERED receiver's inbound open request WITHOUT the
// caller doing anything (F5: closure carries no "callers MUST collapse before"
// obligation). Closure is keyed on the MONOTONE dereg fact, not liveness: while
// the receiver is still a registered member the scan leaves it (a successor may
// yet answer — its callers wait for the request deadline); once it is closed
// forever (deregistered / never a member) the scan materialises
// receiver_unavailable.
func TestReconcile_ClosesDeregisteredReceiver(t *testing.T) {
	req := message.Envelope{
		ID:        "req-1",
		ChannelID: "ch",
		Kind:      message.KindRequest,
		Type:      "x.do",
		Sender:    message.Sender{Kind: actor.KindAgent, ID: "caller"},
		Audience:  message.Audience{"worker"},
	}
	fc := &fakeWriter{}
	gone := false // starts as a live registered member
	ch := newChannel(channelkit.Config{
		ChannelID: "ch",
		SystemPen: fc,
		OpenRequests: fakeQuery{
			reqs:      []storespec.StoredRow{{Envelope: req}},
			receivers: []actor.ActorID{"worker"}, // the open request's receiver
		},
		ClosedForever: func(context.Context, actor.ActorID) (bool, error) { return gone, nil },
		Clock:         time.Now,
	})
	defer ch.Close()

	// While still a registered member, a sweep must NOT close it (a successor
	// could still answer — closure is not owed on mere liveness absence).
	ch.Reconcile(context.Background())
	if fc.count() != 0 {
		t.Fatal("reconciler closed a request whose receiver is still a registered member")
	}

	// Deregister → closed forever. The caller does NOTHING to collapse the request.
	gone = true

	// The level scan alone closes the orphan.
	ch.Reconcile(context.Background())
	if fc.count() != 1 {
		t.Fatalf("reconciler did not close the deregistered receiver's inbound open request (count=%d)", fc.count())
	}
	term := fc.written[0]
	if term.ParentID != "req-1" || term.Sender.ID != actor.SystemActorID {
		t.Fatalf("closure terminal = parent %s sender %s, want req-1 / system", term.ParentID, term.Sender.ID)
	}
	if !contains(string(term.Payload), "receiver_unavailable") {
		t.Fatalf("terminal payload=%s, want receiver_unavailable", term.Payload)
	}
}

// errOpenReqs fails the drain query — the worst closure case (no request can be
// closed → every caller is a black hole). The watcher MUST NOT swallow it.
type errQuery struct{}

func (errQuery) MaxSeq(context.Context) (int64, error) { return 0, nil }
func (errQuery) ReadAfterSeq(context.Context, int64, int) ([]storespec.StoredRow, error) {
	return nil, nil
}
func (errQuery) OpenRequestsForActor(context.Context, actor.ActorID) ([]storespec.StoredRow, error) {
	return nil, errors.New("store down")
}
func (errQuery) DistinctOpenRequestReceivers(context.Context) ([]actor.ActorID, error) {
	return nil, errors.New("store down")
}

// capHandler is a minimal slog.Handler capturing emitted record messages so a
// test can assert a fault was surfaced through the std slog facade. It is mutex-
// guarded because the closure work now runs on channelkit's resident consumer
// goroutine (G0-3), not synchronously in the test's goroutine.
type capHandler struct {
	mu   sync.Mutex
	msgs []string
}

func (*capHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *capHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.msgs = append(h.msgs, r.Message)
	h.mu.Unlock()
	return nil
}
func (h *capHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capHandler) WithGroup(string) slog.Handler      { return h }

// nonFaultMsgs is the closed set of NON-fault channelkit slog messages a test's
// capHandler will see incidentally — steady-state/edge heartbeats, not closure
// faults. They are filtered out of fault assertions so a new heartbeat log never
// silently breaks a fault test (the old waitFirstMsg assumed msgs[0] was always
// the fault, which a Start-time consumer_armed heartbeat now violates).
var nonFaultMsgs = map[string]bool{
	"channelkit.closure.consumer_armed":  true,
	"channelkit.closure.reconcile_swept": true,
}

// waitMsg polls until a message equal to want is captured (skipping incidental
// heartbeats), returning true; false on timeout. This replaces the fragile
// "msgs[0] is the fault" assumption — it waits for the SPECIFIC fault regardless
// of what non-fault heartbeats precede it.
func (h *capHandler) waitMsg(want string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		h.mu.Lock()
		for _, m := range h.msgs {
			if m == want {
				h.mu.Unlock()
				return true
			}
		}
		h.mu.Unlock()
		time.Sleep(2 * time.Millisecond)
	}
	return false
}

// faults returns the captured messages with incidental heartbeats filtered out —
// what "did any closure fault surface" assertions should look at.
func (h *capHandler) faults() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, m := range h.msgs {
		if !nonFaultMsgs[m] {
			out = append(out, m)
		}
	}
	return out
}

// TestClosureDrainFailure_IsSurfaced proves a swallowed-drain regression cannot
// return: when the drain query fails (cannot close anyone → black hole), the
// watcher logs a fault rather than returning silently. The materialisation now
// lives in the behaviour base (author#3); channelkit injects the seams and an
// onFault callback, so this drives the invariant through OnDown — the only
// surface left after the free function moved to behavior.
func TestClosureDrainFailure_IsSurfaced(t *testing.T) {
	h := &capHandler{}
	ch := newChannel(channelkit.Config{
		ChannelID:     "ch",
		SystemPen:     &fakeWriter{},
		OpenRequests:  errQuery{},
		ClosedForever: closedForeverVerdict(true), // deregistered → closure attempted → drain fails
		Clock:         time.Now,
		Logger:        slog.New(h),
	})
	defer ch.Close()
	ch.OnDown(context.Background(), "worker", actorrt.Incarnation{}, nil)
	if !h.waitMsg("channelkit.closure.drain_query_failed", 2*time.Second) {
		t.Fatalf("drain query failed but fault not logged — silent black hole regression; faults=%v", h.faults())
	}
}

// errWriter fails every Write — drives a per-request write fault so OnDown's
// onFault callback fires (the channelkit.closure.write_failed path). The drain
// query itself succeeds (one request returned), so the failure is per-request,
// not drain-level.
type errWriter struct{}

func (errWriter) Write(context.Context, *message.Envelope) (harness.WriteResult, error) {
	return harness.WriteResult{}, errors.New("write down")
}

// TestOnDown_PerRequestWriteFault_IsLogged proves that when the drain query
// succeeds but writing a per-request terminal fails, OnDown surfaces the fault
// via its onFault callback (channelkit.closure.write_failed) rather than
// swallowing it. The drain itself does NOT fail, so the function returns nil and
// only the per-request fault log is emitted.
func TestOnDown_PerRequestWriteFault_IsLogged(t *testing.T) {
	req := message.Envelope{
		ID:        "req-1",
		ChannelID: "ch",
		Kind:      message.KindRequest,
		Type:      "x.do",
		Sender:    message.Sender{Kind: actor.KindAgent, ID: "caller"},
		Audience:  message.Audience{"worker"},
	}
	h := &capHandler{}
	ch := newChannel(channelkit.Config{
		ChannelID:     "ch",
		SystemPen:     errWriter{},
		OpenRequests:  fakeQuery{reqs: []storespec.StoredRow{{Envelope: req}}},
		ClosedForever: closedForeverVerdict(true), // deregistered → closure attempted → per-request write fails
		Clock:         time.Now,
		Logger:        slog.New(h),
	})
	defer ch.Close()
	ch.OnDown(context.Background(), "worker", actorrt.Incarnation{}, nil)
	if !h.waitMsg("channelkit.closure.write_failed", 2*time.Second) {
		t.Fatalf("per-request write failed but fault not logged — silent black hole regression; faults=%v", h.faults())
	}
}

// TestNew_DefaultClockAndSpawnsSystem covers the two New defaults the other
// tests bypass: a nil Clock must fall back (the channel still builds and hosts
// cells), and a non-nil System actor must be spawned at the SystemActorID so it
// is immediately hosted/addressable.
func TestNew_DefaultClockAndSpawnsSystem(t *testing.T) {
	ch := newChannel(channelkit.Config{
		ChannelID: "ch",
		// intrinsic system cell — built against the live runtime, spawned by New.
		System: func(*actorrt.Runtime, actorrt.Incarnation) actorrt.Actor { return liveActor{} },
		// Clock left nil → New must default it (time.Now) without panicking.
	})
	defer ch.Close()
	if _, ok := ch.Cells().Stat(actor.SystemActorID); !ok {
		t.Fatal("New did not spawn the intrinsic System cell at SystemActorID")
	}
}

// TestOnDown_SkipsStillRegisteredReceiver (C7 拔根 · 崩溃在册者不被 RU): the down
// edge is now O(1)-posted and materialised by a resident consumer, which re-derives
// the monotone closed-forever fact before writing — so an edge for an id that is
// still a REGISTERED MEMBER (a crash whose incarnation may yet be re-embodied) is
// skipped (no terminal); its callers wait for the request deadline. This is the
// core semantic change: liveness absence alone no longer authors a terminal.
func TestOnDown_SkipsStillRegisteredReceiver(t *testing.T) {
	req := message.Envelope{
		ID: "req-1", ChannelID: "ch", Kind: message.KindRequest, Type: "x.do",
		Sender: message.Sender{Kind: actor.KindAgent, ID: "caller"}, Audience: message.Audience{"worker"},
	}
	fc := &fakeWriter{}
	called := make(chan actor.ActorID, 4)
	ch := newChannel(channelkit.Config{
		ChannelID:     "ch",
		SystemPen:     fc,
		OpenRequests:  fakeQuery{reqs: []storespec.StoredRow{{Envelope: req}}},
		ClosedForever: closedForeverVerdictNotify(false, called), // worker is still a registered member
		Clock:         time.Now,
	})
	defer ch.Close()

	// An edge for a crashed-but-registered "worker" — the consumer must skip it.
	ch.OnDown(context.Background(), "worker", actorrt.Incarnation{}, errors.New("crashed but still a member"))

	// Wait until the closure predicate has actually been consulted for this edge
	// — otherwise the no-write assertion below would pass vacuously if the edge
	// were never even consumed (a dropped/unscheduled edge, not a skip verdict).
	select {
	case <-called:
	case <-time.After(2 * time.Second):
		t.Fatal("closure predicate was never consulted for the down edge — assertion would be vacuous")
	}

	// closeFor returns immediately once the predicate reports gone=false (no
	// write path is reachable past that point), so a short grace window is
	// enough to catch any regression that writes anyway.
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if fc.count() != 0 {
			t.Fatalf("consumer wrote a terminal for a still-registered receiver — 崩溃在册者被误关 (regression)")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestOnDown_PredicateFailure_SkipsAndLogs (C7 失败政策 · edge 路): a closure-
// predicate LOOKUP failure on the edge path must NEVER be treated as closed — the
// consumer writes no terminal and surfaces a predicate_failed fault; the level
// scan is the backstop. Locks 错误绝不当注销 on the fast path.
func TestOnDown_PredicateFailure_SkipsAndLogs(t *testing.T) {
	req := message.Envelope{
		ID: "req-1", ChannelID: "ch", Kind: message.KindRequest, Type: "x.do",
		Sender: message.Sender{Kind: actor.KindAgent, ID: "caller"}, Audience: message.Audience{"worker"},
	}
	fc := &fakeWriter{}
	h := &capHandler{}
	ch := newChannel(channelkit.Config{
		ChannelID:    "ch",
		SystemPen:    fc,
		OpenRequests: fakeQuery{reqs: []storespec.StoredRow{{Envelope: req}}},
		ClosedForever: func(context.Context, actor.ActorID) (bool, error) {
			return false, errors.New("registry lookup down")
		},
		Clock:  time.Now,
		Logger: slog.New(h),
	})
	defer ch.Close()

	ch.OnDown(context.Background(), "worker", actorrt.Incarnation{}, nil)
	if !h.waitMsg("channelkit.closure.predicate_failed", 2*time.Second) {
		t.Fatalf("predicate failure fault not logged; faults=%v", h.faults())
	}
	if fc.count() != 0 {
		t.Fatalf("a predicate lookup failure must NOT close anyone (错误当注销=误杀), got %d writes", fc.count())
	}
}

// TestClose_QuietDuringInFlightClosure proves closeWithin's signal-then-cancel
// fix: a closeFor call already in flight when Close is invoked must be allowed
// to finish under its still-live ctx, not raced against ctx cancellation.
// Regression this locks: the old cancel-then-signal order cancelled c.ctx
// BEFORE an in-flight closeFor's predicate/drain queries ran, which then failed
// on ctx.Err() and logged the loudest closure fault (predicate_failed /
// drain_query_failed — "every caller of the dead actor is a black hole") for
// what is just an ordinary Close — pure noise, not a real fault. Close must stay
// quiet and the in-flight closure must still complete (bounded shutdown, not a
// silently dropped one).
func TestClose_QuietDuringInFlightClosure(t *testing.T) {
	req := message.Envelope{
		ID: "req-1", ChannelID: "ch", Kind: message.KindRequest, Type: "x.do",
		Sender: message.Sender{Kind: actor.KindAgent, ID: "caller"}, Audience: message.Audience{"worker"},
	}
	fc := &fakeWriter{}
	h := &capHandler{}
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	ch := newChannel(channelkit.Config{
		ChannelID:    "ch",
		SystemPen:    fc,
		OpenRequests: fakeQuery{reqs: []storespec.StoredRow{{Envelope: req}}},
		ClosedForever: func(ctx context.Context, _ actor.ActorID) (bool, error) {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-release // held open until the test lets it proceed, simulating an in-flight closeFor
			return true, ctx.Err()
		},
		Clock:  time.Now,
		Logger: slog.New(h),
	})

	ch.OnDown(context.Background(), "worker", actorrt.Incarnation{}, errors.New("dead"))

	// Wait until closeFor's predicate call is actually in flight (blocked on release).
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("closure predicate was never entered — test setup is broken")
	}

	closeDone := make(chan struct{})
	go func() {
		ch.Close()
		close(closeDone)
	}()

	// Give Close time to observe downStop and start waiting on the consumer's
	// join before releasing the in-flight call — this is the window the old
	// cancel-first order would have raced.
	time.Sleep(50 * time.Millisecond)
	close(release)

	select {
	case <-closeDone:
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not return after the in-flight closure finished")
	}

	if f := h.faults(); len(f) != 0 {
		t.Fatalf("normal Close during in-flight closeFor logged fault(s) %v — should be quiet (heartbeats excluded)", f)
	}
	if fc.count() != 1 {
		t.Fatalf("in-flight closure should have completed and written its terminal, count=%d", fc.count())
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
