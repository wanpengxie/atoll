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
		ChannelID:    "ch",
		SystemPen:    fc,
		OpenRequests: fakeQuery{reqs: []storespec.StoredRow{{Envelope: req}}},
		Clock:        time.Now,
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

// TestReconcile_Despawn_ClosesWithoutCaller proves the closure reconciler (the
// level scan) closes a CLEAN-despawned actor's inbound open request WITHOUT the
// caller doing anything (F5: Despawn carries no "callers MUST collapse before"
// obligation). A clean despawn fires NO death edge — only the level scan, which
// finds the open request's receiver absent from liveness and materialises
// receiver_unavailable.
func TestReconcile_Despawn_ClosesWithoutCaller(t *testing.T) {
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
		ChannelID: "ch",
		SystemPen: fc,
		OpenRequests: fakeQuery{
			reqs:      []storespec.StoredRow{{Envelope: req}},
			receivers: []actor.ActorID{"worker"}, // the open request's receiver
		},
		Clock: time.Now,
	})
	defer ch.Close()
	// Place the worker, then CLEAN despawn it (no panic → no death edge fires).
	workerInc, _, _ := ch.Cells().SpawnIfAbsent("worker", actor.KindTool, func(actorrt.Incarnation) actorrt.Actor { return liveActor{} })

	// While present, a sweep must NOT close it (the live receiver can answer).
	ch.Reconcile(context.Background())
	if fc.count() != 0 {
		t.Fatal("reconciler closed a request whose receiver is still PRESENT")
	}

	// Clean despawn — no edge. The caller does NOTHING to collapse the request.
	ch.Cells().Despawn(workerInc)
	if _, ok := ch.Cells().Stat("worker"); ok {
		t.Fatal("worker still present after Despawn")
	}

	// The level scan alone closes the orphan.
	ch.Reconcile(context.Background())
	if fc.count() != 1 {
		t.Fatalf("despawn reconciler did not close the inbound open request (count=%d)", fc.count())
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

// waitFirstMsg polls until at least one message is captured, returning the first
// (or "" on timeout). The down edge is now async: OnDown only O(1)-posts, the
// resident consumer does the closure work + logging on its own goroutine.
func (h *capHandler) waitFirstMsg(timeout time.Duration) string {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		h.mu.Lock()
		n := len(h.msgs)
		var first string
		if n > 0 {
			first = h.msgs[0]
		}
		h.mu.Unlock()
		if n > 0 {
			return first
		}
		time.Sleep(2 * time.Millisecond)
	}
	return ""
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
		ChannelID:    "ch",
		SystemPen:    &fakeWriter{},
		OpenRequests: errQuery{},
		Clock:        time.Now,
		Logger:       slog.New(h),
	})
	defer ch.Close()
	ch.OnDown(context.Background(), "worker", actorrt.Incarnation{}, nil)
	got := h.waitFirstMsg(2 * time.Second)
	if got == "" {
		t.Fatal("drain query failed but NO fault logged — silent black hole regression")
	}
	if got != "channelkit.closure.drain_query_failed" {
		t.Fatalf("fault msg=%q, want channelkit.closure.drain_query_failed", got)
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
		ChannelID:    "ch",
		SystemPen:    errWriter{},
		OpenRequests: fakeQuery{reqs: []storespec.StoredRow{{Envelope: req}}},
		Clock:        time.Now,
		Logger:       slog.New(h),
	})
	defer ch.Close()
	ch.OnDown(context.Background(), "worker", actorrt.Incarnation{}, nil)
	got := h.waitFirstMsg(2 * time.Second)
	if got == "" {
		t.Fatal("per-request write failed but NO fault logged — silent black hole regression")
	}
	if got != "channelkit.closure.write_failed" {
		t.Fatalf("fault msg=%q, want channelkit.closure.write_failed", got)
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

// TestOnDown_AsyncConsumer_SkipsLivePresentSuccessor (DoD⑤ + G0-3): the down
// edge is now O(1)-posted and materialised by a resident consumer, which RE-CHECKS
// liveness before writing — so a stale edge for an id whose same-id successor is
// already live is skipped (no terminal), exactly as the level scan guards. A
// live "worker" is present, an edge arrives for it, and NO closure terminal is
// written.
func TestOnDown_AsyncConsumer_SkipsLivePresentSuccessor(t *testing.T) {
	req := message.Envelope{
		ID: "req-1", ChannelID: "ch", Kind: message.KindRequest, Type: "x.do",
		Sender: message.Sender{Kind: actor.KindAgent, ID: "caller"}, Audience: message.Audience{"worker"},
	}
	fc := &fakeWriter{}
	ch := newChannel(channelkit.Config{
		ChannelID:    "ch",
		SystemPen:    fc,
		OpenRequests: fakeQuery{reqs: []storespec.StoredRow{{Envelope: req}}},
		Clock:        time.Now,
	})
	defer ch.Close()
	// A live successor occupies "worker".
	_, _, _ = ch.Cells().SpawnIfAbsent("worker", actor.KindTool, func(actorrt.Incarnation) actorrt.Actor { return liveActor{} })

	// A stale edge for "worker" — the async consumer must recheck Present and skip.
	ch.OnDown(context.Background(), "worker", actorrt.Incarnation{}, errors.New("stale predecessor edge"))

	// Give the consumer time to (not) write. No terminal must appear.
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if fc.count() != 0 {
			t.Fatalf("consumer wrote a terminal for a live-present successor — recheck did not skip")
		}
		time.Sleep(5 * time.Millisecond)
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
