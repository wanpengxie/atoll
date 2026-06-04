package channelkit_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/lib/channelkit"
	rtharness "github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// panicActor panics on Receive — simulates a cell dying abnormally.
type panicActor struct{}

func (panicActor) Receive(context.Context, *message.Envelope) error { panic("boom") }

// liveActor is a healthy no-op cell — stands in for a same-id successor.
type liveActor struct{}

func (liveActor) Receive(context.Context, *message.Envelope) error { return nil }

// TestOnDown_DoesNotDespawnSuccessor locks the PresenceWatcher contract: a late
// death edge for an id must NOT despawn whatever currently occupies that id. A
// dead predecessor self-evicts via the runtime's pointer-identity removeIf; if
// OnDown also despawned, a same-id successor would be wrongly killed (Despawn is
// not pointer-checked).
func TestOnDown_DoesNotDespawnSuccessor(t *testing.T) {
	ch := channelkit.New(channelkit.Config{ChannelID: "ch", Clock: time.Now})
	ch.Cells().Spawn("worker", liveActor{}) // the live successor
	if _, ok := ch.Cells().Stat("worker"); !ok {
		t.Fatal("successor not hosted after Spawn")
	}
	// A late presence-down edge for "worker" (e.g. from a replaced predecessor).
	ch.OnDown(context.Background(), "worker", errors.New("late predecessor death"))
	if _, ok := ch.Cells().Stat("worker"); !ok {
		t.Fatal("OnDown despawned the live successor — PresenceWatcher contract violated")
	}
}

// fakeChain records written terminals (implements runtime/harness.Writer).
type fakeChain struct {
	mu      sync.Mutex
	written []*message.Envelope
}

func (f *fakeChain) Write(_ context.Context, env *message.Envelope) (rtharness.WriteResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.written = append(f.written, env)
	return rtharness.WriteResult{MessageID: env.ID}, nil
}
func (f *fakeChain) count() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.written) }

// fakeOpenReqs returns canned in-flight request rows for a dead actor (matching
// the substrate storespec.StoredRow shape the real store returns).
type fakeOpenReqs struct{ reqs []storespec.StoredRow }

func (f fakeOpenReqs) OpenRequestsForActor(context.Context, actor.ActorID) ([]storespec.StoredRow, error) {
	return f.reqs, nil
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
	fc := &fakeChain{}
	ch := channelkit.New(channelkit.Config{
		ChannelID:    "ch",
		Chain:        fc,
		OpenRequests: fakeOpenReqs{reqs: []storespec.StoredRow{{Envelope: req}}},
		Clock:        time.Now,
	})
	ch.Cells().Spawn("worker", panicActor{})

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
		t.Fatal("presence-down closure NOT materialised — OnDown wrote no terminal (black hole regression)")
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

// errOpenReqs fails the drain query — the worst closure case (no request can be
// closed → every caller is a black hole). The watcher MUST NOT swallow it.
type errOpenReqs struct{}

func (errOpenReqs) OpenRequestsForActor(context.Context, actor.ActorID) ([]storespec.StoredRow, error) {
	return nil, errors.New("store down")
}

// capHandler is a minimal slog.Handler capturing emitted record messages so a
// test can assert a fault was surfaced through the std slog facade.
type capHandler struct{ msgs []string }

func (*capHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *capHandler) Handle(_ context.Context, r slog.Record) error {
	h.msgs = append(h.msgs, r.Message)
	return nil
}
func (h *capHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capHandler) WithGroup(string) slog.Handler      { return h }

// TestClosureDrainFailure_IsSurfaced proves a swallowed-drain regression cannot
// return: when the drain query fails (cannot close anyone → black hole), the
// watcher logs a fault rather than returning silently. The materialisation now
// lives in the behaviour base (author#3); channelkit injects the seams and an
// onFault callback, so this drives the invariant through OnDown — the only
// surface left after the free function moved to behavior.
func TestClosureDrainFailure_IsSurfaced(t *testing.T) {
	h := &capHandler{}
	ch := channelkit.New(channelkit.Config{
		ChannelID:    "ch",
		Chain:        &fakeChain{},
		OpenRequests: errOpenReqs{},
		Clock:        time.Now,
		Logger:       slog.New(h),
	})
	ch.OnDown(context.Background(), "worker", nil)
	if len(h.msgs) == 0 {
		t.Fatal("drain query failed but NO fault logged — silent black hole regression")
	}
	if h.msgs[0] != "channelkit.closure.drain_query_failed" {
		t.Fatalf("fault msg=%q, want channelkit.closure.drain_query_failed", h.msgs[0])
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
