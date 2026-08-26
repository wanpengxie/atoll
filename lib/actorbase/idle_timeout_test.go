package actorbase

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
)

func waitTerminal(pen *fakePen, before int, budget time.Duration) bool {
	deadline := time.Now().Add(budget)
	for pen.count() == before && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	return pen.count() > before
}

// A deadline is a sliding window: every provisional response restarts it, so
// a receiver that keeps reporting progress is never closed as unanswered,
// while one that goes silent is closed after one window of silence — and the
// receiver is told to stop.
func TestSlidingDeadlineRestartsOnProgressAndClosesOnSilence(t *testing.T) {
	pen := &fakePen{self: "actor:test"}
	cancelled := make(chan message.ID, 4)
	e := newTestEngine(t, pen, Hooks{Canceller: func(_ actor.ActorID, id message.ID) { cancelled <- id }}, 8, 8)
	e.lifeCtx = context.Background()

	expires := time.Now().Add(40 * time.Millisecond).UnixMilli()
	id, err := e.Submit(behavior.RequestSpec{Type: "work", Audience: message.Audience{"actor:callee"}, Cause: message.Root(), ExpiresAt: &expires})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	// Progress every 15ms for 120ms — three windows past the declared
	// expires_at — never accumulates 40ms of silence.
	before := pen.count()
	for i := 0; i < 8; i++ {
		time.Sleep(15 * time.Millisecond)
		if !e.call.match(responseEnv(id, message.StatusProcessing)) {
			t.Fatal("progress did not match the in-flight entry")
		}
	}
	if pen.count() != before {
		t.Fatalf("progress-fed call was closed: %d extra writes", pen.count()-before)
	}
	if len(e.call.list()) != 1 {
		t.Fatal("entry should still be in flight while progress keeps coming")
	}

	if !waitTerminal(pen, before, 300*time.Millisecond) {
		t.Fatal("silent call was not closed after one window of silence")
	}
	var payload struct {
		ErrorCode string `json:"error_code"`
		Detail    string `json:"detail"`
	}
	_ = json.Unmarshal(pen.last().Payload, &payload)
	if payload.ErrorCode != string(message.TerminalUnansweredTimeout) {
		t.Fatalf("error_code = %q", payload.ErrorCode)
	}
	if payload.Detail == "" {
		t.Fatal("detail missing")
	}
	select {
	case got := <-cancelled:
		if got != id {
			t.Fatalf("receiver told to stop the wrong request: %s", got)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("receiver was not told to stop")
	}
}

// The receiver's in-station window slides the same way: its own progress
// writes restart the ctx deadline, so the handler is not cancelled ahead of
// the caller's window.
func TestServeLedgerTouchRestartsWindow(t *testing.T) {
	l := newServeLedger(func() context.Context { return context.Background() }, 8)
	now := time.Now()
	expires := now.Add(40 * time.Millisecond).UnixMilli()
	env := &message.Envelope{ID: "req-serve", Kind: message.KindRequest, TS: now.UnixMilli(), ExpiresAt: &expires}
	if !l.admit(env) {
		t.Fatal("admit refused")
	}
	ctx, ok := l.ctxFor("req-serve")
	if !ok {
		t.Fatal("no ctx after admit")
	}
	for i := 0; i < 8; i++ {
		time.Sleep(15 * time.Millisecond)
		l.touch("req-serve")
	}
	if ctx.Err() != nil {
		t.Fatal("in-station ctx collapsed while the receiver kept writing progress")
	}
	select {
	case <-ctx.Done():
	case <-time.After(300 * time.Millisecond):
		t.Fatal("in-station ctx did not collapse after one window of silence")
	}
}

// relayPen behaves like a remote cell's proxy pen: it relays the envelope
// without welding the sender into the caller's copy (the home welds its own
// copy), so the out-station ledger never learns who sent the request.
type relayPen struct {
	mu      sync.Mutex
	written []*message.Envelope
}

func (p *relayPen) Write(_ context.Context, env *message.Envelope) (harness.WriteResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	copied := *env
	p.written = append(p.written, &copied)
	return harness.WriteResult{MessageID: env.ID}, nil
}

// A caller whose registered request carries no sender — every daemon-hosted
// actor — must still address its own cancel / timeout terminal to itself.
func TestAuthorTwoTerminalNamesSelfWhenRequestCopyHasNoSender(t *testing.T) {
	pen := &relayPen{}
	e := newTestEngine(t, &fakePen{self: "actor:test"}, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()
	e.pen = pen
	e.call = newCallLedger(e.life, pen, e.clockFn, Hooks{}, func() actor.ActorID { return "tool:remote:1" }, nil)

	id, err := e.Submit(behavior.RequestSpec{Type: "work", Audience: message.Audience{"actor:callee"}, Cause: message.Root()})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if pen.written[0].Sender.ID != "" {
		t.Fatal("test premise: the relayed request copy must carry no sender")
	}
	if err := e.call.cancel(id); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if len(pen.written) != 2 {
		t.Fatalf("cancel wrote %d envelopes, want the request + one terminal", len(pen.written))
	}
	terminal := pen.written[1]
	if terminal.Kind != message.KindResponse || terminal.ParentID != id {
		t.Fatalf("terminal=%+v", terminal)
	}
	if len(terminal.Audience) != 1 || terminal.Audience[0] != "tool:remote:1" {
		t.Fatalf("author#2 terminal audience=%v, want [tool:remote:1] (self)", terminal.Audience)
	}
}
