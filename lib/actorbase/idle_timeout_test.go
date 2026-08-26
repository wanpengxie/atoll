package actorbase

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
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
