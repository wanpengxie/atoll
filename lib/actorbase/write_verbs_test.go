package actorbase

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
)

// write_verbs_test.go covers Emit and Post — the two writes that take on NO
// closure obligation, and therefore share a shape: build the envelope the
// caller specified, write it, and hand back either an id or a TYPED rejection.

// Post takes no caller obligation, so it must not quietly adopt one. The
// deadline is the sharpest case: the call face resolves an absent ExpiresAt to
// its own short default, because a live caller is about to block on it. A
// posted request has nobody blocking, and its deadline is the substrate's to
// stamp — turning an approval that should stand for a day into one that dies
// in half a minute is not a detail.
func TestPostLeavesAnAbsentDeadlineAbsent(t *testing.T) {
	t.Parallel()
	pen := &fakePen{self: "human:alice:1"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()

	if _, err := e.Post(behavior.RequestSpec{
		Type:     "human.approve",
		Audience: message.Audience{actor.ActorID("agent:worker")},
		Cause:    message.Root(),
	}); err != nil {
		t.Fatalf("Post = %v", err)
	}
	if got := pen.last(); got.ExpiresAt != nil {
		t.Fatalf("Post stamped expires_at=%d; an absent deadline must ride through untouched", *got.ExpiresAt)
	}
}

// Post writes nothing into the out-station account. There is no ticket to Wait
// on and no Await frame to consume one, so a registered entry would simply
// accumulate: every answered request left behind as an unread buffered row.
func TestPostRegistersNoCallLedgerEntry(t *testing.T) {
	t.Parallel()
	pen := &fakePen{self: "human:alice:1"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()

	id, err := e.Post(behavior.RequestSpec{
		ID:       "req-posted",
		Type:     "human.approve",
		Audience: message.Audience{actor.ActorID("agent:worker")},
		Cause:    message.Root(),
	})
	if err != nil {
		t.Fatalf("Post = %v", err)
	}
	for _, inflight := range e.List() {
		if inflight == id {
			t.Fatal("Post registered an out-station entry; it takes no caller obligation")
		}
	}
	if len(e.List()) != 0 {
		t.Fatalf("call ledger = %v, want empty", e.List())
	}
}

// Call refuses a self-addressed request because a single worker that Calls
// itself and then Waits deadlocks on a reply only that same, now-blocked
// goroutine could author. Post never waits, so the reason does not apply and
// neither does the refusal.
func TestPostAllowsASelfAddressedRequest(t *testing.T) {
	t.Parallel()
	pen := &fakePen{self: "human:alice:1"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()
	e.actorCtx = &fakeActorContext{self: "human:alice:1"}

	if _, err := e.Post(behavior.RequestSpec{
		Type:     "note.to.self",
		Audience: message.Audience{actor.ActorID("human:alice:1")},
		Cause:    message.Root(),
	}); err != nil {
		t.Fatalf("Post to self = %v, want nil (ErrSelfCall guards Wait, not writing)", err)
	}
	// Call, on the same engine and the same target, still refuses — the guard
	// belongs to the waiting verb, and this test is the contrast.
	if _, err := e.Call(message.Root(), actor.ActorID("human:alice:1"), "note.to.self", nil); !errors.Is(err, ErrSelfCall) {
		t.Fatalf("Call to self = %v, want ErrSelfCall", err)
	}
}

// Both verbs surface a harness rejection TYPED. A formatted error would be
// flattened to a generic "unavailable" at the frame boundary, turning a
// precise, deterministic verdict into a shrug that invites a pointless retry.
func TestEmitAndPostSurfaceHarnessRejectionsTyped(t *testing.T) {
	t.Parallel()
	const reason = "harness_id_duplicate_conflict"

	t.Run("Emit", func(t *testing.T) {
		e := newTestEngine(t, &fakePen{self: "human:alice:1", reject: harness.HarnessRejectReason(reason)}, Hooks{}, 8, 8)
		e.lifeCtx = context.Background()
		_, err := e.Emit(behavior.EventSpec{Type: "human.note", Cause: message.Root()})
		var rejected *WriteRejected
		if !errors.As(err, &rejected) {
			t.Fatalf("Emit reject = %v (%T), want a *WriteRejected", err, err)
		}
		if rejected.Reason != reason {
			t.Fatalf("carried reason = %q, want %q verbatim", rejected.Reason, reason)
		}
	})

	t.Run("Post", func(t *testing.T) {
		e := newTestEngine(t, &fakePen{self: "human:alice:1", reject: harness.HarnessRejectReason(reason)}, Hooks{}, 8, 8)
		e.lifeCtx = context.Background()
		_, err := e.Post(behavior.RequestSpec{
			Type:     "human.approve",
			Audience: message.Audience{actor.ActorID("agent:worker")},
			Cause:    message.Root(),
		})
		var rejected *WriteRejected
		if !errors.As(err, &rejected) {
			t.Fatalf("Post reject = %v (%T), want a *WriteRejected", err, err)
		}
		if rejected.Reason != reason {
			t.Fatalf("carried reason = %q, want %q verbatim", rejected.Reason, reason)
		}
	})
}

// The harness accepts visibility=system — the substrate writes system messages
// itself. The verb table is narrower on purpose: system messages are hidden
// from ordinary history views, so one authored through an actor's pen would
// slip past the read side's enforcement. Absent stays "public", which is what
// every existing frame relies on.
func TestEmitAndPostRestrictVisibilityToTheActorFacingSet(t *testing.T) {
	t.Parallel()
	pen := &fakePen{self: "human:alice:1"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()

	for _, visibility := range []message.Visibility{message.VisibilitySystem, message.Visibility("private")} {
		_, err := e.Emit(behavior.EventSpec{Type: "human.note", Visibility: visibility, Cause: message.Root()})
		var invalid *InvalidVisibilityError
		if !errors.As(err, &invalid) {
			t.Fatalf("Emit(visibility=%s) error = %v, want typed InvalidVisibilityError", visibility, err)
		}
		_, err = e.Post(behavior.RequestSpec{
			Type:       "human.approve",
			Audience:   message.Audience{actor.ActorID("agent:worker")},
			Visibility: visibility,
			Cause:      message.Root(),
		})
		if !errors.As(err, &invalid) {
			t.Fatalf("Post(visibility=%s) error = %v, want typed InvalidVisibilityError", visibility, err)
		}
	}
	if pen.count() != 0 {
		t.Fatalf("a refused visibility still wrote %d envelopes", pen.count())
	}

	// Absent normalises to public rather than reaching truth empty.
	if _, err := e.Emit(behavior.EventSpec{Type: "human.note", Cause: message.Root()}); err != nil {
		t.Fatalf("Emit = %v", err)
	}
	if got := pen.last().Visibility; got != message.VisibilityPublic {
		t.Fatalf("empty visibility landed as %q, want public", got)
	}
	if _, err := e.Post(behavior.RequestSpec{
		Type:     "human.approve",
		Audience: message.Audience{actor.ActorID("agent:worker")},
		Cause:    message.Root(),
	}); err != nil {
		t.Fatalf("Post = %v", err)
	}
	if got := pen.last().Visibility; got != message.VisibilityPublic {
		t.Fatalf("empty visibility landed as %q, want public", got)
	}
}

// Both verbs take the FULL envelope surface, and every field of it must reach
// truth as given — a caller-chosen id, a parent, a correlation, an explicit
// public visibility, a declared deadline. This is what makes them a verb
// table entry rather than sugar: nothing here is quietly overridden.
func TestEmitAndPostCarryTheWholeSpecToTruth(t *testing.T) {
	t.Parallel()
	pen := &fakePen{self: "human:alice:1"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()

	if _, err := e.Emit(behavior.EventSpec{
		ID:            "ev-own-id",
		Type:          "human.note",
		Payload:       json.RawMessage(`{"text":"hi"}`),
		Visibility:    message.VisibilityPublic,
		Audience:      message.Audience{actor.ActorID("agent:worker")},
		Cause:         message.Anchored("req-parent", "corr-1"),
	}); err != nil {
		t.Fatalf("Emit = %v", err)
	}
	ev := pen.last()
	if ev.ID != "ev-own-id" || ev.Kind != message.KindEvent || ev.Type != "human.note" {
		t.Fatalf("event identity = %+v", ev)
	}
	if string(ev.Payload) != `{"text":"hi"}` {
		t.Fatalf("event payload = %s, want the RawMessage verbatim", ev.Payload)
	}
	if ev.Visibility != message.VisibilityPublic {
		t.Fatalf("event visibility = %q, want public", ev.Visibility)
	}
	if ev.ParentID != "req-parent" || ev.CorrelationID != "corr-1" {
		t.Fatalf("event parent/correlation = %q/%q", ev.ParentID, ev.CorrelationID)
	}

	deadline := int64(1893456000000)
	if _, err := e.Post(behavior.RequestSpec{
		ID:            "req-own-id",
		Type:          "human.approve",
		Payload:       json.RawMessage(`{"amount":10}`),
		Audience:      message.Audience{actor.ActorID("agent:worker")},
		Visibility:    message.VisibilityPublic,
		Cause:         message.Anchored("ev-parent", "corr-2"),
		ExpiresAt:     &deadline,
	}); err != nil {
		t.Fatalf("Post = %v", err)
	}
	req := pen.last()
	if req.ID != "req-own-id" || req.Kind != message.KindRequest || req.Type != "human.approve" {
		t.Fatalf("request identity = %+v", req)
	}
	if string(req.Payload) != `{"body":{"amount":10}}` {
		t.Fatalf("request payload = %s, want canonical body envelope", req.Payload)
	}
	if req.Visibility != message.VisibilityPublic {
		t.Fatalf("request visibility = %q, want public", req.Visibility)
	}
	if req.ParentID != "ev-parent" || req.CorrelationID != "corr-2" {
		t.Fatalf("request parent/correlation = %q/%q", req.ParentID, req.CorrelationID)
	}
	if req.ExpiresAt == nil || *req.ExpiresAt != deadline {
		t.Fatalf("request expires_at = %v, want the declared %d", req.ExpiresAt, deadline)
	}
}
