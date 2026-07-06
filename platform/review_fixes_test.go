package platform

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	channelpkg "github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// openWhiteboxHome assembles a Home reachable白盒 (this file is package platform),
// so a test can stamp Host directly via cs.Membership and read cs.Registry — the
// placement facts no public verb exposes.
func openWhiteboxHome(t *testing.T) *Home {
	t.Helper()
	h, err := Open(HomeConfig{
		ChannelID: channelpkg.ID("test-review-fixes"),
		DBPath:    filepath.Join(t.TempDir(), "home.sqlite"),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

// TestAdmit_ReAdmitPreservesHost pins the Admit no-op fix (#3): a re-Admit of an
// ALREADY-active member (an idempotent introduce retry) must NOT reset a
// daemon-stamped Host back to "" — placement authority is the attach/plan path's,
// never Admit's. Before the fix, Admit unconditionally applied a Host="" row, and
// applyMemberAddTx's host-diff UPDATE clobbered the live placement.
func TestAdmit_ReAdmitPreservesHost(t *testing.T) {
	h := openWhiteboxHome(t)
	ctx := context.Background()
	id := actor.ActorID("agent:rev")

	// Genesis Admit → active row, Host="".
	if err := h.Admit(ctx, id, actor.KindAgent); err != nil {
		t.Fatalf("Admit genesis: %v", err)
	}
	// Stamp a daemon Host (what an attach does): active row + host-diff → UPDATE host.
	if err := h.cs.Membership.ApplyMemberTransitions(ctx, []storespec.MemberActorAdd{{
		ID: id, Kind: actor.KindAgent, Host: "daemon-1", At: h.nowMs(),
	}}, nil); err != nil {
		t.Fatalf("stamp host: %v", err)
	}
	if rec, ok, _ := h.cs.Registry.Lookup(ctx, id); !ok || rec.Host != "daemon-1" {
		t.Fatalf("precondition: Host not stamped, rec=%+v ok=%v", rec, ok)
	}

	// Idempotent re-Admit — must be a pure no-op, Host untouched.
	if err := h.Admit(ctx, id, actor.KindAgent); err != nil {
		t.Fatalf("re-Admit: %v", err)
	}
	rec, ok, err := h.cs.Registry.Lookup(ctx, id)
	if err != nil || !ok {
		t.Fatalf("Lookup after re-Admit: ok=%v err=%v", ok, err)
	}
	if rec.Host != "daemon-1" {
		t.Fatalf("re-Admit clobbered Host to %q, want daemon-1 preserved (#3)", rec.Host)
	}
}

// TestRemove_StopsHumanCallerTimeout pins the human-caller lifecycle fix (#4): a
// human subject with an in-flight (armed) request, once REMOVED, must not have its
// caller's timeout still fire an unanswered_timeout terminal through the裸 pen (a
// 死后写). Remove stops the caller's pending timer; after the deadline elapses no
// terminal appears in the log.
func TestRemove_StopsHumanCallerTimeout(t *testing.T) {
	h := openWhiteboxHome(t)
	ctx := context.Background()
	id := actor.ActorID("user:alice")
	if err := h.Admit(ctx, id, actor.KindHuman); err != nil {
		t.Fatalf("Admit: %v", err)
	}

	baseline, err := h.cs.Query.MaxSeq(ctx)
	if err != nil {
		t.Fatalf("MaxSeq baseline: %v", err)
	}

	// Arm an in-flight request with a very short deadline. Arm itself writes nothing
	// — only the timer's fireTimeout would (the terminal we must NOT see).
	expires := h.nowMs() + 30
	req := &message.Envelope{
		ID:        message.ID("req-alice-1"),
		Kind:      message.KindRequest,
		Type:      "ask.do",
		Sender:    message.Sender{ID: id, Kind: actor.KindHuman},
		Audience:  message.Audience{actor.ActorID("agent:x")},
		ExpiresAt: &expires,
	}
	h.humanCaller(id).Arm(req)

	// Remove the subject — its caller's timer must be stopped, index entry dropped.
	if err := h.Remove(ctx, id); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	h.humanCallersMu.Lock()
	_, stillIndexed := h.humanCallers[id]
	h.humanCallersMu.Unlock()
	if stillIndexed {
		t.Fatal("removed subject's caller still in the by-id index (leak)")
	}

	// Wait well past the deadline; no unanswered_timeout terminal must land.
	time.Sleep(200 * time.Millisecond)
	rows, err := h.cs.Query.ReadAfterSeq(ctx, baseline, 500)
	if err != nil {
		t.Fatalf("ReadAfterSeq: %v", err)
	}
	for _, r := range rows {
		if r.Envelope.Kind == message.KindResponse && r.Envelope.ParentID == req.ID {
			t.Fatalf("timeout fired a 死后写 terminal after removal: %+v", r.Envelope)
		}
	}
}
