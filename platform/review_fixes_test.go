package platform

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	channelpkg "github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// (债②已落：期12 humancell 重建后，human-door 机器的测试住
// platform/humandoor_test.go——跨 incarnation/冻结环/presence straddle/
// resource face 真集成；旧 humanCaller 形随 behavior.Caller 拆删不再重建。)

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
