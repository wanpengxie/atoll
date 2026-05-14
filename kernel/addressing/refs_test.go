package addressing_test

import (
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/addressing"
	"github.com/wanpengxie/ActOS/kernel/channel"
)

// TestLocalChannelRefOrgEmpty — single-org / demo ref has empty OrgID.
func TestLocalChannelRefOrgEmpty(t *testing.T) {
	ref := addressing.LocalChannelRef("ch-1")
	if ref.OrgID != "" {
		t.Errorf("OrgID=%q want empty", ref.OrgID)
	}
	if ref.ID != channel.ID("ch-1") {
		t.Errorf("ID=%v want ch-1", ref.ID)
	}
	if !ref.Local() {
		t.Error("LocalChannelRef must report Local()=true")
	}
}

// TestNewChannelRefFederation — populating OrgID marks the ref non-local.
func TestNewChannelRefFederation(t *testing.T) {
	ref := addressing.NewChannelRef("org-A", "ch-1")
	if ref.Local() {
		t.Error("ref with OrgID set must NOT be Local()")
	}
	if ref.OrgID != "org-A" || ref.ID != "ch-1" {
		t.Errorf("ref=%+v want {org-A ch-1}", ref)
	}
}

// TestLocalActorRefDerivesFromChannel — Local() bubbles up the embedded
// channel-ref's Local() result.
func TestLocalActorRefDerivesFromChannel(t *testing.T) {
	ar := addressing.LocalActorRef("ch-1", "agent:a")
	if !ar.Local() {
		t.Error("LocalActorRef must be Local()")
	}
	if ar.Channel.ID != "ch-1" || ar.Actor != "agent:a" {
		t.Errorf("LocalActorRef=%+v want {{ ch-1} agent:a}", ar)
	}
}

// TestNewActorRefFederation — OrgID propagation into Channel.
func TestNewActorRefFederation(t *testing.T) {
	ar := addressing.NewActorRef("org-B", "ch-2", "tool:xhs")
	if ar.Local() {
		t.Error("ActorRef with OrgID set must NOT be Local()")
	}
	if ar.Channel.OrgID != "org-B" || ar.Channel.ID != "ch-2" || ar.Actor != "tool:xhs" {
		t.Errorf("ActorRef=%+v want {{org-B ch-2} tool:xhs}", ar)
	}
}

// TestActorRefValueSemantics — ActorRef is a pure value type and must be
// safe to compare with == and use as a map key (kernel contract).
func TestActorRefValueSemantics(t *testing.T) {
	a := addressing.LocalActorRef("ch-1", "agent:a")
	b := addressing.LocalActorRef("ch-1", "agent:a")
	c := addressing.LocalActorRef("ch-1", "agent:b")
	if a != b {
		t.Error("equal-valued ActorRef should compare == ")
	}
	if a == c {
		t.Error("different-actor ActorRef should compare != ")
	}
	m := map[addressing.ActorRef]int{a: 1}
	if m[b] != 1 {
		t.Error("ActorRef must be usable as map key (value equality)")
	}
	// Sanity — also covers the actor.ActorID interaction.
	if a.Actor != actor.ActorID("agent:a") {
		t.Errorf("Actor=%v want agent:a", a.Actor)
	}
}

// TestLocalRouteTargetEcho — LocalRoute.Target returns the ref unchanged.
func TestLocalRouteTargetEcho(t *testing.T) {
	ref := addressing.LocalActorRef("ch-1", "agent:a")
	r := addressing.LocalRoute{Ref: ref}
	if got := r.Target(); got != ref {
		t.Errorf("LocalRoute.Target()=%+v want %+v", got, ref)
	}
}

// TestRouteInterfaceShape — LocalRoute satisfies addressing.Route.
func TestRouteInterfaceShape(t *testing.T) {
	var r addressing.Route = addressing.LocalRoute{Ref: addressing.LocalActorRef("ch-1", "agent:a")}
	if r.Target().Actor != "agent:a" {
		t.Error("Route.Target() not wired")
	}
}
