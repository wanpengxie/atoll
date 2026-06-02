package storespec

import (
	"context"
	"encoding/json"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
)

// MemberActorAdd is one actor registration transition (membership control
// plane). Applying it mutates the actor_registry projection AND emits a
// system.actor.registered mirror event into the channel log.
type MemberActorAdd struct {
	ID          actor.ActorID
	Kind        actor.Kind
	Binding     actor.Binding
	DisplayName string
	UserID      string
	Role        string
	At          int64
	ProxyHost   MemberActorProxyHost

	// CapabilitySet is the opaque declaration blob echoed verbatim into the
	// system.actor.registered fact so a reconciler can rebuild facade wiring
	// from the channel log alone (推论5 / §4 事实完整性). The store never
	// interprets it; empty for members needing no facade wiring.
	CapabilitySet json.RawMessage
}

// MemberActorProxyHost is optional metadata for proxy-daemon-hosted actors,
// emitted in system.actor.registered only.
type MemberActorProxyHost struct {
	DaemonID   string
	DaemonName string
}

// MemberActorRemove is one actor deregistration transition.
type MemberActorRemove struct {
	ID actor.ActorID
	At int64
}

// DesiredProxyMember is one active runtime_inbound_via_relay tool actor plus
// the capability_set blob from its latest registered fact — the input a
// reconciler needs to rebuild facade wiring from facts alone (§9 DoD #4).
type DesiredProxyMember struct {
	ID            actor.ActorID
	CapabilitySet json.RawMessage
}

// MembershipControlPlane is the membership-management contract — deliberately
// SEGREGATED from the read-only Registry so a pure reader (harness audience
// check, trigger) never receives ApplyMemberTransitions, which emits log mirror
// events (a control-plane write, not a query). Forward-derived from the
// component's role, not from any one downstream consumer.
type MembershipControlPlane interface {
	// ApplyMemberTransitions mutates actor_registry and appends the matching
	// system.actor.* mirror events in one tx (idempotent on retry).
	ApplyMemberTransitions(ctx context.Context, channelID channel.ID, adds []MemberActorAdd, removes []MemberActorRemove) error
	// ListDesiredProxyMembers replays the registration facts and returns the
	// current active runtime_inbound_via_relay tool actors (level-triggered).
	ListDesiredProxyMembers(ctx context.Context) ([]DesiredProxyMember, error)
}
