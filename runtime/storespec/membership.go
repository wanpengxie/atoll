package storespec

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
)

// MemberActorAdd is one actor registration transition (membership control
// plane). Applying it mutates the actor_registry projection AND emits a
// system.actor.registered mirror event into the channel log.
type MemberActorAdd struct {
	ID      actor.ActorID
	Kind    actor.Kind
	Binding actor.Binding
	At      int64
}

// NOTE: an actor's capability/service DECLARATION (name, handled types, skill
// doc) is NOT a substrate membership field — substrate identity is {ID, Kind,
// Binding}. The declaration is an application-level fact the adapter writes as
// an ordinary event (skill-as-document) and reads back via cursor on restart
// (a runtime concern, proto-transparent). The who-can-call-whom AUTHORISATION
// (capability ②, kernel-roadmap §4.2) is a separate, not-yet-built substrate
// concern (actorreg.Record + harness caller-auth), unrelated to this blob.

// MemberActorRemove is one actor deregistration transition.
type MemberActorRemove struct {
	ID actor.ActorID
	At int64
}

// MembershipControlPlane is the full membership-management contract —
// deliberately SEGREGATED from the read-only Registry so a pure reader (the
// harness audience check, the system actor's directory query) never receives
// any membership WRITE. It composes the
// single-actor MembershipWriter (Insert/Deregister) with the batch + log-mirror
// transitions and the desired-set replay. Every method here is a control-plane
// write or a fact replay, never an ambient query. Forward-derived from the
// component's role, not from any one downstream consumer.
type MembershipControlPlane interface {
	MembershipWriter
	// ApplyMemberTransitions mutates actor_registry and appends the matching
	// system.actor.* mirror events in one tx (idempotent on retry).
	ApplyMemberTransitions(ctx context.Context, channelID channel.ID, adds []MemberActorAdd, removes []MemberActorRemove) error
}
