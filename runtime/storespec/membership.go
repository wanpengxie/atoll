package storespec

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/actor"
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
// Binding}. The declaration is application-level self-description, carried as
// an ordinary event (skill-as-document), outside the scope of membership. The
// who-can-call-whom AUTHORISATION (capability ②, kernel-roadmap §4.2) is a
// separate, not-yet-built substrate concern, unrelated to this blob.

// MemberActorRemove is one actor deregistration transition.
type MemberActorRemove struct {
	ID actor.ActorID
	At int64
}

// MembershipControlPlane is the full membership-management contract —
// deliberately SEGREGATED from the read-only Registry so a read-only consumer
// never receives any membership WRITE. It composes the single-actor
// MembershipWriter (Insert/Deregister) with the batch + log-mirror transitions
// and the desired-set replay. Every method here is a control-plane write or a
// fact replay, never an ambient query. Forward-derived from the component's
// role, not from any one consumer.
type MembershipControlPlane interface {
	MembershipWriter
	// ApplyMemberTransitions mutates actor_registry and appends the matching
	// system.actor.* mirror events in one tx (idempotent on retry). No channelID
	// param: the store is bound to one channel at construction, so the mirror
	// events' scope is the binding — a per-call channel arg would be a
	// pseudo-parameter the caller could mis-stamp (cf. MessageLog.FindByID).
	ApplyMemberTransitions(ctx context.Context, adds []MemberActorAdd, removes []MemberActorRemove) error
}
