package storespec

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/protocol/actor"
)

var ErrMemberInactive = errors.New("storespec: member missing or deregistered")

// MemberActorAdd is one actor registration transition (membership control
// plane). Applying it mutates the actor_registry projection AND emits a
// system.actor.registered mirror event into the channel log.
type MemberActorAdd struct {
	ID      actor.ActorID
	Kind    actor.Kind
	Binding actor.Binding
	// Host is the placement locus (see storespec.Record.Host): "" = home,
	// a compute id = the daemon hosting the cell. A host-only change on an
	// already-active member is a placement fact update, not a re-registration,
	// so it mutates the row WITHOUT emitting a system.actor.registered mirror.
	Host string
	At   int64
}

// NOTE: an actor's capability/service DECLARATION (name, handled types, skill
// doc) is NOT a substrate membership field — substrate identity is {ID, Kind,
// Binding}. The declaration is application-level self-description, carried as
// an ordinary event (skill-as-document), outside the scope of membership. The
// who-can-call-whom AUTHORISATION is a separate, not-yet-built substrate
// concern, unrelated to this blob.

// MemberActorRemove is one actor deregistration transition.
type MemberActorRemove struct {
	ID actor.ActorID
	// ExpectedHost, when non-empty, guards the deregistration on the row STILL
	// being placed on that host: the remove only takes effect if the row's host
	// column equals it, and a mismatch is a silent no-op (no cascade, no mirror).
	// This is the attach-reconcile arm's migration-window guard — a stale
	// compute A reconciling against a snapshot taken before the actor re-homed
	// to compute B must not deregister B's active row. Empty = unguarded, the
	// product-level deregistration semantics (identity removal is host-agnostic).
	ExpectedHost string
	At           int64
}

// MembershipControlPlane is the full membership-management contract —
// deliberately SEGREGATED from the read-only Registry so a read-only consumer
// never receives any membership WRITE. It composes the single-actor
// MembershipWriter (Deregister) with the batch + log-mirror transitions
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
	// Admit atomically mints a fresh instance id and registers principal, or
	// returns the existing active instance for an idempotent retry.
	Admit(ctx context.Context, kind actor.Kind, principal string, at int64) (actor.ActorID, error)
	// EnsureSystemActor is the sole fixed-id seed. Application identities must
	// enter through Admit; this arm accepts no caller-selected id.
	EnsureSystemActor(ctx context.Context, at int64) error
}
