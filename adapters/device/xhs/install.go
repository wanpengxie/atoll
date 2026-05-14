package xhs

import (
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// ActorSeed is the actor_registry row composition root MUST insert
// before installing the xhs Module on a channel. Mirrors the L1 §12.2
// minimum field set + L2 §1.4.6 / L2 §1.4.2 install rules.
type ActorSeed struct {
	ID          actor.ActorID
	Kind        message.SenderKind
	Binding     actor.Binding
	DisplayName string
}

// TypeSeed is one type_registry row entry. handler_actor_id always
// equals the xhs adapter actor id (it owns every xhs.* type); the
// handler_binding equals BindingViaServerTransit per L1 §11.7.
//
// MaxPendingMs is the per-type request budget — the framework arms a
// timer of that duration on every kind=request envelope.
//
// AllowEvent indicates the row is event-only (no request/response
// pairing). Today only xhs.note.archived sets it.
type TypeSeed struct {
	Type           string
	HandlerActorID actor.ActorID
	HandlerBinding adapter.BindingKind
	MaxPendingMs   int64
	AllowEvent     bool
}

// InstallSpec is the bundle of registry seed rows composition root
// (cmd/daemon, T7) consumes to bootstrap a channel that hosts the xhs
// adapter. Per L1 §12.3 actor_registry MUST exist before the adapter
// install runs; per L2 §1.4.2 type_registry rows MUST validate against
// actor_registry.handler_actor_id + actor_binding.
//
// The struct intentionally avoids registry write APIs — composition
// root is free to apply the seeds via runtime/store (T3 sqlite) or any
// other backend.
type InstallSpec struct {
	Actor ActorSeed
	Types []TypeSeed
}

// DefaultInstallSpec returns the canonical M1.5 install seeds for the
// xhs adapter: one tool actor + 5 R/R types + 1 event-only type. Used
// by composition root + by tests that need a known-good registry seed.
//
// The actor id defaults to DefaultAdapterActorID; callers that supply a
// non-default Config.AdapterActorID should override Actor.ID + every
// TypeSeed.HandlerActorID accordingly via the returned spec.
func DefaultInstallSpec(maxPendingMs int64) InstallSpec {
	if maxPendingMs <= 0 {
		maxPendingMs = DefaultMaxPendingMs
	}
	actorSeed := ActorSeed{
		ID:          DefaultAdapterActorID,
		Kind:        message.SenderTool,
		Binding:     actor.BindingViaServerTransit,
		DisplayName: "xhs",
	}
	types := make([]TypeSeed, 0, len(AllTypes))
	for _, t := range RequestResponseTypes {
		types = append(types, TypeSeed{
			Type:           t,
			HandlerActorID: actorSeed.ID,
			HandlerBinding: Binding,
			MaxPendingMs:   maxPendingMs,
		})
	}
	types = append(types, TypeSeed{
		Type:           TypeNoteArchived,
		HandlerActorID: actorSeed.ID,
		HandlerBinding: Binding,
		MaxPendingMs:   maxPendingMs,
		AllowEvent:     true,
	})
	return InstallSpec{Actor: actorSeed, Types: types}
}

// WithActorID overrides the actor id on every seed row consistently.
// Useful when composition root wants multiple xhs adapter actors per
// channel (none today) or runs a non-default id for testing.
func (s InstallSpec) WithActorID(id actor.ActorID) InstallSpec {
	if id == "" {
		return s
	}
	out := s
	out.Actor.ID = id
	out.Types = make([]TypeSeed, len(s.Types))
	for i, t := range s.Types {
		t.HandlerActorID = id
		out.Types[i] = t
	}
	return out
}
