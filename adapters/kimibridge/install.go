package kimibridge

import (
	"github.com/wanpengxie/ActOS/kernel/actor"
)

// ActorSeed is the actor_registry row composition root MUST insert
// before installing the Module on a channel. Mirrors the xhs / feishu
// install pattern (L1 §12.2 minimum field set + L2 §1.4.2 install
// rules).
type ActorSeed struct {
	ID          actor.ActorID
	Kind        actor.Kind
	Binding     actor.Binding
	DisplayName string
}

// TypeSeed is one type_registry row entry. handler_actor_id always
// equals the adapter actor id (it owns every kimibridge.* type); the
// handler_binding equals BindingRuntimeOutbound per L1 §11.7.
type TypeSeed struct {
	Type           string
	HandlerActorID actor.ActorID
	HandlerBinding actor.Binding
	MaxPendingMs   int64
	AllowEvent     bool
}

// InstallSpec is the bundle of registry seed rows the composition root
// (cmd/daemon) consumes to bootstrap a channel that hosts the
// kimi-webbridge adapter.
type InstallSpec struct {
	Actor ActorSeed
	Types []TypeSeed
}

// DefaultInstallSpec returns the canonical install seeds for the
// kimi-webbridge adapter: one tool actor + 13 R/R tool types. Used by
// composition root + by tests that need a known-good registry seed.
//
// The actor id defaults to DefaultAdapterActorID; callers that supply
// a non-default Config.AdapterActorID should override Actor.ID + every
// TypeSeed.HandlerActorID via the returned spec's WithActorID method.
func DefaultInstallSpec(maxPendingMs int64) InstallSpec {
	if maxPendingMs <= 0 {
		maxPendingMs = DefaultMaxPendingMs
	}
	actorSeed := ActorSeed{
		ID:          DefaultAdapterActorID,
		Kind:        actor.KindTool,
		Binding:     Binding,
		DisplayName: "kimi-webbridge",
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
	for _, t := range EventOnlyTypes {
		types = append(types, TypeSeed{
			Type:           t,
			HandlerActorID: actorSeed.ID,
			HandlerBinding: Binding,
			MaxPendingMs:   maxPendingMs,
			AllowEvent:     true,
		})
	}
	return InstallSpec{Actor: actorSeed, Types: types}
}

// WithActorID overrides the actor id on every seed row consistently.
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
