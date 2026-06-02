package storespec

import (
	"context"
	"encoding/json"

	"github.com/wanpengxie/ActOS/kernel/actor"
)

// Record is the channel-local actor membership row exposed via the registry
// query API (L1 §12.2). The projection STORAGE (actor_registry table) lives
// in runtime/store.
//
// NOTE (runtime-construction-spec §1.5 / Slack membership≠readiness≠presence):
// Record carries the readiness projection inline today; the membership ↔
// readiness ↔ presence three-way split (separate column families / structs)
// is a store-internal follow-up. presence (compute lease physical online) is
// volatile and does NOT live here at all (it lives in lib/sysactor).
type Record struct {
	ID             actor.ActorID
	Kind           actor.Kind
	Binding        actor.Binding // empty for human / system
	DisplayName    string
	CreatedAt      int64
	DeregisteredAt int64 // 0 = active
	Readiness      Readiness
}

// IsActive reports whether the actor is still active (L1 §12.2).
func (r Record) IsActive() bool { return r.DeregisteredAt == 0 }

// Registry is the channel-local actor membership query contract (L1 §12.1).
// Concrete sqlite backend lives in runtime/store (ActorRegistry).
type Registry interface {
	Lookup(ctx context.Context, id actor.ActorID) (Record, bool, error)
	Exists(ctx context.Context, id actor.ActorID) (bool, error)
	ListActive(ctx context.Context) ([]Record, error)
	Insert(ctx context.Context, rec Record) error
	Deregister(ctx context.Context, id actor.ActorID, at int64) error
}

// Readiness is the readiness projection for one actor (actor's own
// serviceable state, e.g. login complete). The ReadinessState vocabulary
// closed set stays in kernel/actor; this projection VALUE + its update
// shapes are engine state and live here.
type Readiness struct {
	State             actor.ReadinessState `json:"state"`
	Reason            string               `json:"reason,omitempty"`
	Detail            json.RawMessage      `json:"detail,omitempty"`
	LastReadyAt       int64                `json:"last_ready_at,omitempty"`
	LastStateChangeAt int64                `json:"last_state_change_at,omitempty"`
}

// IsReady reports whether this projection is currently callable.
func (r Readiness) IsReady() bool { return r.State == actor.ReadinessReady }

// Normalize fills the baseline unknown readiness shape.
func (r Readiness) Normalize() Readiness {
	if r.State == "" {
		r.State = actor.ReadinessUnknown
	}
	if r.Reason == "" {
		switch r.State {
		case actor.ReadinessReady:
			r.Reason = "ok"
		default:
			r.Reason = "unknown"
		}
	}
	if len(r.Detail) == 0 {
		r.Detail = json.RawMessage(`{}`)
	} else if json.Valid(r.Detail) {
		var v any
		if err := json.Unmarshal(r.Detail, &v); err == nil {
			if raw, err := json.Marshal(v); err == nil {
				r.Detail = raw
			}
		}
	}
	return r
}

// ReadinessUpdate is the write shape accepted by readiness-aware registries.
type ReadinessUpdate struct {
	State     actor.ReadinessState
	Reason    string
	Detail    json.RawMessage
	CheckedAt int64
}

// ReadinessTransition is returned after a readiness write.
type ReadinessTransition struct {
	Previous Readiness
	Current  Readiness
	Changed  bool
}

// ReadinessUpdater is the optional extension implemented by registries that
// persist actor readiness.
type ReadinessUpdater interface {
	UpdateReadiness(ctx context.Context, id actor.ActorID, update ReadinessUpdate) (ReadinessTransition, error)
}
