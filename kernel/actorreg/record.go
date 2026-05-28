package actorreg

import (
	"encoding/json"

	"github.com/wanpengxie/ActOS/kernel/actor"
)

// ReadinessState is the actor_registry readiness closed set used by the
// adapter framework. It is implementation state, not a protocol enum.
type ReadinessState string

const (
	ReadinessUnknown  ReadinessState = "unknown"
	ReadinessReady    ReadinessState = "ready"
	ReadinessNotReady ReadinessState = "not_ready"
)

// Readiness is the actor_registry readiness projection for one actor.
// Detail is raw JSON so sqlite-backed and in-memory registries can copy
// it without taking a dependency on adapter-specific structs.
type Readiness struct {
	State             ReadinessState  `json:"state"`
	Reason            string          `json:"reason,omitempty"`
	Detail            json.RawMessage `json:"detail,omitempty"`
	LastReadyAt       int64           `json:"last_ready_at,omitempty"`
	LastStateChangeAt int64           `json:"last_state_change_at,omitempty"`
}

// IsReady reports whether this projection is currently callable.
func (r Readiness) IsReady() bool { return r.State == ReadinessReady }

// Normalize fills the baseline unknown readiness shape.
func (r Readiness) Normalize() Readiness {
	if r.State == "" {
		r.State = ReadinessUnknown
	}
	if r.Reason == "" {
		switch r.State {
		case ReadinessReady:
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

// Record is the channel-local actor row exposed via the registry query
// API (L1 §12.2 minimum field set).
//
// `Binding` is empty string for human / system actors per L1 §12.2. The
// SQL CHECK in L2 §1.4.6 keeps the column NULL for those rows; this
// kernel-level interface uses zero-value (empty string) to mean the same.
type Record struct {
	ID             actor.ActorID
	Kind           actor.Kind
	Binding        actor.Binding // empty for human / system
	DisplayName    string        // optional; informative only (L1 §12.2 fields optional)
	CreatedAt      int64
	DeregisteredAt int64 // 0 = active; non-zero = soft-deregister timestamp
	Readiness      Readiness
}

// IsActive reports whether the actor is still active per L1 §12.2.
func (r Record) IsActive() bool { return r.DeregisteredAt == 0 }
