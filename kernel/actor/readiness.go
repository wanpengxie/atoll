package actor

import "encoding/json"

// ReadinessState is the actor readiness closed set. It is implementation
// state (a projection vocabulary), not a protocol enum — kept in kernel/actor
// because both the projection store (runtime/store) and the adapter behaviour
// (lib/behavior) reference its values, and neither may depend on the other.
type ReadinessState string

const (
	ReadinessUnknown  ReadinessState = "unknown"
	ReadinessReady    ReadinessState = "ready"
	ReadinessNotReady ReadinessState = "not_ready"
)

// Readiness is the readiness projection for one actor. Detail is raw JSON so
// sqlite-backed and in-memory registries copy it without depending on
// adapter-specific structs.
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

// ReadinessUpdate is the write shape accepted by readiness-aware registries.
// CheckedAt is ms epoch; callers stamp it from their local clock.
type ReadinessUpdate struct {
	State     ReadinessState
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
