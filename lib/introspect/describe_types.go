package introspect

import "encoding/json"

// TypeMeta holds per-type metadata for actor.describe responses. Every actor
// that answers actor.describe with structured type documentation uses this
// shape. Fields beyond Description are optional — an actor with minimal
// documentation fills only what it has.
type TypeMeta struct {
	Description string `json:"description"`
	// AllowedKinds lists the envelope kinds the type accepts (e.g.
	// ["request"], ["event"]). Empty = request-only by convention.
	AllowedKinds []string `json:"allowed_kinds,omitempty"`
	// MaxPendingMs is the type's wait-budget hint for callers: how long a
	// request of this type may reasonably stay pending. 0 = no hint
	// (event-only types, or the actor declines to advertise one).
	MaxPendingMs   int64           `json:"max_pending_ms,omitempty"`
	PayloadExample json.RawMessage `json:"payload_example,omitempty"`
	PayloadFields  []FieldDoc      `json:"payload_fields,omitempty"`
	ErrorCodes     []ErrorDoc      `json:"error_codes,omitempty"`
	Notes          string          `json:"notes,omitempty"`
}

// FieldDoc documents a single payload field.
type FieldDoc struct {
	Name        string `json:"name"`
	Required    bool   `json:"required,omitempty"`
	Description string `json:"description"`
	Example     any    `json:"example,omitempty"`
}

// ErrorDoc documents a known error code.
type ErrorDoc struct {
	Code        string `json:"code"`
	Description string `json:"description"`
	Recovery    string `json:"recovery,omitempty"`
}
