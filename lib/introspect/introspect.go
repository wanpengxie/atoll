package introspect

import "encoding/json"

// The reserved introspection queries — the standard questions any actor / the
// channel answers about itself.
const (
	// QueryDescribe — what can this actor do (its live API surface). The
	// request payload is a DescribeRequest: empty for the full self-answer,
	// or with Type set for a single-type answer.
	QueryDescribe = "actor.describe"
	// QueryList — who is in this channel: the membership ∧ presence directory.
	// This is the AUTHORITATIVE definition of the formula (durable registry
	// membership composed with volatile presence); other sites reference it
	// rather than restating it.
	QueryList = "actor.list"
)

// NOTE: there is no actor.status query. "Is this actor serviceable right now"
// is not a queryable 存量 — it is the OUTCOME of send→terminal (the substrate
// presence-down edge materialises receiver_unavailable when the actor is gone). A
// status query could only answer a trivial constant available=true, which
// carries no truth — a half-built slice that misleads later readers. When a
// concrete actor has non-trivial domain state worth surfacing proactively
// (e.g. an actor with non-trivial login state), an optional status self-answer
// is added additively — pain-driven, not pre-built.

// DescribeRequest is the actor.describe request payload. Empty = the full
// self-answer (Describe); Type set = the single-type answer (DescribeType).
type DescribeRequest struct {
	Type string `json:"type,omitempty"`
}

// Describe is the full actor.describe answer: the actor's identity plus its
// live capability surface. The actor is the sole authority on its own
// capability; a caller discovers it by asking the actor, live.
//
// Kind and binding are deliberately ABSENT: they are registry truth (see
// CatalogEntry via actor.list), not capability — a self-answer restating
// registry facts can only drift from them.
type Describe struct {
	// ActorID is the actor's registry id (e.g. "device:laptop").
	ActorID string `json:"actor_id"`
	// Description is the one-line actor positioning.
	Description string `json:"description"`
	// SkillDoc is the markdown usage guide (workflows, error handling).
	SkillDoc string `json:"skill_doc,omitempty"`
	// Types documents every request type the actor serves.
	Types map[string]TypeMeta `json:"types,omitempty"`
}

// DescribeType is the single-type actor.describe answer (selector form):
// one type's metadata, inlined alongside the identifying pair.
type DescribeType struct {
	ActorID string `json:"actor_id"`
	Type    string `json:"type"`
	TypeMeta
}

// AnswerDescribe resolves an actor.describe request against the actor's full
// Describe. Empty selector → the full answer. A known type selector → the
// single-type answer. An unknown type → ok=false (the actor fails the request
// with its own error convention). This is the ONE standard dispatch every
// actor routes through, so the answer shape never drifts from the convention.
func AnswerDescribe(d Describe, req DescribeRequest) (any, bool) {
	if req.Type == "" {
		return d, true
	}
	meta, ok := d.Types[req.Type]
	if !ok {
		return nil, false
	}
	return DescribeType{ActorID: d.ActorID, Type: req.Type, TypeMeta: meta}, true
}

// ParseDescribeRequest decodes an actor.describe request payload. A nil/empty
// payload is the full-answer request.
func ParseDescribeRequest(payload []byte) (DescribeRequest, error) {
	var req DescribeRequest
	if len(payload) == 0 {
		return req, nil
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		return DescribeRequest{}, err
	}
	return req, nil
}

// CatalogEntry is one row of the actor.list channel directory: membership
// (registry truth) ∧ presence (volatile, read from the substrate's authoritative
// obs). No readiness axis — whether an actor can service a request is the OUTCOME
// of send→terminal, not a field here. No capability axis either — types and
// payload docs are the actor's own self-answer (actor.describe), not directory
// rows.
type CatalogEntry struct {
	ID      string `json:"id"`
	Kind    string `json:"kind"`
	Binding string `json:"binding,omitempty"`
	Present bool   `json:"present"`
	// UptimeMs is the elapsed time since the substrate bound the live instance
	// (now - StartedAt), derived by the system actor from the substrate's
	// authoritative bind-instant. 0 when not present. Substrate-owned obs (the
	// actor never self-reports it).
	UptimeMs int64 `json:"uptime_ms,omitempty"`
}

// Catalog is the actor.list response: the channel-wide directory.
type Catalog struct {
	Actors []CatalogEntry `json:"actors"`
}
