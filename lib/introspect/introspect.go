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

// NOTE: "Is this actor serviceable for one request right now" is NOT actor.status
// — that remains the OUTCOME of send→terminal (the substrate presence-down edge
// materialises receiver_unavailable when the actor is gone). actor.status
// (QueryStatus, status.go) is the additive, pain-driven self-answer for an actor
// whose non-trivial LIVE state (e.g. a device adapter's attach online/offline
// flag) is knowable independent of any in-flight request — a真存量 the actor
// alone can report, not a trivial constant.

// DescribeRequest is the actor.describe request payload. Empty = the full
// self-answer (Describe); Type set = the single-type answer (DescribeType).
type DescribeRequest struct {
	Type string `json:"type,omitempty"`
}

// ObsPresence is the conventional actor-source obs KIND a device-bearing adapter
// PUSHes (PublishObs) to surface its external device's liveness — a best-effort,
// advisory hint, NEVER authoritative reachability (that stays send→terminal).
// Opaque to substrate/platform (守结构不守词汇); shared by the publishing adapter
// and the consuming app/view ONLY. **Absence of any value = unknown, NOT offline**
// — many devices have no liveness signal, so an adapter that cannot observe simply
// never publishes.
const ObsPresence = "presence"

// Presence is the conventional obs VALUE for ObsPresence: the adapter's best-
// effort view of whether its external device is connected right now. The third
// state — unknown — is the ABSENCE of any value (never published / decayed).
type Presence struct {
	Online bool `json:"online"`
}

// MarshalPresence encodes an online/offline edge for PublishObs.
func MarshalPresence(online bool) []byte {
	b, _ := json.Marshal(Presence{Online: online})
	return b
}

// ParsePresence decodes a folded presence snapshot; ok=false on empty/malformed.
func ParsePresence(raw []byte) (p Presence, ok bool) {
	if len(raw) == 0 || json.Unmarshal(raw, &p) != nil {
		return Presence{}, false
	}
	return p, true
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
	// Device is the actor's L3 device-presence (for a device-bearing adapter),
	// folded from its actor-source obs PUSH. nil = UNKNOWN (not a device adapter,
	// no liveness signal, or decayed) — NOT offline. Advisory only (authoritative
	// reachability is send→terminal). Distinct from Present (L2: is the cell/port
	// bound here): an actor can be Present yet its external device offline.
	Device *Presence `json:"device,omitempty"`
}

// Catalog is the actor.list response: the channel-wide directory.
type Catalog struct {
	Actors []CatalogEntry `json:"actors"`
}
