package introspect

import "encoding/json"

// The reserved introspection queries — the standard questions any actor / the
// channel answers about itself.
const (
	// QueryDescribe — what can this actor do (its live API surface). The
	// request payload is a DescribeRequest: empty for the full self-answer,
	// or with Type set for a single-type answer.
	QueryDescribe = "actor.describe"
	// QueryList — who is in this channel: the membership ∧ liveness directory.
	// This is the AUTHORITATIVE definition of the formula (durable registry
	// membership composed with volatile liveness); other sites reference it
	// rather than restating it.
	QueryList = "actor.list"
	// QueryStatus returns the system actor's read-time presence view for one id.
	QueryStatus = "actor.status"
)

// NOTE: actor.status is advisory presence, not a serviceability promise.
// Serviceability remains the OUTCOME of send→terminal.

type StatusRequest struct {
	ActorID string `json:"actor_id"`
}

func ParseStatusRequest(payload []byte) (StatusRequest, error) {
	var req StatusRequest
	err := json.Unmarshal(payload, &req)
	if err == nil && req.ActorID == "" {
		err = errMissingActorID
	}
	return req, err
}

type statusRequestError string

func (e statusRequestError) Error() string { return string(e) }

const errMissingActorID statusRequestError = "actor_id required"

type StatusTestimony struct {
	ReceivedAt         int64           `json:"received_at"`
	StaleFromPriorLife bool            `json:"stale_from_prior_life,omitempty"`
	Device             *DevicePresence `json:"device,omitempty"`
	ValueBase64        string          `json:"value_b64,omitempty"`
}

type Status struct {
	ActorID  string                     `json:"actor_id"`
	Member   bool                       `json:"member"`
	Present  bool                       `json:"present"`
	UptimeMs int64                      `json:"uptime_ms,omitempty"`
	L3       map[string]StatusTestimony `json:"l3,omitempty"`
}

// DescribeRequest is the actor.describe request payload. Empty = the full
// self-answer (Describe); Type set = the single-type answer (DescribeType).
type DescribeRequest struct {
	Type string `json:"type,omitempty"`
}

// ObsDevicePresence is the conventional actor-source obs KIND a device-bearing adapter
// PUSHes (PublishObs) to surface its external device's liveness — a best-effort,
// advisory hint, NEVER authoritative reachability (that stays send→terminal).
// Opaque to substrate/platform (they enforce structure, not vocabulary); shared by the publishing adapter
// and the consuming app/view ONLY. **Absence of any value = unknown, NOT offline**
// — many devices have no liveness signal, so an adapter that cannot observe simply
// never publishes.
const ObsDevicePresence = "device_presence"

// DevicePresence is the conventional obs VALUE for ObsDevicePresence: the adapter's best-
// effort view of whether its external device is connected right now. The third
// state — unknown — is the ABSENCE of any value (never published / decayed).
type DevicePresence struct {
	Online bool `json:"online"`
}

// MarshalDevicePresence encodes an online/offline edge for PublishObs.
func MarshalDevicePresence(online bool) []byte {
	b, _ := json.Marshal(DevicePresence{Online: online})
	return b
}

// ParseDevicePresence decodes a folded device-presence snapshot; ok=false on empty/malformed.
func ParseDevicePresence(raw []byte) (p DevicePresence, ok bool) {
	if len(raw) == 0 || json.Unmarshal(raw, &p) != nil {
		return DevicePresence{}, false
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
// (registry truth) ∧ liveness (volatile, read from the substrate's authoritative
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
	Device *DevicePresence `json:"device,omitempty"`
}

// Catalog is the actor.list response: the channel-wide directory.
type Catalog struct {
	Actors []CatalogEntry `json:"actors"`
}
