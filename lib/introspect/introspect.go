package introspect

import (
	"encoding/json"
)

// The reserved introspection queries — the standard questions any actor / the
// channel answers about itself.
const (
	// QueryDescribe — what can this actor do (its live API surface). The
	// request payload is a DescribeRequest: empty for the full self-answer,
	// or with Type set for a single-type answer.
	QueryDescribe = "actor.describe"
)

// Member presence is advisory, not a serviceability promise.
// Serviceability remains the OUTCOME of send→terminal.

type StatusRequest struct {
	Member string `json:"member"`
}

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
// and the consuming view ONLY. **Absence of any value = unknown, NOT offline**
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

// ParseDevicePresence decodes a folded device-presence snapshot; ok is true
// only when the document carries an explicit boolean online field.
func ParseDevicePresence(raw []byte) (p DevicePresence, ok bool) {
	var wire struct {
		Online *bool `json:"online"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &wire) != nil || wire.Online == nil {
		return DevicePresence{}, false
	}
	return DevicePresence{Online: *wire.Online}, true
}

// Describe is the full actor.describe answer: the actor's identity plus its
// live capability surface. The actor is the sole authority on its own
// capability; a caller discovers it by asking the actor, live.
//
// Kind and binding are deliberately ABSENT: they are registry truth (see
// CatalogEntry via the member directory), not capability — a self-answer restating
// registry facts can only drift from them.
type Describe struct {
	Class        string              `json:"class"`
	Interfaces   []string            `json:"interfaces"`
	Capabilities map[string]bool     `json:"capabilities"`
	Words        map[string]WordSpec `json:"words"`
}

// DescribeType is the single-type actor.describe answer (selector form):
// one type's metadata, inlined alongside the identifying pair.
type DescribeType struct {
	Class string `json:"class"`
	Type  string `json:"type"`
	WordSpec
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
	meta, ok := d.Words[req.Type]
	if !ok {
		return nil, false
	}
	d.Words = map[string]WordSpec{req.Type: meta}
	return d, true
}

// CatalogEntry is one row of the channel member directory: membership
// (registry truth) ∧ liveness (volatile, read from the substrate's authoritative
// obs). Name and Description are declaration facts supplied by the introducer,
// not a restatement of the actor's self-description. No readiness axis —
// whether an actor can service a request is the OUTCOME of send→terminal, not a
// field here. No capability axis either — types and payload docs are the actor's
// own self-answer (actor.describe), not directory rows.
type CatalogEntry struct {
	ID          string `json:"id"`
	Kind        string `json:"kind"`
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
	Present     bool   `json:"present"`
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

// Catalog is the member-list response: the channel-wide directory.
type Catalog struct {
	Actors []CatalogEntry `json:"actors"`
}
