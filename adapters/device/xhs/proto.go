package xhs

import (
	"github.com/wanpengxie/ActOS/kernel/actor"
)

// AdapterName is the framework module name; lookup key the daemon uses
// to route Manager.OnExternalCallback to this Module.
const AdapterName = "xhs"

// DefaultAdapterActorID is the canonical actor_registry id this adapter
// owns. v4-message-definition §1.2.5 + L4 §2.1 mandate sender.id =
// tool:xhs-adapter on every adapter-emitted response.
//
// Concrete deployments MAY override the value via Config.AdapterActorID
// if they run multiple xhs adapter instances in a channel (none today;
// extension reserved).
const DefaultAdapterActorID actor.ActorID = "tool:xhs-adapter"

// Binding is the M1.5 closed-enum value this adapter declares. See
// kernel/actor.BindingViaServerTransit + L1 §11.7.
const Binding = actor.BindingViaServerTransit

// DefaultMaxPendingMs mirrors the M1.3 xhs baseline (5 min). Large
// enough to absorb Chrome extension throttling; short enough that a
// hanging request surfaces a F3 default_timeout terminal within human
// attention span.
const DefaultMaxPendingMs int64 = 5 * 60 * 1000

// Type names — closed set per L4 §2.1.
const (
	TypePublish      = "xhs.publish"
	TypeSearch       = "xhs.search"
	TypeNoteFetch    = "xhs.note.fetch"
	TypeRecentFetch  = "xhs.recent.fetch"
	TypeCookieSync   = "xhs.cookie.sync"
	TypeNoteArchived = "xhs.note.archived" // event-only (extension push)
)

// RequestResponseTypes is the subset that travels request → response.
// Used by Declares() to populate timer registrations only for types the
// framework actually awaits a reply on.
var RequestResponseTypes = []string{
	TypePublish,
	TypeSearch,
	TypeNoteFetch,
	TypeRecentFetch,
	TypeCookieSync,
}

// AllTypes is the full closed set Declares() exposes, including the
// event-only TypeNoteArchived row so type_registry consistency holds
// ("adapter owns the actor → adapter declares every type that
// references the actor as handler").
var AllTypes = append(append([]string{}, RequestResponseTypes...), TypeNoteArchived)

// Command is the outbound wire frame the xhs adapter pushes to the
// Chrome extension. It rides inside the device_transit.send frame's
// payload field (T1.3 frame set).
//
// Field shape stays compatible with the M1.3 legacy extension wire
// (see the archived daemon-go xhs adapter; module path
// internal/adapters/xhs/device_client.go) so a rolling upgrade keeps
// the extension code unchanged on the inbound command path. The CorrelationID slot carries the request_id from
// device_transit.send.request_id, which equals envelope.id — the
// extension echoes it back inside Callback.RequestID.
type Command struct {
	Type          string         `json:"type"`           // always "command"
	CorrelationID string         `json:"correlation_id"` // = envelope.id (carried via device_transit.send.request_id)
	Cmd           string         `json:"cmd"`            // type-suffix (e.g. "publish")
	Params        map[string]any `json:"params"`         // domain payload, with metadata stripped
}

// CommandWireType is the constant value of Command.Type. Exported so
// tests assert it stays "command" and Manager.OnExternalCallback can
// reject unrecognized frame types defensively.
const CommandWireType = "command"

// Callback is the device-side reply. It rides inside the
// device_transit.recv frame's payload field. The extension's existing
// callback body carries `correlation_id` (= envelope.id); the adapter
// recovers the in-flight envelope by that id.
type Callback struct {
	CorrelationID string         `json:"correlation_id"`
	DeviceID      string         `json:"device_id,omitempty"`
	Status        string         `json:"status"` // "ok" | "error" (+ legacy synonyms)
	Result        map[string]any `json:"result,omitempty"`
	ErrorObj      map[string]any `json:"error,omitempty"`
}

// allowedResultKeysByType is the per-type result allow-list. Source:
// L4 §2.2 response schemas; each entry lists every field the schema
// declares beyond `status` / `reason` (which the adapter sets directly
// via RespondOptions). Stowaway keys outside this set are dropped at
// the adapter boundary — that is the R4-FIX-A regression guard (the
// M1.3 union allow-list let cross-type fields slip through and trip
// harness Step 6 silently, surfacing as F3 timeouts).
//
// Origins per spec:
//
//   - xhs.publish        -> note_id, url, device_id, retry_after
//   - xhs.search         -> results
//   - xhs.note.fetch     -> note
//   - xhs.recent.fetch   -> notes
//   - xhs.cookie.sync    -> (none beyond status/reason)
var allowedResultKeysByType = map[string]map[string]struct{}{
	TypePublish: {
		"note_id":     {},
		"url":         {},
		"device_id":   {},
		"retry_after": {},
	},
	TypeSearch: {
		"results": {},
	},
	TypeNoteFetch: {
		"note": {},
	},
	TypeRecentFetch: {
		"notes": {},
	},
	TypeCookieSync: {},
}

// allowedErrorKeysByType is the failure-path per-type allow-list for
// callback error objects. `reason` flows through RespondOptions.Reason
// as a closed terminal_failure_reason, while the callback's original
// reason/code is preserved as payload.error_code. `retry_after` is
// declared only on xhs.publish's failed schema; device_id is also only
// on xhs.publish's response schema.
var allowedErrorKeysByType = map[string]map[string]struct{}{
	TypePublish: {
		"retry_after": {},
		"device_id":   {},
	},
	TypeSearch:      {},
	TypeNoteFetch:   {},
	TypeRecentFetch: {},
	TypeCookieSync:  {},
}

// resultAllowListFor returns the per-type result allow-list, or the
// empty set when the type is unknown / blank (most-restrictive default,
// so a future closed-set drift fails closed).
func resultAllowListFor(requestType string) map[string]struct{} {
	if v, ok := allowedResultKeysByType[requestType]; ok {
		return v
	}
	return map[string]struct{}{}
}

// errorAllowListFor returns the per-type error allow-list.
func errorAllowListFor(requestType string) map[string]struct{} {
	if v, ok := allowedErrorKeysByType[requestType]; ok {
		return v
	}
	return map[string]struct{}{}
}
