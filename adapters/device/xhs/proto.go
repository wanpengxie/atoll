package xhs

import (
	"encoding/json"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
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
// kernel/actor.BindingRuntimeInboundViaRelay + L1 §11.7.
const Binding = actor.BindingRuntimeInboundViaRelay

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
// Chrome extension. It rides inside the `device_transit.recv` frame's
// payload field (impl-layer2 §5.3.2 outbound — adapter → device).
//
// Field shape stays compatible with the M1.3 legacy extension wire
// (see the archived daemon-go xhs adapter; module path
// internal/adapters/xhs/device_client.go) so a rolling upgrade keeps
// the extension code unchanged on the inbound command path. The CorrelationID slot carries the request_id from
// device_transit.recv.request_id, which equals envelope.id — the
// extension echoes it back inside Callback.RequestID.
type Command struct {
	Type          string         `json:"type"`           // always "command"
	CorrelationID string         `json:"correlation_id"` // = envelope.id (carried via device_transit.recv.request_id)
	Cmd           string         `json:"cmd"`            // type-suffix (e.g. "publish")
	Params        map[string]any `json:"params"`         // domain payload, with metadata stripped
}

// CommandWireType is the constant value of Command.Type. Exported so
// tests assert it stays "command" and Manager.OnExternalCallback can
// reject unrecognized frame types defensively.
const CommandWireType = "command"

// Callback is the device-side reply. It rides inside the
// `device_transit.send` frame's payload field (impl-layer2 §5.3.1
// inbound — device → adapter). The extension's existing callback body
// carries `correlation_id` (= envelope.id); the adapter recovers the
// in-flight envelope by that id.
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

// ------------------------------------------------------------------
// TypeSchemas (R5-18): per-type allowed_kinds + payload schemas.
//
// Source of truth: domain-xhs-spec §1.1–§1.6 ("xhs Adapter Type
// Catalog"). Each schema mirrors the spec table 1:1; the framework's
// Step 6 (payload schema) + Step 4/5 (kind allow-list) consult these
// at message-write time.
//
// Notes:
//   - The framework's M1.5 schema validator (adapters/framework/
//     schema.go) only honors a subset of JSON Schema (type, required,
//     properties, items, enum). additionalProperties=false is NOT
//     enforced by the validator — the adapter boundary's per-type
//     result/error allow-lists (allowedResultKeysByType /
//     allowedErrorKeysByType above) carry the "drop unknown fields"
//     guard for outbound responses; the R4-FIX-A regression test
//     covers it.
//   - Response.status enum (closed-set per proto-layer0 §2.5) is
//     declared explicitly so a drift in adapter emit code surfaces at
//     harness Step 6.
//   - FallbackResponseSchema for every R/R type MUST accept the three
//     L2 §1.4.2 system fallback payloads ({status:failed,
//     reason:unanswered_timeout|receiver_internal_error|
//     receiver_unavailable}); framework.ValidateFallbackResponseSchema
//     asserts this at install.
// ------------------------------------------------------------------

// fallbackResponseSchema is the response-failure projection shared by
// every R/R xhs type. Lenient on properties (status / reason are the
// only spec-mandated keys for system fallback emit); the per-type
// response schema below carries the full closed-set guard.
var fallbackResponseSchema = json.RawMessage(`{
  "type": "object",
  "required": ["status", "reason"],
  "properties": {
    "status": {"type": "string", "enum": ["failed"]},
    "reason": {"type": "string"}
  }
}`)

// publishRequestSchema — domain-xhs-spec §1.1 request payload.
var publishRequestSchema = json.RawMessage(`{
  "type": "object",
  "required": ["title", "content"],
  "properties": {
    "title":   {"type": "string"},
    "content": {"type": "string"},
    "tags":    {"type": "array", "items": {"type": "string"}},
    "images":  {"type": "array", "items": {"type": "string"}}
  }
}`)

// publishResponseSchema — domain-xhs-spec §1.1 response payload.
var publishResponseSchema = json.RawMessage(`{
  "type": "object",
  "required": ["status"],
  "properties": {
    "status":      {"type": "string", "enum": ["completed", "failed"]},
    "reason":      {"type": "string"},
    "note_id":     {"type": "string"},
    "url":         {"type": "string"},
    "device_id":   {"type": "string"},
    "retry_after": {"type": "integer"},
    "error_code":  {"type": "string"}
  }
}`)

// searchRequestSchema — domain-xhs-spec §1.2 request payload.
var searchRequestSchema = json.RawMessage(`{
  "type": "object",
  "required": ["query"],
  "properties": {
    "query": {"type": "string"},
    "limit": {"type": "integer"}
  }
}`)

// searchResponseSchema — domain-xhs-spec §1.2 response payload.
var searchResponseSchema = json.RawMessage(`{
  "type": "object",
  "required": ["status"],
  "properties": {
    "status":  {"type": "string", "enum": ["completed", "failed"]},
    "reason":  {"type": "string"},
    "results": {"type": "array", "items": {"type": "object"}}
  }
}`)

// noteFetchRequestSchema — domain-xhs-spec §1.3 request payload.
var noteFetchRequestSchema = json.RawMessage(`{
  "type": "object",
  "required": ["note_id"],
  "properties": {
    "note_id": {"type": "string"}
  }
}`)

// noteFetchResponseSchema — domain-xhs-spec §1.3 response payload.
var noteFetchResponseSchema = json.RawMessage(`{
  "type": "object",
  "required": ["status"],
  "properties": {
    "status": {"type": "string", "enum": ["completed", "failed"]},
    "reason": {"type": "string"},
    "note":   {"type": "object"}
  }
}`)

// recentFetchRequestSchema — domain-xhs-spec §1.4 request payload
// (limit optional, no required keys).
var recentFetchRequestSchema = json.RawMessage(`{
  "type": "object",
  "properties": {
    "limit": {"type": "integer"}
  }
}`)

// recentFetchResponseSchema — domain-xhs-spec §1.4 response payload.
var recentFetchResponseSchema = json.RawMessage(`{
  "type": "object",
  "required": ["status"],
  "properties": {
    "status": {"type": "string", "enum": ["completed", "failed"]},
    "reason": {"type": "string"},
    "notes":  {"type": "array", "items": {"type": "object"}}
  }
}`)

// cookieSyncRequestSchema — domain-xhs-spec §1.5 request payload
// (empty properties; required = []).
var cookieSyncRequestSchema = json.RawMessage(`{
  "type": "object"
}`)

// cookieSyncResponseSchema — domain-xhs-spec §1.5 response payload.
var cookieSyncResponseSchema = json.RawMessage(`{
  "type": "object",
  "required": ["status"],
  "properties": {
    "status": {"type": "string", "enum": ["completed", "failed"]},
    "reason": {"type": "string"}
  }
}`)

// noteArchivedEventSchema — domain-xhs-spec §1.6 event payload.
// agent-emitted event-only; adapter does NOT emit but owns the type
// per impl-vocabulary §3.0 design rule 5.
var noteArchivedEventSchema = json.RawMessage(`{
  "type": "object",
  "required": ["note_id", "archive_path"],
  "properties": {
    "note_id":      {"type": "string"},
    "archive_path": {"type": "string"},
    "archived_at":  {"type": "integer"}
  }
}`)

// DeclarationTypeSchemas returns the kernel/adapter.TypeSchema map the
// Module attaches to its Declaration. Every xhs type in §1.1–§1.6
// gets an entry — adapters/framework/manager.go fails install closed
// when an adapter declares TypeSchemas but leaves a Types entry
// without a row (R5-18 fail-closed policy).
func DeclarationTypeSchemas() map[string]adapter.TypeSchema {
	return map[string]adapter.TypeSchema{
		TypePublish: {
			AllowedKinds: []message.Kind{message.KindRequest, message.KindResponse},
			SchemasByKind: map[message.Kind]json.RawMessage{
				message.KindRequest:  publishRequestSchema,
				message.KindResponse: publishResponseSchema,
			},
			FallbackResponseSchema: fallbackResponseSchema,
			TerminalConvention:     string(adapter.TerminalPayloadStatus),
		},
		TypeSearch: {
			AllowedKinds: []message.Kind{message.KindRequest, message.KindResponse},
			SchemasByKind: map[message.Kind]json.RawMessage{
				message.KindRequest:  searchRequestSchema,
				message.KindResponse: searchResponseSchema,
			},
			FallbackResponseSchema: fallbackResponseSchema,
			TerminalConvention:     string(adapter.TerminalPayloadStatus),
		},
		TypeNoteFetch: {
			AllowedKinds: []message.Kind{message.KindRequest, message.KindResponse},
			SchemasByKind: map[message.Kind]json.RawMessage{
				message.KindRequest:  noteFetchRequestSchema,
				message.KindResponse: noteFetchResponseSchema,
			},
			FallbackResponseSchema: fallbackResponseSchema,
			TerminalConvention:     string(adapter.TerminalPayloadStatus),
		},
		TypeRecentFetch: {
			AllowedKinds: []message.Kind{message.KindRequest, message.KindResponse},
			SchemasByKind: map[message.Kind]json.RawMessage{
				message.KindRequest:  recentFetchRequestSchema,
				message.KindResponse: recentFetchResponseSchema,
			},
			FallbackResponseSchema: fallbackResponseSchema,
			TerminalConvention:     string(adapter.TerminalPayloadStatus),
		},
		TypeCookieSync: {
			AllowedKinds: []message.Kind{message.KindRequest, message.KindResponse},
			SchemasByKind: map[message.Kind]json.RawMessage{
				message.KindRequest:  cookieSyncRequestSchema,
				message.KindResponse: cookieSyncResponseSchema,
			},
			FallbackResponseSchema: fallbackResponseSchema,
			TerminalConvention:     string(adapter.TerminalPayloadStatus),
		},
		TypeNoteArchived: {
			AllowedKinds: []message.Kind{message.KindEvent},
			SchemasByKind: map[message.Kind]json.RawMessage{
				message.KindEvent: noteArchivedEventSchema,
			},
			TerminalConvention: string(adapter.TerminalPayloadStatus),
		},
	}
}
