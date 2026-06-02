package message

import (
	"encoding/json"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
)

// Sender is the nested `sender` object inside an envelope. `Name` is
// optional per L0 §2.1 (`sender.name` ⬜); the other two fields are
// required.
//
// `Name` is kept always-present in JSON (no `omitempty`) so the canonical
// hash input (L2 §1.4.10.2) sees a stable shape.
type Sender struct {
	Kind actor.Kind    `json:"kind"`
	ID   actor.ActorID `json:"id"`
	Name string        `json:"name"`
}

// CrossChannelRef is a weak pointer to a message in another channel.
// It is informative only: it does not route, grant ACLs, or create a
// shared collaboration scope.
type CrossChannelRef struct {
	ChannelID channel.ID `json:"channel_id"`
	MessageID ID         `json:"message_id"`
	Note      *string    `json:"note"`
}

// Envelope is the v4 message envelope (pure proto).
//
// It carries:
//   - the content fields from L0 §2.1 (with `sender.kind/id/name`
//     bundled into the nested Sender object)
//   - the delivery-metadata fields from L0 §2.5
//
// Store-derived columns (`seq`, `is_terminal`, `canonical_hash`) and
// runtime scheduling diagnostics (`delivery_failed_at`, `attempts`) are
// NOT part of the envelope — they live on the runtime/store row that wraps
// it (target-state §3.7). Only the content fields (minus `ts_received`)
// feed CanonicalHash.
//
// **Tri-state semantics**:
//   - ParentID / CorrelationID: empty string ("") means NULL on the wire
//     (Go zero-value naturally serializes via `omitempty`).
//   - DocRefs (`*[]string`): nil pointer means NULL; pointer to empty
//     slice means explicit `[]` (L0 §2.1 "doc_refs 三态").
//   - CrossChannelRefs (`*[]CrossChannelRef`): nil pointer means NULL;
//     pointer to empty slice means explicit `[]`.
//   - NotBefore / ExpiresAt: `*int64` — nil pointer means NULL; otherwise
//     the timestamp value.
//   - Payload (`json.RawMessage`): empty / null means absent; protocol
//     baseline requires non-null (L0 §2.2 — `payload={}` legal,
//     `payload=null` not).
type Envelope struct {
	// --- 17 content fields (L0 §2.1) ----------------------------------

	ID               ID                 `json:"id"`
	TS               int64              `json:"ts"`
	TSReceived       int64              `json:"ts_received,omitempty"`
	ChannelID        channel.ID         `json:"channel_id"`
	Sender           Sender             `json:"sender"`
	Kind             Kind               `json:"kind"`
	Type             string             `json:"type"`
	Payload          json.RawMessage    `json:"payload"`
	ParentID         ID                 `json:"parent_id,omitempty"`
	CorrelationID    ID                 `json:"correlation_id,omitempty"`
	DocRefs          *[]string          `json:"doc_refs,omitempty"`
	CrossChannelRefs *[]CrossChannelRef `json:"cross_channel_refs,omitempty"`
	Visibility       Visibility         `json:"visibility"`
	Audience         Audience           `json:"audience"`
	NotBefore        *int64             `json:"not_before,omitempty"`
	ExpiresAt        *int64             `json:"expires_at,omitempty"`

	// --- delivery metadata (L0 §2.5) ----------------------------------

	DeliveredAt *int64 `json:"delivered_at,omitempty"`
	LastError   string `json:"last_error,omitempty"`
}

// IsFinalStatus reports whether the given payload.status value belongs
// to the Layer 1 final closed set per proto-layer0 §2.5.1. The Layer 1
// set is strictly closed at {"completed","failed"}; expanding it
// requires a protocol-level revision (proto-foundation §F.closure_policy
// guards uniqueness of the final response).
//
// Provisional response statuses — both the Layer 2 core closed set
// (received / queued / processing / deferred / unavailable) and Layer 3
// business namespace extensions (`<adapter>.<name>`) — are NOT final
// and return false here. is_terminal derivation uses this helper:
//
//	is_terminal = (kind == "response" && IsFinalStatus(payload.status))
//
// per proto-layer0 §2.5.1 and proto-foundation §1.6.3.
func IsFinalStatus(status string) bool {
	return status == "completed" || status == "failed"
}

// HashInputFields lists the top-level keys (in alphabetical order)
// that feed CanonicalHash per L2 §1.4.10.2.
//
// Exported so tests + callers in T7 (harness step 0.5 / step 8) can
// reference one source of truth instead of duplicating the list.
var HashInputFields = []string{
	"audience",
	"channel_id",
	"correlation_id",
	"cross_channel_refs",
	"doc_refs",
	"expires_at",
	"id",
	"kind",
	"not_before",
	"parent_id",
	"payload",
	"sender",
	"ts",
	"type",
	"visibility",
}

// ContentFields lists the envelope content field names from L0 §2.1
// (with sender.{kind,id,name} flattened into 3 dotted keys).
//
// Used by the envelope_test.go field-set guard so the Go struct stays
// 1:1 with the spec table; drift on either side trips the test.
var ContentFields = []string{
	"id",
	"ts",
	"ts_received",
	"channel_id",
	"sender.kind",
	"sender.id",
	"sender.name",
	"kind",
	"type",
	"payload",
	"parent_id",
	"correlation_id",
	"doc_refs",
	"cross_channel_refs",
	"visibility",
	"audience",
	"not_before",
	"expires_at",
}

// DeliveryMetadataFields lists delivery-metadata field names from L0
// §2.5 — written by runtime, not part of envelope content.
var DeliveryMetadataFields = []string{
	"delivered_at",
	"last_error",
}
