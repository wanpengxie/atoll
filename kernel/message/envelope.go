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
// 14-key hash input (L2 §1.4.10.2) sees a stable shape.
type Sender struct {
	Kind actor.Kind    `json:"kind"`
	ID   actor.ActorID `json:"id"`
	Name string        `json:"name"`
}

// Envelope is the v4 message envelope.
//
// It carries:
//   - the 17 content fields from L0 §2.1 (with `sender.kind/id/name`
//     bundled into the nested Sender object)
//   - the 4 delivery-metadata fields from L0 §2.5
//   - the 2 store-derived columns (`is_terminal`, `seq`) defined by L2
//     §1.4.1 — these never travel inside an in-flight envelope but live
//     alongside the row in the channel-local messages table
//
// Only the content fields (minus `ts_received`) feed CanonicalHash;
// store-derived fields are excluded by L1 §10.2.2.
//
// **Tri-state semantics**:
//   - ParentID / CorrelationID: empty string ("") means NULL on the wire
//     (Go zero-value naturally serializes via `omitempty`).
//   - DocRefs (`*[]string`): nil pointer means NULL; pointer to empty
//     slice means explicit `[]` (L0 §2.1 "doc_refs 三态").
//   - NotBefore / ExpiresAt: `*int64` — nil pointer means NULL; otherwise
//     the timestamp value.
//   - Payload (`json.RawMessage`): empty / null means absent; protocol
//     baseline requires non-null (L0 §2.2 — `payload={}` legal,
//     `payload=null` not).
type Envelope struct {
	// --- 17 content fields (L0 §2.1) ----------------------------------

	ID            ID              `json:"id"`
	TS            int64           `json:"ts"`
	TSReceived    int64           `json:"ts_received,omitempty"`
	ChannelID     channel.ID      `json:"channel_id"`
	Sender        Sender          `json:"sender"`
	Kind          Kind            `json:"kind"`
	Type          string          `json:"type"`
	Payload       json.RawMessage `json:"payload"`
	ParentID      ID              `json:"parent_id,omitempty"`
	CorrelationID ID              `json:"correlation_id,omitempty"`
	DocRefs       *[]string       `json:"doc_refs,omitempty"`
	Visibility    Visibility      `json:"visibility"`
	Audience      Audience        `json:"audience"`
	NotBefore     *int64          `json:"not_before,omitempty"`
	ExpiresAt     *int64          `json:"expires_at,omitempty"`

	// --- 4 delivery metadata (L0 §2.5) --------------------------------

	DeliveredAt      *int64 `json:"delivered_at,omitempty"`
	DeliveryFailedAt *int64 `json:"delivery_failed_at,omitempty"`
	LastError        string `json:"last_error,omitempty"`
	Attempts         int64  `json:"attempts,omitempty"`

	// --- store-derived (L2 §1.4.1) ------------------------------------

	IsTerminal bool  `json:"is_terminal,omitempty"`
	Seq        int64 `json:"seq,omitempty"`
}

// HashInputFields lists the 14 top-level keys (in alphabetical order)
// that feed CanonicalHash per L2 §1.4.10.2.
//
// Exported so tests + callers in T7 (harness step 0.5 / step 8) can
// reference one source of truth instead of duplicating the list.
var HashInputFields = []string{
	"audience",
	"channel_id",
	"correlation_id",
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

// ContentFields lists the 17 envelope content field names from L0 §2.1
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
	"visibility",
	"audience",
	"not_before",
	"expires_at",
}

// DeliveryMetadataFields lists the 4 delivery-metadata field names from
// L0 §2.5 — written by system, not part of envelope content.
var DeliveryMetadataFields = []string{
	"delivered_at",
	"delivery_failed_at",
	"last_error",
	"attempts",
}

// StoreDerivedFields lists the 2 store-derived column names from L2
// §1.4.1 — populated by the daemon store (harness step 8 / engine).
var StoreDerivedFields = []string{
	"is_terminal",
	"seq",
}
