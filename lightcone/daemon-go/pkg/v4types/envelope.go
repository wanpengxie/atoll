package v4types

import "encoding/json"

// Kind is the v4 message ADT classifier (event / request / response).
//
// Authoritative spec: L0 §2.1 envelope `kind` field + v4-message-definition §3.
// Once a message is written, kind is immutable (L0 §2.2 "kind 写入定死").
type Kind string

// Kind enum — closed set per L0 §3.1 invariant I7.
const (
	KindEvent    Kind = "event"
	KindRequest  Kind = "request"
	KindResponse Kind = "response"
)

// AllKinds enumerates every valid Kind value.
var AllKinds = []Kind{KindEvent, KindRequest, KindResponse}

// SenderKind is the actor physical-position classifier on `sender.kind`.
//
// Authoritative spec: L0 §2.3 — 4-value closed set.
type SenderKind string

// SenderKind enum — closed set per L0 §2.3.
const (
	SenderHuman  SenderKind = "human"
	SenderAgent  SenderKind = "agent"
	SenderSystem SenderKind = "system"
	SenderTool   SenderKind = "tool"
)

// AllSenderKinds enumerates every valid SenderKind value.
var AllSenderKinds = []SenderKind{SenderHuman, SenderAgent, SenderSystem, SenderTool}

// Visibility is the envelope `visibility` field — 3-value closed set
// covering who in the channel can query-see this message.
//
// Authoritative spec: L0 §2.4. Once written, visibility is immutable.
type Visibility string

// Visibility enum — closed set per L0 §2.4.
const (
	VisibilityPublic  Visibility = "public"
	VisibilityPrivate Visibility = "private"
	VisibilitySystem  Visibility = "system"
)

// AllVisibilities enumerates every valid Visibility value.
var AllVisibilities = []Visibility{VisibilityPublic, VisibilityPrivate, VisibilitySystem}

// Sender is the nested `sender` object inside an envelope. `Name` is
// optional per L0 §2.1 (`sender.name` ⬜); the other two fields are
// required.
type Sender struct {
	Kind SenderKind `json:"kind"`
	ID   string     `json:"id"`
	// Name is the display name; protocol allows empty string. We keep it
	// always-present in JSON (no `omitempty`) so the canonical 14-key
	// hash input (L2 §1.4.10.2) sees a stable shape.
	Name string `json:"name"`
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

	ID            string          `json:"id"`
	TS            int64           `json:"ts"`
	TSReceived    int64           `json:"ts_received,omitempty"`
	ChannelID     string          `json:"channel_id"`
	Sender        Sender          `json:"sender"`
	Kind          Kind            `json:"kind"`
	Type          string          `json:"type"`
	Payload       json.RawMessage `json:"payload"`
	ParentID      string          `json:"parent_id,omitempty"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	DocRefs       *[]string       `json:"doc_refs,omitempty"`
	Visibility    Visibility      `json:"visibility"`
	Audience      []string        `json:"audience"`
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
