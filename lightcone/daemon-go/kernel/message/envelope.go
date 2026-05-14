package message

import "encoding/json"

// Envelope is the v4 message envelope.
//
// Spec source of truth: L0 §2.1 (17 content fields) + L0 §2.5 (4
// delivery metadata) + L2 §1.4.1 (2 store-derived columns
// `is_terminal` / `seq`).
//
// Tri-state semantics:
//
//   - ParentID / CorrelationID: empty string ("") means NULL on the
//     wire (Go zero-value naturally serializes via `omitempty`).
//   - DocRefs (*[]string): nil pointer means NULL; pointer to empty
//     slice means explicit `[]` (L0 §2.1 "doc_refs 三态").
//   - NotBefore / ExpiresAt: *int64; nil means NULL.
//   - Payload (json.RawMessage): empty / null means absent; L0 §2.2
//     baseline requires non-null (`payload={}` legal,
//     `payload=null` not).
//
// Only the 14 content fields (minus ts_received) feed
// kernel/message.CanonicalHash; store-derived fields are excluded by L1
// §10.2.2.
//
// TODO(T1): finalize migration from pkg/v4types.Envelope (this stub
// matches the existing field set so callers can land via a type alias).
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

// Sender mirrors actor.Sender at the envelope level. kernel/message
// keeps a local copy to avoid a cross-package cycle when packages
// outside kernel re-export Envelope as a single type. The two structs
// are JSON-compatible.
//
// TODO(T1): unify with kernel/actor.Sender (single source of truth) via
// type alias once T1 settles the package boundary.
type Sender struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
	Name string `json:"name"`
}

// HashInputFields lists the 14 top-level keys (alphabetical) that feed
// CanonicalHash per L2 §1.4.10.2.
//
// Mirrors pkg/v4types.HashInputFields; T1 will collapse the two
// sources.
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
