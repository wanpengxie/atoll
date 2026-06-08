package message

import (
	"encoding/json"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
)

// Sender is the nested `sender` object inside an envelope. It is the stable
// STRUCTURAL identity of the author: ID (stable entity reference) + Kind
// (closed-set, drives closure policy / routing). Mutable / presentation
// attributes (display name, user mapping, role) are domain — they live in the
// domain layer keyed by ID, NOT in the substrate envelope (cf. Unix inode vs
// filename; Slack user-id vs display-name; proto-v2-physical §6.1 二轴).
type Sender struct {
	Kind actor.Kind    `json:"kind"`
	ID   actor.ActorID `json:"id"`
}

// Envelope is the v4 message envelope (pure proto).
//
// It carries the content fields from L0 §2.1 (with `sender.kind/id`
// bundled into the nested Sender object).
//
// Store-derived columns (`seq`, `is_terminal`) are NOT
// part of the envelope — they are persistence-layer derived columns, not
// wire proto fields (target-state §3.7). The substrate carries no delivery/scheduling
// metadata on the message: delivery outcome is the closure terminal
// response (three reasons), delivery observability is the recipient
// cursor, and scheduling is an upstream actor concern — none of which
// needs a mutable field on the envelope.
//
// **Tri-state semantics**:
//   - ParentID / CorrelationID: empty string ("") means NULL on the wire
//     (Go zero-value naturally serializes via `omitempty`).
//   - ExpiresAt: `*int64` — nil pointer means NULL; otherwise the
//     timestamp value.
//   - Payload (`json.RawMessage`): empty / null means absent; protocol
//     baseline requires non-null (L0 §2.2 — `payload={}` legal,
//     `payload=null` not).
type Envelope struct {
	// --- content fields (L0 §2.1) -------------------------------------

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
	Visibility    Visibility      `json:"visibility"`
	Audience      Audience        `json:"audience"`
	ExpiresAt     *int64          `json:"expires_at,omitempty"`
}

// IsFinalStatus reports whether the given payload.status value belongs
// to the Layer 1 final closed set per proto-layer0 §2.5.1. The Layer 1
// set is strictly closed at {"completed","failed"}; expanding it
// requires a protocol-level revision (proto-foundation §F.closure_policy
// guards uniqueness of the final response).
//
// Provisional response statuses — both the Layer 2 core closed set
// (received / queued / processing / deferred / unavailable) and Layer 3
// business namespace extensions (`<namespace>.<name>`) — are NOT final
// and return false here. is_terminal derivation uses this helper:
//
//	is_terminal = (kind == "response" && IsFinalStatus(payload.status))
//
// per proto-layer0 §2.5.1 and proto-foundation §1.6.3.
func IsFinalStatus(status string) bool {
	return status == "completed" || status == "failed"
}

// contentFields lists the envelope field names from L0 §2.1 (with sender
// flattened into the 2 dotted keys sender.{kind,id} — sender carries
// structural identity only).
//
// Unexported: it is the in-package field-set guard's reference list (a
// protocol closed set; an exported mutable slice would let an importer
// rewrite it). The envelope_test.go drift guard keeps the Go struct 1:1
// with it; drift on either side trips the test.
var contentFields = []string{
	"id",
	"ts",
	"ts_received",
	"channel_id",
	"sender.kind",
	"sender.id",
	"kind",
	"type",
	"payload",
	"parent_id",
	"correlation_id",
	"visibility",
	"audience",
	"expires_at",
}
