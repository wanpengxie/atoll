package message

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"

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

// ReservedTypePrefix is the substrate-authoritative message-type namespace
// (proto-layer1 §2.5): a Type under this prefix may only be authored by the
// substrate itself (concrete reserved names live in protocol/actor; the
// PREFIX concept is protocol vocabulary and lives here, its one home). Both
// enforcement halves reference this symbol — the harness reserved-type step
// (the authoritative gate) and the schedule ingress guard (which refuses to
// even accept a reserved-type timer, so a rule change can never strand a
// durable row that fires into a permanent reject) — so the two halves cannot
// drift apart on what "reserved" means.
const ReservedTypePrefix = "system."

// ---------------------------------------------------------------------
// Wire field-set closure (L0 §7.3) — enforced by the TYPE, not by callers.
// ---------------------------------------------------------------------

// UnknownFieldError is the L0 §7.3 fail-closed verdict: the wire JSON carried
// one or more top-level keys outside the envelope's closed field set. Typed
// so a binding can map it onto its own error surface (HTTP 400, an ipc frame
// error) with errors.As.
type UnknownFieldError struct {
	// Keys are the offending top-level keys, sorted (deterministic across the
	// randomized map iteration).
	Keys []string
}

func (e UnknownFieldError) Error() string {
	return "message: envelope top-level field not in spec: " + strings.Join(e.Keys, ", ")
}

// envelopeTopLevelKeys is the closed set of wire keys, derived from the
// Envelope struct's json tags at init — the struct IS the schema, so the set
// can never drift from it (no second hand-maintained list). Store-derived
// columns (seq, is_terminal) are not struct fields, hence rejected; welded /
// substrate-injected fields (sender.id, channel_id, ts_received) ARE legal
// keys here — lying in them is separately made impossible by the write gate
// (the pen fail-fasts pre-stuffed identity; the engine unconditionally
// overwrites ts_received), while the read/deliver path legitimately carries
// them populated.
var envelopeTopLevelKeys = func() map[string]bool {
	keys := map[string]bool{}
	t := reflect.TypeOf(Envelope{})
	for i := 0; i < t.NumField(); i++ {
		name, _, _ := strings.Cut(t.Field(i).Tag.Get("json"), ",")
		if name != "" && name != "-" {
			keys[name] = true
		}
	}
	return keys
}()

// UnmarshalJSON decodes an envelope fail-closed: a top-level key outside the
// struct's field set rejects with UnknownFieldError BEFORE any field is
// decoded. This is L0 §7.3 riding the type itself — every wire entrance that
// decodes an envelope (the HTTP API, the ipc frame codec, any future binding)
// enforces it by construction, with no per-binding plumbing to forget.
// (It replaced the harness's CtxWithRawEnvelope injection seam, whose
// "callers MUST plumb the raw JSON" obligation lived only in a comment and
// was wired by no binding.) In-process Go construction is untouched — a
// struct literal cannot carry an unknown field in the first place.
//
// Scope is deliberately top-level only, faithful to §7.3: unknown keys inside
// nested objects (sender, payload) are the nested vocabulary's concern —
// payload is opaque by axiom and never inspected here.
func (e *Envelope) UnmarshalJSON(data []byte) error {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(data, &top); err != nil {
		return err
	}
	var unknown []string
	for k := range top {
		if !envelopeTopLevelKeys[k] {
			unknown = append(unknown, k)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return UnknownFieldError{Keys: unknown}
	}
	// plain drops the method set, so the inner decode cannot recurse into
	// this UnmarshalJSON.
	type plain Envelope
	var p plain
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	*e = Envelope(p)
	return nil
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
	return status == StatusCompleted || status == StatusFailed
}

// Layer 1 final status closed set — the two wire words per proto-layer0
// §2.5.1. IsFinalStatus above is the membership predicate (the judgment
// face); these consts are the ONE literal home (the vocabulary face).
// Production code MUST reference them instead of bare "completed"/"failed"
// strings; wire/JSON fixtures keep the literal form.
const (
	StatusCompleted = "completed"
	StatusFailed    = "failed"
)

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
