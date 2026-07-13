package subjectgate

import (
	"encoding/json"
	"errors"
	"fmt"
)

// FrameVersion is the wire envelope version every frame carries in `v`. A frame
// with a different version is refused at Unmarshal (fail-closed): the protocol
// evolves additively but a version mismatch is never silently reinterpreted.
const FrameVersion = 1

// MaxFrameBytes caps one serialized frame at 512KB (build spec §S2 envelope
// note). A larger frame is refused (connector maps to a CloseMessageTooBig).
const MaxFrameBytes = 512 * 1024

// FrameType is the closed set of frame kinds (build spec §S2 逐帧字段表). Upstream
// (client → cell) control/business + downstream (cell/gateway → client) push.
type FrameType string

const (
	// Upstream control.
	FrameAttach FrameType = "attach"
	FrameDetach FrameType = "detach"
	// Upstream business (the person's actions, driven onto the cell's own caps).
	FrameSubmit      FrameType = "submit"
	FrameResolve     FrameType = "resolve"
	FrameCancel      FrameType = "cancel"
	FrameAfter       FrameType = "after"
	FrameCancelTimer FrameType = "cancel_timer"
	FrameResource    FrameType = "resource"
	FramePresence    FrameType = "presence"
	// Downstream.
	FrameFeed    FrameType = "feed"
	FrameReceipt FrameType = "receipt"
	FrameError   FrameType = "error"
	FrameNotify  FrameType = "notify"
)

// knownFrameTypes is the closed set enforced at Unmarshal. A frame_type outside
// it is refused (unknown-frame rejection, DoD-12).
var knownFrameTypes = map[FrameType]struct{}{
	FrameAttach: {}, FrameDetach: {}, FrameSubmit: {}, FrameResolve: {},
	FrameCancel: {}, FrameAfter: {}, FrameCancelTimer: {}, FrameResource: {},
	FramePresence: {}, FrameFeed: {}, FrameReceipt: {}, FrameError: {}, FrameNotify: {},
}

// Error-code closed set (build spec 表①; 裁决8 平面词律): an error frame's `code`
// is ALWAYS a single flat word — never a nested write_rejected(code=x) form. A
// WriteRejected surfaces its harness reason verbatim as the code, so this set is
// the door's OWN vocabulary, not exhaustive of every harness reason.
const (
	CodeBadPayload         = "bad_payload"
	CodeNotMember          = "not_member"
	CodeNotInAudience      = "not_in_audience"
	CodeUnauthorizedSender = "unauthorized_sender"
	CodeAlreadyClosed      = "already_closed"
	CodeRequestNotFound    = "request_not_found"
	CodeInvalidDecision    = "invalid_decision"
	CodeUnavailable        = "unavailable"
	CodeStaleBinding       = "stale_binding"
	CodeForbidden          = "forbidden"
	CodeClosed             = "closed"
)

// ErrUnknownFrameType is Unmarshal's verdict for a frame_type outside the closed
// set. Typed so a binding can map it to its own error surface with errors.As/Is.
var ErrUnknownFrameType = errors.New("subjectgate: unknown frame_type")

// ErrFrameVersion is Unmarshal's verdict for a `v` other than FrameVersion.
var ErrFrameVersion = errors.New("subjectgate: unsupported frame version")

// ErrFrameTooBig is Marshal/Unmarshal's verdict for a serialized frame over
// MaxFrameBytes.
var ErrFrameTooBig = errors.New("subjectgate: frame exceeds size limit")

// Frame is the wire envelope every frame shares (build spec §S2). ref lives at
// the top level ONLY (裁决9): receipt/error echo it back here, never duplicated
// inside a payload. binding_gen=0 on an attach request means "binding not yet
// established" (the receipt grants the first generation).
type Frame struct {
	V          int             `json:"v"`
	Type       FrameType       `json:"frame_type"`
	BindingGen int64           `json:"binding_gen"`
	Ref        string          `json:"ref,omitempty"`
	Payload    json.RawMessage `json:"payload,omitempty"`
}

// NewFrame builds a frame with payload marshaled from v (nil → no payload).
func NewFrame(t FrameType, bindingGen int64, ref string, v any) (Frame, error) {
	f := Frame{V: FrameVersion, Type: t, BindingGen: bindingGen, Ref: ref}
	if v != nil {
		raw, err := json.Marshal(v)
		if err != nil {
			return Frame{}, fmt.Errorf("subjectgate: marshal %s payload: %w", t, err)
		}
		f.Payload = raw
	}
	return f, nil
}

// Marshal serializes a frame, enforcing the size cap.
func (f Frame) Marshal() ([]byte, error) {
	b, err := json.Marshal(f)
	if err != nil {
		return nil, err
	}
	if len(b) > MaxFrameBytes {
		return nil, ErrFrameTooBig
	}
	return b, nil
}

// DecodePayload unmarshals the frame payload into out.
func (f Frame) DecodePayload(out any) error {
	if len(f.Payload) == 0 {
		return nil
	}
	return json.Unmarshal(f.Payload, out)
}

// ParseFrame decodes and validates one wire frame: size cap, version, closed
// frame_type set. It is fail-closed on every axis.
func ParseFrame(b []byte) (Frame, error) {
	if len(b) > MaxFrameBytes {
		return Frame{}, ErrFrameTooBig
	}
	var f Frame
	if err := json.Unmarshal(b, &f); err != nil {
		return Frame{}, err
	}
	if f.V != FrameVersion {
		return Frame{}, fmt.Errorf("%w: got %d want %d", ErrFrameVersion, f.V, FrameVersion)
	}
	if _, ok := knownFrameTypes[f.Type]; !ok {
		return Frame{}, fmt.Errorf("%w: %q", ErrUnknownFrameType, f.Type)
	}
	return f, nil
}

// --- Upstream payloads (build spec §S2 逐帧字段表) -------------------------------

// AttachPayload's since is a map form day-1 (裁决7): today a single-element map
// keyed by channel_id (a multi-key payload is refused bad_payload); multi-channel
// single-connection evolution is zero frame change.
type AttachPayload struct {
	ChannelID string           `json:"channel_id"`
	Since     map[string]int64 `json:"since,omitempty"`
}

type DetachPayload struct {
	ChannelID string `json:"channel_id"`
}

type SubmitPayload struct {
	ID         string          `json:"id,omitempty"`
	MsgType    string          `json:"msg_type"`
	Kind       string          `json:"kind,omitempty"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	Audience   []string        `json:"audience,omitempty"`
	Visibility string          `json:"visibility,omitempty"`
	ParentID   string          `json:"parent_id,omitempty"`
	// ExpiresAt is the OPTIONAL declared request deadline in epoch-ms (additive,
	// v0.4.1). Absent (nil) → the harness default TTL. A control-shim submit sets an
	// explicit short TTL (clientRequestTTLMs); it only rides a request (an event has
	// no deadline — behavior.BuildRequest drops it for other kinds).
	ExpiresAt *int64 `json:"expires_at_ms,omitempty"`
}

type ResolvePayload struct {
	ReqID    string          `json:"req_id"`
	Decision string          `json:"decision"`
	Payload  json.RawMessage `json:"payload,omitempty"`
}

type CancelPayload struct {
	ReqID string `json:"req_id"`
}

type AfterPayload struct {
	DurationMs int64           `json:"duration_ms"`
	MsgType    string          `json:"msg_type"`
	Payload    json.RawMessage `json:"payload,omitempty"`
}

type CancelTimerPayload struct {
	TimerID string `json:"timer_id"`
}

// ResourceOp is the closed resource-verb enum (build spec §S2 resource row).
type ResourceOp string

const (
	ResCreate       ResourceOp = "create"
	ResRead         ResourceOp = "read"
	ResWrite        ResourceOp = "write"
	ResDelete       ResourceOp = "delete"
	ResStat         ResourceOp = "stat"
	ResList         ResourceOp = "list"
	ResShareActor   ResourceOp = "share_actor"
	ResShareMembers ResourceOp = "share_members"
)

type ResourceQuery struct {
	Prefix string `json:"prefix,omitempty"`
	Cursor string `json:"cursor,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}

type ResourcePayload struct {
	Op         ResourceOp      `json:"op"`
	ResourceID string          `json:"resource_id"`
	Args       json.RawMessage `json:"args,omitempty"`
	Target     string          `json:"target,omitempty"`
	Ops        []string        `json:"ops,omitempty"`
	Query      *ResourceQuery  `json:"query,omitempty"`
}

// PresencePayload is BOTH the upstream client→gateway hint AND the载荷 of a
// level投递 (its wire serialization). level ∈ {online, offline}; detail is an
// opaque富语义 blob the substrate never interprets.
type PresencePayload struct {
	Level   string          `json:"level"`
	Epoch   int64           `json:"epoch"`
	EdgeSeq int64           `json:"edge_seq"`
	Detail  json.RawMessage `json:"detail,omitempty"`
}

// --- Downstream payloads --------------------------------------------------

type FeedPayload struct {
	ChannelID string          `json:"channel_id"`
	Seq       int64           `json:"seq"`
	Envelope  json.RawMessage `json:"envelope"`
}

// AttachReceipt grants the first (or current) binding generation. binding_gen
// also echoes in the frame envelope; the receipt载荷 carries the granted value.
type AttachReceipt struct {
	BindingGen int64 `json:"binding_gen"`
}

// SubmitReceipt's seq is the harness write seq — NOT a feed cursor (write位≠读位,
// 契约注释钉死, build spec §S2). A client must never advance a feed cursor from it.
type SubmitReceipt struct {
	MessageID string `json:"message_id"`
	Seq       int64  `json:"seq"`
}

type ResolveReceipt struct {
	ReqID string `json:"req_id"`
}

type CancelReceipt struct {
	ReqID string `json:"req_id"`
}

type AfterReceipt struct {
	TimerID string `json:"timer_id"`
}

type CancelTimerReceipt struct {
	TimerID string `json:"timer_id"`
}

// ResourceOutcome is the resource-result form for create/read/write/delete/share.
type ResourceOutcome struct {
	Status string          `json:"status"`
	Detail string          `json:"detail,omitempty"`
	Value  json.RawMessage `json:"value,omitempty"`
}

// ResourceStat is the resource-result form for stat.
type ResourceStat struct {
	Exists bool            `json:"exists"`
	Meta   json.RawMessage `json:"meta,omitempty"`
}

// ResourcePage is the resource-result form for list.
type ResourcePage struct {
	Items []json.RawMessage `json:"items"`
	Next  string            `json:"next,omitempty"`
}

// ErrorPayload carries which frame errored (frame), a flat closed-set code
// (裁决8), and a human detail.
type ErrorPayload struct {
	Frame  string `json:"frame"`
	Code   string `json:"code"`
	Detail string `json:"detail,omitempty"`
}

type NotifyPayload struct {
	ReqID   string          `json:"req_id"`
	MsgType string          `json:"msg_type"`
	Summary json.RawMessage `json:"summary,omitempty"`
}
