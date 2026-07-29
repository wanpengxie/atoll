package subjectgate

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// FrameVersion is the wire envelope version every frame carries in `v`. A frame
// with a different version is refused at Unmarshal (fail-closed): a version
// mismatch is never silently reinterpreted. v2 (连接模型勘误期): the connection
// is channel-blind (attach drops channel_id, business frames carry a required
// channel_id, binding_gen is gone) — a breaking change from v1, so v1 frames are
// refused; the only deployment form is the same-repo atoll-web switching in the
// same batch (atomic deploy).
const FrameVersion = 2

// MaxFrameBytes caps one serialized frame at 512KB (build spec §S2 envelope
// note). A larger frame is refused (connector maps to a CloseMessageTooBig).
const MaxFrameBytes = 512 * 1024

// FrameType is the closed set of frame kinds (build spec §S2 逐帧字段表). Upstream
// (client → cell) control/business + downstream (cell/gateway → client) push.
type FrameType string

const (
	// Upstream control.
	FrameAttach FrameType = "attach"
	// Upstream business (the person's actions, driven onto the cell's own caps).
	FrameSubmit      FrameType = "submit"
	FrameResolve     FrameType = "resolve"
	FrameCancel      FrameType = "cancel"
	FrameAfter       FrameType = "after"
	FrameCancelTimer FrameType = "cancel_timer"
	FrameResource    FrameType = "resource"
	// Downstream.
	FrameFeed    FrameType = "feed"
	FrameReceipt FrameType = "receipt"
	FrameError   FrameType = "error"
)

// Retired words (purity v3 C1/C2 — minted by the spec's frame table for
// closed-set completeness, but with ZERO producers ever wired; a word enters
// the closed set only WITH its producer, 词表第四问):
//   - "presence" (+PresencePayload{Level, Epoch, EdgeSeq, Detail}): presence's
//     real path is the gateway session ledger writing the slot DIRECTLY
//     (same process) — it never crosses this wire. If a remote connector ever
//     needs presence over the wire, re-add as ONE vertical slice: frame const
//     + payload type + knownFrameTypes entry + producer (gateway session
//     edge) + interpreter dispatch, landing together.
//   - "notify" (+NotifyPayload{ReqID, MsgType, Summary}): the notify feature
//     itself was never built. Same rule: const + payload + both sides'
//     dispatch tables land as one slice with the feature, not ahead of it.
//
// Retired words (连接模型勘误期 — the client-visible binding axis was proven a
// false axis: "连接即人", a connection is an authenticated person + one pipe,
// with NO client-controllable channel binding/subscription state):
//   - "detach" frame (+DetachPayload{ChannelID}): the "撤绑定" verb has no
//     ontology — not-listening is the client's own business, leaving a channel
//     is a 户籍 verb, being revoked is server internal务. It was the残余 of the
//     "门动词形" disease (a封臂 machine grew a client trigger). Re-add ONLY if a
//     real, defensible client-side unbind semantics ever appears — as one
//     vertical slice with its producer.
//   - "binding_gen" (Frame envelope field + AttachReceipt grant + BindingGen
//     methods): the client-visible binding generation. With the binding axis
//     dead there is no client-visible binding to echo or version; revocation is
//     server-internal (read-pump per-batch eligibility recheck + lease upper
//     bound), not a client-carried generation.
//   - "stale_binding" (CodeStaleBinding): the错误码 for a client generation
//     mismatch — no client-visible binding can be陈旧; its slots are covered by
//     not_member/forbidden/unavailable.
//   - "not_member" (CodeNotMember): the会话级 membership-color error code. A
//     connection has no身份色 — eligibility is a per-frame/per-batch fact, and
//     an eligibility refusal is uniformly forbidden (表①).

// knownFrameTypes is the closed set enforced at Unmarshal. A frame_type outside
// it is refused (unknown-frame rejection, DoD-12).
var knownFrameTypes = map[FrameType]struct{}{
	FrameAttach: {}, FrameSubmit: {}, FrameResolve: {},
	FrameCancel: {}, FrameAfter: {}, FrameCancelTimer: {}, FrameResource: {},
	FrameFeed: {}, FrameReceipt: {}, FrameError: {},
}

// Error-code closed set (build spec 表①; 裁决8 平面词律): an error frame's `code`
// is ALWAYS a single flat word — never a nested write_rejected(code=x) form. A
// WriteRejected surfaces its harness reason verbatim as the code, so this set is
// the door's OWN vocabulary, not exhaustive of every harness reason.
const (
	CodeBadPayload         = "bad_payload"
	CodeNotInAudience      = "not_in_audience"
	CodeUnauthorizedSender = "unauthorized_sender"
	CodeAlreadyClosed      = "already_closed"
	CodeRequestNotFound    = "request_not_found"
	CodeInvalidDecision    = "invalid_decision"
	CodeUnavailable        = "unavailable"
	CodeRoutingUnavailable = "routing_unavailable"
	CodeForbidden          = "forbidden"
	CodeClosed             = "closed"
	// (CodeNotMember / CodeStaleBinding retired with the client-visible binding
	// axis — see the retired-words note by the FrameType consts. Eligibility
	// refusal is uniformly CodeForbidden.)
)

// ErrUnknownFrameType is Unmarshal's verdict for a frame_type outside the closed
// set. Typed so a binding can map it to its own error surface with errors.As/Is.
var ErrUnknownFrameType = errors.New("subjectgate: unknown frame_type")

// ErrFrameVersion is Unmarshal's verdict for a `v` other than FrameVersion.
var ErrFrameVersion = errors.New("subjectgate: unsupported frame version")

// ErrFrameTooBig is Marshal/Unmarshal's verdict for a serialized frame over
// MaxFrameBytes.
var ErrFrameTooBig = errors.New("subjectgate: frame exceeds size limit")

// ErrMissingChannelID is RequireChannelID's typed verdict when a business frame
// payload's required channel_id is absent/empty/whitespace-only. Typed so the
// upper layer maps it to CodeBadPayload with errors.Is.
var ErrMissingChannelID = errors.New("subjectgate: business frame missing required channel_id")

// Frame is the wire envelope every frame shares (build spec §S2). ref lives at
// the top level ONLY (裁决9): receipt/error echo it back here, never duplicated
// inside a payload. (binding_gen retired with the client-visible binding axis —
// 连接模型勘误期; see the retired-words note by the FrameType consts.)
type Frame struct {
	V       int             `json:"v"`
	Type    FrameType       `json:"frame_type"`
	Ref     string          `json:"ref,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// NewFrame builds a frame with payload marshaled from v (nil → no payload).
func NewFrame(t FrameType, ref string, v any) (Frame, error) {
	f := Frame{V: FrameVersion, Type: t, Ref: ref}
	if v != nil {
		raw, err := json.Marshal(v)
		if err != nil {
			return Frame{}, fmt.Errorf("subjectgate: marshal %s payload: %w", t, err)
		}
		f.Payload = raw
	}
	return f, nil
}

// NewErrorFrame is the single constructor for the flat error-frame contract.
// ErrorPayload contains strings only, so its JSON encoding cannot fail.
func NewErrorFrame(ref, frame, code, detail string) Frame {
	raw, _ := json.Marshal(ErrorPayload{Frame: frame, Code: code, Detail: detail})
	return Frame{V: FrameVersion, Type: FrameError, Ref: ref, Payload: raw}
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

// AttachPayload is the report-in + cursor-table handoff (连接模型勘误期 v2):
// since is a multi-key游标表 keyed by any channel id (channel_id is gone — a
// connection is channel-blind). A key with no eligibility is silently dropped
// (a harmless read位置 — the client may hold a stale cursor for a channel it已
// left). The receipt is empty (报到 ack).
type AttachPayload struct {
	Since map[string]int64 `json:"since,omitempty"`
}

// RequireChannelID validates a business frame payload's required channel_id
// (连接模型勘误期 v2: the connection is channel-blind, so every business frame
// names its channel). Absent / empty / whitespace-only → ErrMissingChannelID,
// a typed verdict the upper layer maps to bad_payload (表①).
func RequireChannelID(chID string) error {
	if strings.TrimSpace(chID) == "" {
		return ErrMissingChannelID
	}
	return nil
}

type SubmitPayload struct {
	ChannelID  string          `json:"channel_id"`
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
	ChannelID string          `json:"channel_id"`
	ReqID     string          `json:"req_id"`
	Decision  string          `json:"decision"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

type CancelPayload struct {
	ChannelID string `json:"channel_id"`
	ReqID     string `json:"req_id"`
}

type AfterPayload struct {
	ChannelID  string          `json:"channel_id"`
	DurationMs int64           `json:"duration_ms"`
	MsgType    string          `json:"msg_type"`
	Payload    json.RawMessage `json:"payload,omitempty"`
}

type CancelTimerPayload struct {
	ChannelID string `json:"channel_id"`
	TimerID   string `json:"timer_id"`
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
	ChannelID  string          `json:"channel_id"`
	Op         ResourceOp      `json:"op"`
	ResourceID string          `json:"resource_id"`
	Args       json.RawMessage `json:"args,omitempty"`
	Target     string          `json:"target,omitempty"`
	Ops        []string        `json:"ops,omitempty"`
	Query      *ResourceQuery  `json:"query,omitempty"`
}

// (PresencePayload retired with the "presence" frame word — see the retired-
// words note by the FrameType consts.)

// --- Downstream payloads --------------------------------------------------

type FeedPayload struct {
	ChannelID string          `json:"channel_id"`
	Seq       int64           `json:"seq"`
	Envelope  json.RawMessage `json:"envelope"`
}

// AttachReceipt is the report-in ack (连接模型勘误期 v2): an empty载荷. attach is
// no longer a binding grant — it just hands over the游标表 and acks receipt.
type AttachReceipt struct{}

// SubmitReceipt acks a write: it says the write was accepted, and names WHAT was
// written. That identity is the message.ID — nothing else belongs here. A row
// position (seq) is assigned by the storage layer, not by the act of writing, so
// it is not part of what a receipt owes the writer.
//
// Reading position is a separate thing with its own carrier: the downstream
// FeedPayload.Seq is the legitimate read cursor, and it is the only seq a client
// ever advances a feed with.
type SubmitReceipt struct {
	MessageID string `json:"message_id"`
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

// (NotifyPayload retired with the "notify" frame word — see the retired-words
// note by the FrameType consts.)
