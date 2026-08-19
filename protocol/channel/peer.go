package channel

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

// Address identifies one channel at an optional space authority. It belongs
// to bindings; call and describe frames do not repeat their bound target.
type Address struct {
	Space   string `json:"space,omitempty"`
	Channel ID     `json:"channel"`
}

// From is the authenticated calling channel's assertion about the member for
// whom it is speaking. Actor is deliberately opaque to the receiving channel.
type From struct {
	Space     string `json:"space,omitempty"`
	Channel   ID     `json:"channel"`
	Actor     string `json:"actor"`
	RequestID string `json:"request_id"`
}

// Request is the peer call frame. Its target comes from the binding.
type Request struct {
	From     From            `json:"from"`
	Deadline int64           `json:"deadline,omitempty"`
	Type     string          `json:"type"`
	Payload  json.RawMessage `json:"payload"`
}

// Progress carries one ordered provisional result back to the caller.
type Progress struct {
	RequestID string          `json:"request_id"`
	Seq       int             `json:"seq"`
	Status    string          `json:"status"`
	Body      json.RawMessage `json:"body,omitempty"`
}

// Result is the single terminal peer frame. Exactly one of Body and Fail is
// present; a JSON null body is still a present successful body.
type Result struct {
	Body json.RawMessage `json:"body,omitempty"`
	Fail *Failure        `json:"fail,omitempty"`
}

// Failure describes whether a call failed at the receiver's gate or after it
// reached a receiver.
type Failure struct {
	Stage  Stage  `json:"stage"`
	Code   string `json:"code"`
	Detail string `json:"detail,omitempty"`
}

type CancelFrom struct {
	Channel ID `json:"channel"`
}

// Cancel is an idempotent request to cancel one in-flight peer call.
type Cancel struct {
	From      CancelFrom `json:"from"`
	RequestID string     `json:"request_id"`
}

// DescribeFrom identifies the authenticated channel asking for the bound
// server's endpoint directory. Describe is not a call and has no actor or
// request exchange id.
type DescribeFrom struct {
	Space   string `json:"space,omitempty"`
	Channel ID     `json:"channel"`
}

type Describe struct {
	From DescribeFrom `json:"from"`
}

// Card is the server channel's materialized endpoint directory. Word values
// stay raw JSON so this zero-import wire package does not depend on manifests.
type Card struct {
	Words map[string]json.RawMessage `json:"words"`
}

// Stage is the closed peer failure locus.
type Stage string

const (
	StageGate     Stage = "gate"
	StageReceiver Stage = "receiver"
)

func ParseStage(raw string) (Stage, bool) {
	s := Stage(raw)
	return s, s == StageGate || s == StageReceiver
}

// GateCode is the closed set of failures produced before a request reaches a
// receiver.
type GateCode string

const (
	GateBadOrigin          GateCode = "bad_origin"
	GateEndpointNotFound   GateCode = "endpoint_not_found"
	GateReceiverInactive   GateCode = "receiver_inactive"
	GateForbidden          GateCode = "forbidden"
	GateChannelUnavailable GateCode = "channel_unavailable"
	GateNoServiceAgent     GateCode = "no_service_agent"
)

func ParseGateCode(raw string) (GateCode, bool) {
	c := GateCode(raw)
	switch c {
	case GateBadOrigin, GateEndpointNotFound, GateReceiverInactive, GateForbidden, GateChannelUnavailable, GateNoServiceAgent:
		return c, true
	default:
		return "", false
	}
}

var (
	ErrMalformedPeerFrame = errors.New("channel: malformed peer frame")
	ErrUnknownPeerField   = errors.New("channel: peer frame contains an unknown field")
	ErrInvalidPeerFrame   = errors.New("channel: semantically invalid peer frame")
)

func decodeClosed(data []byte, out any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return classifyDecodeError(err)
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("%w: multiple JSON values", ErrMalformedPeerFrame)
		}
		return classifyDecodeError(err)
	}
	return nil
}

func classifyDecodeError(err error) error {
	switch {
	case errors.Is(err, ErrUnknownPeerField), errors.Is(err, ErrInvalidPeerFrame), errors.Is(err, ErrMalformedPeerFrame):
		return err
	case strings.Contains(err.Error(), "unknown field"):
		return fmt.Errorf("%w: %v", ErrUnknownPeerField, err)
	default:
		return fmt.Errorf("%w: %v", ErrMalformedPeerFrame, err)
	}
}

func invalidPeer(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidPeerFrame, fmt.Sprintf(format, args...))
}

func objectFields(data []byte) map[string]json.RawMessage {
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(data, &fields)
	return fields
}

func (v *Address) UnmarshalJSON(data []byte) error {
	type plain Address
	if err := decodeClosed(data, (*plain)(v)); err != nil {
		return err
	}
	if v.Channel == "" {
		return invalidPeer("address.channel is required")
	}
	return nil
}

func (v *From) UnmarshalJSON(data []byte) error {
	type plain From
	if err := decodeClosed(data, (*plain)(v)); err != nil {
		return err
	}
	if v.Channel == "" || v.Actor == "" || v.RequestID == "" {
		return invalidPeer("request from.channel, from.actor, and from.request_id are required")
	}
	return nil
}

func (v *Request) UnmarshalJSON(data []byte) error {
	type plain Request
	if err := decodeClosed(data, (*plain)(v)); err != nil {
		return err
	}
	fields := objectFields(data)
	if _, ok := fields["from"]; !ok || v.Type == "" {
		return invalidPeer("request.from and request.type are required")
	}
	if _, ok := fields["payload"]; !ok || len(v.Payload) == 0 {
		return invalidPeer("request.payload is required")
	}
	if v.Deadline < 0 {
		return invalidPeer("request.deadline cannot be negative")
	}
	return nil
}

func (v *Progress) UnmarshalJSON(data []byte) error {
	type plain Progress
	if err := decodeClosed(data, (*plain)(v)); err != nil {
		return err
	}
	if v.RequestID == "" || v.Seq < 1 || v.Status == "" {
		return invalidPeer("progress.request_id, positive seq, and status are required")
	}
	return nil
}

func (v *Result) UnmarshalJSON(data []byte) error {
	type plain Result
	if err := decodeClosed(data, (*plain)(v)); err != nil {
		return err
	}
	_, bodyPresent := objectFields(data)["body"]
	if bodyPresent == (v.Fail != nil) {
		return invalidPeer("result must contain exactly one of body or fail")
	}
	return nil
}

func (v *Failure) UnmarshalJSON(data []byte) error {
	type plain Failure
	if err := decodeClosed(data, (*plain)(v)); err != nil {
		return err
	}
	stage, ok := ParseStage(string(v.Stage))
	if !ok || v.Code == "" {
		return invalidPeer("failure.stage and failure.code are required")
	}
	v.Stage = stage
	if stage == StageGate {
		if _, ok := ParseGateCode(v.Code); !ok {
			return invalidPeer("unknown gate failure code %q", v.Code)
		}
	}
	return nil
}

func (v *CancelFrom) UnmarshalJSON(data []byte) error {
	type plain CancelFrom
	if err := decodeClosed(data, (*plain)(v)); err != nil {
		return err
	}
	if v.Channel == "" {
		return invalidPeer("cancel.from.channel is required")
	}
	return nil
}

func (v *Cancel) UnmarshalJSON(data []byte) error {
	type plain Cancel
	if err := decodeClosed(data, (*plain)(v)); err != nil {
		return err
	}
	if _, ok := objectFields(data)["from"]; !ok || v.RequestID == "" {
		return invalidPeer("cancel.from and cancel.request_id are required")
	}
	return nil
}

func (v *DescribeFrom) UnmarshalJSON(data []byte) error {
	type plain DescribeFrom
	if err := decodeClosed(data, (*plain)(v)); err != nil {
		return err
	}
	if v.Channel == "" {
		return invalidPeer("describe.from.channel is required")
	}
	return nil
}

func (v *Describe) UnmarshalJSON(data []byte) error {
	type plain Describe
	if err := decodeClosed(data, (*plain)(v)); err != nil {
		return err
	}
	if _, ok := objectFields(data)["from"]; !ok {
		return invalidPeer("describe.from is required")
	}
	return nil
}

func (v *Card) UnmarshalJSON(data []byte) error {
	type plain Card
	if err := decodeClosed(data, (*plain)(v)); err != nil {
		return err
	}
	if _, ok := objectFields(data)["words"]; !ok || v.Words == nil {
		return invalidPeer("card.words is required")
	}
	return nil
}
