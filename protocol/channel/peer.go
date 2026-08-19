package channel

import (
	"bytes"
	"encoding/json"
	"errors"
)

// Address identifies one channel at an optional space authority.
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

// Request is the peer call frame.
type Request struct {
	To       Address         `json:"to"`
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

// Result is the single terminal peer frame.
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
	To        Address    `json:"to"`
	From      CancelFrom `json:"from"`
	RequestID string     `json:"request_id"`
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

var ErrUnknownPeerField = errors.New("channel: peer frame contains an unknown field")

func decodeClosed(data []byte, out any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return errors.Join(ErrUnknownPeerField, err)
	}
	if dec.More() {
		return ErrUnknownPeerField
	}
	return nil
}

func (v *Address) UnmarshalJSON(data []byte) error {
	type plain Address
	return decodeClosed(data, (*plain)(v))
}

func (v *From) UnmarshalJSON(data []byte) error {
	type plain From
	return decodeClosed(data, (*plain)(v))
}

func (v *Request) UnmarshalJSON(data []byte) error {
	type plain Request
	return decodeClosed(data, (*plain)(v))
}

func (v *Progress) UnmarshalJSON(data []byte) error {
	type plain Progress
	return decodeClosed(data, (*plain)(v))
}

func (v *Result) UnmarshalJSON(data []byte) error {
	type plain Result
	return decodeClosed(data, (*plain)(v))
}

func (v *Failure) UnmarshalJSON(data []byte) error {
	type plain Failure
	return decodeClosed(data, (*plain)(v))
}

func (v *CancelFrom) UnmarshalJSON(data []byte) error {
	type plain CancelFrom
	return decodeClosed(data, (*plain)(v))
}

func (v *Cancel) UnmarshalJSON(data []byte) error {
	type plain Cancel
	return decodeClosed(data, (*plain)(v))
}
