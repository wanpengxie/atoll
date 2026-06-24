package behavior

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/ActOS/protocol/message"
)

// serve.go is the SERVE face of the behaviour base: the response-envelope
// builder any actor uses to answer a request by emitting a kind=response
// envelope. It depends only on kernel envelope types, so it stays pure-kernel —
// one envelope-build implementation shared by any actor that serves (C3).

// MergeResponsePayload merges a caller-supplied payload object with the
// protocol-owned {status, reason[, dedupe]} fields. payload must be a JSON
// object or empty.
func MergeResponsePayload(payload json.RawMessage, status, reason string) (json.RawMessage, error) {
	m := map[string]json.RawMessage{}
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &m); err != nil {
			return nil, fmt.Errorf("behavior: response payload must be a JSON object: %w", err)
		}
		if m == nil {
			// payload was the JSON literal `null` — unmarshal nils the map but
			// reports no error; treat empty/null as an empty object so the
			// {status,reason} merge below never assigns into a nil map.
			m = map[string]json.RawMessage{}
		}
	}
	sb, _ := json.Marshal(status)
	m["status"] = sb
	if reason != "" {
		rb, _ := json.Marshal(reason)
		m["reason"] = rb
	}
	return json.Marshal(m)
}

// ResponseSpec is the caller-supplied shape of a response (status/reason +
// optional visibility override + the raw payload). The response audience is NOT
// caller-adjustable: a response always goes to the request's sender (harness
// Step 8 enforces audience == parent sender), so there is no audience field.
type ResponseSpec struct {
	Status     string
	Reason     string
	Payload    json.RawMessage
	Visibility message.Visibility
}

// BuildResponseFromRequest assembles a kind=response envelope from the request
// envelope held in hand. Audience defaults to the request sender;
// visibility/correlation inherit from the request. The response id is a random
// uuid correlation anchor; parent_id (=request id) + the One-Law terminal
// uniqueness index — not the id — guarantee "one terminal per request". The
// request is supplied in-hand by the caller: recovering it from truth (when the
// caller does not already hold it) is the actor's job via the RequestLookup
// seam, not this builder's.
//
// Sender / ChannelID are left ZERO: identity is substrate-injected by the pen
// at write time (sealed-pen). The responder's own id is welded onto the pen, so
// the builder neither knows nor fills it.
func BuildResponseFromRequest(
	request *message.Envelope,
	clock func() time.Time,
	spec ResponseSpec,
) (*message.Envelope, error) {
	if request == nil {
		return nil, fmt.Errorf("behavior: response build: nil request in hand")
	}
	merged, err := MergeResponsePayload(spec.Payload, spec.Status, spec.Reason)
	if err != nil {
		return nil, err
	}
	vis := spec.Visibility
	if vis == "" {
		vis = request.Visibility
	}
	audience := message.Audience{request.Sender.ID}
	correlationID := request.CorrelationID
	if correlationID == "" {
		correlationID = request.ID
	}
	return &message.Envelope{
		ID:            message.ID(uuid.NewString()),
		TS:            clock().UnixMilli(),
		Kind:          message.KindResponse,
		Type:          request.Type,
		Payload:       merged,
		ParentID:      request.ID,
		CorrelationID: correlationID,
		Visibility:    vis,
		Audience:      audience,
	}, nil
}
