package behavior

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/protocol/message"
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
// optional visibility override + the raw payload). Audience is the one
// exception to "everything derives from the request in hand": a response
// normally goes to the request's sender (left nil → request.Sender.ID), but a
// CALLER closing its own request (author#2) answers itself and names itself
// here — its registered copy of the request never carried a sender (identity
// is pen-welded, and a remote cell's proxy pen relays the envelope unwelded),
// so the sender is not a source it can read. Harness Step 8 still enforces
// audience == parent sender against truth, so this field cannot redirect a
// response anywhere the substrate would not have sent it.
type ResponseSpec struct {
	Status     string
	Reason     string
	Payload    json.RawMessage
	Visibility message.Visibility
	Audience   message.Audience
}

// BuildResponseFromRequest assembles a kind=response envelope from the request
// envelope held in hand. Audience is spec.Audience, else the request sender;
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
	audience := spec.Audience
	if len(audience) == 0 {
		audience = message.Audience{request.Sender.ID}
	}
	// A response's cause is never in doubt — it is the request in hand. It goes
	// through the same derivation every other envelope uses, so there is one
	// account of what parent and correlation mean, not two that could drift.
	id := message.ID(uuid.NewString())
	parentID, correlationID := message.From(*request).Resolve(id)
	return &message.Envelope{
		ID:            id,
		TS:            clock().UnixMilli(),
		Kind:          message.KindResponse,
		Type:          request.Type,
		Payload:       merged,
		ParentID:      parentID,
		CorrelationID: correlationID,
		Visibility:    vis,
		Audience:      audience,
	}, nil
}
