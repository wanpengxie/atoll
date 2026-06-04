package behavior

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/ActOS/kernel/message"
)

// serve.go is the SERVE face of the behaviour base: the response-envelope
// builders any actor uses to answer a request by emitting a kind=response
// envelope. It depends only on kernel envelope types plus the consumer-side
// RequestLookup seam (defined in seam.go), so it stays pure-kernel — one
// envelope-build implementation shared by any actor that serves (C3).

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

// BuildResponseEnvelope assembles a kind=response envelope from the original
// request, looked up by id. Audience defaults to the request sender;
// visibility/correlation inherit from the request. The response id is a random
// uuid correlation anchor; parent_id (=request id) + the One-Law terminal
// uniqueness index — not the id — guarantee "one terminal per request". Shared
// serve helper.
func BuildResponseEnvelope(
	ctx context.Context,
	lookup RequestLookup,
	clock func() time.Time,
	sender message.Sender,
	requestID CorrelationKey,
	spec ResponseSpec,
) (*message.Envelope, error) {
	if requestID == "" {
		return nil, fmt.Errorf("behavior: response requestID required")
	}
	request, ok, err := lookup.FindByID(ctx, message.ID(requestID))
	if err != nil {
		return nil, fmt.Errorf("behavior: response lookup %s: %w", requestID, err)
	}
	if !ok || request == nil {
		return nil, fmt.Errorf("behavior: response request %s not found", requestID)
	}
	return BuildResponseFromRequest(request, clock, sender, requestID, spec)
}

// BuildResponseFromRequest is the lookup-free core: given an already-retrieved
// request envelope, it assembles the kind=response envelope directly. Use when
// the caller already holds the request in hand and needs no store round-trip.
func BuildResponseFromRequest(
	request *message.Envelope,
	clock func() time.Time,
	sender message.Sender,
	requestID CorrelationKey,
	spec ResponseSpec,
) (*message.Envelope, error) {
	if request == nil {
		return nil, fmt.Errorf("behavior: response request %s not in hand", requestID)
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
		ChannelID:     request.ChannelID,
		Sender:        sender,
		Kind:          message.KindResponse,
		Type:          request.Type,
		Payload:       merged,
		ParentID:      message.ID(requestID),
		CorrelationID: correlationID,
		Visibility:    vis,
		Audience:      audience,
	}, nil
}
