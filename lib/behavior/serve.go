package behavior

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/wanpengxie/ActOS/kernel/message"
)

// serve.go is the SERVE face of the behaviour base: shared helpers any actor
// uses to answer a request by emitting a kind=response envelope. It depends
// only on kernel contracts (message.RequestLookup + the envelope types), so it
// stays pure-kernel and is shared by adapterActor (adapterhost) and the channel
// system actor (sysactor) — one respond implementation, not two (C3).

// MergeResponsePayload merges an adapter/system payload object with the
// framework-owned {status, reason[, dedupe]} fields. payload must be a JSON
// object or empty.
func MergeResponsePayload(payload json.RawMessage, status, reason string, dedupe bool) (json.RawMessage, error) {
	m := map[string]json.RawMessage{}
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &m); err != nil {
			return nil, fmt.Errorf("behavior: response payload must be a JSON object: %w", err)
		}
	}
	sb, _ := json.Marshal(status)
	m["status"] = sb
	if reason != "" {
		rb, _ := json.Marshal(reason)
		m["reason"] = rb
	}
	if dedupe {
		m["dedupe"] = json.RawMessage("true")
	}
	return json.Marshal(m)
}

// ResponseSpec is the caller-supplied shape of a response (status/reason +
// optional visibility/audience override + the raw payload).
type ResponseSpec struct {
	Status     string
	Reason     string
	Payload    json.RawMessage
	Dedupe     bool
	Visibility message.Visibility
	Audience   message.Audience
}

// BuildResponseEnvelope assembles a kind=response envelope from the original
// request, looked up by id. Audience defaults to the request sender;
// visibility/correlation inherit from the request. The deterministic id is
// response:<request_id>:<canonical_hash>. Shared serve helper.
func BuildResponseEnvelope(
	ctx context.Context,
	lookup message.RequestLookup,
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

// BuildResponseFromRequest is the lookup-free core: it builds the response
// envelope directly from an in-hand request envelope. A compute cell (which has
// NO local truth to look up) caches the dispatched request and uses this so its
// Respond/Provisional work without a round-trip to the home harness.
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
	merged, err := MergeResponsePayload(spec.Payload, spec.Status, spec.Reason, spec.Dedupe)
	if err != nil {
		return nil, err
	}
	hash, err := message.CanonicalHashPayload(merged)
	if err != nil {
		return nil, fmt.Errorf("behavior: response hash: %w", err)
	}
	vis := spec.Visibility
	if vis == "" {
		vis = request.Visibility
	}
	audience := message.Audience{request.Sender.ID}
	if len(spec.Audience) > 0 {
		if len(spec.Audience) != 1 || spec.Audience[0] != request.Sender.ID {
			return nil, fmt.Errorf("behavior: response audience %v must equal parent sender %s", spec.Audience, request.Sender.ID)
		}
		audience = spec.Audience
	}
	correlationID := request.CorrelationID
	if correlationID == "" {
		correlationID = request.ID
	}
	return &message.Envelope{
		ID:            message.ID("response:" + requestID.String() + ":" + hash),
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
