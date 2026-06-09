package behavior

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

// respond.go is the SERVE WRITE primitive — closure author#1, ONE
// implementation (P13). Any serving actor authors its response through this one
// kind-neutral primitive: the answering actor's identity is the `sender`
// argument, never a hard-coded kind.
//
// These primitives are STATELESS: the request is supplied in-hand by the caller
// (an in-flight cache is a host optimisation, not part of the base). A
// terminal-duplicate is treated as success — the request was already closed by
// the caller timeout (author#2) or a concurrent author, which is benign.

// Respond commits a FINAL response/terminal for a request held in-hand, through
// the writer. status MUST be final ("completed"/"failed"); empty defaults to
// "completed". A Duplicate outcome returns the message id with no error.
// sender = the answering actor's own identity. author#1.
func Respond(
	ctx context.Context,
	writer harness.Writer,
	clock func() time.Time,
	request *message.Envelope,
	sender message.Sender,
	spec ResponseSpec,
) (message.ID, error) {
	if request == nil {
		return "", fmt.Errorf("behavior: Respond request required")
	}
	if spec.Status == "" {
		spec.Status = "completed"
	}
	if !message.IsFinalStatus(spec.Status) {
		return "", fmt.Errorf("behavior: Respond status must be final; got %q", spec.Status)
	}
	env, err := BuildResponseFromRequest(request, clock, sender, CorrelationKey(request.ID), spec)
	if err != nil {
		return "", err
	}
	out, werr := writer.Write(ctx, env)
	if werr != nil {
		return "", fmt.Errorf("behavior: respond write: %w", werr)
	}
	if out.RejectReason == harness.HarnessTerminalDuplicate {
		return out.MessageID, nil
	}
	if out.RejectReason != "" {
		return "", fmt.Errorf("behavior: respond rejected: %s (%s)", out.RejectReason, out.RejectDetail)
	}
	return out.MessageID, nil
}

// CollapseInternalError closes a request with a receiver_internal_error final
// — the author#1 tail for a Handle that failed hard. detail is carried as the
// payload reason context; it is opaque to the base.
func CollapseInternalError(
	ctx context.Context,
	writer harness.Writer,
	clock func() time.Time,
	request *message.Envelope,
	sender message.Sender,
	detail string,
) (message.ID, error) {
	if request == nil {
		return "", fmt.Errorf("behavior: CollapseInternalError request required")
	}
	var payload json.RawMessage
	if detail != "" {
		b, _ := json.Marshal(map[string]string{"detail": detail})
		payload = b
	}
	return Respond(ctx, writer, clock, request, sender, ResponseSpec{
		Status:  "failed",
		Reason:  string(message.TerminalReceiverInternalError),
		Payload: payload,
	})
}

// RespondJSON marshals result and commits a status=completed final for the
// request held in hand. The serve-side happy-path one-liner: every actor's
// "respond with this value" goes through here instead of hand-marshalling.
func RespondJSON(
	ctx context.Context,
	writer harness.Writer,
	clock func() time.Time,
	request *message.Envelope,
	sender message.Sender,
	result any,
) (message.ID, error) {
	raw, err := json.Marshal(result)
	if err != nil {
		return Fail(ctx, writer, clock, request, sender, "internal_error",
			fmt.Sprintf("marshal result: %v", err))
	}
	return Respond(ctx, writer, clock, request, sender,
		ResponseSpec{Status: "completed", Payload: raw})
}

// Fail commits a status=failed final carrying the conventional failure
// payload {error_code, detail} — the ONE home of that shape. The terminal
// reason is pinned to receiver_internal_error (the serve-side catch-all);
// specificity rides in errorCode, which is the actor's own closed set.
func Fail(
	ctx context.Context,
	writer harness.Writer,
	clock func() time.Time,
	request *message.Envelope,
	sender message.Sender,
	errorCode, detail string,
) (message.ID, error) {
	payload, _ := json.Marshal(map[string]string{"error_code": errorCode, "detail": detail})
	return Respond(ctx, writer, clock, request, sender, ResponseSpec{
		Status:  "failed",
		Reason:  string(message.TerminalReceiverInternalError),
		Payload: payload,
	})
}

// EventSpec is the caller-supplied shape of a kind=event envelope.
type EventSpec struct {
	// ID is optional: empty = a fresh uuid.
	ID            message.ID
	Type          string // required
	Payload       json.RawMessage
	Visibility    message.Visibility
	Audience      message.Audience
	ParentID      message.ID
	CorrelationID message.ID
}

// BuildEvent assembles a kind=event envelope — the ONE home for event
// construction defaults. Bindings stamp transport-edge fields (TSReceived)
// after build; this builder never writes.
func BuildEvent(
	chID channel.ID,
	sender message.Sender,
	clock func() time.Time,
	spec EventSpec,
) (*message.Envelope, error) {
	if spec.Type == "" {
		return nil, fmt.Errorf("behavior: event type required")
	}
	id := spec.ID
	if id == "" {
		id = message.ID(uuid.NewString())
	}
	return &message.Envelope{
		ID:            id,
		TS:            clock().UnixMilli(),
		ChannelID:     chID,
		Sender:        sender,
		Kind:          message.KindEvent,
		Type:          spec.Type,
		Payload:       spec.Payload,
		Visibility:    spec.Visibility,
		Audience:      spec.Audience,
		ParentID:      spec.ParentID,
		CorrelationID: spec.CorrelationID,
	}, nil
}

// EmitEvent emits one kind=event message under the given sender identity.
// kind-neutral: sender carries whatever actor authored the event.
func EmitEvent(
	ctx context.Context,
	writer harness.Writer,
	clock func() time.Time,
	channelID channel.ID,
	sender message.Sender,
	eventType string,
	payload json.RawMessage,
	vis message.Visibility,
	audience message.Audience,
) (message.ID, error) {
	env, err := BuildEvent(channelID, sender, clock, EventSpec{
		Type: eventType, Payload: payload, Visibility: vis, Audience: audience,
	})
	if err != nil {
		return "", err
	}
	out, err := writer.Write(ctx, env)
	if err != nil {
		return "", fmt.Errorf("behavior: emit write: %w", err)
	}
	if !out.Accepted() {
		return "", fmt.Errorf("behavior: emit rejected: %s (%s)", out.RejectReason, out.RejectDetail)
	}
	return out.MessageID, nil
}
