package behavior

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
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
// the pen. status MUST be final (message.StatusCompleted/StatusFailed); empty
// defaults to completed. A Duplicate outcome returns the message id with no error. The
// answering actor's own identity is welded onto the pen (sealed-pen), never a
// parameter. author#1.
func Respond(
	ctx context.Context,
	pen harness.Pen,
	clock func() time.Time,
	request *message.Envelope,
	spec ResponseSpec,
) (message.ID, error) {
	if request == nil {
		return "", fmt.Errorf("behavior: Respond request required")
	}
	if spec.Status == "" {
		spec.Status = message.StatusCompleted
	}
	if !message.IsFinalStatus(spec.Status) {
		return "", fmt.Errorf("behavior: Respond status must be final; got %q", spec.Status)
	}
	env, err := BuildResponseFromRequest(request, clock, spec)
	if err != nil {
		return "", err
	}
	out, werr := pen.Write(ctx, env)
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

// RespondJSON marshals result and commits a status=completed final for the
// request held in hand. The serve-side happy-path one-liner: every actor's
// "respond with this value" goes through here instead of hand-marshalling.
func RespondJSON(
	ctx context.Context,
	pen harness.Pen,
	clock func() time.Time,
	request *message.Envelope,
	result any,
) (message.ID, error) {
	raw, err := json.Marshal(result)
	if err != nil {
		return Fail(ctx, pen, clock, request, "internal_error",
			fmt.Sprintf("marshal result: %v", err))
	}
	return Respond(ctx, pen, clock, request,
		ResponseSpec{Status: message.StatusCompleted, Payload: raw})
}

// Progress marshals result and commits a status=processing PROVISIONAL
// response for the request held in hand — Reply/Fail's non-final sibling,
// living at the same layer so the write discipline (build → pen → settle)
// exists once (purity 手动档 B5: the engine used to hand-roll this whole
// sequence because Respond's final-only gate — load-bearing, it means
// "close the request" — could not carry a provisional). A provisional NEVER
// closes the request: any number may precede the final. Settlement tolerance
// mirrors Respond's: a HarnessTerminalDuplicate reject means the request
// already closed under this progress write's feet — a benign race (the
// caller's final wins), not an error.
func Progress(
	ctx context.Context,
	pen harness.Pen,
	clock func() time.Time,
	request *message.Envelope,
	result any,
) (message.ID, error) {
	raw, err := json.Marshal(result)
	if err != nil {
		return "", fmt.Errorf("behavior: progress marshal: %w", err)
	}
	env, err := BuildResponseFromRequest(request, clock, ResponseSpec{
		Status:  message.StatusProcessing,
		Payload: raw,
	})
	if err != nil {
		return "", err
	}
	out, werr := pen.Write(ctx, env)
	if werr != nil {
		return "", fmt.Errorf("behavior: progress write: %w", werr)
	}
	if out.RejectReason == harness.HarnessTerminalDuplicate {
		return out.MessageID, nil
	}
	if out.RejectReason != "" {
		return "", fmt.Errorf("behavior: progress rejected: %s (%s)", out.RejectReason, out.RejectDetail)
	}
	return out.MessageID, nil
}

// Fail commits a status=failed final carrying the conventional failure
// payload {error_code, detail} — the ONE home of that shape. The terminal
// reason is pinned to receiver_internal_error (the serve-side catch-all);
// specificity rides in errorCode, which is the actor's own closed set.
func Fail(
	ctx context.Context,
	pen harness.Pen,
	clock func() time.Time,
	request *message.Envelope,
	errorCode, detail string,
) (message.ID, error) {
	payload, _ := json.Marshal(map[string]string{"error_code": errorCode, "detail": detail})
	return Respond(ctx, pen, clock, request, ResponseSpec{
		Status:  message.StatusFailed,
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
//
// Sender / ChannelID are left ZERO: identity is substrate-injected by the pen
// at write time (sealed-pen). The authoring actor's id is welded onto the pen,
// so the builder neither knows nor fills it.
func BuildEvent(
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
		Kind:          message.KindEvent,
		Type:          spec.Type,
		Payload:       spec.Payload,
		Visibility:    spec.Visibility,
		Audience:      spec.Audience,
		ParentID:      spec.ParentID,
		CorrelationID: spec.CorrelationID,
	}, nil
}

// SubjectWriteSpec is the caller-supplied shape of an off-process subject's
// drive-write (期12 S2): the FULL envelope surface a subject may author —
// kind=request/event, visibility, parent, own ID, deadline — deliberately
// wider than the in-process Proc sugar (submit hardcodes KindRequest, Emit
// hardcodes VisibilityPublic). Kind/visibility whitelisting is the engine's
// job (the drive verb), not this builder's; this is only the ONE home for the
// envelope literal (archtest envelope-construction confinement).
type SubjectWriteSpec struct {
	ID         message.ID
	Type       string // required
	Kind       message.Kind
	Payload    json.RawMessage
	Audience   message.Audience
	Visibility message.Visibility
	ParentID   message.ID
	ExpiresAt  *int64
}

// BuildSubjectWrite assembles the subject-drive envelope by dispatching onto
// the two existing kind builders (request → BuildRequest, event → BuildEvent
// — no third envelope literal). ExpiresAt only rides a request (an event has
// no closure to deadline); a non-request/event kind is the engine whitelist's
// rejection, surfaced here too as a defensive floor.
func BuildSubjectWrite(clock func() time.Time, spec SubjectWriteSpec) (*message.Envelope, error) {
	switch spec.Kind {
	case message.KindRequest:
		return buildRequest(clock, RequestSpec{
			ID:         spec.ID,
			Type:       spec.Type,
			Payload:    spec.Payload,
			Audience:   spec.Audience,
			Visibility: spec.Visibility,
			ParentID:   spec.ParentID,
			ExpiresAt:  spec.ExpiresAt,
		}, false)
	case message.KindEvent:
		return BuildEvent(clock, EventSpec{
			ID:         spec.ID,
			Type:       spec.Type,
			Payload:    spec.Payload,
			Visibility: spec.Visibility,
			Audience:   spec.Audience,
			ParentID:   spec.ParentID,
		})
	default:
		return nil, fmt.Errorf("behavior: BuildSubjectWrite kind must be request or event; got %q", spec.Kind)
	}
}

// EmitEvent emits one kind=event message through the pen. kind-neutral: the
// authoring actor's identity is welded onto the pen (sealed-pen), never a
// parameter.
func EmitEvent(
	ctx context.Context,
	pen harness.Pen,
	clock func() time.Time,
	eventType string,
	payload json.RawMessage,
	vis message.Visibility,
	audience message.Audience,
) (message.ID, error) {
	env, err := BuildEvent(clock, EventSpec{
		Type: eventType, Payload: payload, Visibility: vis, Audience: audience,
	})
	if err != nil {
		return "", err
	}
	out, err := pen.Write(ctx, env)
	if err != nil {
		return "", fmt.Errorf("behavior: emit write: %w", err)
	}
	if !out.Accepted() {
		return "", fmt.Errorf("behavior: emit rejected: %s (%s)", out.RejectReason, out.RejectDetail)
	}
	return out.MessageID, nil
}
