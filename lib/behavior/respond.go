package behavior

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/protocol/actor"
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
// closes the request: any number may precede the final.
//
// Settlement tolerance mirrors Respond's, but the harness hands a provisional
// TWO distinct words for "the terminal already landed", so both must be
// absorbed:
//
//   - HarnessTerminalDuplicate — the generic terminal-uniqueness verdict.
//   - HarnessProvisionalAfterFinal — the verdict stepResponsePairing reserves
//     for a PROVISIONAL arriving after the final. Absorbing only the former
//     left this one surfacing as an error, contradicting this doc's own
//     "benign race" promise.
//
// Both mean the same thing from this caller's seat: the request closed under
// this write's feet and the caller's final won. Neither is an error.
//
// SCOPE: this tolerance is for a genuine RACE — the window between a live
// 占用者's gate check and its pen write. It is NOT a licence to Progress a
// request already known to be closed; that is misuse and the caller-side gate
// is what rejects it.
func Progress(
	ctx context.Context,
	pen harness.Pen,
	clock func() time.Time,
	request *message.Envelope,
	status string,
	result any,
) (message.ID, error) {
	if !message.IsProvisionalCoreStatus(status) {
		return "", fmt.Errorf("behavior: invalid provisional status %q", status)
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return "", fmt.Errorf("behavior: progress marshal: %w", err)
	}
	env, err := BuildResponseFromRequest(request, clock, ResponseSpec{
		Status:  status,
		Payload: raw,
	})
	if err != nil {
		return "", err
	}
	out, werr := pen.Write(ctx, env)
	if werr != nil {
		return "", fmt.Errorf("behavior: progress write: %w", werr)
	}
	if out.RejectReason == harness.HarnessTerminalDuplicate ||
		out.RejectReason == harness.HarnessProvisionalAfterFinal {
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
	payload, _ := json.Marshal(message.Failure{ErrorCode: errorCode, Detail: detail})
	return Respond(ctx, pen, clock, request, ResponseSpec{
		Status:  message.StatusFailed,
		Reason:  string(message.TerminalReceiverInternalError),
		Payload: payload,
	})
}

// EventSpec is the caller-supplied shape of a kind=event envelope.
type EventSpec struct {
	// ID is optional: empty = a fresh uuid.
	ID         message.ID
	Type       string // required
	Payload    json.RawMessage
	Visibility message.Visibility
	Audience   message.Audience
	// Cause is REQUIRED, on the same law as a request's: an event is caused by
	// something too. An agent's turn event is caused by the request it is
	// working on; a membership event is caused by the word that changed the
	// membership. Only an event that genuinely begins something says Root().
	Cause message.Cause
	// ClientFingerprint is shell-ingress persistence metadata and never a
	// protocol envelope field.
	ClientFingerprint string
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
	if !spec.Cause.Stated() {
		return nil, fmt.Errorf("behavior: BuildEvent cause required: say message.From(<the message this event reports on>), or message.Root() when nothing on this ledger caused it")
	}
	id := spec.ID
	if id == "" {
		id = message.ID(uuid.NewString())
	}
	parentID, correlationID := spec.Cause.Resolve(id)
	return &message.Envelope{
		ID:            id,
		TS:            clock().UnixMilli(),
		Kind:          message.KindEvent,
		Type:          spec.Type,
		Payload:       spec.Payload,
		Visibility:    spec.Visibility,
		Audience:      spec.Audience,
		ParentID:      parentID,
		CorrelationID: correlationID,
	}, nil
}

// EventSpecJSON is the narrow "type + value + audience" sugar for EventSpec:
// it marshals an ordinary Go value into the spec's json.RawMessage payload.
//
// The verb table carries the FULL event surface because that is what an event
// can be; the shape most Proc bodies actually want is convenience, and
// convenience belongs in a library rather than in a second verb.
//
// It takes the cause even though it is sugar: a convenience that hands back a
// spec with a required field still empty invites the caller to forget it, which
// is the whole shape being removed here. Sugar may express LESS than the full
// form; it may not leave a required answer blank for someone else to notice.
//
// This function does NOT write. The pen-writing counterpart that used to live
// here was deleted with the identity verbs: it flattened a harness rejection
// into a formatted error, which a caller mapping verdicts to protocol codes
// cannot tell apart from any other failure — so the write, and the typed
// carrier it must produce, live at the verb.
func EventSpecJSON(cause message.Cause, eventType string, payload any, audience ...actor.ActorID) (EventSpec, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return EventSpec{}, fmt.Errorf("behavior: event payload marshal: %w", err)
	}
	var aud message.Audience
	if len(audience) > 0 {
		aud = message.Audience(audience)
	}
	return EventSpec{Cause: cause, Type: eventType, Payload: raw, Audience: aud}, nil
}
