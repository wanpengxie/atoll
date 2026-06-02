package adapterhost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
)

// Start builds the ModuleContext (closing over this cell) and runs module.Init
// on the cell goroutine. After Start the adapter may call mctx seams from its
// callbacks; all of them touch a.correlation/a.chain serially.
func (a *adapterActor) Start(ctx context.Context, _ actorrt.ActorContext) error {
	if a.correlation == nil {
		a.correlation = map[behavior.CorrelationKey]behavior.CorrelationEntry{}
	}
	a.mctx = a.buildModuleContext()
	return a.module.Init(ctx, a.mctx)
}

// Stop releases the module on the cell goroutine (serial with in-flight Handle).
func (a *adapterActor) Stop(ctx context.Context) error {
	return a.module.Shutdown(ctx)
}

// buildModuleContext constructs the seam bundle handed to module.Init. Every
// closure captures THIS adapterActor, so Respond/Fail/EmitEvent run on the cell
// goroutine and mutate a.correlation with no lock (collapse of buildRespond et
// al. — respond.go — into cell methods; the respondConfig god-bundle is gone).
func (a *adapterActor) buildModuleContext() *behavior.ModuleContext {
	sender := message.Sender{Kind: actor.KindTool, ID: a.self}
	return &behavior.ModuleContext{
		AdapterName:      a.declaration.Name,
		AdapterActorID:   a.self,
		AdapterActorKind: actor.KindTool,
		ChannelID:        a.channelID,
		Respond: func(ctx context.Context, requestID behavior.CorrelationKey, payload json.RawMessage, opts behavior.RespondOptions) (behavior.RespondResult, error) {
			return a.doRespond(ctx, requestID, payload, opts, sender)
		},
		Fail: func(ctx context.Context, requestID behavior.CorrelationKey, payload json.RawMessage, opts behavior.FailOptions) (behavior.RespondResult, error) {
			reason := opts.Reason
			if reason == "" {
				reason = message.TerminalReceiverInternalError
			}
			return a.doRespond(ctx, requestID, payload, behavior.RespondOptions{
				Status:     "failed",
				Reason:     string(reason),
				Visibility: opts.Visibility,
				Audience:   opts.Audience,
			}, sender)
		},
		Provisional: func(ctx context.Context, requestID behavior.CorrelationKey, status string, payload json.RawMessage, opts behavior.ProvisionalOptions) (behavior.RespondResult, error) {
			return a.doProvisional(ctx, requestID, status, payload, opts, sender)
		},
		EmitEvent: func(ctx context.Context, eventType string, payload json.RawMessage, opts behavior.EmitEventOptions) (message.ID, error) {
			return a.doEmitEvent(ctx, eventType, payload, opts, sender)
		},
		UpdateReadiness:        a.doUpdateReadiness,
		ForwardExternalRequest: a.forward, // installer-injected (nil for non-relay)
	}
}

// doRespond writes a FINAL terminal response (collapse of runRespondWithSender
// respond.go:340). On the cell goroutine: look up request → build response
// envelope → harness.Chain.Write → markDone. No policy CancelTimer (closure is
// caller-scoped); no router observeWrite (harness/trigger deliver to caller
// futures).
func (a *adapterActor) doRespond(ctx context.Context, requestID behavior.CorrelationKey, payload json.RawMessage, opts behavior.RespondOptions, sender message.Sender) (behavior.RespondResult, error) {
	if requestID == "" {
		return behavior.RespondResult{}, errors.New("adapterhost: Respond requestID required")
	}
	status := opts.Status
	if status == "" {
		status = "completed"
	}
	if !message.IsFinalStatus(status) {
		return behavior.RespondResult{}, fmt.Errorf("adapterhost: Respond status must be final; got %q (use Provisional)", status)
	}
	env, err := a.buildResponseEnvelope(ctx, requestID, status, opts.Reason, payload, opts.Dedupe, opts.Visibility, opts.Audience, sender)
	if err != nil {
		return behavior.RespondResult{}, err
	}
	res, err := a.chain.Write(ctx, env)
	if err != nil {
		return behavior.RespondResult{}, fmt.Errorf("adapterhost: respond chain write: %w", err)
	}
	if res.RejectReason != "" {
		if res.RejectReason == message.HarnessTerminalDuplicate {
			a.markDone(requestID) // adapter sees dedupe as business success
			return behavior.RespondResult{MessageID: res.MessageID, Deduped: true}, nil
		}
		return behavior.RespondResult{}, fmt.Errorf("adapterhost: respond rejected: %s (%s)", res.RejectReason, res.RejectDetail)
	}
	a.markDone(requestID)
	return behavior.RespondResult{MessageID: res.MessageID}, nil
}

// doProvisional writes a non-terminal interim response. Does NOT markDone — the
// request stays pending until a final Respond/Fail or caller timeout.
func (a *adapterActor) doProvisional(ctx context.Context, requestID behavior.CorrelationKey, status string, payload json.RawMessage, opts behavior.ProvisionalOptions, sender message.Sender) (behavior.RespondResult, error) {
	if status == "" {
		return behavior.RespondResult{}, errors.New("adapterhost: Provisional status required")
	}
	if message.IsFinalStatus(status) {
		return behavior.RespondResult{}, fmt.Errorf("adapterhost: Provisional status %q is final — use Respond/Fail", status)
	}
	env, err := a.buildResponseEnvelope(ctx, requestID, status, "", payload, false, opts.Visibility, opts.Audience, sender)
	if err != nil {
		return behavior.RespondResult{}, err
	}
	res, err := a.chain.Write(ctx, env)
	if err != nil {
		return behavior.RespondResult{}, fmt.Errorf("adapterhost: provisional chain write: %w", err)
	}
	if res.RejectReason != "" {
		return behavior.RespondResult{}, fmt.Errorf("adapterhost: provisional rejected: %s", res.RejectReason)
	}
	return behavior.RespondResult{MessageID: res.MessageID}, nil
}

// doEmitEvent emits one adapter-owned event through the harness.
func (a *adapterActor) doEmitEvent(ctx context.Context, eventType string, payload json.RawMessage, opts behavior.EmitEventOptions, sender message.Sender) (message.ID, error) {
	if eventType == "" {
		return "", errors.New("adapterhost: EmitEvent type required")
	}
	hash, err := message.CanonicalHashPayload(payload)
	if err != nil {
		return "", fmt.Errorf("adapterhost: emit hash: %w", err)
	}
	env := &message.Envelope{
		ID:         message.ID("event:" + eventType + ":" + hash),
		TS:         a.clock().UnixMilli(),
		ChannelID:  a.channelID,
		Sender:     sender,
		Kind:       message.KindEvent,
		Type:       eventType,
		Payload:    payload,
		Visibility: opts.Visibility,
		Audience:   opts.Audience,
	}
	res, err := a.chain.Write(ctx, env)
	if err != nil {
		return "", fmt.Errorf("adapterhost: emit chain write: %w", err)
	}
	if res.RejectReason != "" {
		return "", fmt.Errorf("adapterhost: emit rejected: %s", res.RejectReason)
	}
	return res.MessageID, nil
}

// doUpdateReadiness updates this adapter's own readiness (plain field, cell
// goroutine) and emits actor.readiness.changed so the registry projection
// follows (INVARIANT-0: state changes are envelope-observable).
func (a *adapterActor) doUpdateReadiness(ctx context.Context, update actor.ReadinessUpdate) (actor.ReadinessTransition, error) {
	prev := a.readiness
	next := actor.Readiness{
		State:             update.State,
		Reason:            update.Reason,
		Detail:            update.Detail,
		LastStateChangeAt: update.CheckedAt,
	}.Normalize()
	if next.State == prev.State {
		next.LastStateChangeAt = prev.LastStateChangeAt
	}
	if next.State == actor.ReadinessReady {
		next.LastReadyAt = update.CheckedAt
	} else {
		next.LastReadyAt = prev.LastReadyAt
	}
	a.readiness = next
	changed := prev.State != next.State
	if changed {
		payload, _ := json.Marshal(map[string]any{"state": string(next.State), "reason": next.Reason})
		_, _ = a.doEmitEvent(ctx, "actor.readiness.changed", payload, behavior.EmitEventOptions{},
			message.Sender{Kind: actor.KindTool, ID: a.self})
	}
	return actor.ReadinessTransition{Previous: prev, Current: next, Changed: changed}, nil
}

// buildResponseEnvelope assembles a kind=response envelope from the original
// request (shared by Respond/Fail/Provisional). Audience defaults to the
// request sender; visibility/correlation inherit from the request.
func (a *adapterActor) buildResponseEnvelope(ctx context.Context, requestID behavior.CorrelationKey, status, reason string, payload json.RawMessage, dedupe bool, vis message.Visibility, aud message.Audience, sender message.Sender) (*message.Envelope, error) {
	request, ok, err := a.lookup.FindByID(ctx, message.ID(requestID))
	if err != nil {
		return nil, fmt.Errorf("adapterhost: respond lookup %s: %w", requestID, err)
	}
	if !ok || request == nil {
		return nil, fmt.Errorf("adapterhost: respond request %s not found", requestID)
	}
	merged, err := mergeResponsePayload(payload, status, reason, dedupe)
	if err != nil {
		return nil, err
	}
	hash, err := message.CanonicalHashPayload(merged)
	if err != nil {
		return nil, fmt.Errorf("adapterhost: respond hash: %w", err)
	}
	if vis == "" {
		vis = request.Visibility
	}
	audience := message.Audience{request.Sender.ID}
	if len(aud) > 0 {
		if len(aud) != 1 || aud[0] != request.Sender.ID {
			return nil, fmt.Errorf("adapterhost: respond audience %v must equal parent sender %s", aud, request.Sender.ID)
		}
		audience = aud
	}
	correlationID := request.CorrelationID
	if correlationID == "" {
		correlationID = request.ID
	}
	return &message.Envelope{
		ID:            message.ID("response:" + requestID.String() + ":" + hash),
		TS:            a.clock().UnixMilli(),
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

// mergeResponsePayload merges the adapter's payload object with the
// framework-owned {status, reason} fields (collapse of mergeResponsePayload
// respond.go). payload must be a JSON object or empty.
func mergeResponsePayload(payload json.RawMessage, status, reason string, dedupe bool) (json.RawMessage, error) {
	m := map[string]json.RawMessage{}
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &m); err != nil {
			return nil, fmt.Errorf("adapterhost: response payload must be a JSON object: %w", err)
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
