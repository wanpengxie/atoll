package adapterhost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	rtharness "github.com/wanpengxie/ActOS/runtime/harness"
)

// Start builds the ModuleContext (closing over this cell) and runs module.Init
// on the cell goroutine. After Start the adapter may call mctx seams from its
// callbacks; all of them touch a.inflight/a.chain serially.
func (a *adapterActor) Start(ctx context.Context, _ actorrt.ActorContext) error {
	if a.inflight == nil {
		a.inflight = map[behavior.CorrelationKey]*message.Envelope{}
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
// goroutine and mutate a.inflight with no lock (collapse of buildRespond et al.
// — respond.go — into cell methods; the respondConfig god-bundle is gone).
func (a *adapterActor) buildModuleContext() *behavior.ModuleContext {
	sender := message.Sender{Kind: actor.KindTool, ID: a.self}
	return &behavior.ModuleContext{
		AdapterActorID: a.self,
		ChannelID:      a.channelID,
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
				Reason:     reason,
				Visibility: opts.Visibility,
			}, sender)
		},
		Provisional: func(ctx context.Context, requestID behavior.CorrelationKey, status string, payload json.RawMessage, opts behavior.ProvisionalOptions) (behavior.RespondResult, error) {
			return a.doProvisional(ctx, requestID, status, payload, opts, sender)
		},
		EmitEvent: func(ctx context.Context, eventType string, payload json.RawMessage, opts behavior.EmitEventOptions) (message.ID, error) {
			return a.doEmitEvent(ctx, eventType, payload, opts, sender)
		},
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
	env, err := a.buildResponse(ctx, requestID, sender, behavior.ResponseSpec{
		Status: status, Reason: string(opts.Reason), Payload: payload,
		Visibility: opts.Visibility,
	})
	if err != nil {
		return behavior.RespondResult{}, err
	}
	res, err := a.chain.Write(a.writeCtx(ctx), env)
	if err != nil {
		return behavior.RespondResult{}, fmt.Errorf("adapterhost: respond chain write: %w", err)
	}
	if res.RejectReason != "" {
		if res.RejectReason == rtharness.HarnessTerminalDuplicate {
			a.markDone(requestID) // adapter sees terminal-duplicate as business success
			return behavior.RespondResult{MessageID: res.MessageID}, nil
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
	env, err := a.buildResponse(ctx, requestID, sender, behavior.ResponseSpec{
		Status: status, Payload: payload, Visibility: opts.Visibility,
	})
	if err != nil {
		return behavior.RespondResult{}, err
	}
	res, err := a.chain.Write(a.writeCtx(ctx), env)
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
	env := &message.Envelope{
		ID:         message.ID(uuid.NewString()),
		TS:         a.clock().UnixMilli(),
		ChannelID:  a.channelID,
		Sender:     sender,
		Kind:       message.KindEvent,
		Type:       eventType,
		Payload:    payload,
		Visibility: opts.Visibility,
		Audience:   opts.Audience,
	}
	res, err := a.chain.Write(a.writeCtx(ctx), env)
	if err != nil {
		return "", fmt.Errorf("adapterhost: emit chain write: %w", err)
	}
	if res.RejectReason != "" {
		return "", fmt.Errorf("adapterhost: emit rejected: %s", res.RejectReason)
	}
	return res.MessageID, nil
}
