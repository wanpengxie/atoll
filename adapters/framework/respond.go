package framework

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// respondConfig is the bundle one RespondFunc closure captures.
type respondConfig struct {
	adapterName    string
	adapterActorID actor.ActorID
	channelID      channel.ID

	lookup      RequestLookup
	chain       harness.Chain
	correlation *memoryCorrelationTracker
	policy      *timerPolicy

	clock   func() time.Time
	logger  Logger
	metrics Metrics
}

// validate returns an error if a required field is missing.
func (c respondConfig) validate() error {
	switch {
	case c.adapterName == "":
		return errors.New("framework: respond adapterName required")
	case c.adapterActorID == "":
		return errors.New("framework: respond adapterActorID required")
	case c.channelID == "":
		return errors.New("framework: respond channelID required")
	case c.lookup == nil:
		return errors.New("framework: respond lookup required")
	case c.chain == nil:
		return errors.New("framework: respond chain required")
	case c.correlation == nil:
		return errors.New("framework: respond correlation required")
	case c.clock == nil:
		return errors.New("framework: respond clock required")
	}
	return nil
}

// buildRespond returns a closure satisfying kernel/adapter.RespondFunc.
// The closure performs the full L2 §8.5 ctx.respond pipeline:
//
//  1. Look up the request envelope by id.
//  2. Validate channel scope (request.channel_id == manager.channel_id).
//  3. Merge user payload with {status, reason} per kernel RespondOptions.
//  4. Compute deterministic envelope id = response:<request_id>:<hash>.
//  5. Build response envelope (kind=response, sender=adapter actor,
//     audience inherits from request.sender or override, visibility
//     inherits from request, parent_id=request_id,
//     correlation_id=request.correlation_id, type=request.type).
//  6. Write through harness.Chain. On success: MarkDone on correlation
//     tracker + CancelTimer on policy.
//  7. Map WriteResult to RespondResult — terminal_duplicate maps to
//     Deduped=true per spec ("adapter 视为业务成功").
func buildRespond(cfg respondConfig) (adapter.RespondFunc, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return func(ctx context.Context, requestID adapter.CorrelationKey, payload json.RawMessage, opts adapter.RespondOptions) (adapter.RespondResult, error) {
		return runRespond(ctx, cfg, requestID, payload, opts)
	}, nil
}

func runRespond(
	ctx context.Context,
	cfg respondConfig,
	requestID adapter.CorrelationKey,
	payload json.RawMessage,
	opts adapter.RespondOptions,
) (adapter.RespondResult, error) {
	if requestID == "" {
		return adapter.RespondResult{}, errors.New("framework: Respond requestID required")
	}

	request, ok, err := cfg.lookup.FindByID(ctx, requestID)
	if err != nil {
		return adapter.RespondResult{}, fmt.Errorf("framework: respond lookup %s: %w", requestID, err)
	}
	if !ok || request == nil {
		return adapter.RespondResult{}, fmt.Errorf("framework: respond request %s not found", requestID)
	}
	if request.ChannelID != string(cfg.channelID) {
		return adapter.RespondResult{}, fmt.Errorf("framework: respond channel mismatch: request=%s manager=%s",
			request.ChannelID, cfg.channelID)
	}

	status := opts.Status
	if status == "" {
		status = "completed"
	}

	mergedPayload, err := mergeResponsePayload(payload, status, opts.Reason, opts.Dedupe)
	if err != nil {
		return adapter.RespondResult{}, err
	}

	hash, err := message.CanonicalHashPayload(mergedPayload)
	if err != nil {
		return adapter.RespondResult{}, fmt.Errorf("framework: respond hash: %w", err)
	}
	envID := "response:" + requestID + ":" + hash

	visibility := opts.Visibility
	if visibility == "" {
		visibility = request.Visibility
	}

	audience := opts.Audience
	if len(audience) == 0 {
		audience = []string{string(request.Sender.ID)}
	} else {
		audience = append([]string(nil), audience...)
	}

	correlationID := request.CorrelationID
	if correlationID == "" {
		correlationID = request.ID
	}

	now := cfg.clock().UnixMilli()
	env := &message.Envelope{
		ID:            envID,
		TS:            now,
		ChannelID:     request.ChannelID,
		Sender:        message.Sender{Kind: actor.KindTool, ID: cfg.adapterActorID},
		Kind:          message.KindResponse,
		Type:          request.Type,
		Payload:       mergedPayload,
		ParentID:      requestID,
		CorrelationID: correlationID,
		Visibility:    visibility,
		Audience:      audience,
	}

	res, err := cfg.chain.Write(ctx, env)
	if err != nil {
		return adapter.RespondResult{}, fmt.Errorf("framework: respond chain write: %w", err)
	}

	result := adapter.RespondResult{MessageID: res.MessageID}
	switch {
	case res.Accepted() && res.Deduped:
		result.Deduped = true
	case res.RejectReason == message.HarnessTerminalDuplicate:
		// Spec §8.5: another path already wrote a different response —
		// adapter treats it as business success and surfaces the
		// existing id.
		result.Deduped = true
		if res.PartialMessageID != "" {
			result.MessageID = res.PartialMessageID
		}
		_ = cfg.correlation.MarkRejected(ctx, requestID, string(res.RejectReason))
		return result, nil
	case res.RejectReason != "":
		_ = cfg.correlation.MarkRejected(ctx, requestID, string(res.RejectReason))
		return result, fmt.Errorf("framework: respond rejected: %s (%s)", res.RejectReason, res.RejectDetail)
	}

	if err := cfg.correlation.MarkDone(ctx, requestID); err != nil {
		cfg.logger.Warn("framework.respond.mark_done.error",
			"adapter", cfg.adapterName,
			"request_id", requestID,
			"err", err.Error())
	}
	if cfg.policy != nil {
		_ = cfg.policy.CancelTimer(ctx, requestID)
	}
	cfg.metrics.IncCounter("adapter.respond.ok",
		"adapter", cfg.adapterName,
		"status", status,
		"deduped", boolStr(result.Deduped))
	return result, nil
}

// mergeResponsePayload returns a payload JSON object that contains the
// user-supplied payload's keys plus {status, reason?, dedupe?}.
//
// Rules:
//   - userPayload empty / null → start from empty object.
//   - userPayload non-object → reject (response payloads must be objects).
//   - status MUST land in the merged object (always non-empty).
//   - reason added only when non-empty; dedupe added only when true.
//   - status / reason always overwrite user keys ("framework wins").
func mergeResponsePayload(userPayload json.RawMessage, status, reason string, dedupe bool) (json.RawMessage, error) {
	var fields map[string]any
	if len(userPayload) == 0 || string(userPayload) == "null" {
		fields = map[string]any{}
	} else {
		if err := json.Unmarshal(userPayload, &fields); err != nil {
			return nil, fmt.Errorf("framework: response payload must be a JSON object: %w", err)
		}
		if fields == nil {
			fields = map[string]any{}
		}
	}
	fields["status"] = status
	if reason != "" {
		fields["reason"] = reason
	}
	if dedupe {
		fields["dedupe"] = true
	}
	out, err := json.Marshal(fields)
	if err != nil {
		return nil, fmt.Errorf("framework: marshal merged payload: %w", err)
	}
	return out, nil
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
