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
	declaration    adapter.Declaration

	lookup      RequestLookup
	chain       harness.Chain
	correlation *memoryCorrelationTracker
	policy      *timerPolicy

	clock   func() time.Time
	logger  Logger
	metrics Metrics
}

type terminalWriteFailure struct {
	op  string
	err error
}

func (e *terminalWriteFailure) Error() string {
	return fmt.Sprintf("framework: %s chain write: %v", e.op, e.err)
}

func (e *terminalWriteFailure) Unwrap() error {
	return e.err
}

func isTerminalWriteFailure(err error) bool {
	var target *terminalWriteFailure
	return errors.As(err, &target)
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
		return runRespondWithSender(ctx, cfg, requestID, payload, opts, message.Sender{
			Kind: actor.KindTool,
			ID:   cfg.adapterActorID,
		}, terminalValidationMode{
			requireDeclaredResponseType: true,
		})
	}, nil
}

func buildFail(respond adapter.RespondFunc) adapter.FailFunc {
	return func(ctx context.Context, requestID adapter.CorrelationKey, payload json.RawMessage, opts adapter.FailOptions) (adapter.RespondResult, error) {
		reason := opts.Reason
		if reason == "" {
			reason = message.TerminalReceiverInternalError
		}
		return respond(ctx, requestID, payload, adapter.RespondOptions{
			Status:     "failed",
			Reason:     string(reason),
			Visibility: opts.Visibility,
			Audience:   opts.Audience,
		})
	}
}

func buildCompleteExternalResponse(cfg respondConfig, decl adapter.Declaration) (adapter.ExternalResponseFunc, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return func(ctx context.Context, env *message.Envelope) (adapter.RespondResult, error) {
		if env == nil {
			return adapter.RespondResult{}, errors.New("framework: complete external response envelope nil")
		}
		if env.ID == "" {
			return adapter.RespondResult{}, errors.New("framework: complete external response id required")
		}
		if env.ChannelID == "" {
			return adapter.RespondResult{}, errors.New("framework: complete external response channel_id required")
		}
		if env.Kind != message.KindResponse {
			return adapter.RespondResult{}, fmt.Errorf("framework: complete external response kind=%s (must be response)", env.Kind)
		}
		if env.ParentID == "" {
			return adapter.RespondResult{}, errors.New("framework: complete external response parent_id required")
		}
		if env.Sender.Kind == "" || env.Sender.ID == "" {
			return adapter.RespondResult{}, errors.New("framework: complete external response sender required")
		}
		if env.CorrelationID == "" {
			return adapter.RespondResult{}, errors.New("framework: complete external response correlation_id required")
		}
		requestID := adapter.CorrelationKey(env.ParentID)
		request, _, err := validatePendingTerminalRequest(ctx, cfg, requestID, terminalValidationMode{
			requireDeclaredResponseType: true,
		})
		if err != nil {
			return adapter.RespondResult{}, fmt.Errorf("framework: complete external response parent invalid: %w", err)
		}
		if env.ChannelID != cfg.channelID {
			return adapter.RespondResult{}, fmt.Errorf("framework: complete external response channel mismatch: response=%s manager=%s",
				env.ChannelID, cfg.channelID)
		}
		if env.Sender.ID != cfg.adapterActorID || env.Sender.Kind != actor.KindTool {
			return adapter.RespondResult{}, fmt.Errorf("framework: complete external response sender mismatch: sender=(%s,%s) adapter=(%s,%s)",
				env.Sender.Kind, env.Sender.ID, actor.KindTool, cfg.adapterActorID)
		}
		if env.Type != request.Type {
			return adapter.RespondResult{}, fmt.Errorf("framework: complete external response type mismatch: response=%s request=%s",
				env.Type, request.Type)
		}
		if !declAllowsKind(decl, env.Type, message.KindResponse) {
			return adapter.RespondResult{}, fmt.Errorf("framework: complete external response type %s is not response-capable for adapter %s",
				env.Type, decl.Name)
		}
		if len(env.Audience) != 1 || env.Audience[0] != request.Sender.ID {
			return adapter.RespondResult{}, fmt.Errorf("framework: complete external response audience %v does not match parent sender %s",
				env.Audience, request.Sender.ID)
		}
		expectedCorrelation := request.CorrelationID
		if expectedCorrelation == "" {
			expectedCorrelation = request.ID
		}
		if env.CorrelationID != expectedCorrelation {
			return adapter.RespondResult{}, fmt.Errorf("framework: complete external response correlation mismatch: response=%s request=%s",
				env.CorrelationID, expectedCorrelation)
		}

		res, err := cfg.chain.Write(ctx, env)
		if err != nil {
			return adapter.RespondResult{}, &terminalWriteFailure{op: "complete external response", err: err}
		}
		result, completed, err := resultFromTerminalWrite(res)
		if err != nil {
			return result, fmt.Errorf("framework: complete external response rejected: %s (%s)", res.RejectReason, res.RejectDetail)
		}
		if completed {
			finishTerminalLifecycle(ctx, cfg, requestID)
			cfg.metrics.IncCounter("adapter.complete_external_response.ok",
				"adapter", cfg.adapterName, "deduped", boolStr(result.Deduped))
		}
		return result, nil
	}, nil
}

type terminalFallbackFunc func(
	ctx context.Context,
	requestID adapter.CorrelationKey,
	payload json.RawMessage,
	opts adapter.RespondOptions,
) (adapter.RespondResult, error)

// buildSynthesizedTerminalFallback returns the framework/runtime-owned
// terminal closure path. It intentionally does not satisfy or expose
// adapter.RespondFunc: adapter voluntary responses remain signed by the
// adapter actor, while synthesized fallback terminals are signed by the
// channel system actor.
func buildSynthesizedTerminalFallback(cfg respondConfig) (terminalFallbackFunc, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return func(ctx context.Context, requestID adapter.CorrelationKey, payload json.RawMessage, opts adapter.RespondOptions) (adapter.RespondResult, error) {
		return runRespondWithSender(ctx, cfg, requestID, payload, opts, message.Sender{
			Kind: actor.KindSystem,
			ID:   actor.SystemActorID,
		}, terminalValidationMode{
			allowReservedActorTypes: true,
		})
	}, nil
}

type terminalValidationMode struct {
	requireDeclaredResponseType bool
	allowReservedActorTypes     bool
}

func runRespondWithSender(
	ctx context.Context,
	cfg respondConfig,
	requestID adapter.CorrelationKey,
	payload json.RawMessage,
	opts adapter.RespondOptions,
	sender message.Sender,
	mode terminalValidationMode,
) (adapter.RespondResult, error) {
	if requestID == "" {
		return adapter.RespondResult{}, errors.New("framework: Respond requestID required")
	}

	request, _, err := validatePendingTerminalRequest(ctx, cfg, requestID, mode)
	if err != nil {
		return adapter.RespondResult{}, fmt.Errorf("framework: respond request invalid: %w", err)
	}

	status := opts.Status
	if status == "" {
		status = "completed"
	}
	if err := validateRespondReason(status, opts.Reason); err != nil {
		return adapter.RespondResult{}, err
	}

	mergedPayload, err := mergeResponsePayload(payload, status, opts.Reason, opts.Dedupe)
	if err != nil {
		return adapter.RespondResult{}, err
	}

	hash, err := message.CanonicalHashPayload(mergedPayload)
	if err != nil {
		return adapter.RespondResult{}, fmt.Errorf("framework: respond hash: %w", err)
	}
	envID := message.ID("response:" + requestID.String() + ":" + hash)

	visibility := opts.Visibility
	if visibility == "" {
		visibility = request.Visibility
	}

	audience := opts.Audience
	if len(audience) == 0 {
		audience = message.Audience{request.Sender.ID}
	} else {
		audience = append(message.Audience(nil), audience...)
		if len(audience) != 1 || audience[0] != request.Sender.ID {
			return adapter.RespondResult{}, fmt.Errorf("framework: respond audience %v does not match parent sender %s",
				audience, request.Sender.ID)
		}
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
		Sender:        sender,
		Kind:          message.KindResponse,
		Type:          request.Type,
		Payload:       mergedPayload,
		ParentID:      message.ID(requestID),
		CorrelationID: correlationID,
		Visibility:    visibility,
		Audience:      audience,
	}

	res, err := cfg.chain.Write(ctx, env)
	if err != nil {
		return adapter.RespondResult{}, &terminalWriteFailure{op: "respond", err: err}
	}

	result, completed, err := resultFromTerminalWrite(res)
	if err != nil {
		return result, fmt.Errorf("framework: respond rejected: %s (%s)", res.RejectReason, res.RejectDetail)
	}
	if completed {
		finishTerminalLifecycle(ctx, cfg, requestID)
	}
	cfg.metrics.IncCounter("adapter.respond.ok",
		"adapter", cfg.adapterName,
		"status", status,
		"deduped", boolStr(result.Deduped))
	return result, nil
}

func validatePendingTerminalRequest(
	ctx context.Context,
	cfg respondConfig,
	requestID adapter.CorrelationKey,
	mode terminalValidationMode,
) (*message.Envelope, adapter.CorrelationEntry, error) {
	request, ok, err := cfg.lookup.FindByID(ctx, message.ID(requestID))
	if err != nil {
		return nil, adapter.CorrelationEntry{}, fmt.Errorf("lookup %s: %w", requestID, err)
	}
	if !ok || request == nil {
		return nil, adapter.CorrelationEntry{}, fmt.Errorf("request %s not found", requestID)
	}
	if request.ID != message.ID(requestID) {
		return nil, adapter.CorrelationEntry{}, fmt.Errorf("request id mismatch: key=%s envelope=%s", requestID, request.ID)
	}
	if request.Kind != message.KindRequest {
		return nil, adapter.CorrelationEntry{}, fmt.Errorf("parent kind=%s (must be request)", request.Kind)
	}
	if request.ChannelID != cfg.channelID {
		return nil, adapter.CorrelationEntry{}, fmt.Errorf("channel mismatch: request=%s manager=%s", request.ChannelID, cfg.channelID)
	}
	if len(request.Audience) == 0 || request.Audience[0] != cfg.adapterActorID {
		return nil, adapter.CorrelationEntry{}, fmt.Errorf("parent audience %v does not target %s", request.Audience, cfg.adapterActorID)
	}
	if mode.requireDeclaredResponseType && !declAllowsKind(cfg.declaration, request.Type, message.KindResponse) {
		return nil, adapter.CorrelationEntry{}, fmt.Errorf("type %s is not response-capable for adapter %s", request.Type, cfg.declaration.Name)
	}
	if !mode.requireDeclaredResponseType && !declAllowsKind(cfg.declaration, request.Type, message.KindResponse) {
		if !(mode.allowReservedActorTypes && isReservedActorResponseType(request.Type)) {
			return nil, adapter.CorrelationEntry{}, fmt.Errorf("type %s is not response-capable for adapter %s", request.Type, cfg.declaration.Name)
		}
	}
	entry, ok, err := cfg.correlation.Get(ctx, requestID)
	if err != nil {
		return nil, adapter.CorrelationEntry{}, fmt.Errorf("correlation lookup %s: %w", requestID, err)
	}
	if !ok {
		return nil, adapter.CorrelationEntry{}, fmt.Errorf("no pending correlation for %s", requestID)
	}
	if entry.State != adapter.CorrelationPending {
		return nil, adapter.CorrelationEntry{}, fmt.Errorf("correlation %s state=%s (must be pending)", requestID, entry.State)
	}
	if entry.RequestID != requestID {
		return nil, adapter.CorrelationEntry{}, fmt.Errorf("correlation request_id mismatch: entry=%s request=%s", entry.RequestID, requestID)
	}
	if entry.ParentID != "" && entry.ParentID != request.ID {
		return nil, adapter.CorrelationEntry{}, fmt.Errorf("correlation parent mismatch: entry=%s request=%s", entry.ParentID, request.ID)
	}
	if entry.ChannelID != cfg.channelID {
		return nil, adapter.CorrelationEntry{}, fmt.Errorf("correlation channel mismatch: entry=%s manager=%s", entry.ChannelID, cfg.channelID)
	}
	if entry.AudienceActor != cfg.adapterActorID {
		return nil, adapter.CorrelationEntry{}, fmt.Errorf("correlation audience actor mismatch: entry=%s adapter=%s", entry.AudienceActor, cfg.adapterActorID)
	}
	if request.ExpiresAt != nil && entry.ExpiresAt != *request.ExpiresAt {
		return nil, adapter.CorrelationEntry{}, fmt.Errorf("correlation deadline mismatch: entry=%d request=%d", entry.ExpiresAt, *request.ExpiresAt)
	}
	return request, entry, nil
}

func isReservedActorResponseType(t string) bool {
	switch t {
	case "actor.status", "actor.describe":
		return true
	default:
		return false
	}
}

func resultFromTerminalWrite(res harness.WriteResult) (adapter.RespondResult, bool, error) {
	result := adapter.RespondResult{MessageID: res.MessageID}
	switch {
	case res.Accepted():
		result.Deduped = res.Deduped
		return result, true, nil
	case res.RejectReason == message.HarnessTerminalDuplicate:
		result.Deduped = true
		if res.PartialMessageID != "" {
			result.MessageID = res.PartialMessageID
		}
		return result, true, nil
	case res.RejectReason != "":
		return result, false, errors.New(string(res.RejectReason))
	default:
		return result, false, nil
	}
}

func finishTerminalLifecycle(ctx context.Context, cfg respondConfig, requestID adapter.CorrelationKey) {
	if err := cfg.correlation.MarkDone(ctx, requestID); err != nil {
		cfg.logger.Warn("framework.respond.mark_done.error",
			"adapter", cfg.adapterName,
			"request_id", requestID,
			"err", err.Error())
	}
	if cfg.policy != nil {
		_ = cfg.policy.CancelTimer(ctx, requestID)
	}
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

func validateRespondReason(status, reason string) error {
	if reason == "" {
		return nil
	}
	if status != "failed" {
		return fmt.Errorf("framework: RespondOptions.Reason requires status=failed (status=%q)", status)
	}
	for _, allowed := range message.AllTerminalFailureReasons {
		if reason == string(allowed) {
			return nil
		}
	}
	return fmt.Errorf("framework: RespondOptions.Reason %q not in terminal_failure_reason closed set", reason)
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
