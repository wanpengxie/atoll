package adapter

// respond.go implements L2 §8.5 — ctx.respond. The function looks up
// the original request envelope to reuse its type / correlation_id /
// sender, merges the user payload with {status, reason, ...detail},
// computes a deterministic envelope id, and writes through the
// harness. Same `(request_id, payload)` always lands on the same id so
// adapter network retries / framework boot recovery re-emits dedupe
// at harness Step 0.5.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/coagent-ai/daemon-go/pkg/canonical"
	pkgharness "github.com/coagent-ai/daemon-go/pkg/harness"
	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// respondConfig bundles everything the (ctx.Respond) closure needs.
// One per adapter — built inside Manager.bindModule.
type respondConfig struct {
	adapterName  string
	adapterActor string
	channelID    string
	binding      string

	writer  HarnessWriter
	store   pkgharness.Store
	clock   func() int64
	logger  Logger
	tracker *correlationTracker // for Forget after terminal commit
	policy  *timerPolicy        // for cancelTimer after terminal commit
}

// validate returns an error when a required field is missing.
func (c respondConfig) validate() error {
	switch {
	case strings.TrimSpace(c.adapterName) == "":
		return errors.New("respond: adapterName is required")
	case strings.TrimSpace(c.adapterActor) == "":
		return errors.New("respond: adapterActor is required")
	case strings.TrimSpace(c.channelID) == "":
		return errors.New("respond: channelID is required")
	case c.writer == nil:
		return errors.New("respond: writer is nil")
	case c.store == nil:
		return errors.New("respond: store is nil")
	case c.clock == nil:
		return errors.New("respond: clock is nil")
	}
	return nil
}

// respond is the framework-internal F5 entry point. It performs the
// full L2 §8.5 pipeline:
//
//  1. Look up the request row by id (provides type / correlation_id /
//     sender / channel scope).
//  2. Merge the user payload with {status, reason, ...detail}; refuse
//     when the user passes a non-object JSON payload (response schemas
//     are objects).
//  3. Compute the deterministic envelope id =
//     "response:<request_id>:<canonical_payload_hash>" per
//     L2 §1.4.10.2 application 3.
//  4. Build the response envelope (kind=response, sender=adapter actor,
//     audience=[request.sender.id], visibility=system).
//  5. Write through the harness. On success: cancel any registered F3
//     timer + forget the correlation entry for this request_id.
//  6. Map the WriteResult to RespondResult. terminal_duplicate on the
//     same id => Dedupe=true; on a different id => still Dedupe=true
//     (spec §8.5 race "另一路径已成功 ... adapter 视为业务成功").
func respond(
	ctx context.Context,
	cfg respondConfig,
	requestID string,
	payload json.RawMessage,
	opts RespondOptions,
) (RespondResult, error) {
	if err := cfg.validate(); err != nil {
		return RespondResult{}, err
	}
	if strings.TrimSpace(requestID) == "" {
		return RespondResult{}, errors.New("adapter: Respond requestID is required")
	}
	if !opts.Status.IsValid() {
		return RespondResult{}, fmt.Errorf("adapter: Respond opts.Status %q is invalid (must be completed|failed)", opts.Status)
	}

	// Step 1: look up the request envelope.
	request, err := cfg.store.FindByID(ctx, requestID)
	if err != nil {
		return RespondResult{}, fmt.Errorf("adapter: respond find request: %w", err)
	}
	if request == nil {
		return RespondResult{}, fmt.Errorf("adapter: Respond request %q not found", requestID)
	}
	if request.Kind != v4types.KindRequest {
		return RespondResult{}, fmt.Errorf("adapter: Respond request %q is kind=%q (expected request)", requestID, request.Kind)
	}
	if request.ChannelID != cfg.channelID {
		return RespondResult{}, fmt.Errorf("adapter: Respond request %q belongs to channel %q, manager bound to %q",
			requestID, request.ChannelID, cfg.channelID)
	}

	// Step 2-3: merge payload + compute deterministic id.
	merged, err := mergeRespondPayload(payload, opts)
	if err != nil {
		return RespondResult{}, fmt.Errorf("adapter: respond build payload: %w", err)
	}
	hash, err := canonical.CanonicalHashPayload(merged)
	if err != nil {
		return RespondResult{}, fmt.Errorf("adapter: respond hash payload: %w", err)
	}
	responseID := "response:" + requestID + ":" + hash

	// Step 4: build the response envelope.
	env := &v4types.Envelope{
		ID:        responseID,
		TS:        cfg.clock(),
		ChannelID: request.ChannelID,
		Sender: v4types.Sender{
			Kind: v4types.SenderTool,
			ID:   cfg.adapterActor,
		},
		Kind:          v4types.KindResponse,
		Type:          request.Type,
		Payload:       merged,
		Visibility:    v4types.VisibilitySystem,
		Audience:      []string{request.Sender.ID},
		ParentID:      requestID,
		CorrelationID: request.CorrelationID,
	}

	caller := pkgharness.CallerCtx{
		Authenticated:      true,
		ActorID:            cfg.adapterActor,
		DeclaredSenderKind: v4types.SenderTool,
	}
	if request.CorrelationID != "" {
		caller.Trigger = &pkgharness.TriggerCtx{CorrelationID: request.CorrelationID}
	}

	// Step 5: harness write.
	res, werr := cfg.writer.Write(ctx, env, caller)
	if werr != nil {
		return RespondResult{}, fmt.Errorf("adapter: respond write: %w", werr)
	}

	// Step 6: success / dedupe / terminal_duplicate handling.
	out, ok, mapped := mapWriteResult(res, responseID)
	if !ok {
		// Genuine non-dedupe reject — surface as error for caller.
		return RespondResult{}, mapped
	}

	// Cleanup: cancel registered timer + forget the correlation entry.
	// Best-effort; errors are observability-only.
	if cfg.policy != nil {
		cfg.policy.cancelTimer(requestID)
	}
	if cfg.tracker != nil {
		if ferr := cfg.tracker.Forget(ctx, requestID); ferr != nil {
			cfg.logger.Warn("adapter.respond.forget.error",
				"adapter", cfg.adapterName,
				"request_id", requestID,
				"err", ferr.Error(),
			)
		}
	}
	return out, nil
}

// mergeRespondPayload folds the user-supplied payload with the
// framework-mandated `status` + optional `reason` + optional detail.
//
// Layering order (later wins):
//
//  1. user payload (parsed as JSON object)
//  2. opts.Detail entries
//  3. opts.Status, opts.Reason
//
// An empty / nil user payload is treated as `{}`. Anything else MUST
// parse as a JSON object — the response payload schema baseline is
// `type: object` (see EnsureToolActors).
func mergeRespondPayload(payload json.RawMessage, opts RespondOptions) (json.RawMessage, error) {
	merged := map[string]any{}
	trimmed := strings.TrimSpace(string(payload))
	if trimmed != "" && trimmed != "null" {
		if err := json.Unmarshal(payload, &merged); err != nil {
			return nil, fmt.Errorf("payload must be a JSON object: %w", err)
		}
	}
	for k, v := range opts.Detail {
		merged[k] = v
	}
	merged["status"] = string(opts.Status)
	if opts.Reason != "" {
		merged["reason"] = opts.Reason
	}
	raw, err := json.Marshal(merged)
	if err != nil {
		return nil, fmt.Errorf("marshal merged payload: %w", err)
	}
	return raw, nil
}

// mapWriteResult converts a harness WriteResult into the RespondResult
// the framework hands callers. Returns (result, true, nil) on the
// happy path or dedupe; (zero, false, err) on a genuine reject the
// caller should observe.
//
// terminal_duplicate handling (L2 §8.5 race table):
//
//   - Same response id (we already wrote it) → idempotent success.
//   - Different response id (another emitter beat us with a different
//     payload) → still idempotent success — the spec says adapter
//     should treat "terminal 已经写入" as business success.
func mapWriteResult(res pkgharness.WriteResult, responseID string) (RespondResult, bool, error) {
	if res.OK {
		return RespondResult{
			ID:            res.Value.ID,
			CorrelationID: res.Value.CorrelationID,
			Dedupe:        res.Value.Dedupe,
		}, true, nil
	}
	// Reject path. Need to unwrap RejectError fields.
	if res.Error != nil && res.Error.Reason == v4types.HarnessTerminalDuplicate {
		winner := res.Error.DedupeResponseID
		if winner == "" {
			winner = responseID
		}
		return RespondResult{
			ID:     winner,
			Dedupe: true,
		}, true, nil
	}
	// Anything else — surface to caller as error.
	reason := "unknown"
	detail := "no error body"
	if res.Error != nil {
		reason = string(res.Error.Reason)
		detail = res.Error.Detail
	}
	return RespondResult{}, false, fmt.Errorf("adapter: respond rejected reason=%s detail=%s", reason, detail)
}
