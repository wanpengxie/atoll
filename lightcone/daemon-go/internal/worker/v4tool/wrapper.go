// Package v4tool implements the L2 §3.9.4 "v4 wrapper" mode that turns
// any go-kimi `tools.Tool` into a v4 tool actor — every Execute call
// writes a request + response pair to the channel log via the
// in_worker_bus harness binding (L2 §3.6.2). The wrapper does not
// touch go-kimi itself; the inner tool stays an opaque adapter that
// receives the same params it was built with.
//
// Lifecycle of one Execute (L2 §3.9.4 6-step spec):
//
//  1. ledger.Reserve mints (or replays) the deterministic envelope id
//     keyed on `(turn_id, semantic_action_key)`.
//  2. Emit kind=request via harness (caller_ctx = agent identity).
//  3. Call the inner go-kimi tool with the original params.
//  4. Emit kind=response, parent_id = request.id, sender = tool actor
//     identity (caller_ctx switched to tool actor).
//  5. ledger.Commit closes the action.
//  6. Return the ToolResult to go-kimi for the next LLM step.
//
// Hard contracts:
//   - request audience is always `[ToolActorID]` (kind=request requires
//     exactly one concrete receiver — harness Step 5);
//   - response sender is the tool actor (caller_ctx switched);
//   - both messages carry `visibility=system` so the channel log row
//     stays out of the default UI render (L3 §2.1);
//   - tool actor binding MUST be `in_worker_bus` (the wrapper does NOT
//     leave the worker process — it shares the same harness deps).
//
// Failure modes:
//   - Step 2 harness reject (audience unknown / type unknown / fencing
//     stale / ...) → wrap as `ToolResult{IsError=true,
//     Value={status:'failed', reason:'<harness_reject>', detail:'...'}}`
//     and return WITHOUT invoking the inner tool. The reject row is
//     never persisted (harness returned before INSERT).
//   - Step 3 inner.Execute error → still emit response with
//     `{status:'failed', reason:'tool_error', message:'<err.Error()>'}`
//     so the channel log captures the failure, and propagate the error
//     up to go-kimi.
//   - Step 4 response harness reject → log + return inner result;
//     channel log loses the response row but the agent loop continues
//     (matching the wire bridge's best-effort observability stance).
//
// Replay semantics (L2 §1.4.10.1 + §3.9.4 step 2 "若 dedupe，跳过实际
// 执行直接读旧 response"):
//   - When `ledger.Reserve.Replayed==true` the wrapper looks up the
//     existing response row by parent_id and rebuilds a ToolResult
//     from its payload — inner.Execute is NOT called. This is the
//     exactly-once guarantee for tool side effects within a turn.
//   - If the prior response row is missing (worker crashed AFTER
//     persisting the request but BEFORE persisting the response), the
//     wrapper falls back to calling inner.Execute fresh and re-emitting
//     the response — only "single side-effect per turn" is the spec
//     promise; "exactly two channel log rows" is not.
//   - When `ledger.Reserve.Replayed==false`, the wrapper takes the
//     full 6-step path (Reserve → emit request → inner.Execute → emit
//     response → Commit → return).
package v4tool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/coagent-ai/daemon-go/internal/ledger"
	"github.com/coagent-ai/daemon-go/pkg/canonical"
	pkgharness "github.com/coagent-ai/daemon-go/pkg/harness"
	"github.com/coagent-ai/daemon-go/pkg/v4types"

	"github.com/wanpengxie/go-kimi/pkg/kimi/tools"
	"github.com/wanpengxie/go-kimi/pkg/kimi/types"
)

// MaxResponsePayloadChars caps the JSON-serialised inner ToolResult
// before it lands in the response envelope. go-kimi already caps tool
// text output via tools.MaxOutputChars (50_000); we re-apply the cap on
// the marshalled payload to keep the channel log row size predictable.
const MaxResponsePayloadChars = 60_000

// Config wires the wrapper to the worker-owned harness deps + caller
// identity. All fields except Clock / NowSec / Logger are required.
type Config struct {
	// TypeName is the v4 envelope.type the wrapper emits (e.g. "fs.read").
	// MUST match a type_registry row whose handler_actor_id == ToolActorID.
	TypeName string

	// ToolActorID is the v4 sender.id of the tool actor (e.g.
	// "tool:fs.read"). MUST be registered in actor_registry as
	// actor_kind='tool', actor_binding='in_worker_bus'.
	ToolActorID string

	// CallerActorID is the agent that invokes the tool (the worker's
	// own agent_id). harness Step 3 verifies sender.id matches.
	CallerActorID string

	// ChannelID is the channel both messages land in.
	ChannelID string

	// FencingToken is the worker_locks fencing token. harness Step 3
	// rejects worker_fencing_stale when this no longer matches. The
	// response write (sender=tool) uses FencingToken=0 — tool actors
	// have no worker lease.
	FencingToken int64

	// TurnID seeds the ledger_key. Spec §3.9.3 prescribes
	// hash(actor_id, min_seq_in_batch); M1.3 baseline uses
	// `turn:<agent_id>:<trigger_msg_id>` (caller-provided).
	TurnID string

	// TriggerCorrelationID is the trigger envelope's correlation_id
	// (per L1 §2.2.1 the harness Step 0 normalize uses it as the first
	// correlation_id fallback).
	TriggerCorrelationID string

	// LedgerExec is the executor backing action_ledger Reserve/Commit
	// (typically the channel sqlite *sql.DB).
	LedgerExec ledger.Executor

	// Deps is the harness dependency bundle (Store/Actors/Types/...).
	Deps pkgharness.Deps

	// Clock returns the current wall-clock in milliseconds. Defaults
	// to time.Now().UnixMilli when nil.
	Clock func() int64

	// NowSec returns the current wall-clock in seconds (the unit
	// action_ledger persists). Defaults to time.Now().Unix when nil.
	NowSec func() int64

	// Logger receives "v4tool.wrapper.*" events. Defaults to a no-op
	// logger when nil.
	Logger Logger
}

// Logger is the minimal slog-ish surface used by the wrapper. It mirrors
// the worker package's Logger interface so callers can hand in the same
// logger they pass to the wire bridge.
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// V4ize wraps `inner` so every Execute round-trips through the v4
// harness. The returned tools.Tool exposes `Name() = cfg.TypeName` and
// `Description() = inner.Description()` so the LLM tool catalogue keeps
// its prompt copy while the on-wire identity follows the v4 namespace.
//
// Returns an error when cfg is missing a required field — fail-fast at
// boot, not at first Execute.
func V4ize(inner tools.Tool, cfg Config) (tools.Tool, error) {
	if inner == nil {
		return nil, errors.New("v4tool: inner tool is nil")
	}
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	if cfg.Clock == nil {
		cfg.Clock = func() int64 { return time.Now().UnixMilli() }
	}
	if cfg.NowSec == nil {
		cfg.NowSec = func() int64 { return time.Now().Unix() }
	}
	if cfg.Logger == nil {
		cfg.Logger = noopLogger{}
	}
	return &wrapper{inner: inner, cfg: cfg}, nil
}

// wrapper is the concrete v4 wrapper. It implements tools.Tool by
// forwarding metadata accessors to `inner` and replacing Execute with
// the 6-step v4 round-trip.
type wrapper struct {
	inner tools.Tool
	cfg   Config
}

// Name returns the v4 type name (the wrapper's primary identity).
func (w *wrapper) Name() string { return w.cfg.TypeName }

// Description forwards to the inner tool so the LLM still sees the
// original go-kimi documentation.
func (w *wrapper) Description() string { return w.inner.Description() }

// ParameterSchema forwards to the inner tool — params shape never
// changes through wrapping.
func (w *wrapper) ParameterSchema() json.RawMessage { return w.inner.ParameterSchema() }

// Execute runs the L2 §3.9.4 6-step pipeline.
//
// The function never panics — every harness / ledger / inner tool error
// is converted into a returned (ToolResult, error) pair. go-kimi treats
// `IsError=true` results as observable but recoverable, so the LLM keeps
// running.
func (w *wrapper) Execute(ctx context.Context, params json.RawMessage) (types.ToolResult, error) {
	normalised := normaliseParams(params)

	// ---- Step 1: ledger.Reserve -------------------------------------
	ledgerKey, kerr := computeLedgerKey(w.cfg.TurnID, w.cfg.TypeName, normalised)
	if kerr != nil {
		return w.makeErrorResult("ledger_key_compute_failed", kerr.Error()), kerr
	}
	reserve, rerr := ledger.Reserve(ctx, w.cfg.LedgerExec, ledgerKey, w.cfg.TurnID, w.cfg.CallerActorID, w.cfg.NowSec(), ledger.Options{})
	if rerr != nil {
		w.cfg.Logger.Error("v4tool.wrapper.ledger_reserve",
			"type", w.cfg.TypeName, "err", rerr.Error())
		return w.makeErrorResult("ledger_reserve_failed", rerr.Error()), rerr
	}
	requestID := reserve.EnvelopeID

	// ---- Replay fast path -------------------------------------------
	// Reserve handed back an existing envelope_id: the prior turn
	// already reached at least Step 2. Skip everything that has a side
	// effect (request emit, inner.Execute, response emit) and replay
	// the persisted response. Falls through to the full pipeline when
	// the response row is missing (crash between request + response).
	if reserve.Replayed {
		if result, ok, lerr := w.replayFromPriorResponse(ctx, requestID); lerr != nil {
			w.cfg.Logger.Warn("v4tool.wrapper.replay.lookup",
				"type", w.cfg.TypeName, "id", requestID, "err", lerr.Error())
		} else if ok {
			// Re-commit defensively — Commit is idempotent and the
			// previous turn may have crashed between response write +
			// commit.
			if cerr := ledger.Commit(ctx, w.cfg.LedgerExec, ledgerKey, w.cfg.NowSec()); cerr != nil {
				w.cfg.Logger.Warn("v4tool.wrapper.replay.commit",
					"type", w.cfg.TypeName, "ledger_key", ledgerKey, "err", cerr.Error())
			}
			return result, nil
		}
		w.cfg.Logger.Info("v4tool.wrapper.replay.fallthrough",
			"type", w.cfg.TypeName, "id", requestID,
			"reason", "prior response row not found; re-running inner tool")
	}

	// ---- Step 2: emit kind=request ----------------------------------
	reqEnv := w.buildRequestEnvelope(requestID, normalised)
	reqRes, reqErr := pkgharness.InWorkerBus(ctx, w.cfg.Deps, reqEnv, w.callerCtxAsAgent())
	if reqErr != nil {
		// Infrastructure failure: sql / ctx done. Surface verbatim so
		// the agent loop can decide.
		w.cfg.Logger.Error("v4tool.wrapper.request.infra",
			"type", w.cfg.TypeName, "id", requestID, "err", reqErr.Error())
		return w.makeErrorResult("harness_infra_error", reqErr.Error()), reqErr
	}
	if !reqRes.OK {
		// Protocol reject — never reached the inner tool. Channel log
		// did NOT receive the row (Step 8 ran neither). Return the
		// reject as an error ToolResult so the LLM observes it.
		reason, detail := unwrapReject(reqRes.Error)
		w.cfg.Logger.Warn("v4tool.wrapper.request.reject",
			"type", w.cfg.TypeName, "id", requestID, "reason", reason, "detail", detail)
		return w.makeErrorResult(reason, detail), nil
	}

	// ---- Step 3: call inner go-kimi tool ----------------------------
	innerResult, innerErr := w.inner.Execute(ctx, normalised)

	// ---- Step 4: emit kind=response ---------------------------------
	respEnv := w.buildResponseEnvelope(requestID, innerResult, innerErr)
	respRes, respWErr := pkgharness.InWorkerBus(ctx, w.cfg.Deps, respEnv, w.callerCtxAsTool())
	switch {
	case respWErr != nil:
		w.cfg.Logger.Error("v4tool.wrapper.response.infra",
			"type", w.cfg.TypeName, "id", respEnv.ID, "err", respWErr.Error())
	case !respRes.OK:
		reason, detail := unwrapReject(respRes.Error)
		w.cfg.Logger.Warn("v4tool.wrapper.response.reject",
			"type", w.cfg.TypeName, "id", respEnv.ID, "reason", reason, "detail", detail)
	}

	// ---- Step 5: ledger.Commit --------------------------------------
	if cerr := ledger.Commit(ctx, w.cfg.LedgerExec, ledgerKey, w.cfg.NowSec()); cerr != nil {
		// Commit failure is observability-only — the message rows are
		// already canonical. Log + continue.
		w.cfg.Logger.Warn("v4tool.wrapper.ledger_commit",
			"type", w.cfg.TypeName, "ledger_key", ledgerKey, "err", cerr.Error())
	}

	// ---- Step 6: return to go-kimi ----------------------------------
	return innerResult, innerErr
}

// -----------------------------------------------------------------------------
// Envelope construction
// -----------------------------------------------------------------------------

// buildRequestEnvelope assembles the kind=request envelope. id is the
// envelope_id Reserve returned. visibility is fixed to `system` so the
// row does not flood the default UI (L3 §2.1).
func (w *wrapper) buildRequestEnvelope(id string, payload json.RawMessage) *v4types.Envelope {
	return &v4types.Envelope{
		ID:        id,
		TS:        w.cfg.Clock(),
		ChannelID: w.cfg.ChannelID,
		Sender: v4types.Sender{
			Kind: v4types.SenderAgent,
			ID:   w.cfg.CallerActorID,
		},
		Kind:          v4types.KindRequest,
		Type:          w.cfg.TypeName,
		Payload:       payload,
		Visibility:    v4types.VisibilitySystem,
		Audience:      []string{w.cfg.ToolActorID},
		CorrelationID: w.cfg.TriggerCorrelationID,
	}
}

// buildResponseEnvelope assembles the kind=response envelope. id is
// derived deterministically from the request id + canonical hash over
// the payload (L2 §1.4.10.2 application 3).
func (w *wrapper) buildResponseEnvelope(requestID string, inner types.ToolResult, innerErr error) *v4types.Envelope {
	payload := buildResponsePayload(inner, innerErr)
	id := deriveResponseID(requestID, payload)
	return &v4types.Envelope{
		ID:        id,
		TS:        w.cfg.Clock(),
		ChannelID: w.cfg.ChannelID,
		Sender: v4types.Sender{
			Kind: v4types.SenderTool,
			ID:   w.cfg.ToolActorID,
		},
		Kind:          v4types.KindResponse,
		Type:          w.cfg.TypeName,
		Payload:       payload,
		Visibility:    v4types.VisibilitySystem,
		Audience:      []string{w.cfg.CallerActorID},
		ParentID:      requestID,
		CorrelationID: w.cfg.TriggerCorrelationID,
	}
}

// callerCtxAsAgent is the caller_ctx used for the request write — the
// wrapping agent's identity + worker fencing.
func (w *wrapper) callerCtxAsAgent() pkgharness.CallerCtx {
	cc := pkgharness.CallerCtx{
		Authenticated:      true,
		ActorID:            w.cfg.CallerActorID,
		DeclaredSenderKind: v4types.SenderAgent,
		FencingToken:       w.cfg.FencingToken,
	}
	if w.cfg.TriggerCorrelationID != "" {
		cc.Trigger = &pkgharness.TriggerCtx{CorrelationID: w.cfg.TriggerCorrelationID}
	}
	return cc
}

// callerCtxAsTool is the caller_ctx used for the response write —
// the tool actor's identity. No fencing (tool actors hold no worker
// lease).
func (w *wrapper) callerCtxAsTool() pkgharness.CallerCtx {
	cc := pkgharness.CallerCtx{
		Authenticated:      true,
		ActorID:            w.cfg.ToolActorID,
		DeclaredSenderKind: v4types.SenderTool,
	}
	if w.cfg.TriggerCorrelationID != "" {
		cc.Trigger = &pkgharness.TriggerCtx{CorrelationID: w.cfg.TriggerCorrelationID}
	}
	return cc
}

// -----------------------------------------------------------------------------
// Payload helpers
// -----------------------------------------------------------------------------

// buildResponsePayload encodes the inner tool's outcome into the
// `{status, value | error}` shape every L2 §1.4.2 response schema
// accepts. Failures land as `{status:'failed', reason:'tool_error',
// message:'<text>'}` so the harness's fallback-schema check (which
// every request-allowed type passes) accepts them.
func buildResponsePayload(inner types.ToolResult, innerErr error) json.RawMessage {
	payload := map[string]any{}
	switch {
	case innerErr != nil:
		payload["status"] = "failed"
		payload["reason"] = "tool_error"
		payload["message"] = innerErr.Error()
	case inner.IsError:
		payload["status"] = "failed"
		payload["reason"] = "tool_error"
		// inner.Value carries the structured error from the tool — keep
		// it for observability.
		payload["value"] = inner.Value.Value
	default:
		payload["status"] = "completed"
		payload["value"] = inner.Value.Value
	}
	if inner.Name != "" {
		payload["tool_name"] = inner.Name
	}
	if inner.ToolCallID != "" {
		payload["tool_call_id"] = inner.ToolCallID
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		// Should be unreachable — every value above is JSON-friendly.
		// Surface a minimal payload so the harness still has something
		// schema-valid to persist.
		fallback, _ := json.Marshal(map[string]any{
			"status":  "failed",
			"reason":  "marshal_error",
			"message": err.Error(),
		})
		return fallback
	}
	return clampPayload(raw)
}

// clampPayload trims the marshalled response payload when it exceeds
// MaxResponsePayloadChars. We replace the `value` field with a short
// truncation notice; status / reason stay intact so downstream schema
// validation never rejects the trimmed row.
func clampPayload(raw json.RawMessage) json.RawMessage {
	if len(raw) <= MaxResponsePayloadChars {
		return raw
	}
	var parsed map[string]any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		// raw is malformed — fall back to a minimal envelope.
		fallback, _ := json.Marshal(map[string]any{
			"status":  "completed",
			"reason":  "truncated",
			"message": fmt.Sprintf("payload exceeded %d bytes and could not be parsed for trim", MaxResponsePayloadChars),
		})
		return fallback
	}
	parsed["value"] = map[string]any{
		"truncated": true,
		"reason":    fmt.Sprintf("payload exceeded %d bytes; original value dropped", MaxResponsePayloadChars),
	}
	trimmed, err := json.Marshal(parsed)
	if err != nil {
		// Truly should not happen given the controlled shape.
		fallback, _ := json.Marshal(map[string]any{
			"status": "completed",
			"reason": "truncated",
		})
		return fallback
	}
	return trimmed
}

// normaliseParams gives Reserve / canonical_hash a stable input even
// when the LLM sends `null` or empty bytes. The harness already does the
// same `payload={}` upgrade in Step 0a, but we mirror it here so the
// ledger_key the wrapper computes equals the one harness Step 0.5 sees.
func normaliseParams(p json.RawMessage) json.RawMessage {
	trimmed := strings.TrimSpace(string(p))
	if trimmed == "" || trimmed == "null" {
		return json.RawMessage(`{}`)
	}
	return p
}

// -----------------------------------------------------------------------------
// Key + id derivation
// -----------------------------------------------------------------------------

// computeLedgerKey implements L2 §1.4.10.2 application 2: SHA-256 over
// the canonical encoding of `{turn_id, semantic_action_key}` where the
// semantic key is `<type>:<canonical_hash(params)>`. The same caller
// running the same turn with the same params lands on the same key,
// even across worker restarts.
func computeLedgerKey(turnID, typeName string, params json.RawMessage) (string, error) {
	paramHash, err := canonical.CanonicalHashPayload(params)
	if err != nil {
		return "", fmt.Errorf("hash params: %w", err)
	}
	semanticKey := typeName + ":" + paramHash
	keyDoc, err := json.Marshal(map[string]any{
		"turn_id":             turnID,
		"semantic_action_key": semanticKey,
	})
	if err != nil {
		return "", fmt.Errorf("marshal key doc: %w", err)
	}
	hash, err := canonical.CanonicalHashPayload(keyDoc)
	if err != nil {
		return "", fmt.Errorf("hash key doc: %w", err)
	}
	return hash, nil
}

// deriveResponseID implements L2 §1.4.10.2 application 3:
// `response:<request_id>:<canonical_hash(payload)>`. Same payload +
// same request → same id, so retries collapse via harness Step 0.5
// without manual dedupe in the wrapper.
func deriveResponseID(requestID string, payload json.RawMessage) string {
	hash, err := canonical.CanonicalHashPayload(payload)
	if err != nil {
		// Canonical hash of a JSON-validated payload should not fail.
		// Fall back to a stable-but-readable id so logs still link
		// request to response.
		return "response:" + requestID + ":hash-error"
	}
	return "response:" + requestID + ":" + hash
}

// -----------------------------------------------------------------------------
// Error-result helpers
// -----------------------------------------------------------------------------

// makeErrorResult builds the ToolResult returned to go-kimi when the
// wrapper itself fails (harness reject, ledger error). The shape mirrors
// the response payload's failed-branch fields so downstream code can
// treat wrapper-level + tool-level failures uniformly.
func (w *wrapper) makeErrorResult(reason, detail string) types.ToolResult {
	return types.ToolResult{
		Name:    w.cfg.TypeName,
		IsError: true,
		Value: types.ToolReturnValue{Value: map[string]any{
			"status":  "failed",
			"reason":  reason,
			"message": detail,
		}},
	}
}

// replayFromPriorResponse hunts the channel sqlite for the response
// row whose `parent_id == requestID` and reconstructs a ToolResult
// from its payload. Returns (result, true, nil) on hit;
// (zero, false, nil) when no response row exists yet; (zero, false,
// err) on sql error.
//
// Implementation note: this is the one place in the wrapper that
// reaches outside the harness contract — we need a "find response by
// parent" query and pkg/harness.Store doesn't expose one. Using
// cfg.LedgerExec directly keeps the wrapper colocated with the same
// sqlite handle the rest of the worker shares, at the cost of one
// hard sqlite dependency. M1.3 baseline accepts the coupling; a future
// generalisation can promote the query into Store.
func (w *wrapper) replayFromPriorResponse(
	ctx context.Context, requestID string,
) (types.ToolResult, bool, error) {
	row := w.cfg.LedgerExec.QueryRowContext(ctx,
		`SELECT payload FROM messages
		  WHERE kind = 'response' AND parent_id = ?
		  ORDER BY seq ASC
		  LIMIT 1`, requestID)
	var payload string
	if err := row.Scan(&payload); err != nil {
		// sql.ErrNoRows surfaces as Scan err — treat as "no response
		// yet" without bubbling the error.
		if isNoRows(err) {
			return types.ToolResult{}, false, nil
		}
		return types.ToolResult{}, false, fmt.Errorf("v4tool: replay query: %w", err)
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(payload), &parsed); err != nil {
		return types.ToolResult{}, false, fmt.Errorf("v4tool: replay parse payload: %w", err)
	}
	res := types.ToolResult{
		Name:  w.cfg.TypeName,
		Value: types.ToolReturnValue{Value: parsed["value"]},
	}
	if status, _ := parsed["status"].(string); status == "failed" {
		res.IsError = true
	}
	return res, true, nil
}

// isNoRows reports whether err is the canonical "no row" error from
// the database/sql layer. We string-match because action_ledger does
// the same trick to avoid importing the sql.ErrNoRows sentinel into
// every helper.
func isNoRows(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "sql: no rows in result set")
}

// unwrapReject extracts the reason / detail strings from a harness
// RejectError pointer, defensively handling nils.
func unwrapReject(rerr *pkgharness.RejectError) (string, string) {
	if rerr == nil {
		return "harness_reject", "harness returned no reject detail"
	}
	return string(rerr.Reason), rerr.Detail
}

// -----------------------------------------------------------------------------
// Config validation
// -----------------------------------------------------------------------------

func validateConfig(cfg Config) error {
	switch {
	case strings.TrimSpace(cfg.TypeName) == "":
		return errors.New("v4tool: cfg.TypeName is required")
	case strings.TrimSpace(cfg.ToolActorID) == "":
		return errors.New("v4tool: cfg.ToolActorID is required")
	case strings.TrimSpace(cfg.CallerActorID) == "":
		return errors.New("v4tool: cfg.CallerActorID is required")
	case strings.TrimSpace(cfg.ChannelID) == "":
		return errors.New("v4tool: cfg.ChannelID is required")
	case strings.TrimSpace(cfg.TurnID) == "":
		return errors.New("v4tool: cfg.TurnID is required")
	case cfg.LedgerExec == nil:
		return errors.New("v4tool: cfg.LedgerExec is required")
	}
	if err := validateDeps(cfg.Deps); err != nil {
		return fmt.Errorf("v4tool: cfg.Deps: %w", err)
	}
	return nil
}

// validateDeps is the wrapper-side mirror of harness.validateDeps. We
// duplicate the check so a missing dep fails at V4ize time instead of
// every Execute.
func validateDeps(d pkgharness.Deps) error {
	if d.Store == nil {
		return errors.New("store is nil")
	}
	if d.Actors == nil {
		return errors.New("actors lookup is nil")
	}
	if d.Types == nil {
		return errors.New("types lookup is nil")
	}
	if d.Dispatcher == nil {
		return errors.New("dispatcher is nil")
	}
	if d.Clock == nil {
		return errors.New("clock is nil")
	}
	if strings.TrimSpace(d.ChannelID) == "" {
		return errors.New("channel_id is empty")
	}
	return nil
}

// noopLogger is the default logger when Config.Logger is left nil. It
// matches the worker package's pattern so callers can hand the same
// instance to wrapper + wire bridge.
type noopLogger struct{}

func (noopLogger) Info(string, ...any)  {}
func (noopLogger) Warn(string, ...any)  {}
func (noopLogger) Error(string, ...any) {}
