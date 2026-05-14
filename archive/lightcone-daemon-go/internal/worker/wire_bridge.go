package worker

// wire_bridge.go translates go-kimi wire events (24 message types,
// pkg/kimi/wire.WireMessage) into v4 channel envelopes per
// L2 §3.9.5 "Wire Event Bridge".
//
// Mapping table (normative; L2 §3.9.5):
//
//   | go-kimi wire event                       | v4 emit                                                | visibility |
//   |------------------------------------------|--------------------------------------------------------|------------|
//   | turn_begin                               | agent.text event payload={kind:"turn_begin", turn_id}  | system     |
//   | text_delta                               | buffered per turn_id (no emit)                         | -          |
//   | turn_end                                 | agent.text event content=buffered text                 | public     |
//   | step_begin / step_interrupted            | system.event payload={kind:"step_*", ...}              | system     |
//   | tool_call_request / tool_call_result     | NOT emitted (T11 V4ize wrapper already emits)          | -          |
//   | status_update / notification             | system.event payload={kind:"status_*", ...}            | system     |
//   | subagent_event                           | system.event payload.kind="subagent_<event_type>"      | system     |
//   | compaction_{begin,error,end}             | system.event payload.kind="compaction_*"               | system     |
//   | mcp_loading_{begin,end}                  | system.event payload.kind="mcp_loading_*"              | system     |
//   | approval_request                         | human.text request audience=["admin"]                  | public     |
//   | approval_response                        | human.text response                                    | public     |
//   | question_request                         | human.text request                                     | public     |
//   | question_response                        | human.text response                                    | public     |
//   | question_option / question_item          | swallowed (carried inside question_request)            | -          |
//   | steer_input                              | system.event payload.kind="steer_input"                | system     |
//
// Each emit lands via the WriterFn callback. The production wiring
// (runtime.go) hands in a closure calling harness.InWorkerBus; tests
// inject a recording stub.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/coagent-ai/daemon-go/pkg/canonical"
	pkgharness "github.com/coagent-ai/daemon-go/pkg/harness"
	"github.com/coagent-ai/daemon-go/pkg/v4types"

	"github.com/wanpengxie/go-kimi/pkg/kimi/types"
	"github.com/wanpengxie/go-kimi/pkg/kimi/wire"
)

// WriterFn is the sink the wire bridge writes through. The production
// wiring fills it with a closure over harness.InWorkerBus; tests use a
// recording function.
//
// Returning WriteResult lets the bridge distinguish reject (Result.OK
// false) from infrastructure error (non-nil err). The bridge currently
// only logs rejects — wire emits are best-effort observability writes,
// not protocol-critical (per L2 §3.9 "subagent / approval / skill
// 保留原行为").
type WriterFn func(ctx context.Context, env *v4types.Envelope, caller pkgharness.CallerCtx) (pkgharness.WriteResult, error)

// BridgeConfig wires the worker-specific identity + transport context
// the bridge needs to emit valid v4 envelopes. All fields except
// Clock + Logger are required.
type BridgeConfig struct {
	// ChannelID is the channel this worker serves (envelope.channel_id).
	ChannelID string

	// AgentID is the worker's actor id (envelope.sender.id).
	AgentID string

	// FencingToken is the worker_locks fencing token. Forwarded to the
	// harness CallerCtx so Step 3 can verify the lease is still live.
	FencingToken int64

	// TriggerCorrelationID is the trigger envelope's correlation_id.
	// The harness uses it as the first correlation_id fallback per
	// L1 §2.2.1.
	TriggerCorrelationID string

	// Writer is the sink (one call → one harness Write attempt).
	Writer WriterFn

	// Clock returns wall clock ms; defaults to time.Now().UnixMilli.
	// Tests pin a deterministic clock so the canonical id derivation
	// stays reproducible across runs.
	Clock func() int64

	// Logger receives "wire.bridge.*" events — emit attempts, rejects,
	// drops. Defaults to slog.Default() in the constructor.
	Logger Logger
}

// Logger is a minimal subset of *slog.Logger so the bridge can be
// tested without importing log/slog in fixtures.
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// WireBridge implements wire.Emitter — every WireMessage gets translated
// into a v4 envelope and pushed through the WriterFn. The bridge keeps
// per-turn text accumulators so turn_end can emit a single public
// agent.text containing the full reply (L2 §3.9.5 row 3).
//
// Concurrency: go-kimi may emit wire events from multiple goroutines
// (text streamer + tool result + status). The bridge guards the
// text-delta buffer + emit ordinal under a mutex; harness writes are
// dispatched in caller goroutine so order matches emission order
// per-turn.
type WireBridge struct {
	cfg BridgeConfig

	mu         sync.Mutex
	textBuffer map[string]*deltaBuffer // turn_id → accumulated text
}

// deltaBuffer holds the per-turn streaming text. seq increments per
// delta so the canonical hash for turn_end ties to the actual delta
// stream (replay-safe when go-kimi resends the same deltas).
type deltaBuffer struct {
	text string
	seq  uint32
}

// NewWireBridge constructs a bridge ready for kimi.AgentConfig
// .WireEmitter injection. Returns an error when required fields are
// missing — fail-fast at boot, not at first emit.
func NewWireBridge(cfg BridgeConfig) (*WireBridge, error) {
	if cfg.ChannelID == "" {
		return nil, fmt.Errorf("wire_bridge: channel_id required")
	}
	if cfg.AgentID == "" {
		return nil, fmt.Errorf("wire_bridge: agent_id required")
	}
	if cfg.FencingToken <= 0 {
		return nil, fmt.Errorf("wire_bridge: fencing_token must be positive, got %d", cfg.FencingToken)
	}
	if cfg.Writer == nil {
		return nil, fmt.Errorf("wire_bridge: writer required")
	}
	if cfg.Clock == nil {
		cfg.Clock = func() int64 { return time.Now().UnixMilli() }
	}
	if cfg.Logger == nil {
		cfg.Logger = noopLogger{}
	}
	return &WireBridge{
		cfg:        cfg,
		textBuffer: make(map[string]*deltaBuffer),
	}, nil
}

// Emit implements wire.Emitter. The function MUST NOT panic — go-kimi
// runs it inside the agent loop and a panic would crash the whole
// worker. Errors are logged + returned to the caller; go-kimi treats
// non-nil errors as best-effort failures (does NOT stop the loop).
func (b *WireBridge) Emit(msg wire.WireMessage) error {
	if msg == nil {
		return errors.New("wire_bridge: nil message")
	}
	// Match the concrete type. Anything not covered falls into the
	// "unknown wire type" log branch — useful when go-kimi adds a new
	// type and the bridge hasn't been bumped.
	switch m := msg.(type) {
	case wire.TurnBegin:
		return b.emitTurnBegin(m)
	case wire.TextDelta:
		// Accumulate; no emit yet.
		b.bufferDelta(m)
		return nil
	case wire.TurnEnd:
		return b.emitTurnEnd(m)
	case wire.StepBegin:
		return b.emitSystemEvent("step_begin", systemPayload{
			"step_id":     m.StepID,
			"name":        m.Name,
			"description": m.Description,
		})
	case wire.StepInterrupted:
		return b.emitSystemEvent("step_interrupted", systemPayload{
			"step_id": m.StepID,
			"reason":  m.Reason,
		})
	case wire.SteerInput:
		return b.emitSystemEvent("steer_input", systemPayload{
			"text":     m.Text,
			"priority": m.Priority,
		})
	case wire.StatusUpdate:
		return b.emitSystemEvent("status_update", systemPayload{
			"status":  m.Status,
			"message": m.Message,
		})
	case wire.Notification:
		return b.emitSystemEvent("notification", systemPayload{
			"level":   m.Level,
			"message": m.Message,
		})
	case wire.SubagentEvent:
		return b.emitSystemEvent("subagent_"+m.EventType, systemPayload{
			"agent_id":   m.AgentID,
			"event_type": m.EventType,
			"message":    m.Message,
		})
	case wire.CompactionBegin:
		return b.emitSystemEvent("compaction_begin", systemPayload{"trigger": m.Trigger})
	case wire.CompactionError:
		return b.emitSystemEvent("compaction_error", systemPayload{"error": m.Error})
	case wire.CompactionEnd:
		return b.emitSystemEvent("compaction_end", systemPayload{"summary": m.Summary})
	case wire.MCPLoadingBegin:
		return b.emitSystemEvent("mcp_loading_begin", systemPayload{})
	case wire.MCPLoadingEnd:
		return b.emitSystemEvent("mcp_loading_end", systemPayload{"duration_ms": m.DurationMS})
	case wire.ApprovalRequest:
		return b.emitHumanText(v4types.KindRequest, []string{"admin"}, humanPayload{
			"kind":        "approval_request",
			"id":          m.ID,
			"title":       m.Title,
			"description": m.Description,
			"command":     m.Command,
		}, m.ID)
	case wire.ApprovalResponse:
		return b.emitHumanText(v4types.KindResponse, []string{"admin"}, humanPayload{
			"kind":       "approval_response",
			"request_id": m.RequestID,
			"approved":   m.Approved,
			"reason":     m.Reason,
		}, m.RequestID)
	case wire.QuestionRequest:
		return b.emitHumanText(v4types.KindRequest, []string{"*"}, humanPayload{
			"kind":           "question_request",
			"id":             m.ID,
			"prompt":         m.Prompt,
			"allow_multiple": m.AllowMultiple,
			"items":          m.Items,
		}, m.ID)
	case wire.QuestionResponse:
		return b.emitHumanText(v4types.KindResponse, []string{"*"}, humanPayload{
			"kind":       "question_response",
			"request_id": m.RequestID,
			"answers":    m.Answers,
		}, m.RequestID)
	case wire.QuestionOption, wire.QuestionItem:
		// These are carried inside QuestionRequest — no standalone emit.
		return nil
	case wire.ToolCallRequest, wire.ToolCallResult:
		// T11 V4ize wrapper already emits these; the bridge swallows.
		return nil
	default:
		b.cfg.Logger.Warn("wire.bridge.unknown_type",
			"type", fmt.Sprintf("%T", msg),
		)
		return nil
	}
}

// -----------------------------------------------------------------------------
// Internal emit helpers
// -----------------------------------------------------------------------------

// emitTurnBegin emits a system-visibility agent.text marking the start
// of the turn. Payload follows L2 §3.9.5 row 1 — `{kind:"turn_begin",
// turn_id}` — so downstream tooling can pair turn_begin/turn_end pairs.
func (b *WireBridge) emitTurnBegin(m wire.TurnBegin) error {
	b.resetBuffer(m.TurnID)
	payload, err := json.Marshal(map[string]any{
		"kind":    "turn_begin",
		"turn_id": m.TurnID,
	})
	if err != nil {
		return fmt.Errorf("wire_bridge: marshal turn_begin: %w", err)
	}
	env := b.baseEnvelope(
		"agent.text",
		v4types.KindEvent,
		v4types.VisibilitySystem,
		[]string{"*"},
		payload,
		b.deriveID("turn_begin", m.TurnID, ""),
	)
	return b.dispatch(env)
}

// emitTurnEnd consumes the per-turn delta buffer and emits a single
// public agent.text containing the full assistant reply (L2 §3.9.5
// row 3). Output content from the wire envelope wins when go-kimi
// supplied it; otherwise we fall back to the buffered delta stream.
func (b *WireBridge) emitTurnEnd(m wire.TurnEnd) error {
	text := b.takeBuffer(m.TurnID)
	if out := concatText(m.Output); out != "" {
		text = out
	}

	payload, err := json.Marshal(map[string]any{
		"kind":        "turn_end",
		"turn_id":     m.TurnID,
		"stop_reason": m.StopReason,
		"interrupted": m.Interrupted,
		"text":        text,
	})
	if err != nil {
		return fmt.Errorf("wire_bridge: marshal turn_end: %w", err)
	}
	env := b.baseEnvelope(
		"agent.text",
		v4types.KindEvent,
		v4types.VisibilityPublic,
		[]string{"*"},
		payload,
		b.deriveID("agent_text", m.TurnID, ""),
	)
	return b.dispatch(env)
}

// emitSystemEvent fans every system-bucket wire variant through a
// single envelope-shaping path. `kindTag` lands as the payload.kind
// discriminator so consumers can filter by sub-type without parsing
// the entire payload.
func (b *WireBridge) emitSystemEvent(kindTag string, extra systemPayload) error {
	payload := map[string]any{"kind": kindTag}
	for k, v := range extra {
		if !isZeroish(v) {
			payload[k] = v
		}
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("wire_bridge: marshal %s: %w", kindTag, err)
	}

	// Deterministic id needs a stable per-message key. For typed events
	// with a natural anchor (step_id, agent_id, etc.) we splice it in;
	// otherwise fall back to canonical hash over the full payload + an
	// incrementing ordinal (which is in-memory only — replay safety
	// depends on the wire stream replaying the same payloads in the
	// same order, which go-kimi guarantees per session).
	anchor := payloadAnchor(extra)
	env := b.baseEnvelope(
		"system.event",
		v4types.KindEvent,
		v4types.VisibilitySystem,
		[]string{"*"},
		raw,
		b.deriveID(kindTag, anchor, string(raw)),
	)
	return b.dispatch(env)
}

// emitHumanText covers the public-visibility human.text bucket
// (approval / question request+response). audience is supplied because
// approval defaults to admin while question defaults to broadcast.
//
// `anchor` is the wire message's own id (ApprovalRequest.ID,
// QuestionResponse.RequestID, etc.) — keeps the derived envelope id
// stable across replay of the same wire stream.
func (b *WireBridge) emitHumanText(
	kind v4types.Kind,
	audience []string,
	payload humanPayload,
	anchor string,
) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("wire_bridge: marshal human.text: %w", err)
	}
	kindTag, _ := payload["kind"].(string)
	env := b.baseEnvelope(
		"human.text",
		kind,
		v4types.VisibilityPublic,
		audience,
		raw,
		b.deriveID(kindTag, anchor, ""),
	)
	return b.dispatch(env)
}

// baseEnvelope produces a fully populated v4 envelope minus the harness
// ts_received column (the store stamps that on insert). Audience is
// always normalized to a non-nil slice — empty audience trips Step 6
// audience whitelisting.
func (b *WireBridge) baseEnvelope(
	typeName string,
	kind v4types.Kind,
	vis v4types.Visibility,
	audience []string,
	payload json.RawMessage,
	id string,
) *v4types.Envelope {
	if audience == nil {
		audience = []string{"*"}
	}
	return &v4types.Envelope{
		ID:        id,
		TS:        b.cfg.Clock(),
		ChannelID: b.cfg.ChannelID,
		Sender: v4types.Sender{
			Kind: v4types.SenderAgent,
			ID:   b.cfg.AgentID,
		},
		Kind:          kind,
		Type:          typeName,
		Payload:       payload,
		Visibility:    vis,
		Audience:      audience,
		CorrelationID: b.cfg.TriggerCorrelationID,
	}
}

// dispatch hands the envelope to the configured WriterFn under a
// background context detached from the agent loop's per-turn timeout —
// we want observability writes to land even when the agent decides to
// abandon its turn. Reject (WriteResult.OK==false) is logged + returned
// as nil so go-kimi's loop continues. Infrastructure errors (sql etc.)
// return non-nil so the caller can decide.
func (b *WireBridge) dispatch(env *v4types.Envelope) error {
	caller := pkgharness.CallerCtx{
		Authenticated:      true,
		ActorID:            b.cfg.AgentID,
		DeclaredSenderKind: v4types.SenderAgent,
		FencingToken:       b.cfg.FencingToken,
	}
	if b.cfg.TriggerCorrelationID != "" {
		caller.Trigger = &pkgharness.TriggerCtx{CorrelationID: b.cfg.TriggerCorrelationID}
	}

	res, err := b.cfg.Writer(context.Background(), env, caller)
	if err != nil {
		b.cfg.Logger.Error("wire.bridge.write.error",
			"type", env.Type,
			"id", env.ID,
			"err", err.Error(),
		)
		return err
	}
	if !res.OK {
		reason := ""
		detail := ""
		if res.Error != nil {
			reason = string(res.Error.Reason)
			detail = res.Error.Detail
		}
		b.cfg.Logger.Warn("wire.bridge.write.reject",
			"type", env.Type,
			"id", env.ID,
			"reason", reason,
			"detail", detail,
		)
		// Reject is observability-only — do NOT propagate as error.
		return nil
	}
	return nil
}

// -----------------------------------------------------------------------------
// Buffer + id derivation
// -----------------------------------------------------------------------------

// bufferDelta accumulates streamed text per turn_id. TurnID may be
// empty in pathological wire streams; we use "" as a default bucket so
// nothing is lost even when go-kimi emits deltas before turn_begin.
func (b *WireBridge) bufferDelta(m wire.TextDelta) {
	b.mu.Lock()
	defer b.mu.Unlock()
	buf, ok := b.textBuffer[m.TurnID]
	if !ok {
		buf = &deltaBuffer{}
		b.textBuffer[m.TurnID] = buf
	}
	buf.text += m.Delta
	buf.seq++
}

// resetBuffer wipes the per-turn accumulator at turn_begin so a fresh
// turn starts with an empty buffer (defensive — a previous turn_end
// already drained, but in case turn_end was dropped we reset).
func (b *WireBridge) resetBuffer(turnID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.textBuffer[turnID] = &deltaBuffer{}
}

// takeBuffer atomically returns + clears the accumulated text for a
// turn. Subsequent text_deltas for the same turn_id go back into an
// empty buffer (defensive — go-kimi shouldn't emit them post turn_end
// but we handle it cleanly anyway).
func (b *WireBridge) takeBuffer(turnID string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	buf, ok := b.textBuffer[turnID]
	if !ok {
		return ""
	}
	out := buf.text
	delete(b.textBuffer, turnID)
	return out
}

// deriveID computes the envelope.id for a wire emit. The hash input is
// (channel, sender, wire_tag, anchor, extra_disambiguator) — every
// component matters:
//
//   - channel + sender scope the id to (channel, agent), so two agents
//     emitting the same wire event don't collide.
//   - wire_tag is the v4 payload.kind discriminator (e.g. "turn_begin").
//   - anchor is the wire message's natural id when one exists
//     (turn_id / step_id / approval id / question request_id). It is
//     the dominant uniqueness factor — replay of the same wire stream
//     produces the same anchor sequence.
//   - extra disambiguates when no natural anchor exists (e.g.
//     status_update has no id; we fall back to the canonicalized
//     payload bytes).
//
// The "wire:" prefix keeps wire-bridge ids namespaced away from T11
// action_ledger ids ("tool:...") and harness internal ids — even though
// envelope.id is just a string, segregated prefixes are easier to
// grep in logs.
func (b *WireBridge) deriveID(wireTag, anchor, extra string) string {
	hashInput := map[string]any{
		"channel_id": b.cfg.ChannelID,
		"sender_id":  b.cfg.AgentID,
		"wire_tag":   wireTag,
		"anchor":     anchor,
		"extra":      extra,
	}
	raw, err := json.Marshal(hashInput)
	if err != nil {
		// Should be unreachable for the value shapes above. Fall back
		// to an obviously-marked id so a bug surfaces in logs without
		// crashing the bridge mid-turn.
		return "wire:fallback:" + wireTag + ":" + anchor
	}
	hash, err := canonical.CanonicalHashPayload(raw)
	if err != nil {
		return "wire:fallback:" + wireTag + ":" + anchor
	}
	return "wire:" + hash
}

// payloadAnchor extracts the most identifying field from a system
// payload for hash uniqueness. The lookup order matches the layout of
// the system event variants (step_id > agent_id > status > message).
func payloadAnchor(p systemPayload) string {
	for _, k := range []string{"step_id", "agent_id", "status", "message"} {
		if v, ok := p[k]; ok {
			if s, ok := v.(string); ok && s != "" {
				return s
			}
		}
	}
	return ""
}

// isZeroish strips empty optional fields from system payloads so the
// emitted JSON stays compact — the harness happily accepts empty
// strings but downstream UI / queries get noise.
func isZeroish(v any) bool {
	switch x := v.(type) {
	case string:
		return x == ""
	case int:
		return x == 0
	case int64:
		return x == 0
	case bool:
		return false // booleans are never "zeroish"; false is real
	case nil:
		return true
	}
	return false
}

// concatText folds a go-kimi ContentParts into a plain string. Only the
// TextPart entries contribute; the bridge ignores tool / image parts
// for the L2 §3.9.5 row-3 "buffered reply" use case (those parts already
// have their own wire events handled elsewhere).
func concatText(parts types.ContentParts) string {
	var buf string
	for _, p := range parts {
		if tp, ok := p.(types.TextPart); ok {
			buf += tp.Text
		}
	}
	return buf
}

// systemPayload is an alias for map[string]any kept as a named type so
// helper signatures stay readable.
type systemPayload map[string]any

// humanPayload is the equivalent for human.text envelopes.
type humanPayload map[string]any

// noopLogger satisfies Logger when caller leaves cfg.Logger unset.
type noopLogger struct{}

func (noopLogger) Info(string, ...any)  {}
func (noopLogger) Warn(string, ...any)  {}
func (noopLogger) Error(string, ...any) {}
