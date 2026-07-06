package base

import (
	"context"

	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/message"
)

// Engine is the model-adaptation contract — the ONE thing a provider writes
// (spec §1 三件套 + describe/checkpoint data). Everything else (the Proc loop,
// the mailbox, response分拣, describe dispatch, per-turn checkpoint挂账) lives in
// the base skeleton. The three件套:
//
//	① Turn        — one round of engine reasoning (§1 回合弧). The engine's
//	                message-shape difference (claudecode 同步 SDK结构化消息 vs
//	                go-kimi 流式 wire帧状态机) is封在实现内; the base never sees it.
//	② tool access — see §2 S2: the base OUTPUTS the meta-tool MCP catalog
//	                (BuildMCPCatalog); how an engine ingests it (native MCP vs
//	                翻译成 AdditionalTools) is适配件内政, done inside the provider's
//	                Engine construction — NOT a method here.
//	③ output map  — the engine writes 0..n intermediate + 1 terminal Output to
//	                the Sink; the base maps each onto sys.Emit (§1 输出形).
//
// Describe/Checkpoint are the会话记忆 + 自答 data the provider fills; the base
// owns their persistence/dispatch姿势.
type Engine interface {
	// Turn drives one round of reasoning over trigger, resolving any tool
	// calls internally, and writes its outputs to sink. It returns a non-nil
	// error ONLY for an unrecoverable plumbing failure (the base propagates it
	// as loud死); an engine/LLM error is surfaced as a terminal Output (Final,
	// NextAction="failed") and returns nil so the actor stays alive.
	Turn(ctx context.Context, trigger Trigger, sink Sink) error

	// Describe returns the provider's actor.describe self-answer data. ActorID
	// is left to the base (it stamps sys.Self() — a self-answer must never
	// hardcode identity, the A2 fix); the provider fills Description/SkillDoc/
	// Types only.
	Describe() introspect.Describe

	// Checkpoint returns the durable resume seed to persist AFTER a turn
	// (session id等). nil = nothing to persist this turn. The base owns the
	// persistence姿势 (sys.State, per-turn); the provider only gives the bytes.
	Checkpoint() []byte

	// Close releases the engine's resources at incarnation death (the Proc's
	// defer). Called exactly once.
	Close() error
}

// Trigger is one turn's input: the trigger envelope, its correlation anchor,
// and the 1-based turn index the base stamps onto every emitted envelope. A
// provider threads Trigger into its own tool RuntimeContext (metatool.Trigger)
// — the curTurn/RuntimeContext 合一 (spec §1): one turn value passed as a Turn
// parameter, not two per-provider mutex/ctx threading mechanisms.
type Trigger struct {
	// Envelope is the trigger delivery — the provider builds its tool
	// RuntimeContext (parent id + correlation) from it.
	Envelope message.Envelope
	// CorrelationID is the resolved correlation root (trigger's own
	// correlation_id, falling back to its id).
	CorrelationID message.ID
	// Index is the 1-based turn number within this incarnation.
	Index int
}

// Output is one unit an engine produces during a turn — the unified输出形 the
// base maps onto sys.Emit. An engine may push 0..n intermediate outputs
// (Final=false) followed by exactly one terminal (Final=true). This is the
// MINIMAL UNION of the two providers' current behaviour (双线审 F7): claudecode
// emits zero intermediate (天然 no-op), go-kimi's per-tool-step progress rides
// the intermediate port. NOT an M4预留 — M4零预留红线不破.
type Output struct {
	// Final marks the turn's terminal output (vs an intermediate progress).
	Final bool
	// Text is the human-facing body.
	Text string
	// NextAction is the turn-control hint ("done"/"failed"/"max_tokens"/
	// "continue"/…). Empty = omitted.
	NextAction string
	// Reason is the failure bucket for a failed terminal ("llm_rate_limit"等).
	// Empty = omitted.
	Reason string
	// Extra carries provider-specific payload fields (step_index, tool_calls,
	// stop_reason, …) merged into the emitted payload.
	Extra map[string]any
}

// Sink is the base-supplied output port an engine writes a turn's outputs to.
// The one implementation is procSink (maps onto sys.Emit); a stub Engine's
// tests supply their own.
type Sink interface {
	// Emit writes one Output as an agent.text envelope addressed to the
	// trigger sender. A non-nil error is a plumbing failure (emit rejected /
	// write error) the engine SHOULD propagate out of Turn as loud死.
	Emit(o Output) error
}
