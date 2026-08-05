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
//	③ output map  — the engine reports typed tool phases and exactly one
//	                terminal value/failure; the base selects Emit/Reply/Fail.
//
// Describe/Checkpoint are the会话记忆 + 自答 data the provider fills; the base
// owns their persistence/dispatch姿势.
type Engine interface {
	// Turn drives one round of reasoning over trigger, resolving any tool
	// calls internally, and writes its outputs to sink. It returns a non-nil
	// error ONLY for an unrecoverable plumbing failure. An engine/LLM error is
	// surfaced through Sink.Fail and returns nil so the actor stays alive.
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

// FinalValue is the full terminal value of one request-backed turn.
type FinalValue struct {
	Text string
	// NextAction is the turn-control hint ("done"/"failed"/"max_tokens"/
	// "continue"/…). Empty = omitted.
	NextAction string
	// Extra carries provider-specific full-value fields merged into the reply.
	Extra map[string]any
}

// Failure is a business failure. ErrorCode and Detail are written by Sys.Fail;
// substrate terminal reason remains owned by the response machinery.
type Failure struct {
	ErrorCode string
	Detail    string
}

// ToolActivity is a complete provider-reported tool phase identity.
type ToolActivity struct {
	CallID string
	Tool   string
	Status string
	Detail string
}

// Sink is the typed base-supplied output port. Providers never choose envelope
// kinds: tool phases become activity events, terminal values become Reply, and
// failures become Fail.
type Sink interface {
	ToolStarted(ToolActivity) error
	ToolEnded(ToolActivity) error
	Complete(FinalValue) error
	Fail(Failure) error
}
