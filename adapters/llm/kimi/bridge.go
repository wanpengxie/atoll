package kimi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gokimi "github.com/wanpengxie/go-kimi/pkg/kimi"
	"github.com/wanpengxie/go-kimi/pkg/kimi/config"
	kimierrors "github.com/wanpengxie/go-kimi/pkg/kimi/errors"
	"github.com/wanpengxie/go-kimi/pkg/kimi/llm"
	kimisession "github.com/wanpengxie/go-kimi/pkg/kimi/session"
	"github.com/wanpengxie/go-kimi/pkg/kimi/wire"

	// Force-register the anthropic provider factory. go-kimi's factory
	// uses init() side-effects to populate its provider constructor map;
	// without this import the DeepSeek anthropic-compat configuration
	// surfaces as "provider not found" at NewAgent time.
	_ "github.com/wanpengxie/go-kimi/pkg/kimi/llm/anthropic"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// Env keys read at Config.NewFromEnv time. Kept exported so tests and
// cmd/worker share one source of truth for the env contract.
const (
	EnvKeyAPIKey  = "KIMI_API_KEY"
	EnvKeyBaseURL = "KIMI_BASE_URL"
	EnvKeyModel   = "KIMI_MODEL"

	// EnvKeyChannelType / EnvKeyDomainPrompt are inherited from the
	// worker spawn env (M1.6-T5 phase-3). They feed the prompt-cache
	// friendly base prompt (L4 §2.4 + L0-L2 platform teaching).
	EnvKeyChannelType  = "COAGENT_CHANNEL_TYPE"
	EnvKeyDomainPrompt = "COAGENT_DOMAIN_PROMPT"

	// EnvKeyChannelID + EnvKeyChannelDB are daemon-set spawn env vars
	// pointing the worker at its own channel-local sqlite (the
	// authoritative actor_registry / type_registry source). The kimi
	// bridge opens channel.sqlite read-only via OpenChannelStore and
	// snapshots the registries at boot (system prompt + initial LLM
	// tool list) and on each turn boundary (dynamic LLM tool refresh
	// when the agent allows it).
	//
	// These replace the legacy `COAGENT_CHANNEL_CONTEXT_FILE` static
	// JSON snapshot the daemon used to write at worker spawn time —
	// that file froze channel state at spawn and could not reflect
	// later type install / actor register events.
	EnvKeyChannelID = "COAGENT_CHANNEL_ID"
	EnvKeyChannelDB = "COAGENT_CHANNEL_DB"
)

// ChannelContext is a snapshot of the channel-local registries the LLM
// system prompt + tool list need (actor list + type list). Produced by
// ChannelStore.Snapshot from channel.sqlite — channel.sqlite is the
// authoritative truth, ChannelContext is just a transport struct used
// to thread the data through BuildBasePrompt and Config.
//
// Prompt-cache friendliness: a freshly-snapshotted ChannelContext is
// byte-stable as long as channel.sqlite has not changed; consumers
// that re-snapshot (per-turn refresh) MUST treat any field difference
// as a cache invalidation signal.
type ChannelContext struct {
	// ChannelID is the channel this snapshot describes. Surfaces in the
	// rendered prompt header so a debug operator can grep session logs.
	ChannelID string `json:"channel_id,omitempty"`

	// ChannelType is the L4 channel-template key (e.g. "xhs-creator").
	// Empty for legacy / generic channels.
	ChannelType string `json:"channel_type,omitempty"`

	// Actors is the active set from actor_registry (deregistered rows
	// filtered out). Includes system / human members / agent self /
	// tool adapters. Order: actor_id ascending (matches store.ActorRegistry.ListActive).
	Actors []ActorInfo `json:"actors,omitempty"`

	// Types is the channel-local type_registry list — every business
	// type the harness will accept, plus its handler actor binding and
	// allowed kinds. The LLM uses this to pick which envelope.type to
	// emit for a given user request.
	Types []TypeInfo `json:"types,omitempty"`

	// Devices is an optional active device actor projection. It is empty
	// for normal channel-store snapshots; tests and future diagnostics can
	// use it to show which device actor route is available.
	Devices []DeviceInfo `json:"devices,omitempty"`
}

// ActorInfo is one actor_registry row projected into the LLM prompt.
type ActorInfo struct {
	ActorID     string `json:"actor_id"`
	Kind        string `json:"kind"`                   // human | agent | tool | system
	Binding     string `json:"binding,omitempty"`      // empty for human/system; embedded / runtime_inbound_via_relay / runtime_outbound for tools
	DisplayName string `json:"display_name,omitempty"` // optional human-readable label
	Description string `json:"description,omitempty"`  // optional actor-CLI one-line positioning
	SkillDoc    string `json:"skill_doc,omitempty"`    // optional actor-CLI markdown usage guide
}

// TypeInfo is one type_registry row projected into the LLM prompt.
//
// Level A (proto-layer0 §1.4.1): payload is opaque to the protocol
// layer; the type_registry stores no payload schema fields, so no
// payload-schema projection appears here either.
type TypeInfo struct {
	Type           string             `json:"type"`             // e.g. "xhs.publish"
	HandlerActorID string             `json:"handler_actor_id"` // e.g. "tool:xhs-adapter"
	HandlerBinding string             `json:"handler_binding,omitempty"`
	AllowedKinds   []string           `json:"allowed_kinds,omitempty"` // subset of {event, request, response}
	MaxPendingMs   int64              `json:"max_pending_ms,omitempty"`
	Description    string             `json:"description,omitempty"`
	PayloadExample json.RawMessage    `json:"payload_example,omitempty"`
	PayloadFields  []adapter.FieldDoc `json:"payload_fields,omitempty"`
	ErrorCodes     []adapter.ErrorDoc `json:"error_codes,omitempty"`
	Notes          string             `json:"notes,omitempty"`
}

// DeviceInfo is one device actor route projected into the LLM prompt.
type DeviceInfo struct {
	ActorID    string `json:"actor_id"`
	DeviceID   string `json:"device_id,omitempty"`
	DeviceType string `json:"device_type,omitempty"`
	Status     string `json:"status,omitempty"`
}

// Config drives a Bridge. All fields optional unless documented; sane
// defaults come from NewConfigFromEnv.
type Config struct {
	// APIKey is the provider API key. Required (NewConfigFromEnv reads
	// from KIMI_API_KEY).
	APIKey string

	// BaseURL is the provider endpoint. For DeepSeek anthropic-compat
	// the .env ships e.g. "https://api.deepseek.com/anthropic" — leave
	// empty to fall through to the SDK default (Moonshot).
	BaseURL string

	// Model is the provider-side model identifier (e.g. "deepseek-v4-pro").
	Model string

	// ProviderType — defaults to "anthropic" (DeepSeek's anthropic-compat
	// endpoint behaves like Anthropic's wire format).
	ProviderType string

	// SystemPrompt is the stable prefix written to the kimi agent
	// SystemPrompt override. cmd/worker assembles this from
	// L0-L2 platform teaching + the L4 domain prompt the daemon
	// injects via COAGENT_DOMAIN_PROMPT. Cached by the provider so
	// long as it stays byte-stable across turns.
	SystemPrompt string

	// ChannelContext is the snapshot folded into SystemPrompt at boot.
	// The bridge also derives go-kimi AdditionalTools from
	// ChannelContext.Types — that initial tool list seeds the kimi
	// agent so the LLM has channel-local request tools available on
	// turn 1 without a sqlite round-trip.
	ChannelContext ChannelContext

	// ChannelStore is the live read-only handle onto channel.sqlite.
	// When non-nil the bridge re-snapshots the registries on turn
	// boundaries so dynamic type install / actor register events
	// observed inside the worker subprocess can refresh the LLM tool
	// list without a respawn. Nil store falls back to the boot-time
	// ChannelContext (frozen for the lifetime of this worker process).
	ChannelStore *ChannelStore

	// MaxTurns caps the bridge — same semantics as MockBridge. A
	// non-positive value means UNLIMITED: the LLM itself decides when
	// to stop via stop_reason=end_turn / stop, and external cancellation
	// (ctx done, IPC EOF) is the only hard-stop signal. Tests can set
	// a positive cap to bound runs deterministically.
	MaxTurns int

	// WorkDir is the directory go-kimi uses for sessions / wire log /
	// approvals. Defaults to a per-process tmp dir.
	WorkDir string

	// NowFn returns unix-ms. Defaults to time.Now.UnixMilli.
	NowFn func() int64
}

// NewConfigFromEnv populates a Config from the documented env vars.
// Returns an error when a required field (KIMI_API_KEY) is missing,
// so cmd/worker can surface a clean fail-fast at flag-parse time.
func NewConfigFromEnv(systemPrompt string) (Config, error) {
	cfg := Config{
		APIKey:       strings.TrimSpace(os.Getenv(EnvKeyAPIKey)),
		BaseURL:      strings.TrimSpace(os.Getenv(EnvKeyBaseURL)),
		Model:        strings.TrimSpace(os.Getenv(EnvKeyModel)),
		ProviderType: "anthropic",
		SystemPrompt: systemPrompt,
	}
	if cfg.APIKey == "" {
		return Config{}, fmt.Errorf("kimi: %s env required for the kimi provider", EnvKeyAPIKey)
	}
	if cfg.Model == "" {
		return Config{}, fmt.Errorf("kimi: %s env required (pick a deepseek model id from your provider)", EnvKeyModel)
	}
	return cfg, nil
}

// IPCFacade is the worker-side IPC surface the Bridge depends on. It
// matches the M1.6-T1 runtime/worker.IPCClient shape closely enough
// that cmd/worker can plug one into the other without a deeper
// abstraction layer. Keeping it here (rather than importing
// runtime/worker.IPCClient directly) means adapters/llm/kimi stays
// inside the .go-arch-lint adapters→{kernel, adapters, pkg} envelope.
type IPCFacade interface {
	// ChannelID returns the post-handshake channel id snapshot.
	ChannelID() channel.ID
	// WorkerID returns the worker process id snapshot.
	WorkerID() string
	// WorkerActorID returns the agent sender id stamped on every emitted
	// envelope.
	WorkerActorID() actor.ActorID
	// Triggers returns the daemon → worker push channel. Bridge ranges
	// over it; the channel closes when the IPC link tears down.
	Triggers() <-chan TriggerPayload
	// WriteEnvelope writes one envelope through the daemon harness chain.
	// Bridge does not consult the WriteMessageResult (M1.6 mock bridge
	// also discards it) so a single error return is sufficient.
	WriteEnvelope(ctx context.Context, env message.Envelope) error
}

// TriggerPayload is the local mirror of runtime/ipc.TriggerPayload —
// duplicated here so adapters/llm/kimi avoids pulling runtime/** in.
// cmd/worker writes a one-liner converter that fan-outs the IPCClient
// trigger stream into this shape.
type TriggerPayload struct {
	Envelope      message.Envelope
	CorrelationID message.ID
	Cursor        int64
}

// Bridge drives one go-kimi Agent per worker process. Run blocks until
// MaxTurns, the trigger channel closes, ctx is cancelled, or the LLM
// returns a fatal error. Errors are emitted as a terminal failed-state
// envelope (see classifyLLMError) before Run returns.
type Bridge struct {
	cfg Config

	mu              sync.Mutex
	agentNew        func(gokimi.AgentConfig) (kimiAgent, error) // test hook
	testWireEmitter wire.Emitter                                // populated by Run; tests reach in via export_test.go
	envelopeSeq     atomic.Uint64

	pendingMu    sync.Mutex
	pendingTools map[message.ID]chan toolResponse
}

// kimiAgent is the subset of go-kimi.Agent the bridge consumes. Carved
// out so the test suite can stub the LLM without spinning provider HTTP.
type kimiAgent interface {
	Run(ctx context.Context, input string) error
	Close() error
}

// NewBridge builds a Bridge. Returns an error when cfg.APIKey is empty.
func NewBridge(cfg Config) (*Bridge, error) {
	if cfg.APIKey == "" {
		return nil, errors.New("kimi: Config.APIKey empty")
	}
	if cfg.Model == "" {
		return nil, errors.New("kimi: Config.Model empty")
	}
	if cfg.ProviderType == "" {
		cfg.ProviderType = "anthropic"
	}
	// MaxTurns <= 0 means UNLIMITED. The Run loop checks `> 0` before
	// enforcing the cap so daemon-spawned bridges never get prematurely
	// truncated.
	if cfg.NowFn == nil {
		cfg.NowFn = func() int64 { return time.Now().UnixMilli() }
	}
	if cfg.WorkDir == "" {
		tmp, err := os.MkdirTemp("", "coagent-kimi-")
		if err != nil {
			return nil, fmt.Errorf("kimi: workdir: %w", err)
		}
		cfg.WorkDir = tmp
	}

	b := &Bridge{cfg: cfg}
	b.agentNew = b.defaultAgentFactory
	return b, nil
}

// defaultAgentFactory wraps gokimi.NewAgent + the kimiAgent shim.
func (b *Bridge) defaultAgentFactory(ac gokimi.AgentConfig) (kimiAgent, error) {
	return gokimi.NewAgent(ac)
}

// Run executes one worker reaction loop. Returns nil on graceful exit
// (max turns reached, trigger channel closed, terminal failed envelope
// emitted) and propagates ctx.Err on cancellation.
//
// Run is single-shot per worker process. A single go-kimi Agent is built
// once and reused across all turns; go-kimi's SoulContext.Append already
// preserves history correctly now that adjacent TextPart runs are
// collapsed upstream (go-kimi commit 5336deb removed the chunked-content
// self-perpetuation bug at every layer that touches ContentParts —
// streaming accumulation, wire merger, anthropic encoder, and the
// normalizeHistory read defense). The bridge no longer needs to rebuild
// the agent or sanitize context.jsonl between turns.
func (b *Bridge) Run(ctx context.Context, ipc IPCFacade) error {
	if ipc == nil {
		return errors.New("kimi: Run nil ipc facade")
	}
	if ipc.WorkerActorID() == "" {
		return errors.New("kimi: Run actor id empty (handshake?)")
	}

	provider, err := b.buildProvider()
	if err != nil {
		return b.emitTerminalLLMError(ctx, ipc, err, "", "")
	}

	wireCh := make(chan wire.WireMessage, 128)
	emitter := wire.ChannelEmitter{Ch: wireCh}
	b.mu.Lock()
	b.testWireEmitter = emitter
	b.mu.Unlock()

	agent, err := b.buildAgent(provider, emitter)
	if err != nil {
		return b.emitTerminalLLMError(ctx, ipc, err, "", "")
	}
	defer func() {
		if agent != nil {
			_ = agent.Close()
		}
	}()

	runCtx, stopRouter := context.WithCancel(ctx)
	defer stopRouter()

	turns := 0
	triggers := b.routeTriggers(runCtx, ipc.Triggers())
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case payload, ok := <-triggers:
			if !ok {
				return nil
			}
			turns++
			if err := b.runTurn(ctx, ipc, agent, wireCh, payload, turns); err != nil {
				// runTurn already emitted a terminal envelope; surface
				// the error to Runtime so the caller can decide whether
				// to exit non-zero.
				return err
			}
			// MaxTurns > 0 enforces a cap (deterministic exit for tests).
			// MaxTurns <= 0 (the production default) lets the LLM drive
			// turn cadence — stop_reason=end_turn / stop on the wire is
			// the natural exit. ctx.Done() / triggers close still terminate.
			if b.cfg.MaxTurns > 0 && turns >= b.cfg.MaxTurns {
				if err := b.emitTerminal(ctx, ipc, payload, "agent.text", map[string]any{
					"text":        "kimi bridge reached max_turns",
					"next_action": "done",
				}); err != nil {
					return err
				}
				return nil
			}
		}
	}
}

// buildAgent builds a fresh kimiAgent bound to (workDir, emitter,
// provider). When a prior session exists under <workDir>/.kimi/sessions
// (i.e. last_session_id resolves), the build pins that session id so
// history Restore lands on the right session; otherwise go-kimi creates
// one.
func (b *Bridge) buildAgent(provider llm.ChatProvider, emitter wire.Emitter) (kimiAgent, error) {
	sessionID := ""
	if sess, err := kimisession.Continue(b.cfg.WorkDir); err == nil && sess != nil {
		sessionID = sess.ID
	}
	return b.agentNew(gokimi.AgentConfig{
		WorkDir:         b.cfg.WorkDir,
		SessionID:       sessionID,
		Config:          config.NewDefaultConfig(),
		Provider:        provider,
		WireEmitter:     emitter,
		AdditionalTools: b.channelTools(),
		Overrides: gokimi.AgentOverrides{
			SystemPrompt: b.cfg.SystemPrompt,
			Model:        b.cfg.Model,
		},
	})
}

// runTurn drives one Agent.Run call: it composes the user input from
// the trigger envelope, kicks the agent in a goroutine, and consumes
// wire events until the turn completes or the agent errors out.
//
// turnIndex is 1-based; it is stamped on every envelope this turn emits
// so the UI / observability layer can order progress vs. text events.
func (b *Bridge) runTurn(
	ctx context.Context,
	ipc IPCFacade,
	agent kimiAgent,
	wireCh chan wire.WireMessage,
	trigger TriggerPayload,
	turnIndex int,
) error {
	input, err := composeUserInput(trigger)
	if err != nil {
		return b.emitTerminalLLMError(ctx, ipc, err, trigger.Envelope.ID, terminalErrorCorrelationID(trigger))
	}

	turnCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	turnCtx = context.WithValue(turnCtx, channelToolRuntimeKey{}, channelToolRuntime{
		ipc:     ipc,
		trigger: trigger,
	})

	// agent.Run drives wire events into wireCh; consumeWire collates
	// them into envelopes. We signal turn completion via an explicit
	// `agentDone` channel rather than closing wireCh — closing wireCh
	// would prevent the next runTurn call from reusing the same agent.
	agentDone := make(chan struct{})
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- agent.Run(turnCtx, input)
		close(agentDone)
	}()

	consumeErr := b.consumeWire(turnCtx, ipc, wireCh, agentDone, trigger, turnIndex)
	if consumeErr != nil {
		cancel()
		select {
		case runErr := <-runErrCh:
			if runErr != nil && !errors.Is(runErr, context.Canceled) {
				return errors.Join(consumeErr, runErr)
			}
			return consumeErr
		case <-ctx.Done():
			return errors.Join(consumeErr, ctx.Err())
		}
	}
	runErr := <-runErrCh
	if runErr != nil {
		return b.emitTerminalLLMError(ctx, ipc, runErr, trigger.Envelope.ID, terminalErrorCorrelationID(trigger))
	}
	return nil
}

// turnState tracks the in-flight signals the bridge observes inside a
// single Agent.Run wire stream. Each go-kimi soul "step" inside that run
// emits a batch of ToolCallRequest events, followed by ToolCallResult
// events as the tools complete. We use the first ToolCallResult of a
// batch as the boundary to flush one progress envelope (agent.text +
// visibility=system, see emitTurnProgress): the LLM finished one
// reasoning step (typed by `step_index`), is about to feed tool results
// back to the next inference, and the UI gets a process bubble so the
// user is not staring at silence for 30-60s.
//
// On TurnEnd the bridge emits the single terminal agent.text
// (visibility=public) envelope. Stream-level TextDelta keeps buffering
// into the final text — never as envelope spam (chunk-spam was
// explicitly excluded by owner).
type turnState struct {
	textBuf       strings.Builder
	pendingTools  []wireToolCall
	stepIndex     int
	progressEmits int
}

// consumeWire reads the wire stream and:
//   - buffers TextDelta into the final text accumulator,
//   - collects ToolCallRequest events into per-step batches,
//   - emits one progress envelope (agent.text + visibility=system) at
//     each step boundary (first ToolCallResult after a ToolCallRequest
//     batch),
//   - emits one terminal agent.text (visibility=public) envelope on
//     TurnEnd.
//
// LLM streaming chunks are a transport-layer artifact and MUST NOT leak
// into the protocol envelope layer (the One Law: business change = new
// message; a chunk is not a business change). Per proto-layer0
// single-response semantics a request gets one final response envelope;
// intermediate progress is the same `agent.text`
// type carrying `visibility=system` per impl-vocabulary §2.3 (the
// historical standalone `agent.progress` type was collapsed during
// m1.3 freeze). Owner decision (M1.6): per-step progress + one
// terminal agent.text per turn.
func (b *Bridge) consumeWire(
	ctx context.Context,
	ipc IPCFacade,
	wireCh chan wire.WireMessage,
	agentDone <-chan struct{},
	trigger TriggerPayload,
	turnIndex int,
) error {
	state := &turnState{}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-agentDone:
			// Agent returned. Drain any wire events still buffered in
			// the channel so a scripted TurnEnd that fires just before
			// agent.Run returns is not lost. Non-blocking drain —
			// anything the agent emitted is already in the buffer.
			for drained := true; drained; {
				select {
				case msg, ok := <-wireCh:
					if !ok {
						drained = false
						break
					}
					done, err := b.handleWireMsg(ctx, ipc, msg, state, trigger, turnIndex)
					if err != nil {
						return err
					}
					if done {
						return nil
					}
				default:
					drained = false
				}
			}
			// No TurnEnd seen. Do NOT emit a partial envelope here —
			// the caller's failed-terminal path will surface the error
			// with the full accumulated text once we return nil.
			return nil
		case msg, ok := <-wireCh:
			if !ok {
				return nil
			}
			done, err := b.handleWireMsg(ctx, ipc, msg, state, trigger, turnIndex)
			if err != nil {
				return err
			}
			if done {
				return nil
			}
		}
	}
}

// handleWireMsg routes one wire message. Returns done=true only when
// TurnEnd fires (i.e. the turn is finished and the consumer should
// stop reading the wire stream). Tool call request/result events are
// folded into the per-step progress envelope as described in the
// consumeWire godoc.
func (b *Bridge) handleWireMsg(
	ctx context.Context,
	ipc IPCFacade,
	msg wire.WireMessage,
	state *turnState,
	trigger TriggerPayload,
	turnIndex int,
) (bool, error) {
	switch m := msg.(type) {
	case wire.TextDelta:
		state.textBuf.WriteString(m.Delta)
		return false, nil
	case wire.ToolCallRequest:
		state.pendingTools = append(state.pendingTools, wireToolCall{
			ID:        m.ToolCall.ID,
			Name:      m.ToolCall.Name,
			Arguments: toolCallArgumentsJSON(m.ToolCall.Arguments),
		})
		return false, nil
	case wire.ToolCallResult:
		// First result after a batch of requests marks the step boundary.
		// Flush one progress envelope summarising the step's tool calls,
		// then clear the pending list so the next step starts fresh.
		if len(state.pendingTools) == 0 {
			return false, nil
		}
		state.stepIndex++
		if err := b.emitTurnProgress(ctx, ipc, trigger, turnIndex, state.stepIndex, state.pendingTools); err != nil {
			return false, err
		}
		state.pendingTools = state.pendingTools[:0]
		state.progressEmits++
		return false, nil
	case wire.TurnEnd:
		// If the LLM yielded with tool_use but the tools never resolved
		// (e.g. provider error mid-step) we still flush any pending
		// requests as a progress envelope so the UI shows what the agent
		// attempted before the final text.
		if len(state.pendingTools) > 0 {
			state.stepIndex++
			if err := b.emitTurnProgress(ctx, ipc, trigger, turnIndex, state.stepIndex, state.pendingTools); err != nil {
				return false, err
			}
			state.pendingTools = state.pendingTools[:0]
			state.progressEmits++
		}
		return true, b.emitTurnEnd(ctx, ipc, trigger, m, state.textBuf.String(), turnIndex)
	default:
		return false, nil
	}
}

// toolCallArgumentsJSON normalises go-kimi's `types.JsonType` (= any)
// argument blob into the json.RawMessage the bridge stores per pending
// tool call. Empty / nil arguments yield an empty RawMessage — the
// preview builder treats that as "no preview available".
func toolCallArgumentsJSON(args any) json.RawMessage {
	if args == nil {
		return nil
	}
	b, err := json.Marshal(args)
	if err != nil {
		return nil
	}
	return b
}

// emitTurnProgress writes one progress envelope summarising a completed
// step. Per impl-vocabulary §2.3 progress is `agent.text` carrying
// `visibility=system` (intermediate output / not delivered to view by
// default) — the old standalone `agent.progress` type was collapsed
// into agent.text + visibility=system during the m1.3 freeze and is no
// longer a registered core type. Payload shape:
//
//	{
//	  "turn_index":  <1-based bridge turn>,
//	  "step_index":  <1-based within-turn step>,
//	  "tool_calls":  [{"name": "...", "preview": "..."}, ...],
//	}
//
// The progress envelope sits next to the eventual terminal agent.text
// (visibility=public) envelope under the same parent_id /
// correlation_id, so harness ordering keeps them grouped.
func (b *Bridge) emitTurnProgress(
	ctx context.Context,
	ipc IPCFacade,
	trigger TriggerPayload,
	turnIndex int,
	stepIndex int,
	tools []wireToolCall,
) error {
	payload := map[string]any{
		"turn_index": turnIndex,
		"step_index": stepIndex,
	}
	if summary := summariseToolCalls(tools, 240); len(summary) > 0 {
		payload["tool_calls"] = summary
	}
	return b.emitEnvelope(ctx, ipc, trigger, "agent.text", message.VisibilitySystem, payload)
}

// emitTurnEnd writes the single terminal agent.text envelope for one
// completed Agent.Run. Per-step progress envelopes have already been
// emitted by handleWireMsg at each ToolCallResult boundary — this
// function only produces the final reply.
//
// `accumulated` is the full TextDelta-buffered string; the TurnEnd's
// own Output ContentParts (text + think) are preferred, falling back to
// the buffered stream when Output is empty (providers vary).
//
// turnIndex is 1-based and stamps `payload.turn_index` so the UI can
// thread the envelope to the user's trigger.
func (b *Bridge) emitTurnEnd(
	ctx context.Context,
	ipc IPCFacade,
	trigger TriggerPayload,
	end wire.TurnEnd,
	accumulated string,
	turnIndex int,
) error {
	stop := strings.ToLower(strings.TrimSpace(end.StopReason))
	parts := parseOutputParts(end.Output)

	nextAction := "continue"
	switch stop {
	case "end_turn", "stop", "completed", "finish", "":
		nextAction = "done"
	case "max_tokens":
		nextAction = "max_tokens"
	case "tool_use":
		// go-kimi's soul aggregates tool steps internally and only
		// emits TurnEnd at Agent.Run completion. Seeing stop_reason=
		// tool_use at the bridge boundary means the run ended while
		// the LLM was still yielding — surface as `done` so the trigger
		// turn closes cleanly; per-step progress bubbles already gave
		// the UI visibility into what was attempted.
		nextAction = "done"
	}
	text := parts.text
	if text == "" {
		text = accumulated
	}
	payload := map[string]any{
		"text":        text,
		"next_action": nextAction,
		"stop_reason": end.StopReason,
		"turn_index":  turnIndex,
	}
	return b.emitEnvelope(ctx, ipc, trigger, "agent.text", message.VisibilityPublic, payload)
}

// emitEnvelope assembles + writes one envelope. Audience is the
// channel wildcard so the daemon's harness routes it to viewcache +
// pushhub like any human-visible event.
func (b *Bridge) emitEnvelope(
	ctx context.Context,
	ipc IPCFacade,
	trigger TriggerPayload,
	envType string,
	visibility message.Visibility,
	payload map[string]any,
) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("kimi: marshal payload: %w", err)
	}
	now := b.cfg.NowFn()
	// Reply audience derives from the trigger sender — Erlang-style
	// `From` routing. The worker has no awareness of other actors and
	// always emits back to whoever sent the trigger.
	audience := workerReplyAudience(trigger.Envelope.Sender.ID)
	env := message.Envelope{
		ID:            b.envelopeID(ipc, now),
		ChannelID:     ipc.ChannelID(),
		Type:          envType,
		Kind:          message.KindEvent,
		Sender:        message.Sender{Kind: actor.KindAgent, ID: ipc.WorkerActorID()},
		Visibility:    visibility,
		Audience:      audience,
		Payload:       body,
		CorrelationID: trigger.CorrelationID,
		ParentID:      trigger.Envelope.ID,
		TS:            now,
		TSReceived:    now,
	}
	return ipc.WriteEnvelope(ctx, env)
}

// workerReplyAudience returns the audience for a worker reply. Falls
// back to the system actor when the trigger sender id is empty (boot
// path / boot-failed terminal error).
func workerReplyAudience(triggerSender actor.ActorID) message.Audience {
	if triggerSender == "" {
		return message.Audience{actor.SystemActorID}
	}
	return message.Audience{triggerSender}
}

// emitTerminal is a helper for the max-turns / explicit-done envelope.
func (b *Bridge) emitTerminal(
	ctx context.Context,
	ipc IPCFacade,
	trigger TriggerPayload,
	envType string,
	payload map[string]any,
) error {
	return b.emitEnvelope(ctx, ipc, trigger, envType, message.VisibilityPublic, payload)
}

// emitTerminalLLMError classifies the error, emits a failed terminal
// envelope, then returns the underlying error unchanged so the caller
// (cmd/worker) can decide on the process exit code. err == nil short-
// circuits to a no-op for convenience.
func (b *Bridge) emitTerminalLLMError(
	ctx context.Context,
	ipc IPCFacade,
	err error,
	parentEnvID message.ID,
	correlationID message.ID,
) error {
	if err == nil {
		return nil
	}
	reason := classifyLLMError(err)
	payload := map[string]any{
		"text":        fmt.Sprintf("llm bridge failed: %v", err),
		"next_action": "failed",
		"reason":      reason,
	}
	body, _ := json.Marshal(payload)
	now := b.cfg.NowFn()
	env := message.Envelope{
		ID:         b.envelopeID(ipc, now),
		ChannelID:  ipc.ChannelID(),
		Type:       "agent.text",
		Kind:       message.KindEvent,
		Sender:     message.Sender{Kind: actor.KindAgent, ID: ipc.WorkerActorID()},
		Visibility: message.VisibilityPublic,
		// LLM-error terminal: emit as observation-only addressed to
		// system (no business actor fan-out). The originating trigger
		// sender is not available on this path.
		Audience:      message.Audience{actor.SystemActorID},
		Payload:       body,
		ParentID:      parentEnvID,
		CorrelationID: correlationID,
		TS:            now,
		TSReceived:    now,
	}
	if writeErr := ipc.WriteEnvelope(ctx, env); writeErr != nil {
		return errors.Join(err, writeErr)
	}
	return err
}

func terminalErrorCorrelationID(trigger TriggerPayload) message.ID {
	if trigger.CorrelationID != "" {
		return trigger.CorrelationID
	}
	return trigger.Envelope.CorrelationID
}

// envelopeID generates a deterministic-shape id for emitted envelopes.
// The per-bridge sequence keeps multiple emits in the same millisecond
// unique while preserving the worker/time prefix for debugging.
func (b *Bridge) envelopeID(ipc IPCFacade, nowMs int64) message.ID {
	workerID := ipc.WorkerID()
	if workerID == "" {
		workerID = "anon"
	}
	return message.ID(fmt.Sprintf("kimi-%s-%d-%d", workerID, nowMs, b.envelopeSeq.Add(1)))
}

// buildProvider hands a fully-configured llm.ChatProvider to
// gokimi.NewAgent. We bypass the kimi config.Provider lookup because
// the cvmax deploy uses one fixed env-driven provider, not the
// multi-provider config file shape.
func (b *Bridge) buildProvider() (llm.ChatProvider, error) {
	p, err := llm.NewProvider(llm.ProviderConfig{
		Type:    b.cfg.ProviderType,
		BaseURL: b.cfg.BaseURL,
		APIKey:  b.cfg.APIKey,
		Model:   b.cfg.Model,
	})
	if err != nil {
		return nil, fmt.Errorf("kimi: provider %q: %w", b.cfg.ProviderType, err)
	}
	if b.cfg.Model != "" {
		p = p.WithModel(b.cfg.Model)
	}
	return p, nil
}

// composeUserInput turns the trigger envelope into the string passed
// to Agent.Run. Heuristic for M1.6:
//   - if payload has a top-level "text" string, use it verbatim.
//   - else encode payload as compact JSON.
//   - prepend a 1-line sender label so the LLM knows who triggered it.
func composeUserInput(trigger TriggerPayload) (string, error) {
	env := trigger.Envelope
	var bodyText string
	if len(env.Payload) > 0 {
		var asMap map[string]any
		if err := json.Unmarshal(env.Payload, &asMap); err == nil {
			if t, ok := asMap["text"].(string); ok && strings.TrimSpace(t) != "" {
				bodyText = t
			}
		}
		if bodyText == "" {
			bodyText = string(env.Payload)
		}
	}
	if bodyText == "" {
		bodyText = "(empty trigger payload)"
	}
	senderLabel := fmt.Sprintf("[trigger sender=%s id=%s type=%s]", env.Sender.ID, env.ID, env.Type)
	return senderLabel + "\n" + bodyText, nil
}

// classifyLLMError maps a go-kimi error into one of 5 reason buckets
// the worker emits as payload.reason on the failed terminal envelope.
// The mapping is deliberately coarse — UI handlers + cvmax operators
// care about retryable vs fatal, not provider-specific quirks.
func classifyLLMError(err error) string {
	if err == nil {
		return ""
	}
	var llmErr *kimierrors.LLMError
	if errors.As(err, &llmErr) {
		switch {
		case llmErr.StatusCode == 429:
			return "llm_rate_limit"
		case llmErr.StatusCode == 401 || llmErr.StatusCode == 403:
			return "llm_auth"
		case llmErr.StatusCode >= 500 && llmErr.StatusCode < 600:
			return "llm_server"
		case llmErr.StatusCode > 0:
			return "llm_unknown"
		}
	}
	// network-shape errors — DNS / refused / timeout / tls — surface
	// as net.* types through the stdlib http stack.
	var netErr net.Error
	if errors.As(err, &netErr) {
		return "llm_network"
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return "llm_network"
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "llm_network"
	}
	return "llm_unknown"
}

// outputParts is the bridge-side breakdown of one TurnEnd.Output slice
// into the three flavours we care about: plain text (the public reply),
// tool calls (intermediate progress signal), and thinking (internal
// reasoning preview for progress envelopes).
type outputParts struct {
	text     string
	tools    []wireToolCall
	thinking string // accumulated raw think text (pre-trim)
}

// wireToolCall is the JSON-shape of one ToolCallPart we decode from the
// TurnEnd output stream. Field names align with go-kimi's wire format
// so json.Unmarshal round-trips without a translator.
type wireToolCall struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

// parseOutputParts flattens kimi ContentParts (text / think / tool_call)
// into outputParts. Reflection-free JSON round-trip — we don't want to
// pull go-kimi's types surface into the adapter's import set.
func parseOutputParts(parts any) outputParts {
	var out outputParts
	if parts == nil {
		return out
	}
	raw, err := json.Marshal(parts)
	if err != nil {
		return out
	}
	var slice []struct {
		Type     string        `json:"type"`
		Text     string        `json:"text,omitempty"`
		Think    string        `json:"think,omitempty"`
		ToolCall *wireToolCall `json:"tool_call,omitempty"`
	}
	if err := json.Unmarshal(raw, &slice); err != nil {
		return out
	}
	var (
		textBuf  strings.Builder
		thinkBuf strings.Builder
	)
	for i := range slice {
		switch slice[i].Type {
		case "text":
			textBuf.WriteString(slice[i].Text)
		case "think":
			thinkBuf.WriteString(slice[i].Think)
		case "tool_call":
			if slice[i].ToolCall != nil {
				out.tools = append(out.tools, *slice[i].ToolCall)
			}
		default:
			// Unknown discriminator. If there's a text field anyway,
			// take it (defensive — providers sometimes drop the `type`
			// for plain text turns).
			if slice[i].Text != "" {
				textBuf.WriteString(slice[i].Text)
			}
		}
	}
	out.text = textBuf.String()
	out.thinking = thinkBuf.String()
	return out
}

// summariseToolCalls builds the `tool_calls` array carried on
// progress envelopes (agent.text + visibility=system). Each entry is
// `{name, preview}` where preview is a short truncated string built
// from the arguments JSON — enough for an operator skimming the
// channel log to recognise what the agent is doing without exposing
// the full payload.
func summariseToolCalls(tools []wireToolCall, maxPreview int) []map[string]string {
	if len(tools) == 0 {
		return nil
	}
	if maxPreview <= 0 {
		maxPreview = 200
	}
	out := make([]map[string]string, 0, len(tools))
	for i := range tools {
		entry := map[string]string{"name": tools[i].Name}
		preview := buildToolPreview(tools[i].Arguments)
		if preview != "" {
			if len(preview) > maxPreview {
				preview = preview[:maxPreview] + "…"
			}
			entry["preview"] = preview
		}
		out = append(out, entry)
	}
	return out
}

// buildToolPreview turns a tool_call.arguments JSON blob into a one-line
// preview. Heuristic: when the payload is an object, prefer the first
// string-valued field (covers shell.cmd / read_file.path / write_file.path
// shapes). Fall back to the raw JSON when no such field exists.
func buildToolPreview(args json.RawMessage) string {
	if len(args) == 0 {
		return ""
	}
	var obj map[string]any
	if err := json.Unmarshal(args, &obj); err != nil {
		return strings.TrimSpace(string(args))
	}
	for _, key := range []string{"cmd", "command", "path", "file", "query", "input", "url"} {
		if v, ok := obj[key].(string); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	// No conventional key — encode back as a single compact JSON line.
	compact, err := json.Marshal(obj)
	if err != nil {
		return strings.TrimSpace(string(args))
	}
	return string(compact)
}

// BuildBasePrompt assembles the prompt-cache friendly stable prefix
// for the kimi system prompt. Layout:
//
//	[L0-L2 platform teaching]
//	[Channel context appendix — actors + types + devices]
//	[L4 domain prompt — from COAGENT_DOMAIN_PROMPT]
//
// channelType is purely informational (helps a debug operator grep
// session logs). Empty COAGENT_DOMAIN_PROMPT (legacy channels) yields
// the platform prompt alone. A zero ChannelContext is also accepted —
// the appendix is omitted and the prompt is byte-identical to the
// pre-channel-context behaviour, so legacy tests pass unchanged.
//
// The channel context section sits BETWEEN platform teaching and the
// domain prompt so the LLM reads the L0-L2 envelope contract first
// ("what is a coagent worker"), then sees the concrete actors / types
// it can address inside this channel, then finally the L4 domain
// playbook ("for an xhs publish do …"). That ordering makes the
// domain prompt's tool references (xhs.publish, xhs.search …)
// directly resolvable against the type list rendered immediately
// above it.
func BuildBasePrompt(channelType, domainPrompt string, channelCtx ChannelContext) string {
	var b strings.Builder
	b.WriteString(platformTeachingPrompt)

	if appendix := renderChannelContext(channelCtx); appendix != "" {
		b.WriteString("\n\n")
		b.WriteString(appendix)
	}

	domain := strings.TrimSpace(domainPrompt)
	if domain != "" {
		b.WriteString("\n\n")
		if channelType != "" {
			b.WriteString("# Channel template: ")
			b.WriteString(channelType)
			b.WriteString("\n\n")
		}
		b.WriteString(domain)
	}
	return b.String()
}

// renderChannelContext folds the registry snapshot into a markdown
// section the LLM can parse. Returns "" when the snapshot is empty so
// BuildBasePrompt skips the appendix entirely (legacy channels stay
// byte-identical to pre-injection behaviour).
//
// Format choice — markdown, not yaml, because:
//   - the rest of the system prompt is markdown
//   - LLMs index tables ("| col | col |") + bullet lists particularly well
//   - one read pass at spawn is cheap, and the result is cached
//
// Field order matches the struct definition above so the rendering
// stays deterministic across builds (prompt cache hits depend on
// byte-stable output).
func renderChannelContext(c ChannelContext) string {
	if len(c.Actors) == 0 && len(c.Types) == 0 && len(c.Devices) == 0 && c.ChannelID == "" {
		return ""
	}
	var b strings.Builder
	b.WriteString("# Channel context")
	if c.ChannelType != "" {
		b.WriteString(" (")
		b.WriteString(c.ChannelType)
		b.WriteString(")")
	}
	b.WriteString("\n")
	if c.ChannelID != "" {
		b.WriteString("channel_id: ")
		b.WriteString(c.ChannelID)
		b.WriteString("\n")
	}

	if len(c.Actors) > 0 {
		b.WriteString("\n## Actors in this channel\n")
		for _, a := range c.Actors {
			b.WriteString("- ")
			b.WriteString(a.ActorID)
			b.WriteString(" (kind=")
			if a.Kind == "" {
				b.WriteString("?")
			} else {
				b.WriteString(a.Kind)
			}
			if a.Binding != "" {
				b.WriteString(", binding=")
				b.WriteString(a.Binding)
			}
			b.WriteString(")")
			if a.DisplayName != "" {
				b.WriteString(" — ")
				b.WriteString(a.DisplayName)
			}
			b.WriteString("\n")
		}
	}

	if len(c.Types) > 0 {
		// Tool surface (substrate-native invocation):
		//
		//   list_actors    — scan the full actor + type registry.
		//   describe_actor — fetch one actor's skill doc.
		//   describe_type  — fetch payload examples and error docs.
		//   call_actor     — invoke any request-callable pair.
		//
		// We deliberately do NOT inline a markdown table of every
		// type here — the Anthropic / OpenAI native tools API list
		// (already bundled with this turn) lists the actor-CLI meta
		// tools with full input_schema. Listing every type
		// twice (once in tools API, once in prompt) was the legacy
		// inject-per-type fallback; the substrate-native path is one
		// list_actors round-trip per task.
		fmt.Fprintf(&b, "\n## Tool invocation\nThis channel exposes %d request-callable type(s) spanning the actors listed above. Use `list_actors` for the universe scan, `describe_actor(actor_id)` to deep-dive one actor's skill doc and workflows, `describe_type(actor_id, type)` to fetch a specific type's payload example and error codes, then `call_actor` to invoke. The registry is live - newly installed types appear in the next list_actors call without restart.\n", countRequestTypes(c.Types))
	}

	if len(c.Devices) > 0 {
		b.WriteString("\n## Device actors\n")
		for _, d := range c.Devices {
			b.WriteString("- ")
			b.WriteString(d.ActorID)
			if d.DeviceType != "" {
				b.WriteString(" (")
				b.WriteString(d.DeviceType)
				if d.DeviceID != "" {
					b.WriteString("/")
					b.WriteString(d.DeviceID)
				}
				b.WriteString(")")
			}
			if d.Status != "" {
				b.WriteString(" status=")
				b.WriteString(d.Status)
			}
			b.WriteString("\n")
		}
	}

	return b.String()
}

// countRequestTypes counts TypeInfo rows whose allowed_kinds contains
// "request" — these are the ones invokable via call_actor. Used by
// the system prompt builder to give the LLM a numeric hint about how
// many tools list_actors will return.
func countRequestTypes(types []TypeInfo) int {
	n := 0
	for _, t := range types {
		if typeAllowsKind(t, string(message.KindRequest)) {
			n++
		}
	}
	return n
}

// platformTeachingPrompt is the L0-L2 stable prefix every coagent
// worker carries. Intentionally short — the goal is to anchor the
// agent on the coagent envelope protocol without exploding the cache
// surface. Future ticket can extend with concrete examples.
const platformTeachingPrompt = `You are a coagent worker — an LLM-backed actor inside a channel-scoped runtime.

Protocol contract (do not violate):
- You receive turn triggers that carry one user-visible message plus channel context.
- You reply by emitting one or more agent.text events. The runtime stamps sender/audience automatically.
- Public events are visible to the channel's other participants; system events are operational telemetry only.
- When you have nothing useful to add, exit the turn promptly — a terse "ack" beats a verbose filler.
- Tool calls (xhs publish, search, get-note, etc.) flow through the channel's adapter actors. Reference them by their declared type; the daemon harness routes the request.

Stay grounded in the trigger payload and the channel's domain template below.`
