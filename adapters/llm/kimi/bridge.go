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
	"github.com/wanpengxie/ActOS/lib/behavior"
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

	// EnvKeyChannelID identifies the daemon-owned channel for this worker.
	// The worker never receives a frozen actor/type snapshot — current
	// actor/type state is queried live through the actor.list / actor.describe
	// reserved envelope calls.
	EnvKeyChannelID = "COAGENT_CHANNEL_ID"
)

// ChannelContext is the decoded payload of a LIVE actor.list response —
// the channel's actor/type catalog at the moment the daemon answered. It
// is a transient query result, NOT a frozen prompt snapshot: list_actors
// decodes it on each call and reprojects it for the LLM via
// formatActorRegistryForLLM.
type ChannelContext struct {
	// ChannelID is the channel this catalog describes.
	ChannelID string `json:"channel_id,omitempty"`

	// ChannelType is the L4 channel-template key (e.g. "xhs-creator").
	// Empty for legacy / generic channels.
	ChannelType string `json:"channel_type,omitempty"`

	// Actors is the live active actor catalog. Includes system / human
	// members / agent self / tool adapters.
	Actors []ActorInfo `json:"actors,omitempty"`

	// Types is the live business type catalog plus handler actor binding
	// and allowed kinds.
	Types []TypeInfo `json:"types,omitempty"`
}

// ActorInfo is one actor catalog row from an actor.list response.
type ActorInfo struct {
	ActorID           string          `json:"actor_id"`
	Kind              string          `json:"kind"`                   // human | agent | tool | system
	Binding           string          `json:"binding,omitempty"`      // empty for human/system; embedded / runtime_inbound_via_relay / runtime_outbound for tools
	DisplayName       string          `json:"display_name,omitempty"` // optional human-readable label
	Description       string          `json:"description,omitempty"`  // optional actor-CLI one-line positioning
	SkillDoc          string          `json:"skill_doc,omitempty"`    // optional actor-CLI markdown usage guide
	Ready             bool            `json:"ready"`
	ReadyReason       string          `json:"ready_reason,omitempty"`
	ReadyDetail       json.RawMessage `json:"ready_detail,omitempty"`
	LastReadyAt       int64           `json:"last_ready_at,omitempty"`
	LastStateChangeAt int64           `json:"last_state_change_at,omitempty"`
}

// TypeInfo is one request type from an actor.list response.
type TypeInfo struct {
	Type           string             `json:"type"`             // e.g. "xhs.publish"
	HandlerActorID string             `json:"handler_actor_id"` // e.g. "tool:xhs"
	HandlerBinding string             `json:"handler_binding,omitempty"`
	AllowedKinds   []string           `json:"allowed_kinds,omitempty"` // subset of {event, request, response}
	MaxPendingMs   int64              `json:"max_pending_ms,omitempty"`
	Description    string             `json:"description,omitempty"`
	PayloadExample json.RawMessage    `json:"payload_example,omitempty"`
	PayloadFields  []behavior.FieldDoc `json:"payload_fields,omitempty"`
	ErrorCodes     []behavior.ErrorDoc `json:"error_codes,omitempty"`
	Notes          string             `json:"notes,omitempty"`
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

type triggerAcker interface {
	AckTrigger(ctx context.Context, trigger TriggerPayload, accepted bool, reason string) error
}

// TriggerPayload is the local mirror of runtime/ipc.TriggerPayload —
// duplicated here so adapters/llm/kimi avoids pulling runtime/** in.
// cmd/worker writes a one-liner converter that fan-outs the IPCClient
// trigger stream into this shape.
type TriggerPayload struct {
	Envelope      message.Envelope
	CorrelationID message.ID
	Cursor        int64
	AckID         string
}

func ackTrigger(ctx context.Context, ipc IPCFacade, trigger TriggerPayload, accepted bool, reason string) error {
	acker, ok := ipc.(triggerAcker)
	if !ok {
		return nil
	}
	return acker.AckTrigger(ctx, trigger, accepted, reason)
}

type terminalEmittedError struct {
	cause error
}

func (e terminalEmittedError) Error() string {
	if e.cause == nil {
		return ""
	}
	return e.cause.Error()
}

func (e terminalEmittedError) Unwrap() error { return e.cause }

func isTerminalEmittedError(err error) bool {
	var handled terminalEmittedError
	return errors.As(err, &handled)
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

	// caller is the worker-side caller helper (owns its own
	// futurereg.FutureRegistry). Lazily built via caller(); guarded by mu.
	callerHelper *bridgeCaller
}

// caller returns the bridge's worker-side caller helper, building it lazily
// on first use. One registry instance per Bridge process.
func (b *Bridge) caller() *bridgeCaller {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.callerHelper == nil {
		b.callerHelper = newBridgeCaller()
	}
	return b.callerHelper
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

	// §3 ack 三分 / §6 step1 — the trigger ACK is the ACCEPT signal only.
	//
	// The main loop reads each trigger, immediately ACKs "accepted", and
	// hands the turn to a single background runner goroutine via an
	// internal queue. The runner executes turns serially (go-kimi's Agent
	// is not safe for concurrent Run and there is one session per process)
	// and reports the result purely through envelope emits. Because accept
	// is decoupled from turn completion, PushTrigger returns in ms-seconds
	// and the daemon never times a long turn out on the trigger deadline.
	//
	//   accept   — this loop's ackTrigger(accepted) on dequeue.
	//   complete — the agent.text envelope a turn emits (or the failed
	//              terminal envelope on LLM error).
	//   alive    — the heartbeat the IPC layer drives independently.
	//
	// Bounded retry + terminal closure for never-completing turns live in
	// the daemon (scanLongPending); the bridge only guarantees accept is
	// never blocked on the turn.
	triggers := b.routeTriggers(runCtx, ipc, ipc.Triggers())
	turnQueue := make(chan queuedTurn, 64)
	turnErrCh := make(chan error, 1)

	var runnerWG sync.WaitGroup
	runnerWG.Add(1)
	go func() {
		defer runnerWG.Done()
		for qt := range turnQueue {
			if err := b.runTurn(ctx, ipc, agent, wireCh, qt.payload, qt.index); err != nil {
				if !isTerminalEmittedError(err) {
					// Lower-level write/consume failure. The trigger was
					// already accepted, so it cannot be nacked; treat as
					// fatal so the worker recycles. The daemon's bounded
					// retry + long-pending closure drive the request to a
					// terminal state.
					select {
					case turnErrCh <- err:
					default:
					}
					return
				}
			}
		}
	}()

	turns := 0
	// stopAndDrain closes the queue, waits for the runner, and surfaces any
	// fatal (non-terminal-emitted) turn error the runner recorded — so a
	// worker-recycling failure is never lost to a racing triggers-close.
	stopAndDrain := func() error {
		close(turnQueue)
		runnerWG.Wait()
		select {
		case err := <-turnErrCh:
			return err
		default:
			return nil
		}
	}

	for {
		select {
		case <-ctx.Done():
			if derr := stopAndDrain(); derr != nil {
				return derr
			}
			return ctx.Err()
		case err := <-turnErrCh:
			// Drain the runner (already returned) and recycle the worker.
			_ = stopAndDrain()
			return err
		case payload, ok := <-triggers:
			if !ok {
				return stopAndDrain()
			}
			turns++
			turnIndex := turns

			// ACCEPT immediately, then enqueue. The runner reports COMPLETE
			// via envelope; accept latency stays independent of turn time.
			if err := ackTrigger(ctx, ipc, payload, true, ""); err != nil {
				_ = stopAndDrain()
				return err
			}
			select {
			case turnQueue <- queuedTurn{payload: payload, index: turnIndex}:
			case <-ctx.Done():
				_ = stopAndDrain()
				return ctx.Err()
			case err := <-turnErrCh:
				_ = stopAndDrain()
				return err
			}

			// MaxTurns > 0 enforces a cap (deterministic exit for tests).
			// MaxTurns <= 0 (production default) lets the LLM drive cadence.
			// We accept + enqueue the capped turn first so its work still
			// runs, then drain the runner and emit the canonical terminal.
			if b.cfg.MaxTurns > 0 && turnIndex >= b.cfg.MaxTurns {
				if derr := stopAndDrain(); derr != nil {
					return derr
				}
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

// queuedTurn is one accepted trigger awaiting serial execution by the
// bridge's turn runner.
type queuedTurn struct {
	payload TriggerPayload
	index   int
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
		// SkillRoots: empty non-nil slice = hermetic skill discovery.
		// coagent's worker MUST NOT pick up arbitrary SKILL.md files from
		// the user's $HOME/.kimi/skills (those belong to other tools like
		// Claude Code's skill catalog). go-kimi's DefaultSkillRoots would
		// otherwise scan there and either inject unrelated skills into
		// our agent or fail boot on parse errors. Hermetic = the agent
		// sees only the channel-type catalog we explicitly install.
		SkillRoots: []string{},
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
				// Agent.Run failed (e.g. provider 429 / 500 / auth) and
				// consumeWire reported a missing TurnEnd. The provider
				// error is the meaningful signal — emit a public failed
				// terminal envelope so the LLM error surfaces in the
				// channel log instead of being swallowed by the
				// no-TurnEnd consumeErr.
				return b.emitTerminalLLMError(ctx, ipc, runErr, trigger.Envelope.ID, terminalErrorCorrelationID(trigger))
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
			// No TurnEnd means no terminal agent.text was emitted. Treat it
			// as delivery failure so the trigger is not ACKed as handled.
			return errors.New("kimi: agent completed without TurnEnd")
		case msg, ok := <-wireCh:
			if !ok {
				return errors.New("kimi: wire channel closed before TurnEnd")
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

// emitEnvelope assembles + writes one envelope. Audience is derived
// from the trigger sender so the daemon routes it to the conversation
// participant without wildcard fanout.
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
	return terminalEmittedError{cause: err}
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
//	[L4 domain prompt — from COAGENT_DOMAIN_PROMPT]
//
// The prompt carries NO frozen actor/type snapshot. The concrete set of
// actors and request-callable types is dynamic channel state, discovered
// live at call time via the list_actors / describe_* meta tools — baking
// it into the cached prefix would (a) go stale the instant an actor joins
// after spawn and (b) churn the cache prefix. channelType is purely
// informational (helps a debug operator grep session logs). Empty
// COAGENT_DOMAIN_PROMPT (legacy channels) yields the platform prompt alone.
func BuildBasePrompt(channelType, domainPrompt string) string {
	var b strings.Builder
	b.WriteString(platformTeachingPrompt)

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
- Tool calls (xhs publish, search, get-note, etc.) flow through the channel's adapter actors via call_actor. Reference them by their declared type; the daemon harness routes the request.
- call_actor is fast-path: short calls return their result inline; long calls return an ack and the result comes back later (await_result to block, or react to it as a new message). Fan out with wait=false, then await_result/abandon. See "Tool invocation" in the channel context for the full pattern.

Stay grounded in the trigger payload and the channel's domain template below.`
