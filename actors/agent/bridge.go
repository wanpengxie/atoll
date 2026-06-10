// Package agent is the LLM agent actor: the go-kimi cognitive engine bound
// to the one actor face (lib/behavior). The Bridge is a host-agnostic
// actorrt.Actor implementation — the same package is spawned as a server
// cell (built-in fallback agent) or installed by a daemon registry (fat
// daemon plugin); cell/port is the channel runtime's link attribute, never
// this implementation's concern.
//
// Structure (async is the STRUCTURE, sync is an EXPERIENCE):
//   - Receive (cell goroutine) never blocks: requests/events enqueue a turn;
//     responses Match the author#2 caller (disarm the timeout timer) and
//     Deliver to the private futures (wake a bounded-window waiter), falling
//     through as a new turn when nobody waits.
//   - A private LLM loop goroutine (the CLIENT EDGE — blocking is legal
//     here, §2.7) runs go-kimi turns serially; tool calls build requests via
//     behavior.BuildRequest, Arm author#2, and wait a bounded fast-path
//     window for the sync experience the model's training distribution
//     expects.
package agent

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

	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/lib/introspect"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

// Env keys read at NewConfigFromEnv time. Kept exported so tests and the
// daemon/server assembly share one source of truth for the env contract.
const (
	EnvKeyAPIKey  = "KIMI_API_KEY"
	EnvKeyBaseURL = "KIMI_BASE_URL"
	EnvKeyModel   = "KIMI_MODEL"

	// EnvKeyChannelType / EnvKeyDomainPrompt feed the prompt-cache
	// friendly base prompt (L4 §2.4 + L0-L2 platform teaching).
	EnvKeyChannelType  = "COAGENT_CHANNEL_TYPE"
	EnvKeyDomainPrompt = "COAGENT_DOMAIN_PROMPT"
)

// turnQueueCap bounds the turn backlog. On overflow the OLDEST queued turn
// is evicted (newest input wins) and a system-visibility note records the
// drop — Receive never blocks (cell serial contract).
const turnQueueCap = 64

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
	// SystemPrompt override. The host assembles this from the platform
	// teaching prompt + the channel's domain prompt. Cached by the
	// provider so long as it stays byte-stable across turns.
	SystemPrompt string

	// WorkDir is the directory go-kimi uses for sessions / wire log /
	// approvals. Defaults to a per-process tmp dir.
	WorkDir string

	// FastPathWindow bounds the inline wait of one tool call before it
	// degrades to an ack (the sync EXPERIENCE for the model). Defaults
	// to metatool's 15s fast-path.
	FastPathWindow time.Duration

	// NowFn returns unix-ms. Defaults to time.Now.UnixMilli.
	NowFn func() int64
}

// NewConfigFromEnv populates a Config from the documented env vars.
// Returns an error when a required field (KIMI_API_KEY) is missing,
// so the host can surface a clean fail-fast at assembly time.
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

// turnItem is one mailbox envelope awaiting serial execution by the
// private LLM loop.
type turnItem struct {
	env message.Envelope
}

// correlationOf resolves the correlation anchor for a turn: the trigger
// envelope's correlation id, falling back to its own id.
func (t turnItem) correlationID() message.ID {
	return behavior.CorrelationID("", t.env.CorrelationID, t.env.ID)
}

// Bridge drives one go-kimi Agent as an actor cell. It implements
// actorrt.Actor (Receive) plus the Starter/Stopper lifecycle hooks; the
// runtime guarantees all three run serially on the cell goroutine.
type Bridge struct {
	cfg    Config
	self   actor.ActorID
	chID   channel.ID
	writer harness.Writer

	mu              sync.Mutex
	agentNew        func(gokimi.AgentConfig) (kimiAgent, error) // test hook
	testWireEmitter wire.Emitter                                // populated by Start; tests reach in via export_test.go
	envelopeSeq     atomic.Uint64

	// futures is the agent-PRIVATE collector for the bounded fast-path
	// wait (the client-edge half; never exposed outside this package).
	futures *requestCorrelator
	// caller is closure author#2: arms a timeout terminal per outbound
	// request, disarmed by Receive's Match on the response.
	//
	// behavior.Caller is lock-free by contract (all touches on one cell
	// goroutine). This agent crosses two goroutines — Arm fires on the
	// private LLM loop, Match on the cell goroutine — so the agent guards
	// every caller touch with callerMu (downstream adapts; the base stays
	// lock-free for single-goroutine actors).
	callerMu sync.Mutex
	caller   *behavior.Caller

	// Private LLM loop plumbing — created in Start, torn down in Stop.
	turnQ    chan turnItem
	stopOnce sync.Once
	loopStop context.CancelFunc
	loopWG   sync.WaitGroup
	kagent   kimiAgent
	wireCh   chan wire.WireMessage

	// fatal records an unrecoverable loop failure. The next Receive
	// panics with it so the cell dies POSITIVELY (death edge → author#3
	// reaps, presence drops, fallback routing takes over) instead of
	// silently wedging.
	fatalMu sync.Mutex
	fatal   error
}

// kimiAgent is the subset of go-kimi.Agent the bridge consumes. Carved
// out so the test suite can stub the LLM without spinning provider HTTP.
type kimiAgent interface {
	Run(ctx context.Context, input string) error
	Close() error
}

// NewBridge builds a Bridge bound to its identity and writing seam. The
// host closes over (self, chID, writer) at assembly time — the factory
// shape is func(w harness.Writer) actorrt.Actor.
func NewBridge(cfg Config, self actor.ActorID, chID channel.ID, w harness.Writer) (*Bridge, error) {
	if cfg.APIKey == "" {
		return nil, errors.New("kimi: Config.APIKey empty")
	}
	if cfg.Model == "" {
		return nil, errors.New("kimi: Config.Model empty")
	}
	if self == "" {
		return nil, errors.New("kimi: actor id empty")
	}
	if chID == "" {
		return nil, errors.New("kimi: channel id empty")
	}
	if w == nil {
		return nil, errors.New("kimi: writer nil")
	}
	if cfg.ProviderType == "" {
		cfg.ProviderType = "anthropic"
	}
	if cfg.NowFn == nil {
		cfg.NowFn = func() int64 { return time.Now().UnixMilli() }
	}
	if cfg.FastPathWindow <= 0 {
		cfg.FastPathWindow = 15 * time.Second
	}
	if cfg.WorkDir == "" {
		tmp, err := os.MkdirTemp("", "coagent-kimi-")
		if err != nil {
			return nil, fmt.Errorf("kimi: workdir: %w", err)
		}
		cfg.WorkDir = tmp
	}

	b := &Bridge{
		cfg:     cfg,
		self:    self,
		chID:    chID,
		writer:  w,
		futures: newRequestCorrelator(),
	}
	b.agentNew = b.defaultAgentFactory
	return b, nil
}

var _ actorrt.Actor = (*Bridge)(nil)
var _ actorrt.Starter = (*Bridge)(nil)
var _ actorrt.Stopper = (*Bridge)(nil)

// defaultAgentFactory wraps gokimi.NewAgent + the kimiAgent shim.
func (b *Bridge) defaultAgentFactory(ac gokimi.AgentConfig) (kimiAgent, error) {
	return gokimi.NewAgent(ac)
}

func (b *Bridge) sender() message.Sender {
	return message.Sender{Kind: actor.KindAgent, ID: b.self}
}

func (b *Bridge) clock() time.Time { return time.UnixMilli(b.cfg.NowFn()) }

// armCaller / matchCaller serialize author#2 touches across the cell
// goroutine (Match in Receive) and the private loop (Arm in tool calls).
func (b *Bridge) armCaller(env *message.Envelope) {
	b.callerMu.Lock()
	defer b.callerMu.Unlock()
	if b.caller != nil {
		b.caller.Arm(env)
	}
}

func (b *Bridge) matchCaller(env *message.Envelope) {
	b.callerMu.Lock()
	defer b.callerMu.Unlock()
	if b.caller != nil {
		b.caller.Match(env)
	}
}

// Start boots the cognitive engine and the private LLM loop. A boot
// failure returns the error so the cell dies fast (positive death) —
// no half-alive agent ever registers as serviceable.
func (b *Bridge) Start(ctx context.Context, _ actorrt.ActorContext) error {
	provider, err := b.buildProvider()
	if err != nil {
		return err
	}

	wireCh := make(chan wire.WireMessage, 128)
	emitter := wire.ChannelEmitter{Ch: wireCh}
	b.mu.Lock()
	b.testWireEmitter = emitter
	b.mu.Unlock()

	kagent, err := b.buildAgent(provider, emitter)
	if err != nil {
		return err
	}

	// author#2: every outbound request arms a caller-scoped timeout
	// terminal; Receive's Match disarms it when the response lands.
	b.caller = behavior.NewCaller(b.sender(), b.writer, b.clock)

	loopCtx, cancel := context.WithCancel(ctx)
	b.loopStop = cancel
	b.kagent = kagent
	b.wireCh = wireCh
	b.turnQ = make(chan turnItem, turnQueueCap)

	b.loopWG.Add(1)
	go b.runLoop(loopCtx)
	return nil
}

// Stop tears the private loop down: cancel the in-flight turn, close the
// queue, join the loop, disarm all author#2 timers, close the engine.
// The runtime guarantees no Receive is in flight or will follow.
func (b *Bridge) Stop(_ context.Context) error {
	var closeErr error
	b.stopOnce.Do(func() {
		if b.loopStop != nil {
			b.loopStop()
		}
		if b.turnQ != nil {
			close(b.turnQ)
		}
		b.loopWG.Wait()
		b.callerMu.Lock()
		if b.caller != nil {
			b.caller.Stop()
		}
		b.callerMu.Unlock()
		if b.kagent != nil {
			closeErr = b.kagent.Close()
		}
	})
	return closeErr
}

// Receive is the mailbox entry — it NEVER blocks (cell serial contract).
//
//   - response envelopes: Match (author#2 disarm) then Deliver to the
//     private futures; a final nobody waits for becomes a new turn (the
//     async result feeding the next reasoning step).
//   - everything else (requests, events): a new turn.
func (b *Bridge) Receive(ctx context.Context, env *message.Envelope) error {
	b.fatalMu.Lock()
	fatal := b.fatal
	b.fatalMu.Unlock()
	if fatal != nil {
		// Positive death: the LLM loop is gone; a silent wedge would leave
		// callers hanging until their own timeouts. Panic is recovered by
		// the cell and published as the death edge (author#3 reaps).
		panic(fmt.Sprintf("agent %s: llm loop dead: %v", b.self, fatal))
	}
	if env == nil {
		return nil
	}

	// Mechanical self-answers (actor citizenship) — never fed to the LLM.
	if env.Kind == message.KindRequest && env.Type == introspect.QueryDescribe {
		return b.handleDescribe(ctx, env)
	}

	if env.Kind == message.KindResponse && env.ParentID != "" {
		b.matchCaller(env)
		_, final := behavior.ParseFinalStatus(env.Payload)
		disp := b.futures.Deliver(env)
		if disp == noActiveWaiter && final {
			b.enqueueTurn(*env)
		}
		return nil
	}

	b.enqueueTurn(*env)
	return nil
}

// enqueueTurn pushes a turn without ever blocking: on overflow the oldest
// queued turn is evicted (newest input wins) and a system-visibility note
// records the drop.
func (b *Bridge) enqueueTurn(env message.Envelope) {
	item := turnItem{env: env}
	select {
	case b.turnQ <- item:
		return
	default:
	}
	// Queue full: evict the oldest, then push (both non-blocking — the
	// private loop is the only consumer and Receive the only producer).
	var dropped turnItem
	select {
	case dropped = <-b.turnQ:
	default:
	}
	select {
	case b.turnQ <- item:
	default:
	}
	if dropped.env.ID != "" {
		payload := map[string]any{
			"text":        fmt.Sprintf("turn queue overflow: dropped oldest trigger %s (%s)", dropped.env.ID, dropped.env.Type),
			"next_action": "dropped",
		}
		// Best-effort note; the write seam is concurrency-safe.
		_ = b.emitEnvelope(context.Background(), turnItem{env: dropped.env}, "agent.text", message.VisibilitySystem, payload)
	}
}

// runLoop is the private LLM loop — the client edge where blocking is
// legal. Turns run strictly serially (go-kimi's Agent is not safe for
// concurrent Run; one session per actor).
func (b *Bridge) runLoop(ctx context.Context) {
	defer b.loopWG.Done()
	turns := 0
	for {
		select {
		case <-ctx.Done():
			return
		case item, ok := <-b.turnQ:
			if !ok {
				return
			}
			turns++
			if err := b.runTurn(ctx, item, turns); err != nil {
				if isTerminalEmittedError(err) {
					continue // failure already surfaced as a terminal envelope
				}
				if errors.Is(err, context.Canceled) {
					return
				}
				// Unrecoverable plumbing failure: record + die positively on
				// the next contact.
				b.fatalMu.Lock()
				b.fatal = err
				b.fatalMu.Unlock()
				return
			}
		}
	}
}

// runTurn drives one Agent.Run call: it composes the user input from
// the trigger envelope, kicks the agent in a goroutine, and consumes
// wire events until the turn completes or the agent errors out.
//
// turnIndex is 1-based; it is stamped on every envelope this turn emits
// so the UI / observability layer can order progress vs. text events.
func (b *Bridge) runTurn(ctx context.Context, item turnItem, turnIndex int) error {
	input := composeUserInput(item.env)

	turnCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	turnCtx = context.WithValue(turnCtx, channelToolRuntimeKey{}, item)

	// agent.Run drives wire events into wireCh; consumeWire collates
	// them into envelopes. We signal turn completion via an explicit
	// `agentDone` channel rather than closing wireCh — closing wireCh
	// would prevent the next runTurn call from reusing the same agent.
	agentDone := make(chan struct{})
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- b.kagent.Run(turnCtx, input)
		close(agentDone)
	}()

	consumeErr := b.consumeWire(turnCtx, agentDone, item, turnIndex)
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
				return b.emitTerminalLLMError(ctx, runErr, item.env.ID, item.correlationID())
			}
			return consumeErr
		case <-ctx.Done():
			return errors.Join(consumeErr, ctx.Err())
		}
	}
	runErr := <-runErrCh
	if runErr != nil {
		return b.emitTerminalLLMError(ctx, runErr, item.env.ID, item.correlationID())
	}
	return nil
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
		// coagent's agent MUST NOT pick up arbitrary SKILL.md files from
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
// intermediate progress is the same `agent.text` type carrying
// `visibility=system` per impl-vocabulary §2.3. Owner decision (M1.6):
// per-step progress + one terminal agent.text per turn.
func (b *Bridge) consumeWire(
	ctx context.Context,
	agentDone <-chan struct{},
	item turnItem,
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
				case msg, ok := <-b.wireCh:
					if !ok {
						drained = false
						break
					}
					done, err := b.handleWireMsg(ctx, msg, state, item, turnIndex)
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
			// as turn failure so the trigger is not silently swallowed.
			return errors.New("kimi: agent completed without TurnEnd")
		case msg, ok := <-b.wireCh:
			if !ok {
				return errors.New("kimi: wire channel closed before TurnEnd")
			}
			done, err := b.handleWireMsg(ctx, msg, state, item, turnIndex)
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
	msg wire.WireMessage,
	state *turnState,
	item turnItem,
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
		if err := b.emitTurnProgress(ctx, item, turnIndex, state.stepIndex, state.pendingTools); err != nil {
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
			if err := b.emitTurnProgress(ctx, item, turnIndex, state.stepIndex, state.pendingTools); err != nil {
				return false, err
			}
			state.pendingTools = state.pendingTools[:0]
			state.progressEmits++
		}
		return true, b.emitTurnEnd(ctx, item, m, state.textBuf.String(), turnIndex)
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
// default). Payload shape:
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
	item turnItem,
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
	return b.emitEnvelope(ctx, item, "agent.text", message.VisibilitySystem, payload)
}

// emitTurnEnd writes the single terminal agent.text envelope for one
// completed Agent.Run. Per-step progress envelopes have already been
// emitted by handleWireMsg at each ToolCallResult boundary — this
// function only produces the final reply.
//
// `accumulated` is the full TextDelta-buffered string; the TurnEnd's
// own Output ContentParts (text + think) are preferred, falling back to
// the buffered stream when Output is empty (providers vary).
func (b *Bridge) emitTurnEnd(
	ctx context.Context,
	item turnItem,
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
	return b.emitEnvelope(ctx, item, "agent.text", message.VisibilityPublic, payload)
}

// emitEnvelope assembles + writes one event envelope. Audience is derived
// from the trigger sender — Erlang-style `From` routing: the agent always
// replies to whoever triggered it.
func (b *Bridge) emitEnvelope(
	ctx context.Context,
	item turnItem,
	envType string,
	visibility message.Visibility,
	payload map[string]any,
) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("kimi: marshal payload: %w", err)
	}
	env, err := b.buildAgentEvent(envType, visibility,
		replyAudience(item.env.Sender.ID), body,
		item.env.ID, item.correlationID())
	if err != nil {
		return err
	}
	return b.write(ctx, env)
}

// write commits one envelope through the harness chain; a reject is an
// error (the agent must know its emit did not land).
func (b *Bridge) write(ctx context.Context, env message.Envelope) error {
	res, err := b.writer.Write(ctx, &env)
	if err != nil {
		return err
	}
	if !res.Accepted() {
		return fmt.Errorf("kimi: emit rejected: %s (%s)", res.RejectReason, res.RejectDetail)
	}
	return nil
}

// agentDescription / agentSkillDoc are the agent's actor.describe
// self-answer. The agent serves no request-type closed set — its surface is
// conversational (any request becomes an LLM turn), so Types stays empty and
// discovery guidance lives in the skill doc.
const agentDescription = "LLM agent: the channel's conversational brain. Send it any request — it reasons over the channel context and orchestrates the channel's tool actors via call_actor."

const agentSkillDoc = "# agent\n\n" +
	"Conversational actor backed by an LLM. It accepts any kind=request as a " +
	"turn trigger (no closed type set), replies with agent.text events " +
	"(public terminal + system progress), and calls other actors through the " +
	"channel's meta tools.\n"

// handleDescribe serves the actor.describe self-answer through the standard
// introspect dispatch (mechanical — the LLM never sees reserved queries).
func (b *Bridge) handleDescribe(ctx context.Context, env *message.Envelope) error {
	req, err := introspect.ParseDescribeRequest(env.Payload)
	if err != nil {
		_, ferr := behavior.Fail(ctx, b.writer, b.clock, env, b.sender(),
			"payload_invalid", fmt.Sprintf("decode describe payload: %v", err))
		return ferr
	}
	answer, ok := introspect.AnswerDescribe(introspect.Describe{
		ActorID:     string(b.self),
		Description: agentDescription,
		SkillDoc:    agentSkillDoc,
	}, req)
	if !ok {
		_, ferr := behavior.Fail(ctx, b.writer, b.clock, env, b.sender(),
			"type_unsupported", fmt.Sprintf("agent has no type %s", req.Type))
		return ferr
	}
	_, rerr := behavior.RespondJSON(ctx, b.writer, b.clock, env, b.sender(), answer)
	return rerr
}

// replyAudience returns the audience for an agent reply. Falls back to
// the system actor when the trigger sender id is empty (boot path /
// boot-failed terminal error).
func replyAudience(triggerSender actor.ActorID) message.Audience {
	if triggerSender == "" {
		return message.Audience{actor.SystemActorID}
	}
	return message.Audience{triggerSender}
}

// emitTerminalLLMError classifies the error, emits a failed terminal
// envelope, then wraps the underlying error as terminalEmittedError so
// the loop knows the failure already surfaced in the channel log.
// err == nil short-circuits to a no-op for convenience.
func (b *Bridge) emitTerminalLLMError(
	ctx context.Context,
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
	// LLM-error terminal: emit as observation-only addressed to
	// system (no business actor fan-out). The originating trigger
	// sender is not available on this path.
	env, buildErr := b.buildAgentEvent("agent.text", message.VisibilityPublic,
		message.Audience{actor.SystemActorID}, body, parentEnvID, correlationID)
	if buildErr != nil {
		return errors.Join(err, buildErr)
	}
	if writeErr := b.write(ctx, env); writeErr != nil {
		return errors.Join(err, writeErr)
	}
	return terminalEmittedError{cause: err}
}

// envelopeID generates a deterministic-shape id for emitted envelopes.
// The per-bridge sequence keeps multiple emits in the same millisecond
// unique while preserving the actor/time prefix for debugging.
func (b *Bridge) envelopeID(nowMs int64) message.ID {
	short := strings.TrimPrefix(string(b.self), "agent:")
	if short == "" {
		short = "anon"
	}
	return message.ID(fmt.Sprintf("kimi-%s-%d-%d", short, nowMs, b.envelopeSeq.Add(1)))
}

// buildProvider hands a fully-configured llm.ChatProvider to
// gokimi.NewAgent. We bypass the kimi config.Provider lookup because
// the deploy uses one fixed env-driven provider, not the
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
// to Agent.Run. Heuristic:
//   - if payload has a top-level "text" string, use it verbatim.
//   - else encode payload as compact JSON.
//   - prepend a 1-line sender label so the LLM knows who triggered it.
func composeUserInput(env message.Envelope) string {
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
	return senderLabel + "\n" + bodyText
}

// classifyLLMError maps a go-kimi error into one of 5 reason buckets
// the agent emits as payload.reason on the failed terminal envelope.
// The mapping is deliberately coarse — UI handlers + operators care
// about retryable vs fatal, not provider-specific quirks.
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

// Situation carries the FACTS of one agent instance's circumstances — and
// only facts. No role labels: "bootloader guide" and "working principal"
// are behaviours the ONE prompt skeleton derives from these facts (an actor
// is never boxed into a fixed role); the same instance changes behaviour
// the moment its facts change (a device attaches, a workspace appears).
type Situation struct {
	// HasWorkspace: a private device workspace (bash/files of its own)
	// exists for this instance.
	HasWorkspace bool
	// WorkspaceDir is the workspace path when HasWorkspace.
	WorkspaceDir string
	// Host is where this instance runs ("server" / "daemon") — purely for
	// situational honesty when talking to users.
	Host string
}

// BuildSystemPrompt assembles the prompt-cache friendly stable prefix:
//
//	[L0-L2 platform teaching]        — stable skeleton, byte-identical
//	[situation block]                 — facts + the behaviour they imply
//	[L4 domain prompt]                — channel-type template
//
// The prompt carries NO frozen actor/type snapshot. The concrete set of
// actors and request-callable types is dynamic channel state, discovered
// live at call time via the list_actors / describe_* meta tools — baking
// it into the cached prefix would (a) go stale the instant an actor joins
// after spawn and (b) churn the cache prefix. channelType is purely
// informational. Empty domainPrompt yields skeleton + situation alone.
func BuildSystemPrompt(sit Situation, channelType, domainPrompt string) string {
	var b strings.Builder
	b.WriteString(platformTeachingPrompt)
	b.WriteString("\n\n")
	b.WriteString(situationPrompt(sit))

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

// situationPrompt renders the situation FACTS plus the behaviour the
// skeleton derives from them. The no-workspace branch IS the bootstrap
// guide behaviour; the workspace branch IS the working-principal
// discipline — same skeleton, different facts.
func situationPrompt(sit Situation) string {
	var b strings.Builder
	b.WriteString("# Your situation\n\n")
	host := sit.Host
	if host == "" {
		host = "unknown"
	}
	fmt.Fprintf(&b, "- You run on: %s.\n", host)
	if sit.HasWorkspace {
		fmt.Fprintf(&b, "- You HAVE a private workspace at %s — your home. ", sit.WorkspaceDir)
		b.WriteString("Persist durable working rules, plans and notes there " +
			"(via the channel's device file tools); the channel's public " +
			"messages are the shared memory of record — important outcomes " +
			"belong there, not only in your private files.\n")
	} else {
		b.WriteString("- You have NO private workspace: no bash or files of " +
			"your own. You can still converse and call ANY actor the " +
			"channel's daemons provide (call_actor).\n")
		b.WriteString("- Be honest about this limit. When a task needs " +
			"compute or files and list_actors shows no device actor, tell " +
			"the user plainly and guide them to attach one (install the " +
			"daemon / run the connect command they have). After they act, " +
			"check list_actors again and confirm what arrived before " +
			"proceeding.\n")
	}
	return b.String()
}

// platformTeachingPrompt is the L0-L2 stable prefix every coagent
// agent carries. Intentionally short — the goal is to anchor the
// agent on the coagent envelope protocol without exploding the cache
// surface. Future ticket can extend with concrete examples.
const platformTeachingPrompt = `You are a coagent agent — an LLM-backed actor inside a channel-scoped runtime.

Protocol contract (do not violate):
- You receive turn triggers that carry one user-visible message plus channel context.
- You reply by emitting one or more agent.text events. The runtime stamps sender/audience automatically.
- Public events are visible to the channel's other participants; system events are operational telemetry only.
- When you have nothing useful to add, exit the turn promptly — a terse "ack" beats a verbose filler.
- Tool calls (xhs publish, search, get-note, etc.) flow through the channel's adapter actors via call_actor. Reference them by their declared type; the harness routes the request.
- call_actor is fast-path: short calls return their result inline; long calls return an ack and the result comes back later (await_result to block, or react to it as a new message). Fan out with wait=false, then await_result/abandon. See "Tool invocation" in the channel context for the full pattern.

Stay grounded in the trigger payload and the channel's domain template below.`

// buildAgentEvent assembles a kind=event envelope through the behavior
// builder (ONE home for event defaults), then stamps the binding-edge
// fields this path owns (deterministic per-actor id, TSReceived).
func (b *Bridge) buildAgentEvent(
	envType string,
	visibility message.Visibility,
	audience message.Audience,
	payload []byte,
	parentID message.ID,
	correlationID message.ID,
) (message.Envelope, error) {
	now := b.cfg.NowFn()
	env, err := behavior.BuildEvent(b.chID, b.sender(),
		func() time.Time { return time.UnixMilli(now) },
		behavior.EventSpec{
			ID:            b.envelopeID(now),
			Type:          envType,
			Payload:       payload,
			Visibility:    visibility,
			Audience:      audience,
			ParentID:      parentID,
			CorrelationID: correlationID,
		})
	if err != nil {
		return message.Envelope{}, err
	}
	env.TSReceived = now
	return *env, nil
}
