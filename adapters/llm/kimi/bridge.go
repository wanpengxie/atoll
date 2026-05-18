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
	"github.com/wanpengxie/go-kimi/pkg/kimi/wire"

	// Force-register the anthropic provider factory. go-kimi's factory
	// uses init() side-effects to populate its provider constructor map;
	// without this import the DeepSeek anthropic-compat configuration
	// surfaces as "provider not found" at NewAgent time.
	_ "github.com/wanpengxie/go-kimi/pkg/kimi/llm/anthropic"

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
)

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

	// MaxTurns caps the bridge — same semantics as MockBridge. Defaults
	// to 32 (more than 8 because LLM turns are larger units of work).
	MaxTurns int

	// WorkDir is the directory go-kimi uses for sessions / wire log /
	// approvals. Defaults to a per-process tmp dir.
	WorkDir string

	// NowFn returns unix-ms. Defaults to time.Now.UnixMilli.
	NowFn func() int64

	// TextDeltaFlushInterval batches TextDelta wire events into a
	// single agent.text envelope. Defaults to 250ms — short enough
	// that the UI sees progress in near-realtime, long enough that
	// envelope traffic stays sane on noisy streams.
	TextDeltaFlushInterval time.Duration
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
	WorkerActorID() string
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
	CorrelationID string
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
	if cfg.MaxTurns <= 0 {
		cfg.MaxTurns = 32
	}
	if cfg.NowFn == nil {
		cfg.NowFn = func() int64 { return time.Now().UnixMilli() }
	}
	if cfg.TextDeltaFlushInterval <= 0 {
		cfg.TextDeltaFlushInterval = 250 * time.Millisecond
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
// Run is single-shot: spinning up a new go-kimi Agent inside Run keeps
// the session ID stable across turns within one Run (kimi's session
// stores the conversation history); a new Run call gets a fresh session.
func (b *Bridge) Run(ctx context.Context, ipc IPCFacade) error {
	if ipc == nil {
		return errors.New("kimi: Run nil ipc facade")
	}
	if ipc.WorkerActorID() == "" {
		return errors.New("kimi: Run actor id empty (handshake?)")
	}

	provider, err := b.buildProvider()
	if err != nil {
		return b.emitTerminalLLMError(ctx, ipc, err, "")
	}

	wireCh := make(chan wire.WireMessage, 128)
	emitter := wire.ChannelEmitter{Ch: wireCh}
	b.mu.Lock()
	b.testWireEmitter = emitter
	b.mu.Unlock()

	agent, err := b.agentNew(gokimi.AgentConfig{
		WorkDir:     b.cfg.WorkDir,
		Config:      config.NewDefaultConfig(),
		Provider:    provider,
		WireEmitter: emitter,
		Overrides: gokimi.AgentOverrides{
			SystemPrompt: b.cfg.SystemPrompt,
			Model:        b.cfg.Model,
		},
	})
	if err != nil {
		return b.emitTerminalLLMError(ctx, ipc, err, "")
	}
	defer func() { _ = agent.Close() }()

	turns := 0
	triggers := ipc.Triggers()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case payload, ok := <-triggers:
			if !ok {
				return nil
			}
			turns++
			if err := b.runTurn(ctx, ipc, agent, wireCh, payload); err != nil {
				// runTurn already emitted a terminal envelope; surface
				// the error to Runtime so the caller can decide whether
				// to exit non-zero.
				return err
			}
			if turns >= b.cfg.MaxTurns {
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

// runTurn drives one Agent.Run call: it composes the user input from
// the trigger envelope, kicks the agent in a goroutine, and consumes
// wire events until the turn completes or the agent errors out.
func (b *Bridge) runTurn(
	ctx context.Context,
	ipc IPCFacade,
	agent kimiAgent,
	wireCh chan wire.WireMessage,
	trigger TriggerPayload,
) error {
	input, err := composeUserInput(trigger)
	if err != nil {
		return b.emitTerminalLLMError(ctx, ipc, err, trigger.Envelope.ID)
	}

	turnCtx, cancel := context.WithCancel(ctx)
	defer cancel()

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

	consumeErr := b.consumeWire(turnCtx, ipc, wireCh, agentDone, trigger)
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
		return b.emitTerminalLLMError(ctx, ipc, runErr, trigger.Envelope.ID)
	}
	return nil
}

// consumeWire flushes wire events into envelopes until a TurnEnd
// arrives, agentDone closes (agent.Run returned with or without an
// error before emitting TurnEnd — typical of an LLM connection error),
// or the context expires. The flush cadence (cfg.TextDeltaFlushInterval)
// batches TextDelta increments so a streaming LLM response doesn't
// produce one envelope per chunk.
func (b *Bridge) consumeWire(
	ctx context.Context,
	ipc IPCFacade,
	wireCh chan wire.WireMessage,
	agentDone <-chan struct{},
	trigger TriggerPayload,
) error {
	var buffered strings.Builder
	flush := time.NewTicker(b.cfg.TextDeltaFlushInterval)
	defer flush.Stop()

	emitBuffered := func(final bool) error {
		text := buffered.String()
		if text == "" {
			return nil
		}
		buffered.Reset()
		visibility := message.VisibilitySystem
		payload := map[string]any{"text": text}
		if final {
			visibility = message.VisibilityPublic
			payload["next_action"] = "continue"
		}
		return b.emitEnvelope(ctx, ipc, trigger, "agent.text", visibility, payload)
	}

	for {
		select {
		case <-ctx.Done():
			_ = emitBuffered(false)
			return ctx.Err()
		case <-agentDone:
			// Agent returned (success or error). Drain any wire events
			// still buffered in the channel before exiting so the test
			// double's scripted TurnEnd is not lost when it fires just
			// before agent.Run returns. Non-blocking drain — anything
			// the agent emitted is already in the channel buffer.
			for drained := true; drained; {
				select {
				case msg, ok := <-wireCh:
					if !ok {
						drained = false
						break
					}
					if err := b.handleWireMsg(ctx, ipc, msg, &buffered, trigger); err != nil {
						return err
					}
					if _, isEnd := msg.(wire.TurnEnd); isEnd {
						return nil
					}
				default:
					drained = false
				}
			}
			// No TurnEnd seen — flush any remaining text so the UI
			// doesn't lose the trailing fragment. The caller emits the
			// terminal failed envelope when runErr != nil.
			_ = emitBuffered(false)
			return nil
		case <-flush.C:
			if err := emitBuffered(false); err != nil {
				return err
			}
		case msg, ok := <-wireCh:
			if !ok {
				return nil
			}
			if err := b.handleWireMsg(ctx, ipc, msg, &buffered, trigger); err != nil {
				return err
			}
			if _, isEnd := msg.(wire.TurnEnd); isEnd {
				return nil
			}
		}
	}
}

// handleWireMsg routes one wire message to the appropriate envelope
// emission path (or drops it). Returns the first envelope-write error.
func (b *Bridge) handleWireMsg(
	ctx context.Context,
	ipc IPCFacade,
	msg wire.WireMessage,
	buffered *strings.Builder,
	trigger TriggerPayload,
) error {
	switch m := msg.(type) {
	case wire.TextDelta:
		buffered.WriteString(m.Delta)
		return nil
	case wire.TurnEnd:
		// Flush buffered text as a system envelope first, then emit
		// the public terminal envelope stamping next_action.
		if buffered.Len() > 0 {
			payload := map[string]any{"text": buffered.String()}
			buffered.Reset()
			if err := b.emitEnvelope(ctx, ipc, trigger, "agent.text", message.VisibilitySystem, payload); err != nil {
				return err
			}
		}
		return b.emitTurnEnd(ctx, ipc, trigger, m)
	default:
		// Other wire events dropped in M1.6 scope. Future promotion:
		// tool_call_request → tool.invocation envelope, etc.
		return nil
	}
}

func (b *Bridge) emitTurnEnd(
	ctx context.Context,
	ipc IPCFacade,
	trigger TriggerPayload,
	end wire.TurnEnd,
) error {
	nextAction := "continue"
	switch strings.ToLower(strings.TrimSpace(end.StopReason)) {
	case "end_turn", "stop", "completed", "finish":
		nextAction = "done"
	case "max_tokens":
		nextAction = "max_tokens"
	case "tool_use":
		// LLM yielded back to wait for an external tool call. In M1.6
		// we treat this as a logical pause (still done from the agent's
		// turn perspective).
		nextAction = "tool_use"
	}
	payload := map[string]any{
		"text":        contentPartsToText(end.Output),
		"next_action": nextAction,
		"stop_reason": end.StopReason,
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
	env := message.Envelope{
		ID:            b.envelopeID(ipc, now),
		ChannelID:     string(ipc.ChannelID()),
		Type:          envType,
		Kind:          message.KindEvent,
		Sender:        message.Sender{Kind: message.SenderAgent, ID: ipc.WorkerActorID()},
		Visibility:    visibility,
		Audience:      []string{"*"},
		Payload:       body,
		CorrelationID: trigger.CorrelationID,
		ParentID:      trigger.Envelope.ID,
		TS:            now,
		TSReceived:    now,
	}
	return ipc.WriteEnvelope(ctx, env)
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
	parentEnvID string,
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
		ChannelID:  string(ipc.ChannelID()),
		Type:       "agent.text",
		Kind:       message.KindEvent,
		Sender:     message.Sender{Kind: message.SenderAgent, ID: ipc.WorkerActorID()},
		Visibility: message.VisibilityPublic,
		Audience:   []string{"*"},
		Payload:    body,
		ParentID:   parentEnvID,
		TS:         now,
		TSReceived: now,
	}
	if writeErr := ipc.WriteEnvelope(ctx, env); writeErr != nil {
		return errors.Join(err, writeErr)
	}
	return err
}

// envelopeID generates a deterministic-shape id for emitted envelopes.
// The per-bridge sequence keeps multiple emits in the same millisecond
// unique while preserving the worker/time prefix for debugging.
func (b *Bridge) envelopeID(ipc IPCFacade, nowMs int64) string {
	workerID := ipc.WorkerID()
	if workerID == "" {
		workerID = "anon"
	}
	return fmt.Sprintf("kimi-%s-%d-%d", workerID, nowMs, b.envelopeSeq.Add(1))
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

// contentPartsToText flattens kimi ContentParts (text + tool_use parts)
// into the plain-text representation we emit on the public envelope.
// Tool use parts are dropped in M1.6 (no UI consumer yet).
func contentPartsToText(parts any) string {
	if parts == nil {
		return ""
	}
	// The go-kimi types.ContentParts is a []types.ContentPart slice with
	// a per-part TextPart variant. We use reflection-free JSON
	// marshaling because we don't want to import the entire types
	// package surface here (keeps the adapter dependency footprint
	// minimal). The shape is well-known.
	raw, err := json.Marshal(parts)
	if err != nil {
		return ""
	}
	var slice []struct {
		Type string `json:"type"`
		Text string `json:"text,omitempty"`
	}
	if err := json.Unmarshal(raw, &slice); err != nil {
		// Fall back to the raw JSON so we don't black-hole content
		// when the format changes upstream.
		return string(raw)
	}
	var b strings.Builder
	for i := range slice {
		if slice[i].Type == "text" || slice[i].Text != "" {
			b.WriteString(slice[i].Text)
		}
	}
	return b.String()
}

// BuildBasePrompt assembles the prompt-cache friendly stable prefix
// for the kimi system prompt. Layout:
//
//	[L0-L2 platform teaching]
//	[L4 domain prompt — from COAGENT_DOMAIN_PROMPT]
//
// channelType is purely informational (helps a debug operator grep
// session logs). Empty COAGENT_DOMAIN_PROMPT (legacy channels) yields
// the platform prompt alone.
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
- Tool calls (xhs publish, search, get-note, etc.) flow through the channel's adapter actors. Reference them by their declared type; the daemon harness routes the request.

Stay grounded in the trigger payload and the channel's domain template below.`
