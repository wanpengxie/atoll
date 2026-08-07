// Package kimi adapts go-kimi to the asynchronous base.Engine contract. The
// provider's streaming wire state machine remains private; complete phases and
// the terminal value return through EventPort while base owns mailbox
// arbitration, progress, activity, persistence, and request settlement.
//
// The engine drives the shared actor-invocation machinery through a
// metatool.Exec (the substrate JobTable + sys.Call face) built from Sys — it
// does not re-implement the call mechanism; "100 agents, 100 job-control
// implementations" is the anti-pattern the meta-tool positioning closes.
package kimi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	gokimi "github.com/wanpengxie/go-kimi/pkg/kimi"
	"github.com/wanpengxie/go-kimi/pkg/kimi/config"
	"github.com/wanpengxie/go-kimi/pkg/kimi/llm"
	"github.com/wanpengxie/go-kimi/pkg/kimi/wire"

	// Force-register the anthropic provider factory (init() side-effects).
	_ "github.com/wanpengxie/go-kimi/pkg/kimi/llm/anthropic"

	"github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/lib/metatool"
)

// Env keys read at config time. The ATOLL_CHANNEL_TYPE / ATOLL_DOMAIN_PROMPT
// env keys are GONE (A3 / Q7): the per-channel domain prompt now rides
// InstanceSpec.Config (the applied actor declaration version), the one config carrier.
const (
	EnvKeyAPIKey  = "KIMI_API_KEY"
	EnvKeyBaseURL = "KIMI_BASE_URL"
	EnvKeyModel   = "KIMI_MODEL"

	defaultFastPathWindow = 15 * time.Second
)

// Config drives a kimi engine. Sane defaults come from NewConfigFromSpec.
type Config struct {
	APIKey         string
	BaseURL        string
	Model          string
	ProviderType   string
	SystemPrompt   string
	FastPathWindow time.Duration
}

// specOverlay is the per-instance applied declaration config the looper
// self-parses: creds/model overrides plus the channel's domain prompt facts
// (A3/Q7 — what ATOLL_CHANNEL_TYPE / ATOLL_DOMAIN_PROMPT once carried).
type specOverlay struct {
	Model        string `json:"model"`
	BaseURL      string `json:"base_url"`
	APIKey       string `json:"api_key"`
	ProviderType string `json:"provider_type"`
	ChannelType  string `json:"channel_type"`
	DomainPrompt string `json:"domain_prompt"`
}

// NewConfigFromSpec builds a Config from the platform env DEFAULTS, then
// overlays the per-instance spec.Config, assembling the system prompt from the
// host situation + the config's domain facts. A required field (APIKey / Model)
// missing from BOTH is a hard error (fail fast at assembly).
func NewConfigFromSpec(raw json.RawMessage, sit Situation) (Config, error) {
	var overlay specOverlay
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &overlay); err != nil {
			return Config{}, fmt.Errorf("kimi: parse spec config: %w", err)
		}
	}
	cfg := Config{
		APIKey:         firstNonEmpty(overlay.APIKey, strings.TrimSpace(os.Getenv(EnvKeyAPIKey))),
		BaseURL:        firstNonEmpty(overlay.BaseURL, strings.TrimSpace(os.Getenv(EnvKeyBaseURL))),
		Model:          firstNonEmpty(overlay.Model, strings.TrimSpace(os.Getenv(EnvKeyModel))),
		ProviderType:   firstNonEmpty(overlay.ProviderType, "anthropic"),
		SystemPrompt:   BuildSystemPrompt(sit, overlay.ChannelType, overlay.DomainPrompt),
		FastPathWindow: defaultFastPathWindow,
	}
	if cfg.APIKey == "" {
		return Config{}, fmt.Errorf("kimi: %s env or config api_key required for the kimi provider", EnvKeyAPIKey)
	}
	if cfg.Model == "" {
		return Config{}, fmt.Errorf("kimi: %s env or config model required (pick a deepseek model id)", EnvKeyModel)
	}
	return cfg, nil
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// kimiAgent is the subset of go-kimi.Agent the engine consumes. Carved out so
// the test suite can stub the LLM without spinning provider HTTP.
type kimiAgent interface {
	Run(ctx context.Context, input string) error
	Close() error
}

// engine is the go-kimi base.Engine. The go-kimi Agent + its wire channel are
// built once (per incarnation) and reused across turns.
type engine struct {
	cfg     Config
	workDir string
	x       *metatool.Exec

	kagent kimiAgent
	wireCh chan wire.WireMessage
	events base.EventPort
	life   context.Context
	mu     sync.Mutex
	cancel context.CancelFunc
	booted bool
	closed bool
}

var _ base.Engine = (*engine)(nil)

// agentFactory builds a kimiAgent from an AgentConfig (swapped in tests).
type agentFactory func(gokimi.AgentConfig) (kimiAgent, error)

func defaultAgentFactory(ac gokimi.AgentConfig) (kimiAgent, error) {
	return gokimi.NewAgent(ac)
}

// newEngineFn returns the base.NewEngine the Constructor closes over: it builds
// the exec face from Sys, then the provider + go-kimi Agent (with the meta-tool
// AdditionalTools installed). A boot failure is loud死 (no half-alive agent).
func newEngineFn(cfg Config, factory agentFactory) base.NewEngine {
	return func(sys actorbase.Sys, seed []byte, events base.EventPort) (base.Engine, error) {
		workDir, err := os.MkdirTemp("", "atoll-kimi-")
		if err != nil {
			return nil, fmt.Errorf("kimi: workdir: %w", err)
		}
		e := &engine{cfg: cfg, workDir: workDir, events: events, life: sys.Life()}
		e.x = base.ExecFace(sys, cfg.FastPathWindow)

		provider, err := e.buildProvider()
		if err != nil {
			return nil, err
		}
		e.wireCh = make(chan wire.WireMessage, 128)
		emitter := wire.ChannelEmitter{Ch: e.wireCh}
		kagent, err := e.buildAgent(provider, emitter, factory)
		if err != nil {
			return nil, err
		}
		e.kagent = kagent
		return e, nil
	}
}

// buildAgent builds a fresh kimiAgent bound to (workDir, emitter, provider) with
// the meta-tool surface installed. Cold start: kimi's session lives on-disk in
// the ephemeral WorkDir, so the durable resume seed is NOT consumed here (a
// restored session id would point at files a fresh tmp dir does not carry).
func (e *engine) buildAgent(provider llm.ChatProvider, emitter wire.Emitter, factory agentFactory) (kimiAgent, error) {
	return factory(gokimi.AgentConfig{
		WorkDir:         e.workDir,
		Config:          config.NewDefaultConfig(),
		Provider:        provider,
		WireEmitter:     emitter,
		AdditionalTools: e.channelTools(),
		// Hermetic skill discovery: the agent sees ONLY the channel-type catalog
		// we install, never arbitrary $HOME/.kimi/skills.
		SkillRoots: []string{},
		Overrides: gokimi.AgentOverrides{
			SystemPrompt: e.cfg.SystemPrompt,
			Model:        e.cfg.Model,
		},
	})
}

// buildProvider hands a fully-configured llm.ChatProvider to gokimi.NewAgent
// (bypassing the multi-provider config file — the deploy uses one fixed
// env/config-driven provider).
func (e *engine) buildProvider() (llm.ChatProvider, error) {
	p, err := llm.NewProvider(llm.ProviderConfig{
		Type:    e.cfg.ProviderType,
		BaseURL: e.cfg.BaseURL,
		APIKey:  e.cfg.APIKey,
		Model:   e.cfg.Model,
	})
	if err != nil {
		return nil, fmt.Errorf("kimi: provider %q: %w", e.cfg.ProviderType, err)
	}
	if e.cfg.Model != "" {
		p = p.WithModel(e.cfg.Model)
	}
	return p, nil
}

// runTurn drives one Agent.Run: compose the user input from the trigger, kick the
// agent in a goroutine, and consume wire events until the turn completes. The
// tool phases + terminal full value are collected and then forwarded through
// EventPort. An engine/LLM error becomes a failed turn; collector plumbing
// errors remain loud implementation faults.
func (e *engine) runTurn(ctx context.Context, trigger base.Trigger, sink turnSink) error {
	input := composeUserInput(trigger.Envelope)

	turnCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	turnCtx = context.WithValue(turnCtx, channelToolRuntimeKey{}, rcFromTrigger(trigger))

	agentDone := make(chan struct{})
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- e.kagent.Run(turnCtx, input)
		close(agentDone)
	}()

	consumeErr := e.consumeWire(turnCtx, agentDone, trigger, sink)
	if consumeErr != nil {
		// A collector write failure is a plumbing break. A missing TurnEnd
		// means the run ended abnormally; the provider error
		// (runErr) is the meaningful signal, surfaced as a failed terminal.
		if errors.Is(consumeErr, errSinkWrite) {
			return consumeErr
		}
		cancel()
		select {
		case runErr := <-runErrCh:
			if runErr != nil && !errors.Is(runErr, context.Canceled) {
				return e.emitTerminalLLMError(sink, runErr)
			}
			// No LLM error to explain the missing TurnEnd — surface consumeErr
			// itself as a failed terminal (stay alive), not a silent swallow.
			return e.emitTerminalLLMError(sink, consumeErr)
		case <-ctx.Done():
			return errors.Join(consumeErr, ctx.Err())
		}
	}
	runErr := <-runErrCh
	if runErr != nil {
		return e.emitTerminalLLMError(sink, runErr)
	}
	return nil
}

// Describe returns the provider's actor.describe data (the base stamps ActorID).
func (e *engine) Describe() introspect.Describe {
	return introspect.Describe{
		Description: agentDescription,
		SkillDoc:    agentSkillDoc,
	}
}

func (e *engine) Boot(context.Context, base.BootPort) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return errors.New("kimi: engine closed")
	}
	e.booted = true
	return nil
}

type eventSink struct {
	e      *engine
	turnID string
	final  *finalValue
	failed *failure
}

func (s *eventSink) ToolStarted(a toolActivity) error {
	s.e.events.Tool(s.turnID, a.CallID, "started", a.Tool, "started", a.Detail)
	return nil
}
func (s *eventSink) ToolEnded(a toolActivity) error {
	s.e.events.Tool(s.turnID, a.CallID, "ended", a.Tool, a.Status, a.Detail)
	return nil
}
func (s *eventSink) Complete(v finalValue) error { s.final = &v; return nil }
func (s *eventSink) Fail(f failure) error        { s.failed = &f; return nil }

func (e *engine) StartTurn(op base.OpID, batch []base.Trigger, background []base.ContextItem) error {
	e.mu.Lock()
	if !e.booted || e.closed {
		e.mu.Unlock()
		return errors.New("kimi: engine unavailable")
	}
	if e.cancel != nil {
		e.mu.Unlock()
		return errors.New("kimi: turn already active")
	}
	ctx, cancel := context.WithCancel(e.life)
	e.cancel = cancel
	e.mu.Unlock()
	if len(batch) == 0 {
		e.events.TurnRejected(op, "provider_failed", "empty batch")
		return nil
	}
	turnID := string(op)
	e.events.TurnStarted(op, turnID)
	tr := batch[len(batch)-1]
	var input strings.Builder
	if len(background) > 0 {
		input.WriteString("频道最近记录（可能与你已知重叠）：\n")
		for _, item := range background {
			input.WriteString(item.Rendered)
			input.WriteByte('\n')
		}
	}
	for _, item := range batch {
		input.WriteString(composeUserInput(item.Envelope))
		input.WriteByte('\n')
	}
	raw, _ := json.Marshal(map[string]any{"text": strings.TrimSpace(input.String())})
	tr.Envelope.Payload = raw
	go func() {
		defer func() { e.mu.Lock(); e.cancel = nil; e.mu.Unlock() }()
		sink := &eventSink{e: e, turnID: turnID}
		err := e.runTurn(ctx, tr, sink)
		if errors.Is(err, context.Canceled) {
			e.events.TurnEnded(turnID, base.TurnStatusInterrupted, "", err.Error())
			return
		}
		if err != nil {
			e.events.TurnEnded(turnID, base.TurnStatusFailed, "", err.Error())
			return
		}
		if sink.failed != nil {
			e.events.TurnEnded(turnID, base.TurnStatusFailed, "", sink.failed.Detail)
			return
		}
		text := ""
		if sink.final != nil {
			text = sink.final.Text
		}
		e.events.TurnEnded(turnID, base.TurnStatusOK, text, "")
	}()
	return nil
}
func (e *engine) Steer(op base.OpID, _ base.Trigger) error {
	e.events.ControlDone(op, base.ControlNotSteerable, "", "kimi has no steer primitive")
	return nil
}
func (e *engine) Interrupt(op base.OpID) error {
	e.mu.Lock()
	cancel := e.cancel
	e.mu.Unlock()
	if cancel == nil {
		e.events.ControlDone(op, base.ControlNoActiveTurn, "", "")
	} else {
		cancel()
		e.events.ControlDone(op, base.ControlAccepted, "", "")
	}
	return nil
}
func (e *engine) Terminate() error {
	e.mu.Lock()
	cancel := e.cancel
	e.booted = false
	e.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if e.kagent != nil {
		return e.kagent.Close()
	}
	return nil
}
func (e *engine) EnsureAlive(op base.OpID) error {
	e.events.ControlDone(op, base.ControlRPCError, "", "kimi engine cannot reopen a closed agent")
	return nil
}
func (e *engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	cancel := e.cancel
	e.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if e.kagent != nil {
		return e.kagent.Close()
	}
	return nil
}

// agentDescription / agentSkillDoc are the agent's actor.describe self-answer.
const agentDescription = "LLM agent: the channel's conversational brain. Send it any request — it reasons over the channel context and orchestrates the channel's tool actors via call_actor."

const agentSkillDoc = "# agent\n\n" +
	"Conversational actor backed by an LLM. It accepts any kind=request as a " +
	"turn trigger (no closed type set), replies with a terminal response, emits " +
	"typed activity phases, and calls other actors through the " +
	"channel's meta tools.\n"
