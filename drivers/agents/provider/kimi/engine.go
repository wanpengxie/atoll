// Package kimi is the go-kimi agent engine, built on agent/base's skeleton
// (期10 S5). It implements base.Engine (the model-adaptation三件套 — Turn /
// Describe / Checkpoint / Close); the mailbox loop, turn queue, response分拣,
// describe dispatch, per-turn checkpoint挂账, and emit all live in agent/base.
// The go-kimi流式 wire帧 state machine (wire_parse.go) is封 entirely inside
// Turn — the base never sees it.
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
	return func(sys actorbase.Sys, seed []byte) (base.Engine, error) {
		workDir, err := os.MkdirTemp("", "atoll-kimi-")
		if err != nil {
			return nil, fmt.Errorf("kimi: workdir: %w", err)
		}
		e := &engine{cfg: cfg, workDir: workDir}
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
// restored session id would point at files a fresh tmp dir does not carry) —
// durable kimi resume needs a durable WorkDir,申报 defer. Checkpoint returns
// nil in kind.
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

// Turn drives one Agent.Run: compose the user input from the trigger, kick the
// agent in a goroutine, and consume wire events until the turn completes. The
// per-step progress + terminal reply are written to the base Sink (Final=false /
// Final=true). An engine/LLM error becomes a failed terminal Output (actor stays
// alive, Turn returns nil); only a Sink write failure (A1) propagates as loud死.
func (e *engine) Turn(ctx context.Context, trigger base.Trigger, sink base.Sink) error {
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
		// A Sink write failure is a plumbing break (A1 — propagate loud). A
		// missing TurnEnd means the run ended abnormally; the provider error
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

// Checkpoint returns nil — kimi's session lives on-disk in the ephemeral
// WorkDir, so there is no durable resume seed to persist across incarnations
// this period (申报: durable kimi resume needs a durable WorkDir, defer; the
// claude engine demonstrates the checkpoint path on its server-side sessions).
func (e *engine) Checkpoint() []byte { return nil }

// Close releases the go-kimi agent at incarnation death.
func (e *engine) Close() error {
	if e.kagent != nil {
		return e.kagent.Close()
	}
	return nil
}

// agentDescription / agentSkillDoc are the agent's actor.describe self-answer.
const agentDescription = "LLM agent: the channel's conversational brain. Send it any request — it reasons over the channel context and orchestrates the channel's tool actors via call_actor."

const agentSkillDoc = "# agent\n\n" +
	"Conversational actor backed by an LLM. It accepts any kind=request as a " +
	"turn trigger (no closed type set), replies with agent.text events " +
	"(public terminal + system progress), and calls other actors through the " +
	"channel's meta tools.\n"
