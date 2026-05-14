package worker

// agent.go assembles the go-kimi Agent instance that powers a single
// worker process. The factory packages the v4 WireEmitter bridge +
// optional T11-supplied AdditionalTools + a minimal config (no MCP,
// no Moonshot fetch/search — those add network calls we don't want
// during PM-unattended operation).
//
// The factory is the L2 §3.9.6 "Spec / config 集成" seam: callers
// supply the LLM Provider (echo for tests, real provider via daemon
// config in production), the WireEmitter (the bridge from
// wire_bridge.go), and any v4-wrapped tools (T11 will hand a non-empty
// slice once V4ize lands). M1.3 baseline keeps WorkDir simple — the
// worker process owns a per-channel working dir under
// `<channel_workdir>/agents/<actor_id>/`.

import (
	"errors"
	"fmt"
	"strings"

	kimi "github.com/wanpengxie/go-kimi/pkg/kimi"
	"github.com/wanpengxie/go-kimi/pkg/kimi/config"
	"github.com/wanpengxie/go-kimi/pkg/kimi/llm"
	"github.com/wanpengxie/go-kimi/pkg/kimi/tools"
	"github.com/wanpengxie/go-kimi/pkg/kimi/wire"
)

// AgentConfig is the input to NewAgent. WorkDir + Provider + Emitter
// are required; everything else is optional (zero values give the M1.3
// "minimal go-kimi" baseline).
type AgentConfig struct {
	// WorkDir is the agent's working directory (the channel workdir or
	// a per-actor subdirectory). go-kimi writes session state there.
	WorkDir string

	// SessionID lets the caller pin a session id so a respawned worker
	// resumes the same session. Empty defaults to go-kimi's auto-id.
	SessionID string

	// Provider is the LLM chat provider. Tests use the echo provider
	// (llm.NewEchoChatProvider) — see kimismoke for the reference
	// wiring. Production callers wire a real provider out of the
	// daemon config.
	Provider llm.ChatProvider

	// Model is the model name to advertise. Defaults to "echo-worker"
	// when empty so the echo provider has something to report.
	Model string

	// Emitter is the wire.Emitter sink. The worker normally hands in
	// *WireBridge; tests can substitute wire.NoopEmitter for fast
	// paths that don't care about emitted events.
	Emitter wire.Emitter

	// AdditionalTools is the slice of v4-wrapped tools T11 will inject.
	// M1.3 baseline ships with this empty — agents fall back to the
	// in-process go-kimi default tool set. When T11 lands, the runtime
	// substitutes V4ize(tool)-wrapped versions here so every tool call
	// also lands in channel log.
	AdditionalTools []tools.Tool

	// DisableStandardSandboxTools mirrors kimi.AgentConfig — when T11
	// adds its full sandbox tool catalog, set true so AdditionalTools
	// wins the candidate-dedup race.
	DisableStandardSandboxTools bool
}

// NewAgent builds a go-kimi *kimi.Agent ready for Run. The returned
// instance owns its session + provider; callers MUST call Close() when
// done (the runtime does this on shutdown).
//
// Errors:
//   - returns an error wrapping ErrAgentConfigInvalid when required
//     fields are missing;
//   - wraps any go-kimi NewAgent error verbatim (config parse, session
//     open, etc.).
func NewAgent(cfg AgentConfig) (*kimi.Agent, error) {
	if strings.TrimSpace(cfg.WorkDir) == "" {
		return nil, fmt.Errorf("%w: workdir required", ErrAgentConfigInvalid)
	}
	if cfg.Provider == nil {
		return nil, fmt.Errorf("%w: provider required", ErrAgentConfigInvalid)
	}
	if cfg.Emitter == nil {
		return nil, fmt.Errorf("%w: emitter required", ErrAgentConfigInvalid)
	}

	model := strings.TrimSpace(cfg.Model)
	if model == "" {
		model = "echo-worker"
	}

	kcfg := kimi.AgentConfig{
		WorkDir:                     cfg.WorkDir,
		SessionID:                   cfg.SessionID,
		Config:                      newMinimalKimiConfig(model, cfg.Provider),
		Provider:                    cfg.Provider,
		WireEmitter:                 cfg.Emitter,
		AdditionalTools:             cfg.AdditionalTools,
		DisableStandardSandboxTools: cfg.DisableStandardSandboxTools,
	}
	agent, err := kimi.NewAgent(kcfg)
	if err != nil {
		return nil, fmt.Errorf("worker_agent: kimi.NewAgent: %w", err)
	}
	return agent, nil
}

// ErrAgentConfigInvalid is the sentinel callers use to distinguish
// configuration errors from runtime / driver failures.
var ErrAgentConfigInvalid = errors.New("worker_agent_config_invalid")

// newMinimalKimiConfig builds a go-kimi config.Config that mirrors the
// kimismoke approach: one provider, one model, all external services
// disabled, no MCP clients. M1.3 baseline avoids network calls
// (Moonshot fetch / search) because the protocol-baseline LLM provider
// is selected at the daemon level.
//
// The provider argument is the registered name + type — we register
// it under the canonical name "primary" so config indirection works
// without exposing the underlying provider type to callers.
func newMinimalKimiConfig(model string, provider llm.ChatProvider) config.Config {
	providerType := string(llm.ProviderTypeEcho)
	if provider != nil {
		// Use the provider's reported model to derive a stable type
		// tag where possible. For known providers (echo) we know the
		// type; for everything else fall back to "echo" so go-kimi's
		// factory does not refuse to start when the type is opaque
		// (the actual provider is injected directly via
		// AgentConfig.Provider so the factory tag is informational).
		_ = provider.ModelName()
	}
	cfg := config.NewDefaultConfig()
	cfg.Providers = []config.LLMProvider{
		{Name: "primary", Type: providerType},
	}
	cfg.Models = []config.LLMModel{
		{Name: model, Provider: "primary"},
	}
	cfg.DefaultProvider = "primary"
	cfg.DefaultModel = model
	cfg.Services.MoonshotFetch.Enabled = false
	cfg.Services.MoonshotSearch.Enabled = false
	cfg.MCP.Clients = nil
	return cfg
}
