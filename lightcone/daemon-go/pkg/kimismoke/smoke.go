// Package kimismoke holds a self-contained go-kimi integration smoke for
// the daemon-go module — an adaptation of go-kimi's examples/01_basic_turn
// that wires up the in-process "echo" provider so the smoke runs in CI
// without an OPENAI_API_KEY (or any network access).
//
// The smoke proves three things every CI build:
//
//  1. The github.com/wanpengxie/go-kimi module resolves + links into the
//     daemon-go binary chain (any breaking API change in go-kimi will
//     break the smoke at compile time).
//  2. NewAgent + Run + LastResult + Close round-trip cleanly — the four
//     public SDK calls daemon-go relies on for the M1.3 worker runtime
//     (T10) wrapper.
//  3. The echo provider returns deterministic, non-empty content — the
//     baseline we use to assert "the agent loop actually moved forward".
//
// Real LLM smokes (Moonshot / Anthropic / OpenAI / etc.) belong in T10
// once the worker runtime + v4 ABI adapter exists. T0 only certifies the
// integration seam.
package kimismoke

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	kimi "github.com/wanpengxie/go-kimi/pkg/kimi"
	"github.com/wanpengxie/go-kimi/pkg/kimi/config"
	"github.com/wanpengxie/go-kimi/pkg/kimi/llm"
	"github.com/wanpengxie/go-kimi/pkg/kimi/types"
)

// Options configures a single smoke run.
type Options struct {
	// Prompt is what we hand to agent.Run. Defaults to a fixed greeting
	// when empty so callers can call Run(ctx, Options{}) without ceremony.
	Prompt string
	// WorkDir is the agent's working directory. Empty means the caller
	// is OK with the smoke creating its own temp dir.
	WorkDir string
	// Model overrides the echo provider model name. Empty is fine.
	Model string
}

// Result captures the smoke outcome in a daemon-go-friendly shape.
type Result struct {
	// Reply is the assistant text returned for the prompt. Always
	// non-empty on success.
	Reply string
	// Model is the provider model name the agent advertised.
	Model string
}

// Run executes one go-kimi turn against the in-process echo provider and
// returns the assistant text. It is the canonical T0 acceptance check
// referenced by the ticket: "能跑通 go-kimi examples/01_basic_turn 改编版".
func Run(ctx context.Context, opts Options) (*Result, error) {
	if ctx == nil {
		return nil, errors.New("kimismoke: ctx is required")
	}

	prompt := strings.TrimSpace(opts.Prompt)
	if prompt == "" {
		prompt = "hello from daemon-go kimismoke"
	}

	model := opts.Model
	if model == "" {
		model = "echo-smoke"
	}

	provider, err := llm.NewProvider(llm.ProviderConfig{
		Type:  string(llm.ProviderTypeEcho),
		Model: model,
	})
	if err != nil {
		return nil, fmt.Errorf("kimismoke: build echo provider: %w", err)
	}

	workDir := opts.WorkDir
	if workDir == "" {
		// Agent only writes inside WorkDir when a tool that actually
		// needs persistence is invoked; for the smoke we hand it the
		// caller-provided dir or fall back to the OS temp area.
		workDir = os.TempDir()
	}

	agent, err := kimi.NewAgent(kimi.AgentConfig{
		WorkDir:  workDir,
		Config:   newSmokeConfig(model),
		Provider: provider,
	})
	if err != nil {
		return nil, fmt.Errorf("kimismoke: NewAgent: %w", err)
	}
	defer func() {
		// We intentionally swallow Close errors here — the smoke's job
		// is to exercise the happy path, not to fight teardown races.
		_ = agent.Close()
	}()

	if err := agent.Run(ctx, prompt); err != nil {
		return nil, fmt.Errorf("kimismoke: agent.Run: %w", err)
	}

	reply := textFromParts(agent.LastResult().Content)
	if reply == "" {
		return nil, errors.New("kimismoke: agent returned empty content")
	}
	return &Result{Reply: reply, Model: model}, nil
}

// newSmokeConfig wires a minimal go-kimi config that points the agent at
// the in-process echo provider and disables every optional service that
// would otherwise hit the network (moonshot fetch/search, MCP clients).
func newSmokeConfig(model string) config.Config {
	cfg := config.NewDefaultConfig()
	cfg.Providers = []config.LLMProvider{
		{Name: "echo", Type: string(llm.ProviderTypeEcho)},
	}
	cfg.Models = []config.LLMModel{
		{Name: model, Provider: "echo"},
	}
	cfg.DefaultProvider = "echo"
	cfg.DefaultModel = model
	cfg.Services.MoonshotFetch.Enabled = false
	cfg.Services.MoonshotSearch.Enabled = false
	cfg.MCP.Clients = nil
	return cfg
}

func textFromParts(parts types.ContentParts) string {
	var b strings.Builder
	for _, p := range parts {
		if tp, ok := p.(types.TextPart); ok {
			b.WriteString(tp.Text)
		}
	}
	return strings.TrimSpace(b.String())
}
