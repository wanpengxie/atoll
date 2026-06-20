package claudecode

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/wanpengxie/ActOS/platform"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/registry"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

// Env keys (the claude CLI carries its OWN auth — ANTHROPIC_API_KEY / `claude
// login` — so coagent passes no key; only a model + the prompt situation).
const (
	EnvKeyModel        = "COAGENT_CLAUDE_MODEL"
	EnvKeyChannelType  = "COAGENT_CHANNEL_TYPE"
	EnvKeyDomainPrompt = "COAGENT_DOMAIN_PROMPT"

	defaultModel = "claude-sonnet-4-5"
)

// Config drives a claude Bridge. Lean by design: the engine is the `claude` CLI
// (auth + workspace are its own), so coagent supplies a model, the platform
// system prompt, and the durable resume seam.
type Config struct {
	Model        string
	SystemPrompt string
	// WorkDir is the Cwd for the claude session (the durable resume dir when the
	// state slot provides one; else a per-process tmp).
	WorkDir string
	// ResumeSeed is a claude session id (the state slot blob) to resume on boot.
	ResumeSeed json.RawMessage
	// Checkpoint persists a looper-authored blob (the session id) to the slot.
	Checkpoint func(json.RawMessage) error
	// NowFn returns unix-ms. Defaults to time.Now.UnixMilli.
	NowFn func() int64
}

// NewConfigFromSpec layers env defaults under the per-instance spec.Config
// overlay (the looper self-parses its own schema; agent-spec §三).
func NewConfigFromSpec(raw json.RawMessage, systemPrompt string) (Config, error) {
	cfg := Config{
		Model:        strings.TrimSpace(os.Getenv(EnvKeyModel)),
		SystemPrompt: systemPrompt,
	}
	if len(raw) > 0 {
		var overlay struct {
			Model string `json:"model"`
		}
		if err := json.Unmarshal(raw, &overlay); err != nil {
			return Config{}, fmt.Errorf("claude: parse spec config: %w", err)
		}
		if overlay.Model != "" {
			cfg.Model = overlay.Model
		}
	}
	if cfg.Model == "" {
		cfg.Model = defaultModel
	}
	return cfg, nil
}

// NewDecl is the LooperConstructor the agent core dispatches to when
// agents.looper selects claude. Same shape as the go-kimi looper: one class
// ("agent"), engine chosen by looper; id from the spec; Situation host-derived.
func NewDecl(spec registry.InstanceSpec, ctx registry.Deps) (platform.ActorDecl, error) {
	if ctx.ChannelID == "" {
		return platform.ActorDecl{}, errors.New("claude agent: requires a channel")
	}
	id := spec.ID
	if id == "" {
		return platform.ActorDecl{}, errors.New("claude agent: requires an explicit instance id")
	}
	sit := Situation{Host: "server"}
	if ctx.WorkspaceDir != "" {
		sit = Situation{Host: "daemon", HasWorkspace: true, WorkspaceDir: ctx.WorkspaceDir}
	}
	cfg, err := NewConfigFromSpec(spec.Config, buildSystemPrompt(
		sit, os.Getenv(EnvKeyChannelType), os.Getenv(EnvKeyDomainPrompt)))
	if err != nil {
		return platform.ActorDecl{}, fmt.Errorf("config: %w", err)
	}
	if ctx.State.Dir != "" {
		cfg.WorkDir = ctx.State.Dir
	}
	cfg.ResumeSeed = ctx.State.Seed
	cfg.Checkpoint = ctx.State.Store
	chID := ctx.ChannelID
	return platform.ActorDecl{
		ID:      id,
		Kind:    actor.KindAgent,
		Binding: actor.BindingRuntimeOutbound,
		Factory: func(w harness.Writer) actorrt.Actor {
			b, err := NewBridge(cfg, id, chID, w)
			if err != nil {
				log.Fatalf("claude agent bridge: %v", err)
			}
			return b
		},
	}, nil
}

// Situation is the runtime facts the prompt is built from — facts only, no role
// labels (the engine derives its posture; same principle as the go-kimi looper).
type Situation struct {
	HasWorkspace bool
	WorkspaceDir string
	Host         string
}

// buildSystemPrompt assembles the platform teaching + situation + domain
// template. Claude Code brings its own coding skills; coagent teaches it the
// channel facts: it is ONE actor among others, reached through the coagent
// meta-tools (call_actor …), not acting alone.
func buildSystemPrompt(sit Situation, channelType, domainPrompt string) string {
	var b strings.Builder
	b.WriteString("You are a coagent agent — one actor in a shared channel. ")
	b.WriteString("Other participants (humans and actors) share this channel. ")
	b.WriteString("Reach other actors through the coagent meta-tools (call_actor, ")
	b.WriteString("list_actors, describe_actor, …) — do not assume you act alone.\n")
	if sit.HasWorkspace {
		fmt.Fprintf(&b, "\nYou have a working directory at %s for file work; ", sit.WorkspaceDir)
		b.WriteString("surface durable artifacts that matter to others into the channel.\n")
	} else {
		b.WriteString("\nYou have no private channel workspace; for file / device work, ")
		b.WriteString("discover and call a device actor via call_actor (verify with list_actors).\n")
	}
	if strings.TrimSpace(channelType) != "" {
		fmt.Fprintf(&b, "\nChannel type: %s.\n", strings.TrimSpace(channelType))
	}
	if strings.TrimSpace(domainPrompt) != "" {
		b.WriteString("\n")
		b.WriteString(strings.TrimSpace(domainPrompt))
		b.WriteString("\n")
	}
	return b.String()
}
