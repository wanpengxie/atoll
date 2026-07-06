package claudecode

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/wanpengxie/atoll/agent/base"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/registry"
)

// Env keys. The claude CLI carries its OWN auth (ANTHROPIC_API_KEY / `claude
// login`), so atoll passes no key — only a model default. ATOLL_CHANNEL_TYPE /
// ATOLL_DOMAIN_PROMPT are GONE (A3 / Q7): the per-channel domain prompt now
// rides InstanceSpec.Config (channel_actors.config_json), the ONE config
//承载, never a process-wide env.
const (
	EnvKeyModel = "ATOLL_CLAUDE_MODEL"

	defaultModel = "claude-sonnet-4-5"

	defaultFastPathWindow = 15 * time.Second
)

// Config drives a claude engine. Lean by design: the engine is the `claude` CLI
// (auth + workspace are its own), so atoll supplies a model, the assembled
// system prompt, and the inline-wait window.
type Config struct {
	Model          string
	SystemPrompt   string
	FastPathWindow time.Duration
}

// specOverlay is the per-instance config (channel_actors.config_json) the looper
// self-parses: a model override plus the channel's domain prompt facts (A3/Q7 —
// what ATOLL_CHANNEL_TYPE / ATOLL_DOMAIN_PROMPT once carried, now per-instance).
type specOverlay struct {
	Model        string `json:"model"`
	ChannelType  string `json:"channel_type"`
	DomainPrompt string `json:"domain_prompt"`
}

// NewConfigFromSpec layers env defaults under the per-instance spec.Config
// overlay, assembling the system prompt from the host situation + the config's
// domain facts.
func NewConfigFromSpec(raw json.RawMessage, sit Situation) (Config, error) {
	var overlay specOverlay
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &overlay); err != nil {
			return Config{}, fmt.Errorf("claude: parse spec config: %w", err)
		}
	}
	model := strings.TrimSpace(os.Getenv(EnvKeyModel))
	if overlay.Model != "" {
		model = overlay.Model
	}
	if model == "" {
		model = defaultModel
	}
	return Config{
		Model:          model,
		SystemPrompt:   buildSystemPrompt(sit, overlay.ChannelType, overlay.DomainPrompt),
		FastPathWindow: defaultFastPathWindow,
	}, nil
}

// NewDecl is the claude engine's Constructor — its OWN flat actor class
// ("claude", kind=agent). id from the spec; Situation host-derived. It builds
// the agent as a base.Def (a Proc over agent/base's skeleton), NOT a raw
// actorrt.Actor (期10 S5: the mailbox loop / turn queue live in the base).
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
	cfg, err := NewConfigFromSpec(spec.Config, sit)
	if err != nil {
		return platform.ActorDecl{}, fmt.Errorf("config: %w", err)
	}
	def, err := base.Def(agentSkillDoc, base.Config{
		NewEngine: newEngineFn(cfg, defaultClientFactory),
	})
	if err != nil {
		return platform.ActorDecl{}, fmt.Errorf("claude agent def: %w", err)
	}
	return platform.ActorDecl{
		ID:      id,
		Kind:    actor.KindAgent,
		Factory: platform.ActorFactory{Proc: def},
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
// template. Claude Code brings its own coding skills; atoll teaches it the
// channel facts: it is ONE actor among others, reached through the atoll
// meta-tools (call_actor …), not acting alone.
func buildSystemPrompt(sit Situation, channelType, domainPrompt string) string {
	var b strings.Builder
	b.WriteString("You are a atoll agent — one actor in a shared channel. ")
	b.WriteString("Other participants (humans and actors) share this channel. ")
	b.WriteString("Reach other actors through the atoll meta-tools (call_actor, ")
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
