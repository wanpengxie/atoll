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
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gokimi "github.com/wanpengxie/go-kimi/pkg/kimi"
	"github.com/wanpengxie/go-kimi/pkg/kimi/config"
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
	// terminal; Receive's Match disarms it when the response lands. onFault is
	// the per-request liveness-break face (symmetric with author#3): a timeout
	// terminal that fails to land leaves the request closeable by no path, so
	// the host logs it rather than swallowing it.
	b.caller = behavior.NewCaller(b.sender(), b.writer, b.clock, func(reqID message.ID, err error) {
		slog.Default().Warn("agent.caller_timeout_fault", "request_id", string(reqID), "err", err)
	})

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
