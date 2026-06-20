package claudecode

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

	claude "github.com/wanpengxie/go-claude-agent-sdk"

	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/lib/introspect"
	"github.com/wanpengxie/ActOS/lib/metatool"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

const (
	turnQueueCap          = 64
	defaultFastPathWindow = 15 * time.Second
)

// claudeClient is the subset of *claude.ClaudeClient the bridge consumes —
// carved out so the test suite stubs the engine without spawning the `claude`
// CLI (symmetric with the go-kimi looper's kimiAgent interface).
type claudeClient interface {
	Connect(ctx context.Context) error
	Query(ctx context.Context, prompt string) error
	ReceiveResponse(ctx context.Context) <-chan claude.Message
	Interrupt(ctx context.Context) error
	Close() error
}

// Bridge drives one claude session as an actor cell. It implements actorrt.Actor
// (Receive) plus Starter/Stopper; the runtime guarantees the three run serially
// on the cell goroutine. Async is the STRUCTURE (responses always re-enter via
// Receive → shell.Deliver); the fast-path window gives the sync EXPERIENCE.
type Bridge struct {
	cfg    Config
	self   actor.ActorID
	chID   channel.ID
	writer harness.Writer

	clientNew   func() (claudeClient, error) // test hook (defaultClientFactory)
	envelopeSeq atomic.Uint64

	// shell is the channel's shared actor-invocation machinery (correlation +
	// sync/async + author#2). The agent HOLDS one; it does not own the call
	// mechanism. Built in Start; MCP tool handlers resolve it lazily.
	shell *metatool.Shell

	// curTurn carries the in-flight turn's trigger so the MCP tool handlers
	// (invoked by the claude CLI mid-turn, on the SDK's own ctx) can thread
	// parent / correlation. Turns are serial, so it is well-defined per turn.
	curMu   sync.Mutex
	curTurn *turnItem

	turnQ    chan turnItem
	stopOnce sync.Once
	loopStop context.CancelFunc
	loopWG   sync.WaitGroup
	client   claudeClient

	fatalMu sync.Mutex
	fatal   error
}

// NewBridge builds a Bridge bound to its identity and writing seam.
func NewBridge(cfg Config, self actor.ActorID, chID channel.ID, w harness.Writer) (*Bridge, error) {
	if self == "" {
		return nil, errors.New("claude: actor id empty")
	}
	if chID == "" {
		return nil, errors.New("claude: channel id empty")
	}
	if w == nil {
		return nil, errors.New("claude: writer nil")
	}
	if cfg.Model == "" {
		cfg.Model = defaultModel
	}
	if cfg.NowFn == nil {
		cfg.NowFn = func() int64 { return time.Now().UnixMilli() }
	}
	if cfg.WorkDir == "" {
		tmp, err := os.MkdirTemp("", "coagent-claude-")
		if err != nil {
			return nil, fmt.Errorf("claude: workdir: %w", err)
		}
		cfg.WorkDir = tmp
	}
	b := &Bridge{cfg: cfg, self: self, chID: chID, writer: w}
	b.clientNew = b.defaultClientFactory
	return b, nil
}

var (
	_ actorrt.Actor   = (*Bridge)(nil)
	_ actorrt.Starter = (*Bridge)(nil)
	_ actorrt.Stopper = (*Bridge)(nil)
)

// defaultClientFactory builds a claude.NewClient wired with the coagent
// meta-tool MCP server, the durable Cwd, the system prompt, and a resume seed.
func (b *Bridge) defaultClientFactory() (claudeClient, error) {
	opts := []claude.Option{
		claude.WithModel(b.cfg.Model),
		claude.WithPermissionMode(claude.PermissionBypassPermissions),
		claude.WithMcpServers(map[string]claude.McpServerConfig{"coagent": b.buildMCPServer()}),
		claude.WithCwd(b.cfg.WorkDir),
	}
	if strings.TrimSpace(b.cfg.SystemPrompt) != "" {
		opts = append(opts, claude.WithSystemPrompt(b.cfg.SystemPrompt))
	}
	if resume := strings.TrimSpace(string(b.cfg.ResumeSeed)); resume != "" {
		opts = append(opts, claude.WithResume(resume))
	}
	return claude.NewClient(opts...), nil
}

func (b *Bridge) sender() message.Sender { return message.Sender{Kind: actor.KindAgent, ID: b.self} }
func (b *Bridge) clock() time.Time       { return time.UnixMilli(b.cfg.NowFn()) }

// Start connects the claude client and starts the private loop. A connect
// failure returns the error so the cell dies fast (positive death) — no
// half-alive agent registers as serviceable.
func (b *Bridge) Start(ctx context.Context, _ actorrt.ActorContext) error {
	client, err := b.clientNew()
	if err != nil {
		return err
	}
	if err := client.Connect(ctx); err != nil {
		_ = client.Close()
		return fmt.Errorf("claude: connect: %w", err)
	}
	b.client = client

	// buildMCPServer (in clientNew above) captured b and resolves b.shell LAZILY
	// at tool-execute time, so assigning b.shell after the client build is safe.
	b.shell = metatool.NewShell(metatool.ShellConfig{
		Writer:         b.writer,
		ChannelID:      b.chID,
		Sender:         b.sender(),
		Clock:          b.clock,
		EnvelopeID:     b.envelopeID,
		FastPathWindow: defaultFastPathWindow,
		OnFault: func(reqID message.ID, err error) {
			slog.Default().Warn("claude.caller_timeout_fault", "request_id", string(reqID), "err", err)
		},
	})

	loopCtx, cancel := context.WithCancel(ctx)
	b.loopStop = cancel
	b.turnQ = make(chan turnItem, turnQueueCap)
	b.loopWG.Add(1)
	go b.runLoop(loopCtx)
	return nil
}

// Stop tears the loop down idempotently and closes the engine.
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
		if b.shell != nil {
			b.shell.Stop()
		}
		if b.client != nil {
			closeErr = b.client.Close()
		}
	})
	return closeErr
}

// Receive is the mailbox entry — it NEVER blocks (cell serial contract).
func (b *Bridge) Receive(ctx context.Context, env *message.Envelope) error {
	b.fatalMu.Lock()
	fatal := b.fatal
	b.fatalMu.Unlock()
	if fatal != nil {
		panic(fmt.Sprintf("claude agent %s: loop dead: %v", b.self, fatal))
	}
	if env == nil {
		return nil
	}

	// Mechanical self-answers (actor citizenship) — never fed to the engine.
	if env.Kind == message.KindRequest && env.Type == introspect.QueryDescribe {
		return b.handleDescribe(ctx, env)
	}
	if env.Kind == message.KindRequest && env.Type == introspect.QueryStatus {
		return b.handleStatus(ctx, env)
	}

	if env.Kind == message.KindResponse && env.ParentID != "" {
		// shell Matches author#2 (disarms timeout) + routes to any fast-path
		// waiter; a final nobody waited for becomes a new turn — EXCEPT one we
		// authored ourselves (self-addressed terminal → unbounded self-loop).
		_, final := behavior.ParseFinalStatus(env.Payload)
		if consumed := b.shell.Deliver(env); !consumed && final && env.Sender.ID != b.self {
			b.enqueueTurn(*env)
		}
		return nil
	}

	// An actor never reacts to its OWN emissions.
	if env.Sender.ID == b.self {
		return nil
	}

	b.enqueueTurn(*env)
	return nil
}
