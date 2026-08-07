package claudecode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	claude "github.com/wanpengxie/go-claude-agent-sdk"

	"github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/lib/metatool"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/registry"
)

type engine struct {
	cfg     Config
	client  claudeClient
	x       *metatool.Exec
	workDir string
	events  base.EventPort
	life    context.Context

	mu      sync.Mutex
	curRC   metatool.RuntimeContext
	cancel  context.CancelFunc
	session string
	booted  bool
	closed  bool
}

var _ base.Engine = (*engine)(nil)

type claudeClient interface {
	Connect(ctx context.Context) error
	Query(ctx context.Context, prompt string) error
	ReceiveResponseWithErrors(ctx context.Context) (<-chan claude.Message, <-chan error)
	Close() error
}
type clientFactory func(e *engine, resumeSeed []byte) (claudeClient, error)

func newEngineFn(cfg Config, factory clientFactory) base.NewEngine {
	return func(sys actorbase.Sys, seed []byte, events base.EventPort) (base.Engine, error) {
		workDir, err := os.MkdirTemp("", "atoll-claude-")
		if err != nil {
			return nil, fmt.Errorf("claude: workdir: %w", err)
		}
		e := &engine{cfg: cfg, workDir: workDir, events: events, life: sys.Life()}
		e.x = base.ExecFace(sys, cfg.FastPathWindow)
		client, err := factory(e, seed)
		if err != nil {
			return nil, err
		}
		e.client = client
		return e, nil
	}
}

func defaultClientFactory(e *engine, resumeSeed []byte) (claudeClient, error) {
	opts := []claude.Option{claude.WithModel(e.cfg.Model), claude.WithPermissionMode(claude.PermissionBypassPermissions), claude.WithMcpServers(map[string]claude.McpServerConfig{"atoll": e.buildMCPServer()}), claude.WithCwd(e.workDir)}
	if strings.TrimSpace(e.cfg.SystemPrompt) != "" {
		opts = append(opts, claude.WithSystemPrompt(e.cfg.SystemPrompt))
	}
	if len(resumeSeed) > 0 {
		opts = append(opts, claude.WithResume(string(resumeSeed)))
	}
	return claude.NewClient(opts...), nil
}

func (e *engine) Boot(ctx context.Context, _ base.BootPort) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return errors.New("claude: engine closed")
	}
	if e.booted {
		return errors.New("claude: engine already booted")
	}
	if err := e.client.Connect(ctx); err != nil {
		return fmt.Errorf("claude: connect: %w", err)
	}
	e.booted = true
	return nil
}

func (e *engine) StartTurn(op base.OpID, batch []base.Trigger, background []base.ContextItem) error {
	e.mu.Lock()
	if !e.booted || e.closed {
		e.mu.Unlock()
		return errors.New("claude: engine unavailable")
	}
	if e.cancel != nil {
		e.mu.Unlock()
		return errors.New("claude: turn already active")
	}
	ctx, cancel := context.WithCancel(e.life)
	e.cancel = cancel
	e.mu.Unlock()
	turnID := string(op)
	go e.runTurn(ctx, op, turnID, batch, background)
	return nil
}

func (e *engine) runTurn(ctx context.Context, op base.OpID, turnID string, batch []base.Trigger, background []base.ContextItem) {
	defer func() { e.mu.Lock(); e.cancel = nil; e.mu.Unlock() }()
	if len(batch) == 0 {
		e.events.TurnRejected(op, "provider_failed", "empty batch")
		return
	}
	e.events.TurnStarted(op, turnID)
	last := batch[len(batch)-1]
	e.setCurrentRC(metatool.RuntimeContext{Trigger: metatool.Trigger{Envelope: last.Envelope, CorrelationID: last.CorrelationID}})
	input := composeBatchInput(batch, background)
	if err := e.client.Query(ctx, input); err != nil {
		if errors.Is(err, context.Canceled) {
			e.events.TurnEnded(turnID, base.TurnStatusInterrupted, "", err.Error())
		} else {
			e.events.TurnEnded(turnID, base.TurnStatusFailed, "", fmt.Sprintf("claude query: %v", err))
		}
		return
	}
	pending := map[string]string{}
	order := []string{}
	final := ""
	failed := ""
	endPending := func() {
		for _, id := range order {
			if tool, ok := pending[id]; ok {
				e.events.Tool(turnID, id, "ended", tool, registry.ActivityToolEndedStatusFailed, "turn ended before tool result")
				delete(pending, id)
			}
		}
	}
	messages, errs := e.client.ReceiveResponseWithErrors(ctx)
	for msg := range messages {
		switch m := msg.(type) {
		case *claude.AssistantMessage:
			if m.Error != "" {
				failed = fmt.Sprintf("assistant error: %s", m.Error)
				continue
			}
			for _, block := range m.Content {
				if b, ok := block.(*claude.ToolUseBlock); ok {
					pending[b.ID] = b.Name
					order = append(order, b.ID)
					e.events.Tool(turnID, b.ID, "started", b.Name, registry.ActivityStartedStatus, "")
				}
			}
		case *claude.UserMessage:
			blocks, ok := m.Content.([]claude.ContentBlock)
			if !ok {
				continue
			}
			for _, block := range blocks {
				r, ok := block.(*claude.ToolResultBlock)
				if !ok {
					continue
				}
				tool, ok := pending[r.ToolUseID]
				if !ok {
					continue
				}
				status := registry.ActivityToolEndedStatusCompleted
				if r.IsError != nil && *r.IsError {
					status = registry.ActivityToolEndedStatusFailed
				}
				e.events.Tool(turnID, r.ToolUseID, "ended", tool, status, "")
				delete(pending, r.ToolUseID)
			}
		case *claude.ResultMessage:
			if m.SessionID != "" {
				e.mu.Lock()
				e.session = m.SessionID
				e.mu.Unlock()
				e.events.Persist(base.ResumeSeedKey, []byte(m.SessionID))
			}
			final = strings.TrimSpace(m.Result)
			if m.IsError {
				failed = "claude result: " + final
			}
		}
	}
	if err, ok := <-errs; ok && err != nil {
		if errors.Is(err, context.Canceled) {
			endPending()
			e.events.TurnEnded(turnID, base.TurnStatusInterrupted, "", err.Error())
			return
		}
		failed = "claude stream: " + err.Error()
	}
	endPending()
	if failed != "" {
		e.events.TurnEnded(turnID, base.TurnStatusFailed, "", failed)
	} else {
		e.events.TurnEnded(turnID, base.TurnStatusOK, final, "")
	}
}

func composeBatchInput(batch []base.Trigger, background []base.ContextItem) string {
	var b strings.Builder
	if len(background) > 0 {
		b.WriteString("频道最近记录（可能与你已知重叠）：\n")
		for _, item := range background {
			b.WriteString(item.Rendered)
			b.WriteByte('\n')
		}
		b.WriteString("\n当前消息：\n")
	}
	for _, tr := range batch {
		b.WriteString(composeUserInput(tr.Envelope))
		b.WriteByte('\n')
	}
	return strings.TrimSpace(b.String())
}

func (e *engine) Steer(op base.OpID, _ base.Trigger) error {
	e.events.ControlDone(op, base.ControlNotSteerable, "", "claude sdk has no steer primitive")
	return nil
}
func (e *engine) Interrupt(op base.OpID) error {
	e.mu.Lock()
	cancel := e.cancel
	e.mu.Unlock()
	if cancel == nil {
		e.events.ControlDone(op, base.ControlNoActiveTurn, "", "")
		return nil
	}
	cancel()
	e.events.ControlDone(op, base.ControlAccepted, "", "")
	return nil
}
func (e *engine) Terminate() error {
	e.mu.Lock()
	cancel := e.cancel
	e.cancel = nil
	e.booted = false
	e.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return e.client.Close()
}
func (e *engine) EnsureAlive(op base.OpID) error {
	go func() {
		if err := e.client.Connect(e.life); err != nil {
			e.events.ControlDone(op, base.ControlRPCError, "", err.Error())
			return
		}
		e.mu.Lock()
		e.booted = true
		e.mu.Unlock()
		e.events.ControlDone(op, base.ControlAccepted, "", "")
	}()
	return nil
}
func (e *engine) Describe() introspect.Describe {
	return introspect.Describe{Description: agentDescription, SkillDoc: agentSkillDoc}
}
func (e *engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	cancel := e.cancel
	e.cancel = nil
	e.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return e.client.Close()
}
func (e *engine) setCurrentRC(rc metatool.RuntimeContext) { e.mu.Lock(); e.curRC = rc; e.mu.Unlock() }
func (e *engine) currentRC() metatool.RuntimeContext {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.curRC
}

func composeUserInput(env message.Envelope) string {
	text := extractText(env.Payload)
	sender := string(env.Sender.ID)
	if sender == "" {
		return text
	}
	return fmt.Sprintf("[from %s]\n%s", sender, text)
}
func extractText(payload []byte) string {
	var p struct {
		Text string `json:"text"`
	}
	if json.Unmarshal(payload, &p) == nil && p.Text != "" {
		return p.Text
	}
	return string(payload)
}
func classifyAssistantError(e claude.AssistantMessageError) string {
	switch e {
	case claude.AssistantErrorAuthenticationFailed:
		return "llm_auth"
	case claude.AssistantErrorBillingError:
		return "llm_billing"
	case claude.AssistantErrorRateLimit:
		return "llm_rate_limit"
	case claude.AssistantErrorInvalidRequest:
		return "llm_invalid"
	case claude.AssistantErrorServerError:
		return "llm_server"
	default:
		return "llm_unknown"
	}
}

const agentDescription = "Claude Code agent: the channel's conversational brain (claude looper). Send it any request — it reasons over the channel context and orchestrates the channel's tool actors via call_actor."
const agentSkillDoc = "# agent (claude looper)\n\nConversational actor backed by the Claude Code engine. Accepts any kind=request as a turn trigger, replies with a terminal response, and calls other actors through the channel's meta tools.\n"
