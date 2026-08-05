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
)

// engine is the claude-code base.Engine (期10 S5): the ONE thing this provider
// writes now that the mailbox loop / turn queue / response分拣 / describe
// dispatch / per-turn checkpoint挂账 all live in agent/base. It封s the claude
// CLI's synchronous SDK message shape entirely — the base never sees it.
type engine struct {
	cfg     Config
	client  claudeClient
	x       *metatool.Exec // the meta-tool execution face (built from Sys)
	workDir string         // the claude session's per-process Cwd

	// curMu guards curRC — the in-flight turn's RuntimeContext, read by the MCP
	// tool handlers (which fire on the SDK's own goroutine mid-turn). Turns are
	// serial (the base loop), so it is well-defined per turn.
	curMu sync.Mutex
	curRC metatool.RuntimeContext

	// session captures the claude session id from each ResultMessage. It seeds
	// the durable resume checkpoint (WithResume on the next incarnation). Guarded
	// by curMu (written on the Turn goroutine, read by Checkpoint on the same
	// base loop goroutine — the mutex is belt-and-braces).
	session string
}

var _ base.Engine = (*engine)(nil)

// claudeClient is the subset of *claude.ClaudeClient the engine consumes — carved
// out so the test suite stubs the engine without spawning the `claude` CLI.
type claudeClient interface {
	Connect(ctx context.Context) error
	Query(ctx context.Context, prompt string) error
	ReceiveResponseWithErrors(ctx context.Context) (<-chan claude.Message, <-chan error)
	Close() error
}

// clientFactory builds a claudeClient; the engine passes its own MCP server and
// the resume seed. Swapped in tests (see export_test.go).
type clientFactory func(e *engine, resumeSeed []byte) (claudeClient, error)

// newEngineFn returns the base.NewEngine the Constructor closes over: it builds
// the exec face from Sys, mints the claude client (wired with the meta-tool MCP
// server + resume seed), and connects it. A connect failure is loud死 (no
// half-alive agent).
func newEngineFn(cfg Config, factory clientFactory) base.NewEngine {
	return func(sys actorbase.Sys, seed []byte) (base.Engine, error) {
		workDir, err := os.MkdirTemp("", "atoll-claude-")
		if err != nil {
			return nil, fmt.Errorf("claude: workdir: %w", err)
		}
		e := &engine{cfg: cfg, workDir: workDir}
		e.x = base.ExecFace(sys, cfg.FastPathWindow)
		client, err := factory(e, seed)
		if err != nil {
			return nil, err
		}
		if err := client.Connect(sys.Life()); err != nil {
			_ = client.Close()
			return nil, fmt.Errorf("claude: connect: %w", err)
		}
		e.client = client
		return e, nil
	}
}

// defaultClientFactory builds a claude.NewClient wired with the atoll meta-tool
// MCP server, the per-process Cwd, the system prompt, and — when the durable
// resume seed is present — WithResume(sessionID) so a restarted incarnation
// continues its prior claude session (10.0 durable resume on sys.State).
func defaultClientFactory(e *engine, resumeSeed []byte) (claudeClient, error) {
	opts := []claude.Option{
		claude.WithModel(e.cfg.Model),
		claude.WithPermissionMode(claude.PermissionBypassPermissions),
		claude.WithMcpServers(map[string]claude.McpServerConfig{"atoll": e.buildMCPServer()}),
		claude.WithCwd(e.workDir),
	}
	if strings.TrimSpace(e.cfg.SystemPrompt) != "" {
		opts = append(opts, claude.WithSystemPrompt(e.cfg.SystemPrompt))
	}
	if len(resumeSeed) > 0 {
		opts = append(opts, claude.WithResume(string(resumeSeed)))
	}
	return claude.NewClient(opts...), nil
}

// setCurrentRC/currentRC thread the in-flight runtime context to MCP handlers
// (which run on the SDK's ctx, not the base loop's).
func (e *engine) setCurrentRC(rc metatool.RuntimeContext) {
	e.curMu.Lock()
	e.curRC = rc
	e.curMu.Unlock()
}

func (e *engine) currentRC() metatool.RuntimeContext {
	e.curMu.Lock()
	defer e.curMu.Unlock()
	return e.curRC
}

// Turn drives one claude exchange: Query the composed input, drain
// ReceiveResponse (ending at the ResultMessage), and report the full terminal
// value through the base Sink. Tool calls resolve through the atoll MCP server
// and report typed phases there. An engine/LLM error uses Sink.Fail and returns
// nil so the actor stays alive; only a
// Sink write failure (A1: never swallowed) propagates as loud死.
func (e *engine) Turn(ctx context.Context, trigger base.Trigger, sink base.Sink) error {
	e.setCurrentRC(metatool.RuntimeContext{
		Trigger: metatool.Trigger{
			Envelope:      trigger.Envelope,
			CorrelationID: trigger.CorrelationID,
		},
	})

	input := composeUserInput(trigger.Envelope)
	if err := e.client.Query(ctx, input); err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}
		return emitFailed(sink, err, "claude_query")
	}

	pendingTools := map[string]string{}
	var toolOrder []string
	endPendingTools := func() error {
		for _, callID := range toolOrder {
			tool, found := pendingTools[callID]
			if !found {
				continue
			}
			if err := sink.ToolEnded(base.ToolActivity{CallID: callID, Tool: tool, Status: "failed", Detail: "turn ended before tool result"}); err != nil {
				return err
			}
			delete(pendingTools, callID)
		}
		return nil
	}
	messages, streamErrors := e.client.ReceiveResponseWithErrors(ctx)
	for msg := range messages {
		switch m := msg.(type) {
		case *claude.AssistantMessage:
			if m.Error != "" {
				if err := endPendingTools(); err != nil {
					return err
				}
				return emitFailed(sink, fmt.Errorf("assistant error: %s", m.Error), classifyAssistantError(m.Error))
			}
			for _, block := range m.Content {
				switch b := block.(type) {
				case *claude.ToolUseBlock:
					pendingTools[b.ID] = b.Name
					toolOrder = append(toolOrder, b.ID)
					if err := sink.ToolStarted(base.ToolActivity{CallID: b.ID, Tool: b.Name}); err != nil {
						return err
					}
					// Text/thinking blocks are streaming/provider narration. The full
					// ResultMessage.Result is the sole terminal value crossing the adapter.
				}
			}
		case *claude.UserMessage:
			blocks, ok := m.Content.([]claude.ContentBlock)
			if !ok {
				continue
			}
			for _, block := range blocks {
				result, ok := block.(*claude.ToolResultBlock)
				if !ok {
					continue
				}
				tool, found := pendingTools[result.ToolUseID]
				if !found {
					continue
				}
				status := "completed"
				if result.IsError != nil && *result.IsError {
					status = "failed"
				}
				if err := sink.ToolEnded(base.ToolActivity{CallID: result.ToolUseID, Tool: tool, Status: status}); err != nil {
					return err
				}
				delete(pendingTools, result.ToolUseID)
			}
		case *claude.ResultMessage:
			if m.SessionID != "" && m.SessionID != e.session {
				e.session = m.SessionID
			}
			text := strings.TrimSpace(m.Result)
			if m.IsError {
				if err := endPendingTools(); err != nil {
					return err
				}
				return emitFailed(sink, fmt.Errorf("result error: %s", text), "claude_result")
			}
			if err := endPendingTools(); err != nil {
				return err
			}
			if err := sink.Complete(base.FinalValue{Text: text, NextAction: "done"}); err != nil {
				return err // plumbing failure: propagate → loud死 (A1)
			}
		}
	}
	if streamErr, ok := <-streamErrors; ok && streamErr != nil {
		if err := endPendingTools(); err != nil {
			return err
		}
		if errors.Is(streamErr, context.Canceled) || errors.Is(streamErr, context.DeadlineExceeded) {
			return streamErr
		}
		return emitFailed(sink, streamErr, "claude_stream")
	}
	return endPendingTools()
}

// emitFailed surfaces an engine/LLM failure as a terminal failed response and
// returns nil — the failure is now in the channel log, the turn ends, the actor
// stays alive. A Sink write error is propagated (A1: never swallowed).
func emitFailed(sink base.Sink, cause error, reason string) error {
	if err := sink.Fail(base.Failure{
		ErrorCode: reason,
		Detail:    fmt.Sprintf("claude bridge failed: %v", cause),
	}); err != nil {
		return err
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

// Checkpoint returns the durable resume seed (the claude session id) whenever a
// session exists, nil otherwise. No dirty micro-opt: re-returning the unchanged
// value every turn is an idempotent zero-cost upsert (spec F8), and it is what
// makes the base's persist self-healing — a turn whose Put failed re-writes the
// same seed next turn. The base persists it on sys.State per turn.
func (e *engine) Checkpoint() []byte {
	e.curMu.Lock()
	defer e.curMu.Unlock()
	if e.session == "" {
		return nil
	}
	return []byte(e.session)
}

// Close releases the claude client at incarnation death.
func (e *engine) Close() error {
	if e.client != nil {
		return e.client.Close()
	}
	return nil
}

// composeUserInput renders a trigger envelope into the engine's user input,
// stamping the sender so the model knows whose message / result this is.
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
	if err := json.Unmarshal(payload, &p); err == nil && p.Text != "" {
		return p.Text
	}
	return string(payload)
}

// classifyAssistantError maps a claude AssistantMessageError into a coarse reason
// bucket for the failed terminal envelope.
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

// agentDescription / agentSkillDoc are the claude agent's actor.describe
// self-answer (mechanical — the base serves it, the LLM never sees it).
const agentDescription = "Claude Code agent: the channel's conversational brain (claude looper). Send it any request — it reasons over the channel context and orchestrates the channel's tool actors via call_actor."

const agentSkillDoc = "# agent (claude looper)\n\n" +
	"Conversational actor backed by the Claude Code engine. Accepts any kind=request " +
	"as a turn trigger (no closed type set), replies with a terminal response, and calls " +
	"other actors through the channel's meta tools.\n"
