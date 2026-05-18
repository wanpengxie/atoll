package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/ipc"
)

// Env keys M1.6-T5 phase-3 plumbs from daemon → worker spawn so the
// bridge can hash / grep / forward the L4 §2.4 domain prompt without
// any new IPC frame. Kept exported so cmd/cli + tests can reuse the
// constants (one source of truth for the spawn-time contract).
const (
	// EnvKeyChannelType holds the L4 channel-template key (catalog
	// Channel.Type — e.g. "xhs-creator" / "group" / "").
	EnvKeyChannelType = "COAGENT_CHANNEL_TYPE"

	// EnvKeyDomainPrompt holds the L4 §2.4 prompt segment associated
	// with the channel type. Empty / unset for legacy / group channels.
	EnvKeyDomainPrompt = "COAGENT_DOMAIN_PROMPT"
)

// MockBridge is the deterministic Bridge implementation cmd/worker
// uses in M1.6 before a real LLM-backed bridge lands. For every
// incoming KindTrigger envelope it:
//
//  1. Builds an `agent.text` reply envelope echoing the trigger id +
//     turn counter ("mock-reply to <env.ID> (turn N)").
//  2. Calls IPCClient.WriteMessage to push the reply through the
//     daemon harness chain.
//  3. Increments the per-bridge turn counter. When the counter reaches
//     MaxTurns the bridge emits one final reply tagged with
//     next_action=done in the payload and returns nil; Runtime.Run
//     then calls IPCClient.Shutdown and the worker exits.
//
// Errors from WriteMessage abort the bridge. *FenceInvalidError
// propagates up unchanged so Runtime.Run treats it as a fatal exit.
type MockBridge struct {
	// MaxTurns caps the number of triggers the bridge will react to
	// before signalling next_action=done and exiting. Defaults to 8
	// (small enough that integration tests don't run forever; big
	// enough that the M1.6 e2e acceptance #3 reuse-test fits).
	MaxTurns int

	// NowFn returns unix-ms. Defaults to time.Now.
	NowFn func() int64

	// EnvelopeIDFn produces unique envelope ids. Defaults to a
	// counter-based generator ("mock-<workerID>-<turn>").
	EnvelopeIDFn func(workerID string, turn int) string

	// ReplyFn lets tests override the response envelope payload. The
	// default emits `{"text": "mock reply to <env.id> (turn N)"}`.
	ReplyFn func(in ipc.TriggerPayload, turn int) (envelopeType string, payload json.RawMessage)

	// PromptLogWriter receives the one-line domain_prompt_loaded summary
	// the bridge emits when Run starts and finds a non-empty
	// COAGENT_DOMAIN_PROMPT in the environment (M1.6-T5 phase-3
	// acceptance B3). Defaults to os.Stderr so cmd/worker spawned by
	// the daemon writes the line through to the daemon's stderr pipe.
	// Tests set this to a bytes.Buffer to assert the format.
	PromptLogWriter io.Writer

	// EnvLookup returns the value associated with key in the spawn env.
	// Defaults to os.Getenv. Tests override it to drive the bridge with
	// a synthetic env without actually mutating the test process state.
	EnvLookup func(key string) string

	turns int
}

// NewMockBridge returns a MockBridge with defaults applied.
func NewMockBridge() *MockBridge {
	return &MockBridge{}
}

// applyDefaults backfills nil fields. Called once per Run.
func (m *MockBridge) applyDefaults() {
	if m.MaxTurns <= 0 {
		m.MaxTurns = 8
	}
	if m.NowFn == nil {
		m.NowFn = func() int64 { return time.Now().UnixMilli() }
	}
	if m.EnvelopeIDFn == nil {
		m.EnvelopeIDFn = func(workerID string, turn int) string {
			if workerID == "" {
				return fmt.Sprintf("mock-%d", turn)
			}
			return fmt.Sprintf("mock-%s-%d", workerID, turn)
		}
	}
	if m.ReplyFn == nil {
		m.ReplyFn = func(in ipc.TriggerPayload, turn int) (string, json.RawMessage) {
			body := fmt.Sprintf("mock reply to %s (turn %d)", in.Envelope.ID, turn)
			payload, _ := json.Marshal(map[string]string{"text": body})
			return "agent.text", payload
		}
	}
	if m.PromptLogWriter == nil {
		m.PromptLogWriter = os.Stderr
	}
	if m.EnvLookup == nil {
		m.EnvLookup = os.Getenv
	}
}

// logDomainPromptOnce writes the M1.6-T5 acceptance-B3 grep target
// (`mock_bridge: domain_prompt_loaded ...`) to PromptLogWriter when the
// spawn env supplies a non-empty COAGENT_DOMAIN_PROMPT. The line is
// stable + idempotent: caller is Run, which fires exactly once per
// worker.Runtime invocation. Empty prompts emit a "no_domain_prompt"
// line keyed by channel_type so log scans can still distinguish
// "phase-3 not wired" from "phase-3 wired but template is generic".
func (m *MockBridge) logDomainPromptOnce(workerID string) {
	channelType := m.EnvLookup(EnvKeyChannelType)
	prompt := m.EnvLookup(EnvKeyDomainPrompt)
	if prompt == "" {
		// Surface the absence so an operator grepping for
		// "domain_prompt_loaded" sees a deterministic counterpart line
		// for legacy / group channels. Keeps `acceptance B3` honest:
		// the grep target is positive only on type=xhs-creator boots.
		fmt.Fprintf(m.PromptLogWriter,
			"mock_bridge: no_domain_prompt worker=%s channel_type=%q\n",
			workerID, channelType)
		return
	}
	sum := sha256.Sum256([]byte(prompt))
	fmt.Fprintf(m.PromptLogWriter,
		"mock_bridge: domain_prompt_loaded worker=%s channel_type=%q len=%d sha256=%s\n",
		workerID, channelType, len(prompt), hex.EncodeToString(sum[:8]))
}

// Run implements Bridge. Blocks until ctx is cancelled, the trigger
// channel closes (IPC EOF), MaxTurns is hit, or WriteMessage fails.
func (m *MockBridge) Run(ctx context.Context, client *IPCClient) error {
	m.applyDefaults()
	if client == nil {
		return errors.New("worker: MockBridge nil client")
	}
	if client.WorkerActorID() == "" {
		return errors.New("worker: MockBridge handshake did not populate actor id")
	}
	// M1.6-T5 phase-3 — emit the domain_prompt_loaded breadcrumb once
	// per Run. Stable target for acceptance B3 (`grep prompt log`).
	m.logDomainPromptOnce(client.WorkerID())
	triggers := client.Triggers()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case payload, ok := <-triggers:
			if !ok {
				return nil
			}
			if err := m.react(ctx, client, payload); err != nil {
				return err
			}
			m.turns++
			if m.turns >= m.MaxTurns {
				// Emit a terminal envelope marking next_action=done so
				// the channel log carries an observable exit point.
				if err := m.emitTerminal(ctx, client); err != nil {
					return err
				}
				return nil
			}
		}
	}
}

// react builds and writes the per-trigger reply envelope.
func (m *MockBridge) react(ctx context.Context, client *IPCClient, in ipc.TriggerPayload) error {
	envType, payload := m.ReplyFn(in, m.turns+1)
	env := message.Envelope{
		ID:            m.EnvelopeIDFn(client.WorkerID(), m.turns+1),
		ChannelID:     string(client.ChannelID()),
		Type:          envType,
		Kind:          message.KindEvent,
		Sender:        message.Sender{Kind: message.SenderAgent, ID: client.WorkerActorID()},
		Visibility:    message.VisibilityPublic,
		Audience:      []string{"*"},
		Payload:       payload,
		CorrelationID: in.CorrelationID, // L1 propagation
		ParentID:      in.Envelope.ID,
		TS:            m.NowFn(),
		TSReceived:    m.NowFn(),
	}
	if _, err := client.WriteMessage(ctx, env); err != nil {
		return err
	}
	return nil
}

// emitTerminal pushes the bridge's exit-of-turn-budget signal. The
// payload literally contains `{"text": "...", "next_action": "done"}`
// so test harnesses can pattern-match on the final envelope without
// adding a new envelope.type.
func (m *MockBridge) emitTerminal(ctx context.Context, client *IPCClient) error {
	body := map[string]any{
		"text":        "max_turns reached",
		"next_action": "done",
	}
	payload, _ := json.Marshal(body)
	env := message.Envelope{
		ID:         m.EnvelopeIDFn(client.WorkerID(), m.turns+1) + "-done",
		ChannelID:  string(client.ChannelID()),
		Type:       "agent.text",
		Kind:       message.KindEvent,
		Sender:     message.Sender{Kind: message.SenderAgent, ID: client.WorkerActorID()},
		Visibility: message.VisibilityPublic,
		Audience:   []string{"*"},
		Payload:    payload,
		TS:         m.NowFn(),
		TSReceived: m.NowFn(),
	}
	if _, err := client.WriteMessage(ctx, env); err != nil {
		return err
	}
	return nil
}
