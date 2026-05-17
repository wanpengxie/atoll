package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/ipc"
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
