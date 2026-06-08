// workerrt.go — local worker runtime for cmd/worker.
//
// Replaces the deleted runtime/worker package. Uses the v2 port-wire
// protocol (runtime/ipc) which is fire-and-forget, connection-is-identity.
//
// The worker subprocess lifecycle:
//   1. Handshake: send lease_id, receive bound ActorID.
//   2. Read loop: read KindDeliver frames → fan out as TriggerPayloads.
//   3. Bridge.Run: process triggers, emit envelopes via WriteEnvelope.
//   4. Shutdown: send KindDown frame, close connection.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/ActOS/actors/agent"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/ipc"
)

// Bridge is the agent loop contract. The worker runtime calls
// Bridge.Run after handshake completes; the bridge blocks until done.
type Bridge interface {
	Run(ctx context.Context, ipcFacade agent.IPCFacade) error
}

// BridgeFunc adapts a function to Bridge.
type BridgeFunc func(ctx context.Context, ipcFacade agent.IPCFacade) error

func (f BridgeFunc) Run(ctx context.Context, ipcFacade agent.IPCFacade) error {
	return f(ctx, ipcFacade)
}

// workerConfig wires a workerRuntime.
type workerConfig struct {
	// LeaseID identifies the worker lease (assigned by daemon).
	LeaseID string
	// In / Out are the stdio streams for IPC (os.Stdin / os.Stdout).
	In  io.Reader
	Out io.Writer
	// Bridge processes triggers and emits envelopes.
	Bridge Bridge
}

// workerRuntime is the worker subprocess main loop.
type workerRuntime struct {
	cfg    workerConfig
	codec  *ipc.Codec
	client *ipcClient
}

// newWorkerRuntime builds a workerRuntime.
func newWorkerRuntime(cfg workerConfig) (*workerRuntime, error) {
	if cfg.LeaseID == "" {
		return nil, errors.New("worker: Config.LeaseID empty")
	}
	if cfg.In == nil || cfg.Out == nil {
		return nil, errors.New("worker: Config.In/Out nil")
	}
	codec := ipc.NewCodec(cfg.In, cfg.Out)
	return &workerRuntime{
		cfg:   cfg,
		codec: codec,
		client: &ipcClient{
			codec:     codec,
			triggerCh: make(chan agent.TriggerPayload, 32),
		},
	}, nil
}

// Run executes the worker main loop.
func (r *workerRuntime) Run(ctx context.Context) error {
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	// Step 1: Handshake — present lease, learn bound ActorID.
	if err := r.handshake(runCtx); err != nil {
		return fmt.Errorf("worker: handshake: %w", err)
	}

	// Step 2: Start read loop (delivers → triggerCh).
	go r.readLoop(runCtx, cancelRun)

	// Step 3: Run bridge.
	var runErr error
	if r.cfg.Bridge != nil {
		runErr = r.cfg.Bridge.Run(runCtx, r.client)
	}

	// Step 4: Send down frame on exit.
	reason := "bridge exited"
	if runErr != nil {
		reason = runErr.Error()
	}
	_ = r.sendDown(reason)

	return runErr
}

// handshake performs the v2 port-wire handshake.
func (r *workerRuntime) handshake(ctx context.Context) error {
	payload, err := json.Marshal(ipc.HandshakePayload{LeaseID: r.cfg.LeaseID})
	if err != nil {
		return err
	}
	if err := r.codec.Write(ipc.Frame{
		Kind:    ipc.KindHandshake,
		Payload: payload,
	}); err != nil {
		return err
	}

	// Read the ack. In v2 port-wire, the next frame MUST be handshake_ack.
	frame, err := r.codec.Read()
	if err != nil {
		return err
	}
	if frame.Kind != ipc.KindHandshakeAck {
		return fmt.Errorf("expected handshake_ack, got %s", frame.Kind)
	}

	var ack ipc.HandshakeAckPayload
	if err := json.Unmarshal(frame.Payload, &ack); err != nil {
		return err
	}

	r.client.mu.Lock()
	r.client.actorID = ack.Actor
	r.client.mu.Unlock()

	return nil
}

// readLoop reads frames from the host and dispatches them.
func (r *workerRuntime) readLoop(ctx context.Context, cancelRun context.CancelFunc) {
	defer close(r.client.triggerCh)
	var cursor int64
	for {
		if ctx.Err() != nil {
			return
		}
		frame, err := r.codec.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				cancelRun()
				return
			}
			cancelRun()
			return
		}
		switch frame.Kind {
		case ipc.KindDeliver:
			var dp ipc.DeliverPayload
			if err := json.Unmarshal(frame.Payload, &dp); err != nil {
				continue // skip malformed
			}
			// Extract channelID from the first delivered envelope.
			if dp.Envelope.ChannelID != "" {
				r.client.mu.Lock()
				if r.client.channelID == "" {
					r.client.channelID = dp.Envelope.ChannelID
				}
				r.client.mu.Unlock()
			}
			c := atomic.AddInt64(&cursor, 1)
			tp := agent.TriggerPayload{
				Envelope:      dp.Envelope,
				CorrelationID: dp.Envelope.CorrelationID,
				Cursor:        c,
			}
			select {
			case r.client.triggerCh <- tp:
			case <-ctx.Done():
				return
			}
		case ipc.KindControl:
			// Control signals — not handled by the agent bridge today.
			// Future: graceful shutdown, pause/resume.
		default:
			// Unknown frame kind — skip.
		}
	}
}

// sendDown sends a KindDown frame to the host.
func (r *workerRuntime) sendDown(reason string) error {
	payload, err := json.Marshal(ipc.DownPayload{Reason: reason})
	if err != nil {
		return err
	}
	return r.codec.Write(ipc.Frame{
		Kind:    ipc.KindDown,
		Payload: payload,
	})
}

// ipcClient implements agent.IPCFacade for the v2 port-wire protocol.
type ipcClient struct {
	codec     *ipc.Codec
	triggerCh chan agent.TriggerPayload

	mu        sync.Mutex
	actorID   actor.ActorID
	channelID channel.ID
}

func (c *ipcClient) ChannelID() channel.ID {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.channelID
}

func (c *ipcClient) WorkerID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return string(c.actorID)
}

func (c *ipcClient) WorkerActorID() actor.ActorID {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.actorID
}

func (c *ipcClient) Triggers() <-chan agent.TriggerPayload {
	return c.triggerCh
}

// WriteEnvelope emits an envelope upward to the host via KindEmit.
func (c *ipcClient) WriteEnvelope(ctx context.Context, env message.Envelope) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	payload, err := json.Marshal(ipc.EmitPayload{Envelope: env})
	if err != nil {
		return err
	}
	return c.codec.Write(ipc.Frame{
		Kind:    ipc.KindEmit,
		Payload: payload,
	})
}

// AckTrigger is a no-op in the v2 port-wire protocol. The connection
// itself is the acknowledgement boundary — there is no per-trigger ack
// frame. The method exists so ipcClient satisfies the triggerAcker
// interface that actors/agent probes for.
func (c *ipcClient) AckTrigger(_ context.Context, _ agent.TriggerPayload, _ bool, _ string) error {
	return nil
}

// Compile-time check that ipcClient implements agent.IPCFacade.
var _ agent.IPCFacade = (*ipcClient)(nil)

// mockBridge is a deterministic Bridge for dev/CI — no external deps.
// Every trigger gets a single agent.text reply with next_action=done.
type mockBridge struct {
	MaxTurns int
	NowFn    func() int64
	turns    int
}

func newMockBridge() *mockBridge {
	return &mockBridge{}
}

func (m *mockBridge) Run(ctx context.Context, facade agent.IPCFacade) error {
	if m.NowFn == nil {
		m.NowFn = func() int64 { return time.Now().UnixMilli() }
	}
	triggers := facade.Triggers()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case tp, ok := <-triggers:
			if !ok {
				return nil
			}
			m.turns++
			body := map[string]any{
				"text":        fmt.Sprintf("mock reply to %s (turn %d)", tp.Envelope.ID, m.turns),
				"next_action": "done",
			}
			payload, _ := json.Marshal(body)
			env := message.Envelope{
				ID:            message.ID(fmt.Sprintf("mock-%d", m.turns)),
				Type:          "agent.text",
				Kind:          message.KindEvent,
				Sender:        message.Sender{Kind: actor.KindAgent, ID: facade.WorkerActorID()},
				Visibility:    message.VisibilityPublic,
				Payload:       payload,
				CorrelationID: tp.CorrelationID,
				ParentID:      tp.Envelope.ID,
				TS:            m.NowFn(),
				TSReceived:    m.NowFn(),
			}
			if err := facade.WriteEnvelope(ctx, env); err != nil {
				return err
			}
			if m.MaxTurns > 0 && m.turns >= m.MaxTurns {
				return nil
			}
		}
	}
}
