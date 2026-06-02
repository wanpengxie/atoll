package workerhost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/ipc"
)

// EmitSink is the upward forwarding seam the host uses to hand a worker's
// emitted envelope (and death signal) to the server. v2: the worker no longer
// writes the channel log locally (truth is on server); the daemon host
// forwards each emit UP to the server harness (the single writer) via this
// injected sink. The concrete impl (computebus uplink) lives in the daemon
// assembly layer — runtime stays pure (no wire/server import).
type EmitSink interface {
	// Emit forwards one worker-emitted envelope upward, stamping the worker's
	// actor as caller principal. Returns the server's accept/reject.
	Emit(ctx context.Context, caller actor.ActorID, env message.Envelope) error
	// Down forwards an actor/worker death signal upward (closure §6).
	Down(ctx context.Context, dead actor.ActorID, reason string) error
}

// HostConfig wires a Host.
type HostConfig struct {
	ChannelID  channel.ID
	WorkerID   string
	LeaseID    string
	LeaseToken string // opaque worker-lease instance token (v2; replaces channel fencing)

	// Emit is the upward forwarding sink (see EmitSink). REQUIRED — replaces
	// the v1 local harness Chain.Write (worker no longer writes truth).
	Emit EmitSink

	// WorkerActorID identifies which actor the worker speaks as. Every
	// inbound emit frame is stamped with this actor as the caller principal
	// (the server harness step 6 sender_mismatch enforces the match).
	// REQUIRED.
	WorkerActorID actor.ActorID

	// NowFn returns unix-ms; required.
	NowFn func() int64

	// OnHeartbeat is called after a successful heartbeat. May be nil.
	OnHeartbeat func(int64)

	// OnShutdown is called after IPCShutdown is received. May be nil.
	OnShutdown func()
}

// Host is the daemon-side IPC server for one worker.
type Host struct {
	cfg   HostConfig
	codec *ipc.Codec

	// ready closes after the worker completes its handshake. The
	// WorkerBridge waits on this before pushing the first KindTrigger
	// frame so that the worker's IPCClient is already running its read
	// loop (otherwise the trigger arrives before the worker is ready
	// to dispatch into Bridge.Triggers()).
	ready     chan struct{}
	readyOnce sync.Once

	mu              sync.Mutex
	closed          bool
	triggerSeq      atomic.Int64
	pendingTriggers map[string]chan ipc.TriggerAckPayload
}

// NewHost wires a Host around an IPC stream (typically WorkerProc.Stdout
// for input + WorkerProc.Stdin for output).
func NewHost(in io.Reader, out io.Writer, cfg HostConfig) (*Host, error) {
	if cfg.Emit == nil {
		return nil, errors.New("workerhost: HostConfig.Emit nil")
	}
	if cfg.WorkerActorID == "" {
		return nil, errors.New("workerhost: HostConfig.WorkerActorID nil")
	}
	if cfg.NowFn == nil {
		return nil, errors.New("workerhost: HostConfig.NowFn nil")
	}
	return &Host{
		cfg:             cfg,
		codec:           ipc.NewCodec(in, out),
		ready:           make(chan struct{}),
		pendingTriggers: make(map[string]chan ipc.TriggerAckPayload),
	}, nil
}

// Ready returns a channel that closes once the worker handshake ack is
// flushed. Used by WorkerBridge to gate the first KindTrigger push.
func (h *Host) Ready() <-chan struct{} { return h.ready }

// ErrAcceptTimeout is returned by PushTrigger when the trigger frame was
// successfully written to the worker transport but the worker did not return
// an accept ACK before the context deadline. It is distinct from a transport
// write failure (which surfaces as the raw write/ctx error): an accept
// timeout on a heartbeat-fresh worker means "live but not yet accepted", NOT
// "dead transport", so the bridge must NOT kill the worker on this error —
// it retries delivery under bounded attempts (§3 ack 三分 / §6 step3;
// codex P1 bridge.go:255).
var ErrAcceptTimeout = errors.New("workerhost: trigger accept timeout")

// PushTrigger emits a daemon → worker KindTrigger frame carrying the
// post-harness envelope + propagation context. The worker must answer
// with KindTriggerAck after processing or rejecting the trigger; a nack
// turns into a PushTrigger error so the caller can keep the delivery
// retryable. Safe to call from any goroutine — the underlying ipc.Codec
// serialises writes with an internal mutex.
//
// The frame is stamped with the host's (channel, fencing_token,
// daemon_epoch) tuple so the worker's IPCClient observes the same
// fence context it expects to stamp on its own outbound frames. The
// frame ID correlates the trigger ack.
func (h *Host) PushTrigger(ctx context.Context, payload ipc.TriggerPayload) error {
	frameID := fmt.Sprintf("trig-%s-%d", payload.Envelope.ID, h.triggerSeq.Add(1))
	payload.AckID = frameID
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("workerhost: encode trigger: %w", err)
	}
	frame := ipc.Frame{
		ID:         frameID,
		Kind:       ipc.KindTrigger,
		ChannelID:  h.cfg.ChannelID,
		WorkerID:   ipc.WorkerID(h.cfg.WorkerID),
		LeaseToken: h.cfg.LeaseToken,
		Payload:    raw,
	}
	ackCh := make(chan ipc.TriggerAckPayload, 1)
	if err := h.registerPendingTrigger(frame.ID, ackCh); err != nil {
		return err
	}

	done := make(chan error, 1)
	go func() { done <- h.codec.Write(frame) }()

	select {
	case err := <-done:
		if err != nil {
			h.unregisterPendingTrigger(frame.ID, ackCh)
			return err
		}
	case <-ctx.Done():
		h.unregisterPendingTrigger(frame.ID, ackCh)
		return ctx.Err()
	}

	select {
	case ack, ok := <-ackCh:
		if !ok {
			return errors.New("workerhost: trigger ack closed")
		}
		if ack.Cursor != payload.Cursor {
			return fmt.Errorf("workerhost: trigger ack cursor mismatch: got %d want %d", ack.Cursor, payload.Cursor)
		}
		if !ack.Accepted {
			reason := ack.Reason
			if reason == "" {
				reason = "rejected"
			}
			return fmt.Errorf("workerhost: trigger rejected: %s", reason)
		}
		return nil
	case <-ctx.Done():
		h.unregisterPendingTrigger(frame.ID, ackCh)
		// The frame already reached the wire (the write select above
		// succeeded); only the accept ACK is missing. Surface this as the
		// distinct ErrAcceptTimeout so the bridge can keep a heartbeat-fresh
		// worker alive and retry, rather than treating it as dead transport.
		return fmt.Errorf("%w: %v", ErrAcceptTimeout, ctx.Err())
	}
}

// Serve runs the daemon-side read loop. Blocks until the worker
// disconnects (io.EOF) or ctx is cancelled.
func (h *Host) Serve(ctx context.Context) error {
	defer h.closePendingTriggers()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		frame, err := h.codec.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := h.handle(ctx, frame); err != nil {
			return err
		}
		if frame.Kind == ipc.KindShutdown {
			if err := h.codec.Write(ipc.Frame{ID: frame.ID, Kind: ipc.KindShutdownAck}); err != nil {
				return err
			}
			if h.cfg.OnShutdown != nil {
				h.cfg.OnShutdown()
			}
			return nil
		}
	}
}

func (h *Host) handle(ctx context.Context, frame ipc.Frame) error {
	// Handshake is the only kind that bypasses fence — it establishes
	// the fencing context.
	if frame.Kind == ipc.KindHandshake {
		return h.handleHandshake(frame)
	}
	if frame.Kind == ipc.KindShutdown {
		return nil // ack handled in Serve after handle returns
	}

	if ok, fi := Fence(frame, h.cfg.LeaseToken); !ok {
		fiPayload, _ := json.Marshal(fi)
		return h.codec.Write(ipc.Frame{
			ID:      frame.ID,
			Kind:    ipc.KindFenceInvalid,
			Payload: fiPayload,
		})
	}

	switch frame.Kind {
	case ipc.KindEmit:
		return h.handleEmit(ctx, frame)
	case ipc.KindDown:
		return h.handleDown(ctx, frame)
	case ipc.KindHeartbeat:
		return h.handleHeartbeat(frame)
	case ipc.KindTriggerAck:
		return h.handleTriggerAck(frame)
	default:
		reply, _ := ipc.EncodeResult(frame.ID, false, fmt.Sprintf("unknown kind: %s", frame.Kind), nil)
		return h.codec.Write(reply)
	}
}

func (h *Host) handleHandshake(frame ipc.Frame) error {
	ack := ipc.HandshakeAckPayload{
		WorkerID:      ipc.WorkerID(h.cfg.WorkerID),
		ChannelID:     h.cfg.ChannelID,
		WorkerActorID: h.cfg.WorkerActorID,
		LeaseToken:    h.cfg.LeaseToken,
	}
	payload, err := json.Marshal(ack)
	if err != nil {
		return err
	}
	if err := h.codec.Write(ipc.Frame{ID: frame.ID, Kind: ipc.KindHandshakeAck, Payload: payload}); err != nil {
		return err
	}
	// Signal Ready exactly once — gate for PushTrigger from the
	// WorkerBridge. Must happen after the ack flush so the worker
	// has had the chance to populate its IPC client snapshot.
	h.readyOnce.Do(func() { close(h.ready) })
	return nil
}

// handleEmit forwards a worker-emitted envelope UP to the server harness via
// the injected EmitSink (v2: the worker no longer writes the channel log; the
// server is the single writer). The worker's actor is stamped as caller
// principal so the server harness step 6 sender match holds.
func (h *Host) handleEmit(ctx context.Context, frame ipc.Frame) error {
	var payload ipc.EmitPayload
	if err := json.Unmarshal(frame.Payload, &payload); err != nil {
		reply, _ := ipc.EncodeResult(frame.ID, false, "decode: "+err.Error(), nil)
		return h.codec.Write(reply)
	}
	if err := h.cfg.Emit.Emit(ctx, h.cfg.WorkerActorID, payload.Envelope); err != nil {
		reply, _ := ipc.EncodeResult(frame.ID, false, err.Error(), nil)
		return h.codec.Write(reply)
	}
	reply, _ := ipc.EncodeResult(frame.ID, true, "", nil)
	return h.codec.Write(reply)
}

// handleDown forwards an actor/worker death signal UP (closure §6).
func (h *Host) handleDown(ctx context.Context, frame ipc.Frame) error {
	var payload ipc.DownPayload
	if err := json.Unmarshal(frame.Payload, &payload); err != nil {
		reply, _ := ipc.EncodeResult(frame.ID, false, "decode: "+err.Error(), nil)
		return h.codec.Write(reply)
	}
	if err := h.cfg.Emit.Down(ctx, payload.Actor, payload.Reason); err != nil {
		reply, _ := ipc.EncodeResult(frame.ID, false, err.Error(), nil)
		return h.codec.Write(reply)
	}
	reply, _ := ipc.EncodeResult(frame.ID, true, "", nil)
	return h.codec.Write(reply)
}

func (h *Host) handleHeartbeat(frame ipc.Frame) error {
	var payload ipc.HeartbeatPayload
	_ = json.Unmarshal(frame.Payload, &payload)
	if h.cfg.OnHeartbeat != nil {
		h.cfg.OnHeartbeat(payload.NowMs)
	}
	reply, _ := ipc.EncodeResult(frame.ID, true, "", map[string]int64{
		"server_now_ms": h.cfg.NowFn(),
	})
	return h.codec.Write(reply)
}

func (h *Host) handleTriggerAck(frame ipc.Frame) error {
	var payload ipc.TriggerAckPayload
	if err := json.Unmarshal(frame.Payload, &payload); err != nil {
		return fmt.Errorf("workerhost: decode trigger ack: %w", err)
	}
	h.completePendingTrigger(frame.ID, payload)
	return nil
}

func (h *Host) registerPendingTrigger(id string, ch chan ipc.TriggerAckPayload) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return errors.New("workerhost: host closed")
	}
	h.pendingTriggers[id] = ch
	return nil
}

func (h *Host) unregisterPendingTrigger(id string, ch chan ipc.TriggerAckPayload) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if cur, ok := h.pendingTriggers[id]; ok && cur == ch {
		delete(h.pendingTriggers, id)
	}
}

func (h *Host) completePendingTrigger(id string, payload ipc.TriggerAckPayload) {
	h.mu.Lock()
	ch, ok := h.pendingTriggers[id]
	if ok {
		delete(h.pendingTriggers, id)
	}
	h.mu.Unlock()
	if !ok {
		return
	}
	ch <- payload
	close(ch)
}

func (h *Host) closePendingTriggers() {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	h.closed = true
	pending := h.pendingTriggers
	h.pendingTriggers = make(map[string]chan ipc.TriggerAckPayload)
	h.mu.Unlock()
	for _, ch := range pending {
		close(ch)
	}
}
