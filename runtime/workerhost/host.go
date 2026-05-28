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
	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/ledger"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/ipc"
	"github.com/wanpengxie/ActOS/runtime/store"
)

// LedgerOps is the daemon-side ledger operations invoked by the host.
type LedgerOps interface {
	Reserve(ctx context.Context, e ledger.Entry) (ledger.Entry, error)
	Commit(ctx context.Context, key ledger.Key, committedAt int64) error
}

// HostConfig wires a Host.
type HostConfig struct {
	ChannelID    channel.ID
	WorkerID     string
	LeaseID      string
	FencingToken placement.FencingToken
	DaemonEpoch  placement.DaemonEpoch

	// Chain is the daemon-side Message-Write Harness entry point. Every
	// worker IPC write_message frame is routed through Chain.Write so
	// the 9-step validation chain (L1 §10.2) runs before the row
	// reaches the messages-table sink. REQUIRED — host construction
	// refuses nil to prevent the FIX-T1 regression where worker IPC
	// bypassed harness.
	Chain khar.Chain

	// WorkerActorID identifies which actor the worker speaks as. Every
	// inbound write_message frame is stamped with this actor as the
	// caller principal (harness step 3 sender_mismatch enforces the
	// envelope.sender.id match). REQUIRED.
	WorkerActorID actor.ActorID

	Ledger LedgerOps

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
	if cfg.Chain == nil {
		return nil, errors.New("workerhost: HostConfig.Chain nil")
	}
	if cfg.WorkerActorID == "" {
		return nil, errors.New("workerhost: HostConfig.WorkerActorID nil")
	}
	if cfg.Ledger == nil {
		return nil, errors.New("workerhost: HostConfig.Ledger nil")
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
		ID:           frameID,
		Kind:         ipc.KindTrigger,
		ChannelID:    h.cfg.ChannelID,
		WorkerID:     ipc.WorkerID(h.cfg.WorkerID),
		FencingToken: h.cfg.FencingToken,
		DaemonEpoch:  h.cfg.DaemonEpoch,
		Payload:      raw,
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
		return ctx.Err()
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

	if ok, fi := Fence(frame, h.cfg.FencingToken, h.cfg.DaemonEpoch); !ok {
		fiPayload, _ := json.Marshal(fi)
		return h.codec.Write(ipc.Frame{
			ID:      frame.ID,
			Kind:    ipc.KindFenceInvalid,
			Payload: fiPayload,
		})
	}

	switch frame.Kind {
	case ipc.KindWriteMessage:
		return h.handleWrite(ctx, frame)
	case ipc.KindReserveLedger:
		return h.handleReserve(ctx, frame)
	case ipc.KindCommitLedger:
		return h.handleCommit(ctx, frame)
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
		FencingToken:  h.cfg.FencingToken,
		DaemonEpoch:   h.cfg.DaemonEpoch,
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

func (h *Host) handleWrite(ctx context.Context, frame ipc.Frame) error {
	var payload ipc.WriteMessagePayload
	if err := json.Unmarshal(frame.Payload, &payload); err != nil {
		reply, _ := ipc.EncodeResult(frame.ID, false, "decode: "+err.Error(), nil)
		return h.codec.Write(reply)
	}

	// Stamp the caller principal onto ctx so harness step 1 / step 3
	// can verify envelope.sender.id == WorkerActorID. Worker IPC is
	// the daemon's authenticated edge; the caller cannot self-declare
	// kind (AllowProvidedSenderKind=false enforces strict overwrite).
	chainCtx := harness.CtxWithCaller(ctx, harness.CallerContext{
		ActorID:                 h.cfg.WorkerActorID,
		ChannelID:               h.cfg.ChannelID,
		AllowProvidedSenderKind: false,
	})
	res, err := h.cfg.Chain.Write(chainCtx, &payload.Envelope)
	if err != nil {
		reply, _ := ipc.EncodeResult(frame.ID, false, err.Error(), ipc.WriteMessageResult{
			Reason: err.Error(),
		})
		return h.codec.Write(reply)
	}
	if res.RejectReason != "" {
		// Harness reject — surface the closed-set reason to the worker;
		// caller maps to *RejectError on the worker side. OK=false so
		// callers know the envelope did NOT persist.
		reply, _ := ipc.EncodeResult(frame.ID, false,
			fmt.Sprintf("%s: %s", res.RejectReason, res.RejectDetail),
			ipc.WriteMessageResult{
				Reason:  string(res.RejectReason),
				Deduped: res.Deduped,
			})
		return h.codec.Write(reply)
	}
	reply, _ := ipc.EncodeResult(frame.ID, true, "", ipc.WriteMessageResult{
		Seq:     res.Seq,
		Deduped: res.Deduped,
	})
	return h.codec.Write(reply)
}

func (h *Host) handleReserve(ctx context.Context, frame ipc.Frame) error {
	var payload ipc.ReserveLedgerPayload
	if err := json.Unmarshal(frame.Payload, &payload); err != nil {
		reply, _ := ipc.EncodeResult(frame.ID, false, "decode: "+err.Error(), nil)
		return h.codec.Write(reply)
	}
	// FIX-T6 fencing: same rationale as handleWrite.
	ctx = store.CtxWithFencing(ctx, h.cfg.FencingToken, h.cfg.DaemonEpoch)
	got, err := h.cfg.Ledger.Reserve(ctx, payload.Entry)
	if err != nil {
		reply, _ := ipc.EncodeResult(frame.ID, false, err.Error(), nil)
		return h.codec.Write(reply)
	}
	replayed := got.EnvelopeID != payload.Entry.EnvelopeID
	reply, _ := ipc.EncodeResult(frame.ID, true, "", ipc.ReserveLedgerResult{
		Entry: got, Replayed: replayed,
	})
	return h.codec.Write(reply)
}

func (h *Host) handleCommit(ctx context.Context, frame ipc.Frame) error {
	var payload ipc.CommitLedgerPayload
	if err := json.Unmarshal(frame.Payload, &payload); err != nil {
		reply, _ := ipc.EncodeResult(frame.ID, false, "decode: "+err.Error(), nil)
		return h.codec.Write(reply)
	}
	// FIX-T6 fencing: same rationale as handleWrite.
	ctx = store.CtxWithFencing(ctx, h.cfg.FencingToken, h.cfg.DaemonEpoch)
	if err := h.cfg.Ledger.Commit(ctx, payload.Key, payload.CommittedAt); err != nil {
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
