package workerhost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

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
	// WorkerManager waits on this before pushing the first KindTrigger
	// frame so that the worker's IPCClient is already running its read
	// loop (otherwise the trigger arrives before the worker is ready
	// to dispatch into Bridge.Triggers()).
	ready     chan struct{}
	readyOnce sync.Once
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
		cfg:   cfg,
		codec: ipc.NewCodec(in, out),
		ready: make(chan struct{}),
	}, nil
}

// Ready returns a channel that closes once the worker handshake ack is
// flushed. Used by WorkerManager to gate the first KindTrigger push.
func (h *Host) Ready() <-chan struct{} { return h.ready }

// PushTrigger emits a daemon → worker KindTrigger frame carrying the
// post-harness envelope + propagation context. Fire-and-forget: the
// worker reacts via a subsequent KindWriteMessage round-trip; the
// trigger itself has no reply. Safe to call from any goroutine — the
// underlying ipc.Codec serialises writes with an internal mutex.
//
// The frame is stamped with the host's (channel, fencing_token,
// daemon_epoch) tuple so the worker's IPCClient observes the same
// fence context it expects to stamp on its own outbound frames. The
// frame ID is informational (worker drops unsolicited replies).
func (h *Host) PushTrigger(payload ipc.TriggerPayload) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("workerhost: encode trigger: %w", err)
	}
	return h.codec.Write(ipc.Frame{
		ID:           fmt.Sprintf("trig-%s", payload.Envelope.ID),
		Kind:         ipc.KindTrigger,
		ChannelID:    h.cfg.ChannelID,
		WorkerID:     h.cfg.WorkerID,
		FencingToken: h.cfg.FencingToken,
		DaemonEpoch:  h.cfg.DaemonEpoch,
		Payload:      raw,
	})
}

// Serve runs the daemon-side read loop. Blocks until the worker
// disconnects (io.EOF) or ctx is cancelled.
func (h *Host) Serve(ctx context.Context) error {
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
	default:
		reply, _ := ipc.EncodeResult(frame.ID, false, fmt.Sprintf("unknown kind: %s", frame.Kind), nil)
		return h.codec.Write(reply)
	}
}

func (h *Host) handleHandshake(frame ipc.Frame) error {
	ack := ipc.HandshakeAckPayload{
		WorkerID:      h.cfg.WorkerID,
		ChannelID:     h.cfg.ChannelID,
		WorkerActorID: string(h.cfg.WorkerActorID),
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
	// WorkerManager. Must happen after the ack flush so the worker
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
	// FIX-T6 — stamp the host's (fencing_token, daemon_epoch) tuple so
	// runtime/store.Messages.Append validates it inside the same tx as
	// the row INSERT. The frame-level Fence() check above only proves
	// the worker is talking to the right daemon process; the sqlite
	// gate proves the daemon process itself still holds the channel
	// fence (i.e. it isn't a reclaimed/stale daemon).
	chainCtx = store.CtxWithFencing(chainCtx, h.cfg.FencingToken, h.cfg.DaemonEpoch)

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
